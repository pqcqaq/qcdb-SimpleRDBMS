
/*
 * 文件: table_heap.cpp
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述: 表堆存储管理实现，提供基于页面的tuple存储、检索、更新和删除功能
 */

#include "record/table_heap.h"

#include <algorithm>
#include <cstring>
#include <unordered_set>

#include "common/exception.h"
#include "recovery/recovery_manager.h"

namespace SimpleRDBMS {

static const uint32_t TABLE_PAGE_MAGIC = 0xDEADBEEF;  // 表页面魔数

/**
 * Slot结构体定义
 * 用于记录页面中每个tuple的存储位置和大小信息
 */
struct Slot {
    uint16_t offset;  // tuple在页面中的字节偏移量
    uint16_t size;    // tuple的字节大小，0表示已删除

    Slot() : offset(0), size(0) {}
    Slot(uint16_t off, uint16_t sz) : offset(off), size(sz) {}
};

/**
 * 初始化表页面
 * @param page_id 页面ID
 * @param prev_page_id 前一页面ID（当前未使用）
 */
void TablePage::Init(page_id_t page_id, page_id_t prev_page_id) {
    SetPageId(page_id);
    SetLSN(INVALID_LSN);
    (void)prev_page_id;

    // 完全清零页面数据
    std::memset(GetData(), 0, PAGE_SIZE);

    auto* header = GetHeader();
    header->next_page_id = INVALID_PAGE_ID;
    header->lsn = INVALID_LSN;
    header->num_tuples = 0;
    header->free_space_offset = PAGE_SIZE;

    LOG_DEBUG("TablePage::Init: initialized page "
              << page_id
              << " with free_space_offset=" << header->free_space_offset
              << " next_page_id=" << header->next_page_id
              << " num_tuples=" << header->num_tuples);
}

static bool ValidateAndFixTablePageHeader(TablePage::TablePageHeader* header,
                                          page_id_t expected_page_id) {
    bool need_fix = false;

    // 检查next_page_id是否被破坏（包含catalog魔数）
    if (header->next_page_id == static_cast<page_id_t>(0x12345678)) {
        LOG_ERROR(
            "ValidateAndFixTablePageHeader: Detected catalog magic number in "
            "next_page_id, fixing");
        header->next_page_id = INVALID_PAGE_ID;
        need_fix = true;
    }

    // 检查next_page_id是否在合理范围内
    if (header->next_page_id != INVALID_PAGE_ID &&
        (header->next_page_id < 2 || header->next_page_id > 1000000)) {
        LOG_ERROR("ValidateAndFixTablePageHeader: Invalid next_page_id "
                  << header->next_page_id << ", resetting to INVALID_PAGE_ID");
        header->next_page_id = INVALID_PAGE_ID;
        need_fix = true;
    }

    // 检查num_tuples是否合理
    if (header->num_tuples < 0 || header->num_tuples > 1000) {
        LOG_ERROR("ValidateAndFixTablePageHeader: Invalid num_tuples "
                  << header->num_tuples << ", resetting to 0");
        header->num_tuples = 0;
        header->free_space_offset = PAGE_SIZE;
        need_fix = true;
    }

    // 检查free_space_offset是否合理
    size_t min_offset =
        sizeof(TablePage::TablePageHeader) + header->num_tuples * sizeof(Slot);
    if (header->free_space_offset < min_offset ||
        header->free_space_offset > PAGE_SIZE) {
        LOG_ERROR("ValidateAndFixTablePageHeader: Invalid free_space_offset "
                  << header->free_space_offset << ", fixing");
        header->free_space_offset = PAGE_SIZE;
        need_fix = true;
    }

    if (need_fix) {
        LOG_DEBUG("ValidateAndFixTablePageHeader: Fixed header for page "
                  << expected_page_id
                  << " - next_page_id=" << header->next_page_id
                  << " num_tuples=" << header->num_tuples
                  << " free_space_offset=" << header->free_space_offset);
    }

    return !need_fix;  // 返回true表示头部是有效的
}

/**
 * 页面布局说明：
 * [Header] [Slot Directory] [Free Space] [Tuple Data (grows backward)]
 *    ^            ^              ^               ^
 *    0      header_size    slot_end_offset  free_space_offset
 *
 * Header: 存储页面元信息
 * Slot Directory: 存储每个tuple的位置信息，向右增长
 * Free Space: 可用空间
 * Tuple Data: 实际的tuple数据，从右向左增长
 */

/**
 * 验证并修复slot目录结构
 * 这个函数负责检查页面的完整性，清理无效的slot，压缩slot目录
 * @param header 页面头部指针
 * @param page_data 页面数据指针
 * @return 验证是否成功
 */
static bool ValidateAndRepairSlotDirectory(TablePage::TablePageHeader* header,
                                           char* page_data, page_id_t page_id) {
    const size_t header_size = sizeof(TablePage::TablePageHeader);
    const size_t slot_size = sizeof(Slot);

    LOG_DEBUG("ValidateAndRepairSlotDirectory: page "
              << page_id << " num_tuples=" << header->num_tuples
              << " free_space_offset=" << header->free_space_offset
              << " next_page_id=" << header->next_page_id);

    // 首先修复头部
    if (!ValidateAndFixTablePageHeader(header, page_id)) {
        LOG_WARN("ValidateAndRepairSlotDirectory: Fixed corrupted header");
    }

    if (header->num_tuples == 0) {
        header->free_space_offset = PAGE_SIZE;
        LOG_DEBUG(
            "ValidateAndRepairSlotDirectory: empty page, reset "
            "free_space_offset");
        return true;
    }

    size_t slot_end_offset = header_size + header->num_tuples * slot_size;
    if (slot_end_offset > PAGE_SIZE) {
        LOG_ERROR(
            "ValidateAndRepairSlotDirectory: slot directory exceeds page size");
        header->num_tuples = 0;
        header->free_space_offset = PAGE_SIZE;
        return false;
    }

    Slot* slots = reinterpret_cast<Slot*>(page_data + header_size);
    size_t calculated_free_offset = PAGE_SIZE;
    int valid_tuples = 0;

    for (int i = 0; i < header->num_tuples; i++) {
        if (slots[i].size > 0) {
            if (slots[i].offset >= slot_end_offset &&
                slots[i].offset < PAGE_SIZE && slots[i].size <= PAGE_SIZE &&
                slots[i].offset + slots[i].size <= PAGE_SIZE) {
                calculated_free_offset =
                    std::min(calculated_free_offset,
                             static_cast<size_t>(slots[i].offset));
                if (valid_tuples != i) {
                    slots[valid_tuples] = slots[i];
                }
                valid_tuples++;
            } else {
                LOG_DEBUG(
                    "ValidateAndRepairSlotDirectory: removing invalid slot "
                    << i);
            }
        }
    }

    // 清理多余的slot
    for (int i = valid_tuples; i < header->num_tuples; i++) {
        slots[i].offset = 0;
        slots[i].size = 0;
    }

    header->num_tuples = valid_tuples;

    if (calculated_free_offset < header_size + valid_tuples * slot_size) {
        calculated_free_offset = PAGE_SIZE;
    }

    header->free_space_offset = calculated_free_offset;

    LOG_DEBUG("ValidateAndRepairSlotDirectory: page "
              << page_id << " validated with " << valid_tuples
              << " valid tuples"
              << " free_offset=" << header->free_space_offset);

    return true;
}

/**
 * 删除指定RID的tuple
 * 实现思路：将对应slot的size设为0表示删除，不实际移动数据
 * @param rid 要删除的tuple的RID
 * @return 删除是否成功
 */
bool TablePage::DeleteTuple(const RID& rid) {
    auto* header = GetHeader();

    // 检查slot编号的合法性
    if (rid.slot_num < 0 || rid.slot_num >= header->num_tuples) {
        LOG_DEBUG("TablePage::DeleteTuple: Invalid slot number "
                  << rid.slot_num);
        return false;
    }

    const size_t header_size = sizeof(TablePageHeader);
    Slot* slots = reinterpret_cast<Slot*>(GetData() + header_size);

    // 检查slot是否已经被删除
    if (slots[rid.slot_num].size == 0) {
        LOG_DEBUG("TablePage::DeleteTuple: Slot " << rid.slot_num
                                                  << " already empty");
        return false;
    }

    // 标记slot为已删除状态（size=0表示删除）
    slots[rid.slot_num].size = 0;
    slots[rid.slot_num].offset = 0;

    LOG_DEBUG("TablePage::DeleteTuple: Successfully deleted tuple at slot "
              << rid.slot_num);
    return true;
}

/**
 * 更新指定RID的tuple
 * 实现思路：
 * 1. 如果新tuple大小相同，直接原地覆盖
 * 2. 如果新tuple更小，原地更新并调整size
 * 3. 如果新tuple更大，先删除原tuple，再尝试在页面末尾插入新tuple
 * @param tuple 新的tuple数据
 * @param rid 要更新的tuple的RID
 * @return 更新是否成功
 */
bool TablePage::UpdateTuple(const Tuple& tuple, const RID& rid) {
    auto* header = GetHeader();

    // 验证RID的合法性
    if (rid.slot_num < 0 || rid.slot_num >= header->num_tuples) {
        return false;
    }

    const size_t header_size = sizeof(TablePageHeader);
    Slot* slots = reinterpret_cast<Slot*>(GetData() + header_size);

    // 检查slot是否有效
    if (slots[rid.slot_num].size == 0) {
        return false;
    }

    size_t new_tuple_size = tuple.GetSerializedSize();
    size_t old_tuple_size = slots[rid.slot_num].size;

    // 情况1：大小相同，直接原地覆盖
    if (new_tuple_size == old_tuple_size) {
        char* tuple_data = GetData() + slots[rid.slot_num].offset;
        tuple.SerializeTo(tuple_data);
        return true;
    }

    // 情况2：新tuple更小，原地更新
    if (new_tuple_size < old_tuple_size) {
        char* tuple_data = GetData() + slots[rid.slot_num].offset;
        tuple.SerializeTo(tuple_data);
        slots[rid.slot_num].size = new_tuple_size;
        return true;
    }

    // 情况3：新tuple更大，使用删除-插入策略
    // 保存原始slot信息用于可能的回滚
    uint16_t old_offset = slots[rid.slot_num].offset;
    uint16_t old_size = slots[rid.slot_num].size;

    // 先标记原slot为删除状态
    slots[rid.slot_num].size = 0;
    slots[rid.slot_num].offset = 0;

    // 尝试在页面末尾插入新tuple
    size_t slot_end_offset = header_size + header->num_tuples * sizeof(Slot);
    if (header->free_space_offset >= slot_end_offset + new_tuple_size) {
        // 有足够空间，执行插入
        header->free_space_offset -= new_tuple_size;
        char* new_tuple_data = GetData() + header->free_space_offset;
        tuple.SerializeTo(new_tuple_data);

        // 更新原slot信息指向新位置
        slots[rid.slot_num].offset = header->free_space_offset;
        slots[rid.slot_num].size = new_tuple_size;

        return true;
    } else {
        // 空间不足，恢复原状态
        slots[rid.slot_num].offset = old_offset;
        slots[rid.slot_num].size = old_size;
        return false;
    }
}

/**
 * 根据RID获取tuple数据
 * 实现思路：验证RID有效性 -> 验证页面结构 -> 反序列化tuple数据
 * @param rid tuple的RID
 * @param tuple 输出参数，存储获取到的tuple
 * @param schema tuple的schema信息
 * @return 获取是否成功
 */
bool TablePage::GetTuple(const RID& rid, Tuple* tuple, const Schema* schema) {
    // 基本参数检查
    if (!tuple || !schema) {
        LOG_ERROR("TablePage::GetTuple: null tuple or schema pointer");
        return false;
    }

    auto* header = GetHeader();
    if (!header) {
        LOG_ERROR("TablePage::GetTuple: null header");
        return false;
    }

    LOG_DEBUG("TablePage::GetTuple: Attempting to get tuple from slot "
              << rid.slot_num << " (total slots: " << header->num_tuples
              << ")");

    // 验证slot编号范围
    if (rid.slot_num < 0 || rid.slot_num >= header->num_tuples) {
        LOG_DEBUG("TablePage::GetTuple: slot "
                  << rid.slot_num << " out of range [0, " << header->num_tuples
                  << ")");
        return false;
    }

    // 验证和修复页面结构
    if (!ValidateAndRepairSlotDirectory(header, GetData(), GetPageId())) {
        LOG_ERROR("TablePage::GetTuple: page structure validation failed");
        return false;
    }

    const size_t header_size = sizeof(TablePageHeader);
    Slot* slots = reinterpret_cast<Slot*>(GetData() + header_size);

    // 检查slot是否有效（未被删除）
    if (slots[rid.slot_num].size == 0) {
        LOG_DEBUG("TablePage::GetTuple: slot " << rid.slot_num
                                               << " is deleted (size=0)");
        return false;
    }

    LOG_DEBUG("TablePage::GetTuple: slot "
              << rid.slot_num << " has offset=" << slots[rid.slot_num].offset
              << ", size=" << slots[rid.slot_num].size);

    // 验证slot的offset和size的合理性
    if (slots[rid.slot_num].offset < header_size ||
        slots[rid.slot_num].offset + slots[rid.slot_num].size > PAGE_SIZE) {
        LOG_WARN("TablePage::GetTuple: invalid slot "
                 << rid.slot_num << " (offset=" << slots[rid.slot_num].offset
                 << ", size=" << slots[rid.slot_num].size << ")");
        return false;
    }

    // 获取tuple数据并反序列化
    char* tuple_data = GetData() + slots[rid.slot_num].offset;

    try {
        LOG_DEBUG(
            "TablePage::GetTuple: About to deserialize tuple data, schema has "
            << schema->GetColumnCount() << " columns");

        tuple->DeserializeFrom(tuple_data, schema);
        tuple->SetRID(rid);

        LOG_DEBUG("TablePage::GetTuple: Successfully retrieved tuple from slot "
                  << rid.slot_num << " with " << tuple->GetValues().size()
                  << " values");

        return true;
    } catch (const std::exception& e) {
        LOG_ERROR("TablePage::GetTuple: Failed to deserialize tuple from slot "
                  << rid.slot_num << ": " << e.what());
        return false;
    }
}

/**
 * 获取当前RID之后的下一个有效tuple的RID
 * 实现思路：从current_rid.slot_num+1开始遍历，找到第一个size>0的slot
 * @param current_rid 当前RID
 * @param next_rid 输出参数，存储找到的下一个RID
 * @return 是否找到下一个有效tuple
 */
bool TablePage::GetNextTupleRID(const RID& current_rid, RID* next_rid) {
    auto* header = GetHeader();

    // 验证和修复页面结构
    if (!ValidateAndRepairSlotDirectory(header, GetData(), GetPageId())) {
        LOG_ERROR("TablePage::GetNextTupleRID: page corrupted, reinitializing");
        Init(GetPageId(), INVALID_PAGE_ID);
        return false;
    }

    // 空页面直接返回false
    if (header->num_tuples == 0) {
        return false;
    }

    const size_t header_size = sizeof(TablePageHeader);
    Slot* slots = reinterpret_cast<Slot*>(GetData() + header_size);

    // 计算搜索起始位置（从current_rid的下一个slot开始）
    int start_slot = current_rid.slot_num + 1;
    if (start_slot < 0) {
        start_slot = 0;
    }

    // 遍历查找下一个有效的tuple
    for (int i = start_slot; i < header->num_tuples; i++) {
        if (slots[i].size > 0) {  // size>0表示slot有效
            next_rid->page_id = GetPageId();
            next_rid->slot_num = i;
            return true;
        }
    }

    return false;  // 没有找到下一个有效tuple
}

/**
 * 获取下一页面的ID
 */
page_id_t TablePage::GetNextPageId() const {
    auto* header = GetHeader();

    // 验证头部完整性
    TablePage::TablePageHeader temp_header = *header;
    if (!ValidateAndFixTablePageHeader(&temp_header, GetPageId())) {
        LOG_WARN("TablePage::GetNextPageId: Header corruption detected on page "
                 << GetPageId() << ", using fixed value");
        return temp_header.next_page_id;
    }

    return header->next_page_id;
}

/**
 * 设置下一页面的ID
 */
void TablePage::SetNextPageId(page_id_t next_page_id) {
    // 验证输入参数
    if (next_page_id != INVALID_PAGE_ID &&
        (next_page_id < 2 || next_page_id > 1000000)) {
        LOG_ERROR("TablePage::SetNextPageId: Invalid next_page_id "
                  << next_page_id << " for page " << GetPageId());
        return;
    }

    auto* header = GetHeader();

    // 验证当前头部状态
    if (!ValidateAndFixTablePageHeader(header, GetPageId())) {
        LOG_WARN(
            "TablePage::SetNextPageId: Fixed corrupted header before setting "
            "next_page_id");
    }

    header->next_page_id = next_page_id;

    LOG_DEBUG("TablePage::SetNextPageId: Set next_page_id to "
              << next_page_id << " on page " << GetPageId());
}

/**
 * 获取页面头部指针（可写）
 */
TablePage::TablePageHeader* TablePage::GetHeader() {
    return reinterpret_cast<TablePageHeader*>(GetData());
}

/**
 * 获取页面头部指针（只读）
 */
const TablePage::TablePageHeader* TablePage::GetHeader() const {
    return reinterpret_cast<const TablePageHeader*>(GetData());
}

/**
 * TableHeap构造函数 - 创建新的表堆
 * 实现思路：申请第一个页面并初始化为表页面
 * @param buffer_pool_manager 缓冲池管理器
 * @param schema 表的schema
 */
TableHeap::TableHeap(BufferPoolManager* buffer_pool_manager,
                     const Schema* schema)
    : buffer_pool_manager_(buffer_pool_manager),
      schema_(schema),
      first_page_id_(INVALID_PAGE_ID) {
    
    Page* first_page = buffer_pool_manager_->NewPage(&first_page_id_);
    if (first_page == nullptr) {
        throw Exception("Cannot allocate first page for table heap");
    }
    
    // 确保分配的页面不是保留页面
    if (first_page_id_ < 2) {
        LOG_ERROR("TableHeap: Allocated reserved page " << first_page_id_ 
                  << " for table, this should not happen");
        buffer_pool_manager_->UnpinPage(first_page_id_, false);
        buffer_pool_manager_->DeletePage(first_page_id_);
        throw Exception("Allocated reserved page for table heap");
    }
    
    first_page->WLatch();
    auto* table_page = reinterpret_cast<TablePage*>(first_page);
    table_page->Init(first_page_id_, INVALID_PAGE_ID);
    first_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(first_page_id_, true);
    
    LOG_DEBUG("TableHeap: Created new table heap with first_page_id=" 
              << first_page_id_);
}

/**
 * TableHeap构造函数 - 从已存在的页面恢复表堆
 * 这个构造函数用于数据库恢复时重建TableHeap对象
 * @param buffer_pool_manager 缓冲池管理器
 * @param schema 表的schema
 * @param first_page_id 已存在的第一个页面ID
 */
TableHeap::TableHeap(BufferPoolManager* buffer_pool_manager,
                     const Schema* schema, page_id_t first_page_id)
    : buffer_pool_manager_(buffer_pool_manager),
      schema_(schema),
      first_page_id_(first_page_id) {
    LOG_DEBUG("TableHeap: Creating TableHeap with existing first_page_id="
              << first_page_id);
    
    // 确保first_page_id不是保留页面
    if (first_page_id < 2) {
        LOG_ERROR("TableHeap: Invalid first_page_id " << first_page_id 
                  << ", must be >= 2 (pages 0-1 are reserved)");
        throw Exception("Invalid first page ID for table heap");
    }
    
    Page* first_page = buffer_pool_manager_->FetchPage(first_page_id_);
    if (first_page == nullptr) {
        LOG_ERROR("TableHeap: Cannot fetch first page "
                  << first_page_id << " for table heap recovery");
        throw Exception("Cannot fetch first page for table heap recovery");
    }
    
    // 验证页面完整性
    first_page->RLatch();
    auto* table_page = reinterpret_cast<TablePage*>(first_page);
    auto* header = table_page->GetHeader();
    
    // 使用我们的验证函数检查和修复头部
    if (!ValidateAndFixTablePageHeader(header, first_page_id)) {
        LOG_WARN("TableHeap: Fixed corrupted header in first page " << first_page_id);
        first_page->RUnlatch();
        first_page->WLatch();
        first_page->SetDirty(true);
        first_page->WUnlatch();
    } else {
        first_page->RUnlatch();
    }
    
    LOG_DEBUG("TableHeap: Successfully validated first page " << first_page_id);
    buffer_pool_manager_->UnpinPage(first_page_id_, false);
    LOG_DEBUG("TableHeap: TableHeap created successfully");
}

/**
 * TableHeap析构函数
 */
TableHeap::~TableHeap() = default;

/**
 * 向表中插入一个tuple
 * 实现思路：
 * 1. 从第一个页面开始遍历，尝试在现有页面中插入
 * 2. 如果现有页面都满了，创建新页面并插入
 * 3. 记录WAL日志（如果启用了日志管理）
 * @param tuple 要插入的tuple
 * @param rid 输出参数，存储插入后的RID
 * @param txn_id 事务ID
 * @return 插入是否成功
 */
bool TableHeap::InsertTuple(const Tuple& tuple, RID* rid, txn_id_t txn_id) {
    page_id_t current_page_id = first_page_id_;

    // 遍历所有页面寻找可插入的位置
    while (current_page_id != INVALID_PAGE_ID) {
        Page* page = buffer_pool_manager_->FetchPage(current_page_id);
        if (page == nullptr) {
            return false;
        }

        page->WLatch();
        auto* table_page = reinterpret_cast<TablePage*>(page);

        // 尝试在当前页面插入tuple
        if (table_page->InsertTuple(tuple, rid)) {
            // 插入成功，记录INSERT日志
            if (log_manager_ && txn_id != INVALID_TXN_ID) {
                InsertLogRecord log_record(txn_id, INVALID_LSN, *rid, tuple);
                lsn_t lsn = log_manager_->AppendLogRecord(&log_record);
                page->SetLSN(lsn);
            } else {
                page->SetLSN(0);
            }

            page->WUnlatch();
            buffer_pool_manager_->UnpinPage(current_page_id, true);
            return true;
        }

        // 当前页面插入失败，检查是否需要创建新页面
        page_id_t next_page_id = table_page->GetNextPageId();
        if (next_page_id == INVALID_PAGE_ID) {
            // 需要创建新页面
            page_id_t new_page_id;
            Page* new_page = buffer_pool_manager_->NewPage(&new_page_id);
            if (new_page == nullptr) {
                page->WUnlatch();
                buffer_pool_manager_->UnpinPage(current_page_id, false);
                return false;
            }

            // 初始化新页面并链接到当前页面
            new_page->WLatch();
            auto* new_table_page = reinterpret_cast<TablePage*>(new_page);
            new_table_page->Init(new_page_id, current_page_id);
            table_page->SetNextPageId(new_page_id);

            page->WUnlatch();
            buffer_pool_manager_->UnpinPage(current_page_id, true);

            // 在新页面中插入tuple
            if (new_table_page->InsertTuple(tuple, rid)) {
                // 插入成功，记录INSERT日志
                if (log_manager_ && txn_id != INVALID_TXN_ID) {
                    InsertLogRecord log_record(txn_id, INVALID_LSN, *rid,
                                               tuple);
                    lsn_t lsn = log_manager_->AppendLogRecord(&log_record);
                    new_page->SetLSN(lsn);
                } else {
                    new_page->SetLSN(0);
                }

                new_page->WUnlatch();
                buffer_pool_manager_->UnpinPage(new_page_id, true);
                return true;
            } else {
                new_page->WUnlatch();
                buffer_pool_manager_->UnpinPage(new_page_id, true);
                return false;
            }
        } else {
            // 继续检查下一个页面
            page->WUnlatch();
            buffer_pool_manager_->UnpinPage(current_page_id, false);
            current_page_id = next_page_id;
        }
    }
    return false;
}

/**
 * 在页面中插入tuple
 * 实现思路：
 * 1. 验证tuple大小和页面结构
 * 2. 检查是否有足够的空间（slot目录 + tuple数据）
 * 3. 在页面末尾分配空间存储tuple数据
 * 4. 在slot目录中添加新的slot条目
 * @param tuple 要插入的tuple
 * @param rid 输出参数，存储插入后的RID
 * @return 插入是否成功
 */
bool TablePage::InsertTuple(const Tuple& tuple, RID* rid) {
    size_t tuple_size = tuple.GetSerializedSize();
    size_t slot_size = sizeof(Slot);
    auto* header = GetHeader();
    const size_t header_size = sizeof(TablePageHeader);

    LOG_DEBUG("TablePage::InsertTuple: page "
              << GetPageId() << " tuple_size=" << tuple_size
              << " current_tuples=" << header->num_tuples
              << " free_space_offset=" << header->free_space_offset);

    // 基本的tuple大小验证
    if (tuple_size == 0 || tuple_size > PAGE_SIZE / 2) {
        LOG_ERROR("TablePage::InsertTuple: invalid tuple size: " << tuple_size);
        return false;
    }

    // 验证和修复页面结构
    if (!ValidateAndRepairSlotDirectory(header, GetData(), GetPageId())) {
        LOG_ERROR(
            "TablePage::InsertTuple: page severely corrupted, reinitializing");
        Init(GetPageId(), INVALID_PAGE_ID);
        header = GetHeader();
    }

    // 计算插入所需的空间
    size_t slot_end_offset =
        header_size + (header->num_tuples + 1) * slot_size;  // +1 for new slot
    size_t required_data_space = tuple_size;

    // 检查总空间是否足够（slot目录不能与tuple数据重叠）
    if (slot_end_offset + required_data_space > header->free_space_offset) {
        LOG_DEBUG("TablePage::InsertTuple: insufficient space. "
                  << "slot_end=" << slot_end_offset
                  << " + data=" << required_data_space
                  << " > free_offset=" << header->free_space_offset);
        return false;
    }

    // 执行插入操作
    // 1. 在页面末尾分配空间存储tuple数据
    header->free_space_offset -= tuple_size;
    char* tuple_data = GetData() + header->free_space_offset;
    tuple.SerializeTo(tuple_data);

    // 2. 在slot目录中添加新的slot条目
    Slot* slots = reinterpret_cast<Slot*>(GetData() + header_size);
    slots[header->num_tuples].offset = header->free_space_offset;
    slots[header->num_tuples].size = tuple_size;

    // 3. 设置返回的RID
    rid->page_id = GetPageId();
    rid->slot_num = header->num_tuples;

    // 4. 更新tuple计数
    header->num_tuples++;

    LOG_DEBUG("TablePage::InsertTuple: inserted at slot "
              << rid->slot_num << " offset=" << slots[rid->slot_num].offset
              << " size=" << tuple_size);

    return true;
}
/**
 * 删除指定RID位置的tuple
 *
 * 实现思路：
 * 1. 获取目标page并加写锁，确保并发安全
 * 2. 如果需要记录日志，先获取要删除的tuple内容
 * 3. 执行实际的删除操作
 * 4.
 * 记录delete操作到WAL日志中（使用UpdateLogRecord，old_tuple有值，new_tuple为空）
 * 5. 更新page的LSN和dirty标记
 *
 * @param rid 要删除的tuple的RID
 * @param txn_id 事务ID，用于日志记录
 * @return 删除成功返回true，失败返回false
 */
bool TableHeap::DeleteTuple(const RID& rid, txn_id_t txn_id) {
    Page* page = buffer_pool_manager_->FetchPage(rid.page_id);
    if (page == nullptr) {
        return false;
    }

    page->WLatch();
    auto* table_page = reinterpret_cast<TablePage*>(page);
    
    Tuple deleted_tuple;
    bool got_tuple = false;
    if (log_manager_ && txn_id != INVALID_TXN_ID) {
        got_tuple = table_page->GetTuple(rid, &deleted_tuple, schema_);
    }

    bool result = table_page->DeleteTuple(rid);
    if (result) {
        if (log_manager_ && txn_id != INVALID_TXN_ID && got_tuple) {
            // 使用专门的DeleteLogRecord
            DeleteLogRecord log_record(txn_id, INVALID_LSN, rid, deleted_tuple);
            lsn_t lsn = log_manager_->AppendLogRecord(&log_record);
            page->SetLSN(lsn);
        } else {
            page->SetLSN(0);
        }
        page->SetDirty(true);
    }

    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(rid.page_id, result);
    return result;
}

/**
 * 更新指定RID位置的tuple
 *
 * 实现思路：
 * 1. 获取目标page并加写锁
 * 2. 先读取旧的tuple内容用于日志记录
 * 3. 执行update操作
 * 4. 记录update操作到WAL日志（old_tuple和new_tuple都有值）
 * 5. 更新LSN和dirty标记
 *
 * @param tuple 新的tuple内容
 * @param rid 要更新的tuple的RID
 * @param txn_id 事务ID
 * @return 更新成功返回true，失败返回false
 */
bool TableHeap::UpdateTuple(const Tuple& tuple, const RID& rid,
                            txn_id_t txn_id) {
    Page* page = buffer_pool_manager_->FetchPage(rid.page_id);
    if (page == nullptr) {
        return false;
    }

    page->WLatch();
    auto* table_page = reinterpret_cast<TablePage*>(page);

    // 获取更新前的tuple用于WAL日志
    Tuple old_tuple;
    bool got_old_tuple = false;
    if (log_manager_ && txn_id != INVALID_TXN_ID) {
        got_old_tuple = table_page->GetTuple(rid, &old_tuple, schema_);
    }

    // 执行实际的更新操作
    bool result = table_page->UpdateTuple(tuple, rid);

    if (result) {
        // 记录UPDATE日志：包含before和after的tuple内容
        if (log_manager_ && txn_id != INVALID_TXN_ID && got_old_tuple) {
            UpdateLogRecord log_record(txn_id, INVALID_LSN, rid, old_tuple,
                                       tuple);
            lsn_t lsn = log_manager_->AppendLogRecord(&log_record);
            page->SetLSN(lsn);
        } else {
            page->SetLSN(0);
        }
    }

    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(rid.page_id, result);
    return result;
}

/**
 * 根据RID读取tuple内容
 *
 * 实现思路：
 * 1. 获取目标page并加读锁（读操作不需要写锁）
 * 2. 从TablePage中读取tuple内容
 * 3. 释放锁并unpin页面
 *
 * @param rid tuple的RID
 * @param tuple 输出参数，存储读取的tuple内容
 * @param txn_id 事务ID（当前未使用）
 * @return 读取成功返回true，失败返回false
 */
bool TableHeap::GetTuple(const RID& rid, Tuple* tuple, txn_id_t txn_id) {
    Page* page = buffer_pool_manager_->FetchPage(rid.page_id);
    (void)txn_id;  // 当前版本未使用事务ID参数

    if (page == nullptr) {
        return false;
    }

    // 读操作只需要读锁
    page->RLatch();
    auto* table_page = reinterpret_cast<TablePage*>(page);
    bool result = table_page->GetTuple(rid, tuple, schema_);
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(rid.page_id, false);  // 读操作不会修改页面
    return result;
}

/**
 * Iterator构造函数
 *
 * @param table_heap 所属的TableHeap指针
 * @param rid 当前迭代器指向的RID位置
 */
TableHeap::Iterator::Iterator(TableHeap* table_heap, const RID& rid)
    : table_heap_(table_heap), current_rid_(rid) {}

/**
 * 判断迭代器是否到达末尾
 *
 * @return 到达末尾返回true，否则返回false
 */
bool TableHeap::Iterator::IsEnd() const {
    return current_rid_.page_id == INVALID_PAGE_ID;
}

/**
 * 迭代器前进操作（++运算符重载）
 *
 * 实现思路：
 * 1. 先在当前页面查找下一个有效tuple
 * 2. 如果当前页面没有更多tuple，跳转到下一个页面
 * 3. 在下一个页面查找第一个有效tuple
 * 4. 如果所有页面都遍历完，设置为end状态
 *
 * 这个实现支持跨页面的连续遍历，处理了页面链表的导航
 */
void TableHeap::Iterator::operator++() {
    if (current_rid_.page_id == INVALID_PAGE_ID) {
        return;
    }

    LOG_DEBUG("TableHeap::Iterator::operator++: current RID page="
              << current_rid_.page_id << " slot=" << current_rid_.slot_num);

    Page* page =
        table_heap_->buffer_pool_manager_->FetchPage(current_rid_.page_id);
    if (page == nullptr) {
        LOG_ERROR("TableHeap::Iterator: Cannot fetch current page "
                  << current_rid_.page_id);
        current_rid_.page_id = INVALID_PAGE_ID;
        return;
    }

    page->RLatch();
    auto* table_page = reinterpret_cast<TablePage*>(page);

    // 验证页面完整性
    if (page->GetPageId() != current_rid_.page_id) {
        LOG_ERROR("TableHeap::Iterator: Page ID mismatch, expected "
                  << current_rid_.page_id << " but got " << page->GetPageId());
        page->RUnlatch();
        table_heap_->buffer_pool_manager_->UnpinPage(current_rid_.page_id,
                                                     false);
        current_rid_.page_id = INVALID_PAGE_ID;
        return;
    }

    RID next_rid;
    if (table_page->GetNextTupleRID(current_rid_, &next_rid)) {
        LOG_DEBUG("TableHeap::Iterator: found next tuple in same page: "
                  << next_rid.page_id << ":" << next_rid.slot_num);
        current_rid_ = next_rid;
        page->RUnlatch();
        table_heap_->buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
        return;
    }

    // 获取下一个页面ID，并进行验证
    page_id_t next_page_id = table_page->GetNextPageId();
    page_id_t current_page_id = page->GetPageId();

    LOG_DEBUG("TableHeap::Iterator: no more tuples in page "
              << current_page_id << ", next page: " << next_page_id);

    page->RUnlatch();
    table_heap_->buffer_pool_manager_->UnpinPage(current_page_id, false);

    // 检查下一个页面ID是否有效
    if (next_page_id == INVALID_PAGE_ID) {
        LOG_DEBUG("TableHeap::Iterator: reached end of table");
        current_rid_.page_id = INVALID_PAGE_ID;
        return;
    }

    // 额外验证：检查页面ID范围
    int total_pages =
        table_heap_->buffer_pool_manager_->GetDiskManager()->GetNumPages();
    if (next_page_id < 0 || next_page_id >= total_pages) {
        LOG_ERROR("TableHeap::Iterator: Invalid next_page_id "
                  << next_page_id << " (total pages: " << total_pages
                  << "), ending iteration");
        current_rid_.page_id = INVALID_PAGE_ID;
        return;
    }

    page = table_heap_->buffer_pool_manager_->FetchPage(next_page_id);
    if (page == nullptr) {
        LOG_ERROR("TableHeap::Iterator: Cannot fetch next page "
                  << next_page_id << " (total pages: " << total_pages << ")");
        current_rid_.page_id = INVALID_PAGE_ID;
        return;
    }

    page->RLatch();
    table_page = reinterpret_cast<TablePage*>(page);

    // 验证新页面的完整性
    if (page->GetPageId() != next_page_id) {
        LOG_ERROR("TableHeap::Iterator: Next page ID mismatch, expected "
                  << next_page_id << " but got " << page->GetPageId());
        page->RUnlatch();
        table_heap_->buffer_pool_manager_->UnpinPage(next_page_id, false);
        current_rid_.page_id = INVALID_PAGE_ID;
        return;
    }

    RID first_rid{next_page_id, -1};
    if (table_page->GetNextTupleRID(first_rid, &next_rid)) {
        LOG_DEBUG("TableHeap::Iterator: found first tuple in next page: "
                  << next_rid.page_id << ":" << next_rid.slot_num);
        current_rid_ = next_rid;
        page->RUnlatch();
        table_heap_->buffer_pool_manager_->UnpinPage(next_page_id, false);
        return;
    }

    LOG_DEBUG("TableHeap::Iterator: next page "
              << next_page_id << " has no tuples, continuing search");
    page->RUnlatch();
    table_heap_->buffer_pool_manager_->UnpinPage(next_page_id, false);

    // 递归查找下一个有效页面
    current_rid_.page_id = next_page_id;
    current_rid_.slot_num = -1;
    operator++();
}

/**
 * 迭代器解引用操作（*运算符重载）
 *
 * @return 当前位置的Tuple对象
 */
Tuple TableHeap::Iterator::operator*() {
    Tuple tuple;
    table_heap_->GetTuple(current_rid_, &tuple, INVALID_TXN_ID);
    return tuple;
}

/**
 * 获取指向表开始位置的迭代器
 *
 * 实现思路：
 * 1. 从第一个页面开始查找
 * 2. 找到第一个有效的tuple位置
 * 3. 如果整个表为空，返回end迭代器
 *
 * @return 指向第一个有效tuple的迭代器
 */
TableHeap::Iterator TableHeap::Begin() {
    LOG_DEBUG(
        "TableHeap::Begin: starting with first_page_id=" << first_page_id_);

    if (first_page_id_ == INVALID_PAGE_ID) {
        LOG_WARN(
            "TableHeap::Begin: Invalid first page ID, returning end iterator");
        return Iterator(this, RID{INVALID_PAGE_ID, -1});
    }

    // 验证first_page_id的合理性
    int total_pages = buffer_pool_manager_->GetDiskManager()->GetNumPages();
    if (first_page_id_ < 0 || first_page_id_ >= total_pages) {
        LOG_ERROR("TableHeap::Begin: Invalid first_page_id "
                  << first_page_id_ << " (total pages: " << total_pages << ")");
        return Iterator(this, RID{INVALID_PAGE_ID, -1});
    }

    Page* first_page = buffer_pool_manager_->FetchPage(first_page_id_);
    if (first_page == nullptr) {
        LOG_ERROR("TableHeap::Begin: Cannot fetch first page "
                  << first_page_id_);
        return Iterator(this, RID{INVALID_PAGE_ID, -1});
    }

    first_page->RLatch();
    auto* table_page = reinterpret_cast<TablePage*>(first_page);

    // 验证页面完整性
    if (first_page->GetPageId() != first_page_id_) {
        LOG_ERROR("TableHeap::Begin: Page ID mismatch, expected "
                  << first_page_id_ << " but got " << first_page->GetPageId());
        first_page->RUnlatch();
        buffer_pool_manager_->UnpinPage(first_page_id_, false);
        return Iterator(this, RID{INVALID_PAGE_ID, -1});
    }

    // 验证页面头部
    auto* header = table_page->GetHeader();
    if (header->num_tuples < 0 || header->num_tuples > 10000) {
        LOG_ERROR("TableHeap::Begin: Invalid num_tuples " << header->num_tuples
                                                          << " in first page");
        first_page->RUnlatch();
        buffer_pool_manager_->UnpinPage(first_page_id_, false);
        return Iterator(this, RID{INVALID_PAGE_ID, -1});
    }

    RID first_valid_rid;
    RID start_rid{first_page_id_, -1};
    bool found = table_page->GetNextTupleRID(start_rid, &first_valid_rid);

    first_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(first_page_id_, false);

    if (found) {
        LOG_DEBUG("TableHeap::Begin: found first tuple at "
                  << first_valid_rid.page_id << ":"
                  << first_valid_rid.slot_num);
        return Iterator(this, first_valid_rid);
    } else {
        LOG_DEBUG("TableHeap::Begin: no tuples found in first page");
        return Iterator(this, RID{INVALID_PAGE_ID, -1});
    }
}

/**
 * 获取指向表结束位置的迭代器
 *
 * @return end迭代器（RID为INVALID_PAGE_ID表示结束）
 */
TableHeap::Iterator TableHeap::End() {
    return Iterator(this, RID{INVALID_PAGE_ID, -1});
}

}  // namespace SimpleRDBMS