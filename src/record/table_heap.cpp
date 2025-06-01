#include "record/table_heap.h"

#include <algorithm>
#include <cstring>
#include <unordered_set>

#include "common/exception.h"
#include "recovery/recovery_manager.h"

namespace SimpleRDBMS {

struct Slot {
    uint16_t offset;
    uint16_t size;
    
    Slot() : offset(0), size(0) {}
    Slot(uint16_t off, uint16_t sz) : offset(off), size(sz) {}
};

void TablePage::Init(page_id_t page_id, page_id_t prev_page_id) {
    SetPageId(page_id);
    SetLSN(INVALID_LSN);
    (void)prev_page_id;
    
    // 清零整个页面数据
    std::memset(GetData(), 0, PAGE_SIZE);
    
    auto* header = GetHeader();
    header->next_page_id = INVALID_PAGE_ID;
    header->lsn = INVALID_LSN;
    header->num_tuples = 0;
    header->free_space_offset = PAGE_SIZE;
    
    LOG_DEBUG("TablePage::Init: initialized page " << page_id
              << " with free_space_offset=" << header->free_space_offset);
}

// 页面布局：
// [Header] [Slot Directory] [Free Space] [Tuple Data (grows backward)]
//    ^            ^              ^               ^
//    0      header_size    slot_end_offset  free_space_offset

static bool ValidateAndRepairSlotDirectory(TablePage::TablePageHeader* header, char* page_data) {
    const size_t header_size = sizeof(TablePage::TablePageHeader);
    const size_t slot_size = sizeof(Slot);
    
    LOG_DEBUG("ValidateAndRepairSlotDirectory: num_tuples=" << header->num_tuples 
              << ", free_space_offset=" << header->free_space_offset);
    
    if (header->num_tuples < 0) {
        LOG_ERROR("ValidateAndRepairSlotDirectory: negative num_tuples " << header->num_tuples);
        header->num_tuples = 0;
        header->free_space_offset = PAGE_SIZE;
        return false;
    }
    
    if (header->num_tuples == 0) {
        header->free_space_offset = PAGE_SIZE;
        LOG_DEBUG("ValidateAndRepairSlotDirectory: empty page, reset free_space_offset to " << PAGE_SIZE);
        return true;
    }
    
    size_t slot_end_offset = header_size + header->num_tuples * slot_size;
    if (slot_end_offset > PAGE_SIZE) {
        LOG_ERROR("ValidateAndRepairSlotDirectory: slot directory exceeds page size, resetting");
        header->num_tuples = 0;
        header->free_space_offset = PAGE_SIZE;
        return false;
    }
    
    Slot* slots = reinterpret_cast<Slot*>(page_data + header_size);
    size_t calculated_free_offset = PAGE_SIZE;
    int valid_tuples = 0;
    
    // 验证每个槽位
    for (int i = 0; i < header->num_tuples; i++) {
        LOG_TRACE("ValidateAndRepairSlotDirectory: checking slot " << i 
                  << " (offset=" << slots[i].offset << ", size=" << slots[i].size << ")");
        
        if (slots[i].size > 0) {
            // 检查槽位有效性
            if (slots[i].offset >= slot_end_offset && 
                slots[i].offset < PAGE_SIZE &&
                slots[i].size <= PAGE_SIZE &&
                slots[i].offset + slots[i].size <= PAGE_SIZE) {
                
                calculated_free_offset = std::min(calculated_free_offset, static_cast<size_t>(slots[i].offset));
                
                // 如果需要压缩，移动槽位
                if (valid_tuples != i) {
                    LOG_DEBUG("ValidateAndRepairSlotDirectory: moving slot " << i << " to position " << valid_tuples);
                    slots[valid_tuples] = slots[i];
                }
                valid_tuples++;
            } else {
                LOG_DEBUG("ValidateAndRepairSlotDirectory: removing invalid slot " << i 
                          << " (offset=" << slots[i].offset << ", size=" << slots[i].size << ")");
            }
        }
    }
    
    // 清理无效的槽位
    for (int i = valid_tuples; i < header->num_tuples; i++) {
        slots[i].offset = 0;
        slots[i].size = 0;
    }
    
    header->num_tuples = valid_tuples;
    slot_end_offset = header_size + header->num_tuples * slot_size;
    
    // 确保 free space offset 是合理的
    if (calculated_free_offset < slot_end_offset) {
        calculated_free_offset = PAGE_SIZE;
        LOG_WARN("ValidateAndRepairSlotDirectory: page appears full, no free space available");
    }
    
    // 验证 free space offset 的合理性
    if (header->free_space_offset > PAGE_SIZE || header->free_space_offset < slot_end_offset) {
        LOG_DEBUG("ValidateAndRepairSlotDirectory: correcting invalid free_space_offset from " 
                  << header->free_space_offset << " to " << calculated_free_offset);
        header->free_space_offset = calculated_free_offset;
    }
    
    LOG_DEBUG("ValidateAndRepairSlotDirectory: validated page with " << valid_tuples 
              << " valid tuples, slot_end=" << slot_end_offset 
              << ", free_offset=" << header->free_space_offset);
    
    return true;
}

bool TablePage::DeleteTuple(const RID& rid) {
    auto* header = GetHeader();
    if (rid.slot_num < 0 || rid.slot_num >= header->num_tuples) {
        LOG_DEBUG("TablePage::DeleteTuple: Invalid slot number " << rid.slot_num);
        return false;
    }
    
    const size_t header_size = sizeof(TablePageHeader);
    Slot* slots = reinterpret_cast<Slot*>(GetData() + header_size);
    
    if (slots[rid.slot_num].size == 0) {
        LOG_DEBUG("TablePage::DeleteTuple: Slot " << rid.slot_num << " already empty");
        return false;
    }
    
    // 标记槽位为已删除
    slots[rid.slot_num].size = 0;
    slots[rid.slot_num].offset = 0;
    
    LOG_DEBUG("TablePage::DeleteTuple: Successfully deleted tuple at slot " << rid.slot_num);
    return true;
}

bool TablePage::UpdateTuple(const Tuple& tuple, const RID& rid) {
    auto* header = GetHeader();
    if (rid.slot_num < 0 || rid.slot_num >= header->num_tuples) {
        return false;
    }
    const size_t header_size = sizeof(TablePageHeader);
    Slot* slots = reinterpret_cast<Slot*>(GetData() + header_size);
    if (slots[rid.slot_num].size == 0) {
        return false;
    }
    
    size_t new_tuple_size = tuple.GetSerializedSize();
    size_t old_tuple_size = slots[rid.slot_num].size;
    
    // 如果大小相同，直接在原位置覆盖
    if (new_tuple_size == old_tuple_size) {
        char* tuple_data = GetData() + slots[rid.slot_num].offset;
        tuple.SerializeTo(tuple_data);
        return true;
    }
    
    // 如果新tuple更小，可以直接在原位置更新
    if (new_tuple_size < old_tuple_size) {
        char* tuple_data = GetData() + slots[rid.slot_num].offset;
        tuple.SerializeTo(tuple_data);
        slots[rid.slot_num].size = new_tuple_size;
        return true;
    }
    
    // 如果新tuple更大，使用删除-插入的简单策略
    // 保存原始slot信息用于回滚
    uint16_t old_offset = slots[rid.slot_num].offset;
    uint16_t old_size = slots[rid.slot_num].size;
    
    // 先标记原slot为删除状态
    slots[rid.slot_num].size = 0;
    slots[rid.slot_num].offset = 0;
    
    // 尝试在页面末尾插入新tuple
    size_t slot_end_offset = header_size + header->num_tuples * sizeof(Slot);
    if (header->free_space_offset >= slot_end_offset + new_tuple_size) {
        // 有足够空间，直接插入
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

bool TablePage::GetTuple(const RID& rid, Tuple* tuple, const Schema* schema) {
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
              << rid.slot_num << " (total slots: " << header->num_tuples << ")");
    
    // Validate slot number
    if (rid.slot_num < 0 || rid.slot_num >= header->num_tuples) {
        LOG_DEBUG("TablePage::GetTuple: slot " << rid.slot_num 
                  << " out of range [0, " << header->num_tuples << ")");
        return false;
    }
    
    // Validate and repair page structure if needed
    if (!ValidateAndRepairSlotDirectory(header, GetData())) {
        LOG_ERROR("TablePage::GetTuple: page structure validation failed");
        return false;
    }
    
    const size_t header_size = sizeof(TablePageHeader);
    Slot* slots = reinterpret_cast<Slot*>(GetData() + header_size);
    
    // Check if slot is valid (not deleted)
    if (slots[rid.slot_num].size == 0) {
        LOG_DEBUG("TablePage::GetTuple: slot " << rid.slot_num << " is deleted (size=0)");
        return false;
    }
    
    LOG_DEBUG("TablePage::GetTuple: slot " << rid.slot_num 
              << " has offset=" << slots[rid.slot_num].offset 
              << ", size=" << slots[rid.slot_num].size);
    
    // Validate slot offset and size
    if (slots[rid.slot_num].offset < header_size || 
        slots[rid.slot_num].offset + slots[rid.slot_num].size > PAGE_SIZE) {
        LOG_WARN("TablePage::GetTuple: invalid slot " << rid.slot_num 
                 << " (offset=" << slots[rid.slot_num].offset 
                 << ", size=" << slots[rid.slot_num].size << ")");
        return false;
    }
    
    // Get tuple data and deserialize
    char* tuple_data = GetData() + slots[rid.slot_num].offset;
    
    try {
        LOG_DEBUG("TablePage::GetTuple: About to deserialize tuple data, schema has " 
                  << schema->GetColumnCount() << " columns");
        
        tuple->DeserializeFrom(tuple_data, schema);
        tuple->SetRID(rid);
        
        LOG_DEBUG("TablePage::GetTuple: Successfully retrieved tuple from slot " 
                  << rid.slot_num << " with " << tuple->GetValues().size() << " values");
        
        return true;
    } catch (const std::exception& e) {
        LOG_ERROR("TablePage::GetTuple: Failed to deserialize tuple from slot " 
                  << rid.slot_num << ": " << e.what());
        return false;
    }
}

bool TablePage::GetNextTupleRID(const RID& current_rid, RID* next_rid) {
    auto* header = GetHeader();
    
    // 验证和修复页面结构
    if (!ValidateAndRepairSlotDirectory(header, GetData())) {
        LOG_ERROR("TablePage::GetNextTupleRID: page corrupted, reinitializing");
        Init(GetPageId(), INVALID_PAGE_ID);
        return false;
    }

    if (header->num_tuples == 0) {
        return false;
    }

    const size_t header_size = sizeof(TablePageHeader);
    Slot* slots = reinterpret_cast<Slot*>(GetData() + header_size);
    
    int start_slot = current_rid.slot_num + 1;
    if (start_slot < 0) {
        start_slot = 0;
    }

    for (int i = start_slot; i < header->num_tuples; i++) {
        if (slots[i].size > 0) {
            next_rid->page_id = GetPageId();
            next_rid->slot_num = i;
            return true;
        }
    }

    return false;
}

page_id_t TablePage::GetNextPageId() const { return GetHeader()->next_page_id; }

void TablePage::SetNextPageId(page_id_t next_page_id) {
    GetHeader()->next_page_id = next_page_id;
}

TablePage::TablePageHeader* TablePage::GetHeader() {
    return reinterpret_cast<TablePageHeader*>(GetData());
}

const TablePage::TablePageHeader* TablePage::GetHeader() const {
    return reinterpret_cast<const TablePageHeader*>(GetData());
}

TableHeap::TableHeap(BufferPoolManager* buffer_pool_manager,
                     const Schema* schema)
    : buffer_pool_manager_(buffer_pool_manager),
      schema_(schema),
      first_page_id_(INVALID_PAGE_ID) {
    Page* first_page = buffer_pool_manager_->NewPage(&first_page_id_);
    if (first_page == nullptr) {
        throw Exception("Cannot allocate first page for table heap");
    }

    first_page->WLatch();
    auto* table_page = reinterpret_cast<TablePage*>(first_page);
    table_page->Init(first_page_id_, INVALID_PAGE_ID);
    first_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(first_page_id_, true);
}

// 新增构造函数（用于恢复）
TableHeap::TableHeap(BufferPoolManager* buffer_pool_manager,
                     const Schema* schema, page_id_t first_page_id)
    : buffer_pool_manager_(buffer_pool_manager),
      schema_(schema),
      first_page_id_(first_page_id) {
    LOG_DEBUG("TableHeap: Creating TableHeap with existing first_page_id="
              << first_page_id);

    // 验证first_page是否存在
    Page* first_page = buffer_pool_manager_->FetchPage(first_page_id_);
    if (first_page == nullptr) {
        LOG_ERROR("TableHeap: Cannot fetch first page "
                  << first_page_id << " for table heap recovery");
        throw Exception("Cannot fetch first page for table heap recovery");
    }

    LOG_DEBUG("TableHeap: Successfully fetched first page " << first_page_id);
    buffer_pool_manager_->UnpinPage(first_page_id_, false);
    LOG_DEBUG("TableHeap: TableHeap created successfully");
}

TableHeap::~TableHeap() = default;

bool TableHeap::InsertTuple(const Tuple& tuple, RID* rid, txn_id_t txn_id) {
    page_id_t current_page_id = first_page_id_;
    
    while (current_page_id != INVALID_PAGE_ID) {
        Page* page = buffer_pool_manager_->FetchPage(current_page_id);
        if (page == nullptr) {
            return false;
        }
        
        page->WLatch();
        auto* table_page = reinterpret_cast<TablePage*>(page);
        
        if (table_page->InsertTuple(tuple, rid)) {
            // 记录INSERT日志
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
        
        page_id_t next_page_id = table_page->GetNextPageId();
        if (next_page_id == INVALID_PAGE_ID) {
            // 创建新页面的逻辑保持不变
            page_id_t new_page_id;
            Page* new_page = buffer_pool_manager_->NewPage(&new_page_id);
            if (new_page == nullptr) {
                page->WUnlatch();
                buffer_pool_manager_->UnpinPage(current_page_id, false);
                return false;
            }
            
            new_page->WLatch();
            auto* new_table_page = reinterpret_cast<TablePage*>(new_page);
            new_table_page->Init(new_page_id, current_page_id);
            table_page->SetNextPageId(new_page_id);
            
            page->WUnlatch();
            buffer_pool_manager_->UnpinPage(current_page_id, true);
            
            if (new_table_page->InsertTuple(tuple, rid)) {
                // 记录INSERT日志
                if (log_manager_ && txn_id != INVALID_TXN_ID) {
                    InsertLogRecord log_record(txn_id, INVALID_LSN, *rid, tuple);
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
            page->WUnlatch();
            buffer_pool_manager_->UnpinPage(current_page_id, false);
            current_page_id = next_page_id;
        }
    }
    return false;
}

bool TablePage::InsertTuple(const Tuple& tuple, RID* rid) {
    size_t tuple_size = tuple.GetSerializedSize();
    size_t slot_size = sizeof(Slot);
    auto* header = GetHeader();
    const size_t header_size = sizeof(TablePageHeader);

    LOG_DEBUG("TablePage::InsertTuple: page " << GetPageId() 
              << " tuple_size=" << tuple_size
              << " current_tuples=" << header->num_tuples
              << " free_space_offset=" << header->free_space_offset);

    // 基本验证
    if (tuple_size == 0 || tuple_size > PAGE_SIZE / 2) {
        LOG_ERROR("TablePage::InsertTuple: invalid tuple size: " << tuple_size);
        return false;
    }

    // 验证和修复页面结构
    if (!ValidateAndRepairSlotDirectory(header, GetData())) {
        LOG_ERROR("TablePage::InsertTuple: page severely corrupted, reinitializing");
        Init(GetPageId(), INVALID_PAGE_ID);
        header = GetHeader();
    }

    // 计算空间需求
    size_t slot_end_offset = header_size + (header->num_tuples + 1) * slot_size; // +1 for new slot
    size_t required_data_space = tuple_size;
    
    // 检查总空间是否足够
    if (slot_end_offset + required_data_space > header->free_space_offset) {
        LOG_DEBUG("TablePage::InsertTuple: insufficient space. "
                 << "slot_end=" << slot_end_offset 
                 << " + data=" << required_data_space
                 << " > free_offset=" << header->free_space_offset);
        return false;
    }

    // 执行插入
    header->free_space_offset -= tuple_size;
    char* tuple_data = GetData() + header->free_space_offset;
    tuple.SerializeTo(tuple_data);

    // 添加新槽
    Slot* slots = reinterpret_cast<Slot*>(GetData() + header_size);
    slots[header->num_tuples].offset = header->free_space_offset;
    slots[header->num_tuples].size = tuple_size;

    rid->page_id = GetPageId();
    rid->slot_num = header->num_tuples;

    header->num_tuples++;

    LOG_DEBUG("TablePage::InsertTuple: inserted at slot " << rid->slot_num 
              << " offset=" << slots[rid->slot_num].offset
              << " size=" << tuple_size);

    return true;
}

bool TableHeap::DeleteTuple(const RID& rid, txn_id_t txn_id) {
    Page* page = buffer_pool_manager_->FetchPage(rid.page_id);
    if (page == nullptr) {
        return false;
    }
    
    page->WLatch();
    auto* table_page = reinterpret_cast<TablePage*>(page);
    
    // 先获取要删除的记录（用于日志记录）
    Tuple deleted_tuple;
    bool got_tuple = false;
    if (log_manager_ && txn_id != INVALID_TXN_ID) {
        got_tuple = table_page->GetTuple(rid, &deleted_tuple, schema_);
    }
    
    // 执行删除
    bool result = table_page->DeleteTuple(rid);
    
    if (result) {
        // 记录删除操作到日志
        if (log_manager_ && txn_id != INVALID_TXN_ID && got_tuple) {
            // 使用UpdateLogRecord来记录删除操作（old_tuple存在，new_tuple为空）
            Tuple empty_tuple;
            UpdateLogRecord log_record(txn_id, INVALID_LSN, rid, deleted_tuple, empty_tuple);
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

bool TableHeap::UpdateTuple(const Tuple& tuple, const RID& rid, txn_id_t txn_id) {
    Page* page = buffer_pool_manager_->FetchPage(rid.page_id);
    if (page == nullptr) {
        return false;
    }
    
    page->WLatch();
    auto* table_page = reinterpret_cast<TablePage*>(page);
    
    // 先获取旧的tuple用于日志记录
    Tuple old_tuple;
    bool got_old_tuple = false;
    if (log_manager_ && txn_id != INVALID_TXN_ID) {
        got_old_tuple = table_page->GetTuple(rid, &old_tuple, schema_);
    }
    
    bool result = table_page->UpdateTuple(tuple, rid);
    
    if (result) {
        // 记录UPDATE日志
        if (log_manager_ && txn_id != INVALID_TXN_ID && got_old_tuple) {
            UpdateLogRecord log_record(txn_id, INVALID_LSN, rid, old_tuple, tuple);
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

bool TableHeap::GetTuple(const RID& rid, Tuple* tuple, txn_id_t txn_id) {
    Page* page = buffer_pool_manager_->FetchPage(rid.page_id);
    (void)txn_id;  // Unused parameter

    if (page == nullptr) {
        return false;
    }

    page->RLatch();
    auto* table_page = reinterpret_cast<TablePage*>(page);
    bool result = table_page->GetTuple(rid, tuple, schema_);
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(rid.page_id, false);
    return result;
}

TableHeap::Iterator::Iterator(TableHeap* table_heap, const RID& rid)
    : table_heap_(table_heap), current_rid_(rid) {}

bool TableHeap::Iterator::IsEnd() const {
    return current_rid_.page_id == INVALID_PAGE_ID;
}

void TableHeap::Iterator::operator++() {
    // 如果已经是结束状态，直接返回
    if (current_rid_.page_id == INVALID_PAGE_ID) {
        return;
    }

    LOG_DEBUG("TableHeap::Iterator::operator++: current RID page="
              << current_rid_.page_id << " slot=" << current_rid_.slot_num);

    Page* page =
        table_heap_->buffer_pool_manager_->FetchPage(current_rid_.page_id);
    if (page == nullptr) {
        LOG_ERROR("TableHeap::Iterator: Cannot fetch page "
                  << current_rid_.page_id);
        current_rid_.page_id = INVALID_PAGE_ID;
        return;
    }

    page->RLatch();
    auto* table_page = reinterpret_cast<TablePage*>(page);
    RID next_rid;

    // 在当前页面查找下一个tuple
    if (table_page->GetNextTupleRID(current_rid_, &next_rid)) {
        LOG_DEBUG("TableHeap::Iterator: found next tuple in same page: "
                  << next_rid.page_id << ":" << next_rid.slot_num);
        current_rid_ = next_rid;
        page->RUnlatch();
        table_heap_->buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
        return;
    }

    // 当前页面没有更多tuple，查找下一个页面
    page_id_t next_page_id = table_page->GetNextPageId();
    page_id_t current_page_id = page->GetPageId();

    LOG_DEBUG("TableHeap::Iterator: no more tuples in page "
              << current_page_id << ", next page: " << next_page_id);

    page->RUnlatch();
    table_heap_->buffer_pool_manager_->UnpinPage(current_page_id, false);

    // 如果没有下一个页面，设置为结束状态
    if (next_page_id == INVALID_PAGE_ID) {
        LOG_DEBUG("TableHeap::Iterator: reached end of table");
        current_rid_.page_id = INVALID_PAGE_ID;
        return;
    }

    // 尝试在下一个页面找到第一个tuple
    page = table_heap_->buffer_pool_manager_->FetchPage(next_page_id);
    if (page == nullptr) {
        LOG_ERROR("TableHeap::Iterator: Cannot fetch next page "
                  << next_page_id);
        current_rid_.page_id = INVALID_PAGE_ID;
        return;
    }

    page->RLatch();
    table_page = reinterpret_cast<TablePage*>(page);

    // 尝试找到这个页面的第一个tuple
    RID first_rid{next_page_id, -1};
    if (table_page->GetNextTupleRID(first_rid, &next_rid)) {
        LOG_DEBUG("TableHeap::Iterator: found first tuple in next page: "
                  << next_rid.page_id << ":" << next_rid.slot_num);
        current_rid_ = next_rid;
        page->RUnlatch();
        table_heap_->buffer_pool_manager_->UnpinPage(next_page_id, false);
        return;
    }

    // 下一个页面也没有tuple，设置为结束状态
    LOG_DEBUG("TableHeap::Iterator: next page " << next_page_id
                                                << " has no tuples");
    page->RUnlatch();
    table_heap_->buffer_pool_manager_->UnpinPage(next_page_id, false);
    current_rid_.page_id = INVALID_PAGE_ID;
}

Tuple TableHeap::Iterator::operator*() {
    Tuple tuple;
    table_heap_->GetTuple(current_rid_, &tuple, INVALID_TXN_ID);
    return tuple;
}

TableHeap::Iterator TableHeap::Begin() {
    LOG_DEBUG(
        "TableHeap::Begin: starting with first_page_id=" << first_page_id_);

    if (first_page_id_ == INVALID_PAGE_ID) {
        LOG_WARN(
            "TableHeap::Begin: Invalid first page ID, returning end iterator");
        return Iterator(this, RID{INVALID_PAGE_ID, -1});
    }

    // 直接尝试从第一个页面获取第一个有效的tuple
    Page* first_page = buffer_pool_manager_->FetchPage(first_page_id_);
    if (first_page == nullptr) {
        LOG_ERROR("TableHeap::Begin: Cannot fetch first page "
                  << first_page_id_);
        return Iterator(this, RID{INVALID_PAGE_ID, -1});
    }

    first_page->RLatch();
    auto* table_page = reinterpret_cast<TablePage*>(first_page);
    RID first_valid_rid;

    // 从-1开始查找第一个有效的tuple
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

TableHeap::Iterator TableHeap::End() {
    return Iterator(this, RID{INVALID_PAGE_ID, -1});
}

}  // namespace SimpleRDBMS