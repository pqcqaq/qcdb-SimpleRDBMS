#include "record/table_heap.h"

#include <algorithm>
#include <cstring>
#include <unordered_set>

#include "common/exception.h"

namespace SimpleRDBMS {

struct Slot {
    uint16_t offset;
    uint16_t size;
};

void TablePage::Init(page_id_t page_id, page_id_t prev_page_id) {
    SetPageId(page_id);
    SetLSN(INVALID_LSN);
    (void)prev_page_id;  // prev_page_id is not used in this implementation

    // 首先清零整个页面数据
    std::memset(GetData(), 0, PAGE_SIZE);
    auto* header = GetHeader();
    header->next_page_id = INVALID_PAGE_ID;
    header->lsn = INVALID_LSN;
    header->num_tuples = 0;
    header->free_space_offset = PAGE_SIZE;
    LOG_DEBUG("TablePage::Init: initialized page "
              << page_id
              << " with free_space_offset=" << header->free_space_offset);
}

bool TablePage::DeleteTuple(const RID& rid) {
    auto* header = GetHeader();

    if (rid.slot_num >= header->num_tuples) {
        return false;
    }

    Slot* slots = reinterpret_cast<Slot*>(GetData() + sizeof(TablePageHeader));

    if (slots[rid.slot_num].size == 0) {
        return false;
    }

    slots[rid.slot_num].size = 0;

    return true;
}

bool TablePage::UpdateTuple(const Tuple& tuple, const RID& rid) {
    auto* header = GetHeader();

    if (rid.slot_num >= header->num_tuples) {
        return false;
    }

    Slot* slots = reinterpret_cast<Slot*>(GetData() + sizeof(TablePageHeader));

    if (slots[rid.slot_num].size == 0) {
        return false;
    }

    size_t new_tuple_size = tuple.GetSerializedSize();
    size_t old_tuple_size = slots[rid.slot_num].size;

    if (new_tuple_size == old_tuple_size) {
        char* tuple_data = GetData() + slots[rid.slot_num].offset;
        tuple.SerializeTo(tuple_data);
        return true;
    }

    if (new_tuple_size > old_tuple_size) {
        size_t slot_end_offset =
            sizeof(TablePageHeader) + header->num_tuples * sizeof(Slot);
        size_t free_space = header->free_space_offset - slot_end_offset;
        size_t extra_space_needed = new_tuple_size - old_tuple_size;

        if (free_space < extra_space_needed) {
            return false;
        }
    }

    slots[rid.slot_num].size = 0;

    RID new_rid;
    if (!InsertTuple(tuple, &new_rid)) {
        slots[rid.slot_num].size = old_tuple_size;
        return false;
    }

    slots[rid.slot_num] = slots[new_rid.slot_num];
    header->num_tuples--;

    return true;
}

bool TablePage::GetTuple(const RID& rid, Tuple* tuple, const Schema* schema) {
    auto* header = GetHeader();

    if (rid.slot_num >= header->num_tuples) {
        return false;
    }

    Slot* slots = reinterpret_cast<Slot*>(GetData() + sizeof(TablePageHeader));

    if (slots[rid.slot_num].size == 0) {
        return false;
    }

    char* tuple_data = GetData() + slots[rid.slot_num].offset;
    tuple->DeserializeFrom(tuple_data, schema);
    tuple->SetRID(rid);

    return true;
}

bool TablePage::GetNextTupleRID(const RID& current_rid, RID* next_rid) {
    auto* header = GetHeader();
    // 添加边界检查
    if (header->num_tuples == 0) {
        LOG_DEBUG("TablePage::GetNextTupleRID: page has no tuples");
        return false;
    }

    // 根据页面大小动态计算合理的最大记录数
    // 页面头大小 + 最小记录大小(假设16字节) + slot大小(8字节)
    const size_t page_header_size = sizeof(TablePageHeader);
    const size_t min_record_size = 16;  // 最小记录大小
    const size_t slot_size = sizeof(Slot);
    const int max_reasonable_tuples = static_cast<int>(
        (PAGE_SIZE - page_header_size) / (min_record_size + slot_size));

    if (header->num_tuples > max_reasonable_tuples) {
        LOG_ERROR("TablePage::GetNextTupleRID: num_tuples ("
                  << header->num_tuples << ") exceeds reasonable limit ("
                  << max_reasonable_tuples << "), possible corruption");
        return false;
    }

    // 添加free_space_offset的合理性检查
    if (header->free_space_offset > PAGE_SIZE ||
        header->free_space_offset < sizeof(TablePageHeader)) {
        LOG_ERROR("TablePage::GetNextTupleRID: invalid free_space_offset="
                  << header->free_space_offset << " (should be between "
                  << sizeof(TablePageHeader) << " and " << PAGE_SIZE << ")");
        return false;
    }

    Slot* slots = reinterpret_cast<Slot*>(GetData() + sizeof(TablePageHeader));
    // 从当前slot的下一个位置开始查找
    int start_slot = current_rid.slot_num + 1;
    // 确保start_slot在合理范围内
    if (start_slot < 0) {
        start_slot = 0;
    }

    LOG_DEBUG("TablePage::GetNextTupleRID: searching from slot "
              << start_slot << ", total slots: " << header->num_tuples
              << ", free_space_offset: " << header->free_space_offset);

    for (int i = start_slot; i < header->num_tuples; i++) {
        // 改进有效性检查：检查slot的合理性
        if (slots[i].size > 0 && slots[i].size <= PAGE_SIZE &&
            slots[i].offset >=
                header->free_space_offset &&  // offset不能小于free_space_offset
            slots[i].offset + slots[i].size <= PAGE_SIZE &&  // 不能超出页面边界
            slots[i].offset >= sizeof(TablePageHeader)) {    // 不能覆盖页面头

            next_rid->page_id = GetPageId();
            next_rid->slot_num = i;
            LOG_DEBUG("TablePage::GetNextTupleRID: found valid tuple at slot "
                      << i << " offset=" << slots[i].offset
                      << " size=" << slots[i].size);
            return true;
        } else if (slots[i].size > 0) {
            LOG_WARN("TablePage::GetNextTupleRID: invalid slot "
                     << i << " size=" << slots[i].size
                     << " offset=" << slots[i].offset << " (free_space_offset="
                     << header->free_space_offset << ")");
        }
    }

    LOG_DEBUG("TablePage::GetNextTupleRID: no more valid tuples found");
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
    (void)txn_id;  // Unused parameter, can be used for logging or future
                   // enhancements
    while (current_page_id != INVALID_PAGE_ID) {
        Page* page = buffer_pool_manager_->FetchPage(current_page_id);
        if (page == nullptr) {
            return false;
        }
        page->WLatch();
        auto* table_page = reinterpret_cast<TablePage*>(page);
        if (table_page->InsertTuple(tuple, rid)) {
            page->SetLSN(0);
            page->WUnlatch();
            buffer_pool_manager_->UnpinPage(current_page_id, true);
            return true;
        }
        page_id_t next_page_id = table_page->GetNextPageId();
        if (next_page_id == INVALID_PAGE_ID) {
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
            new_page->WUnlatch();
            buffer_pool_manager_->UnpinPage(new_page_id, true);
            next_page_id = new_page_id;
        }
        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(current_page_id, true);
        current_page_id = next_page_id;
    }
    return false;
}

bool TablePage::InsertTuple(const Tuple& tuple, RID* rid) {
    size_t tuple_size = tuple.GetSerializedSize();
    size_t slot_size = sizeof(Slot);
    auto* header = GetHeader();

    LOG_DEBUG("TablePage::InsertTuple: page "
              << GetPageId() << " tuple_size=" << tuple_size
              << " current_tuples=" << header->num_tuples
              << " free_space_offset=" << header->free_space_offset);

    // 添加合理性检查
    if (tuple_size == 0 || tuple_size > PAGE_SIZE) {
        LOG_ERROR("TablePage::InsertTuple: invalid tuple size: " << tuple_size);
        return false;
    }

    size_t required_space = tuple_size + slot_size;
    size_t slot_end_offset =
        sizeof(TablePageHeader) + header->num_tuples * sizeof(Slot);

    // 检查free_space_offset的合理性
    if (header->free_space_offset < slot_end_offset ||
        header->free_space_offset > PAGE_SIZE) {
        LOG_ERROR("TablePage::InsertTuple: invalid free_space_offset="
                  << header->free_space_offset
                  << " slot_end_offset=" << slot_end_offset);
        return false;
    }

    size_t free_space = header->free_space_offset - slot_end_offset;
    if (free_space < required_space) {
        LOG_WARN("TablePage::InsertTuple: insufficient space. required="
                 << required_space << " available=" << free_space);
        return false;
    }

    header->free_space_offset -= tuple_size;
    char* tuple_data = GetData() + header->free_space_offset;
    tuple.SerializeTo(tuple_data);

    Slot* slots = reinterpret_cast<Slot*>(GetData() + sizeof(TablePageHeader));
    slots[header->num_tuples].offset = header->free_space_offset;
    slots[header->num_tuples].size = tuple_size;

    rid->page_id = GetPageId();
    rid->slot_num = header->num_tuples;

    LOG_DEBUG("TablePage::InsertTuple: inserted at slot "
              << header->num_tuples << " offset=" << header->free_space_offset
              << " size=" << tuple_size);

    header->num_tuples++;
    return true;
}

bool TableHeap::DeleteTuple(const RID& rid, txn_id_t txn_id) {
    Page* page = buffer_pool_manager_->FetchPage(rid.page_id);
    (void)txn_id;  // Unused parameter

    if (page == nullptr) {
        return false;
    }

    page->WLatch();
    auto* table_page = reinterpret_cast<TablePage*>(page);
    bool result = table_page->DeleteTuple(rid);
    if (result) {
        page->SetLSN(0);
    }
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(rid.page_id, result);
    return result;
}

bool TableHeap::UpdateTuple(const Tuple& tuple, const RID& rid,
                            txn_id_t txn_id) {
    Page* page = buffer_pool_manager_->FetchPage(rid.page_id);
    (void)txn_id;  // Unused parameter

    if (page == nullptr) {
        return false;
    }

    page->WLatch();
    auto* table_page = reinterpret_cast<TablePage*>(page);
    bool result = table_page->UpdateTuple(tuple, rid);
    if (result) {
        page->SetLSN(0);
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