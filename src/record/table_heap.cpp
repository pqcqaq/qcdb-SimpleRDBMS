#include "record/table_heap.h"
#include <algorithm>
#include <cstring>
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
    
    auto* header = GetHeader();
    header->next_page_id = INVALID_PAGE_ID;
    header->lsn = INVALID_LSN;
    header->num_tuples = 0;
    header->free_space_offset = PAGE_SIZE;
}

bool TablePage::InsertTuple(const Tuple& tuple, RID* rid) {
    size_t tuple_size = tuple.GetSerializedSize();
    size_t slot_size = sizeof(Slot);
    
    auto* header = GetHeader();
    size_t required_space = tuple_size + slot_size;
    
    size_t slot_end_offset = sizeof(TablePageHeader) + header->num_tuples * sizeof(Slot);
    size_t free_space = header->free_space_offset - slot_end_offset;
    
    if (free_space < required_space) {
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
    
    header->num_tuples++;
    
    return true;
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
        size_t slot_end_offset = sizeof(TablePageHeader) + header->num_tuples * sizeof(Slot);
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
    Slot* slots = reinterpret_cast<Slot*>(GetData() + sizeof(TablePageHeader));
    
    for (int i = current_rid.slot_num + 1; i < header->num_tuples; i++) {
        if (slots[i].size != 0) {
            next_rid->page_id = GetPageId();
            next_rid->slot_num = i;
            return true;
        }
    }
    
    return false;
}

page_id_t TablePage::GetNextPageId() const {
    return GetHeader()->next_page_id;
}

void TablePage::SetNextPageId(page_id_t next_page_id) {
    GetHeader()->next_page_id = next_page_id;
}

TablePage::TablePageHeader* TablePage::GetHeader() {
    return reinterpret_cast<TablePageHeader*>(GetData());
}

const TablePage::TablePageHeader* TablePage::GetHeader() const {
    return reinterpret_cast<const TablePageHeader*>(GetData());
}

TableHeap::TableHeap(BufferPoolManager* buffer_pool_manager, const Schema* schema)
    : buffer_pool_manager_(buffer_pool_manager), 
      schema_(schema), 
      first_page_id_(INVALID_PAGE_ID) {  // 初始化 first_page_id_
    
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

TableHeap::~TableHeap() = default;

bool TableHeap::InsertTuple(const Tuple& tuple, RID* rid, txn_id_t txn_id) {
    page_id_t current_page_id = first_page_id_;
    (void) txn_id; // Unused parameter, can be used for logging or future enhancements
    
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

bool TableHeap::DeleteTuple(const RID& rid, txn_id_t txn_id) {
    Page* page = buffer_pool_manager_->FetchPage(rid.page_id);
    (void) txn_id; // Unused parameter, can be used for logging or future enhancements
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

bool TableHeap::UpdateTuple(const Tuple& tuple, const RID& rid, txn_id_t txn_id) {
    Page* page = buffer_pool_manager_->FetchPage(rid.page_id);
    (void) txn_id; // Unused parameter, can be used for logging or future enhancements
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
    (void) txn_id; // Unused parameter, can be used for logging or future enhancements
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
    Page* page = table_heap_->buffer_pool_manager_->FetchPage(current_rid_.page_id);
    if (page == nullptr) {
        current_rid_.page_id = INVALID_PAGE_ID;
        return;
    }
    
    page->RLatch();
    auto* table_page = reinterpret_cast<TablePage*>(page);
    
    RID next_rid;
    if (table_page->GetNextTupleRID(current_rid_, &next_rid)) {
        current_rid_ = next_rid;
        page->RUnlatch();
        table_heap_->buffer_pool_manager_->UnpinPage(current_rid_.page_id, false);
        return;
    }
    
    page_id_t next_page_id = table_page->GetNextPageId();
    page->RUnlatch();
    table_heap_->buffer_pool_manager_->UnpinPage(current_rid_.page_id, false);
    
    while (next_page_id != INVALID_PAGE_ID) {
        page = table_heap_->buffer_pool_manager_->FetchPage(next_page_id);
        if (page == nullptr) {
            current_rid_.page_id = INVALID_PAGE_ID;
            return;
        }
        
        page->RLatch();
        table_page = reinterpret_cast<TablePage*>(page);
        
        RID first_rid{next_page_id, -1};
        if (table_page->GetNextTupleRID(first_rid, &next_rid)) {
            current_rid_ = next_rid;
            page->RUnlatch();
            table_heap_->buffer_pool_manager_->UnpinPage(next_page_id, false);
            return;
        }
        
        page_id_t temp = table_page->GetNextPageId();
        page->RUnlatch();
        table_heap_->buffer_pool_manager_->UnpinPage(next_page_id, false);
        next_page_id = temp;
    }
    
    current_rid_.page_id = INVALID_PAGE_ID;
}

Tuple TableHeap::Iterator::operator*() {
    Tuple tuple;
    table_heap_->GetTuple(current_rid_, &tuple, INVALID_TXN_ID);
    return tuple;
}

TableHeap::Iterator TableHeap::Begin() {
    RID first_rid{first_page_id_, -1};
    Iterator iter(this, first_rid);
    ++iter;
    return iter;
}

TableHeap::Iterator TableHeap::End() {
    return Iterator(this, RID{INVALID_PAGE_ID, -1});
}

}  // namespace SimpleRDBMS