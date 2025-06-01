#pragma once

#include <memory>

#include "buffer/buffer_pool_manager.h"
#include "catalog/schema.h"
#include "record/tuple.h"

namespace SimpleRDBMS {

// Page layout for heap file
class TablePage : public Page {
   public:
    void Init(page_id_t page_id, page_id_t prev_page_id);

    // Insert a tuple
    bool InsertTuple(const Tuple& tuple, RID* rid);

    // Delete a tuple
    bool DeleteTuple(const RID& rid);

    // Update a tuple
    bool UpdateTuple(const Tuple& tuple, const RID& rid);

    // Get a tuple
    bool GetTuple(const RID& rid, Tuple* tuple, const Schema* schema);

    // Get next tuple RID
    bool GetNextTupleRID(const RID& current_rid, RID* next_rid);

    // Get metadata
    page_id_t GetNextPageId() const;
    void SetNextPageId(page_id_t next_page_id);
    // Page header
    struct TablePageHeader {
        page_id_t next_page_id;
        lsn_t lsn;
        uint16_t num_tuples;
        uint16_t free_space_offset;
    };

   private:
    TablePageHeader* GetHeader();
    const TablePageHeader* GetHeader() const;
};

class TableHeap {
   public:
    // 原有构造函数（用于创建新表）
    TableHeap(BufferPoolManager* buffer_pool_manager, const Schema* schema);

    // 新增构造函数（用于从已存在的页面恢复）
    TableHeap(BufferPoolManager* buffer_pool_manager, const Schema* schema,
              page_id_t first_page_id);

    ~TableHeap();

    // Insert a tuple
    bool InsertTuple(const Tuple& tuple, RID* rid, txn_id_t txn_id);
    // Delete a tuple
    bool DeleteTuple(const RID& rid, txn_id_t txn_id);
    // Update a tuple
    bool UpdateTuple(const Tuple& tuple, const RID& rid, txn_id_t txn_id);
    // Get a tuple
    bool GetTuple(const RID& rid, Tuple* tuple, txn_id_t txn_id);

    page_id_t GetFirstPageId() const { return first_page_id_; }

    // Iterator for sequential scan
    class Iterator {
       public:
        Iterator(TableHeap* table_heap, const RID& rid);
        // 默认构造函数
        Iterator()
            : table_heap_(nullptr), current_rid_({INVALID_PAGE_ID, -1}) {}

        bool IsEnd() const;
        void operator++();
        Tuple operator*();

       private:
        TableHeap* table_heap_;
        RID current_rid_;
    };

    Iterator Begin();
    Iterator End();

   private:
    BufferPoolManager* buffer_pool_manager_;
    const Schema* schema_;
    page_id_t first_page_id_;
};

}  // namespace SimpleRDBMS
