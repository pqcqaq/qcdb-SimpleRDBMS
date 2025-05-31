// src/execution/executor.cpp
#include "execution/executor.h"
#include "catalog/catalog.h"
#include "record/table_heap.h"
#include "common/exception.h"

namespace SimpleRDBMS {

// SeqScanExecutor implementation
SeqScanExecutor::SeqScanExecutor(ExecutorContext* exec_ctx, std::unique_ptr<SeqScanPlanNode> plan)
    : Executor(exec_ctx, std::move(plan)), table_info_(nullptr) {
}

void SeqScanExecutor::Init() {
    // Get table info from catalog
    auto* seq_scan_plan = GetSeqScanPlan();
    table_info_ = exec_ctx_->GetCatalog()->GetTable(seq_scan_plan->GetTableName());
    
    if (table_info_ == nullptr) {
        throw ExecutionException("Table not found: " + seq_scan_plan->GetTableName());
    }
    
    // Initialize table iterator
    table_iterator_ = table_info_->table_heap->Begin();
}

bool SeqScanExecutor::Next(Tuple* tuple, RID* rid) {
    auto* seq_scan_plan = GetSeqScanPlan();
    
    while (!table_iterator_.IsEnd()) {
        // Get current tuple
        *tuple = *table_iterator_;
        *rid = tuple->GetRID();
        
        // Move to next tuple for next call
        ++table_iterator_;
        
        // Apply predicate if exists
        Expression* predicate = seq_scan_plan->GetPredicate();
        if (predicate == nullptr) {
            // No predicate, return the tuple
            return true;
        }
        
        // TODO: Evaluate predicate
        // For now, we'll return all tuples
        return true;
    }
    
    return false;
}

// InsertExecutor implementation
InsertExecutor::InsertExecutor(ExecutorContext* exec_ctx, std::unique_ptr<InsertPlanNode> plan)
    : Executor(exec_ctx, std::move(plan)), table_info_(nullptr), current_index_(0) {
}

void InsertExecutor::Init() {
    // Get table info from catalog
    auto* insert_plan = GetInsertPlan();
    table_info_ = exec_ctx_->GetCatalog()->GetTable(insert_plan->GetTableName());
    
    if (table_info_ == nullptr) {
        throw ExecutionException("Table not found: " + insert_plan->GetTableName());
    }
    
    current_index_ = 0;
}

bool InsertExecutor::Next(Tuple* tuple, RID* rid) {
    auto* insert_plan = GetInsertPlan();
    const auto& values_list = insert_plan->GetValues();
    
    if (current_index_ >= values_list.size()) {
        return false;
    }
    
    // Create tuple from values
    const auto& values = values_list[current_index_];
    Tuple insert_tuple(values, table_info_->schema.get());
    
    // Insert into table
    bool success = table_info_->table_heap->InsertTuple(
        insert_tuple, rid, exec_ctx_->GetTransaction()->GetTxnId()
    );
    
    if (!success) {
        throw ExecutionException("Failed to insert tuple");
    }
    
    // Update indexes
    auto* catalog = exec_ctx_->GetCatalog();
    auto indexes = catalog->GetTableIndexes(table_info_->table_name);
    
    for (auto* index_info : indexes) {
        // Extract key values based on index columns
        std::vector<Value> key_values;
        for (const auto& col_name : index_info->key_columns) {
            size_t col_idx = table_info_->schema->GetColumnIdx(col_name);
            key_values.push_back(values[col_idx]);
        }
        
        // For single column index
        if (key_values.size() == 1) {
            // TODO: Insert into B+ tree index
            // This requires index manager integration
            // index_manager->InsertEntry(index_info->index_name, key_values[0], *rid);
        }
    }
    
    current_index_++;
    
    // For INSERT, we typically don't return the inserted tuple
    // Just return a dummy tuple indicating success
    *tuple = Tuple();
    
    return true;
}

}  // namespace SimpleRDBMS