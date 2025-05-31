// src/execution/executor.cpp
#include "execution/executor.h"
#include "catalog/catalog.h"
#include "record/table_heap.h"
#include "common/exception.h"
#include "execution/expression_evaluator.h"
#include "execution/plan_node.h"

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

// UpdateExecutor implementation
UpdateExecutor::UpdateExecutor(ExecutorContext* exec_ctx, std::unique_ptr<UpdatePlanNode> plan)
    : Executor(exec_ctx, std::move(plan)), table_info_(nullptr), current_index_(0), is_executed_(false) {
}

void UpdateExecutor::Init() {
    // Get table info from catalog
    auto* update_plan = GetUpdatePlan();
    table_info_ = exec_ctx_->GetCatalog()->GetTable(update_plan->GetTableName());
    
    if (table_info_ == nullptr) {
        throw ExecutionException("Table not found: " + update_plan->GetTableName());
    }
    
    // 初始化表达式求值器
    evaluator_ = std::make_unique<ExpressionEvaluator>(table_info_->schema.get());
    
    // 扫描表找出所有需要更新的记录
    target_rids_.clear();
    current_index_ = 0;
    is_executed_ = false;
    
    auto iter = table_info_->table_heap->Begin();
    while (!iter.IsEnd()) {
        Tuple tuple = *iter;
        
        // 检查是否满足WHERE条件
        Expression* predicate = update_plan->GetPredicate();
        if (predicate == nullptr || evaluator_->EvaluateAsBoolean(predicate, tuple)) {
            target_rids_.push_back(tuple.GetRID());
        }
        
        ++iter;
    }
}

bool UpdateExecutor::Next(Tuple* tuple, RID* rid) {
    if (is_executed_) {
        return false;  // UPDATE只执行一次
    }
    
    auto* update_plan = GetUpdatePlan();
    int updated_count = 0;
    
    // 对每个目标记录执行更新
    for (const RID& target_rid : target_rids_) {
        // 获取原始记录
        Tuple old_tuple;
        if (!table_info_->table_heap->GetTuple(target_rid, &old_tuple, exec_ctx_->GetTransaction()->GetTxnId())) {
            continue;  // 记录可能已被删除
        }
        
        // 创建新记录的值数组
        std::vector<Value> new_values;
        const auto& columns = table_info_->schema->GetColumns();
        
        for (size_t i = 0; i < columns.size(); ++i) {
            const std::string& column_name = columns[i].name;
            
            // 检查是否有对这个列的更新
            bool found_update = false;
            for (const auto& update_pair : update_plan->GetUpdates()) {
                if (update_pair.first == column_name) {
                    // 求值新的值
                    Value new_value = evaluator_->Evaluate(update_pair.second.get(), old_tuple);
                    new_values.push_back(new_value);
                    found_update = true;
                    break;
                }
            }
            
            if (!found_update) {
                // 保持原值
                new_values.push_back(old_tuple.GetValue(i));
            }
        }
        
        // 创建新记录
        Tuple new_tuple(new_values, table_info_->schema.get());
        
        // 执行更新
        if (table_info_->table_heap->UpdateTuple(new_tuple, target_rid, exec_ctx_->GetTransaction()->GetTxnId())) {
            updated_count++;
            
            // TODO: 更新索引
            // 这里需要根据具体的索引实现来更新相关索引
        }
    }
    
    is_executed_ = true;
    
    // 返回一个表示更新结果的记录（包含更新的记录数）
    std::vector<Value> result_values = {Value(updated_count)};
    *tuple = Tuple(result_values, GetOutputSchema());
    *rid = RID{INVALID_PAGE_ID, -1};  // 虚拟RID
    
    return true;
}

DeleteExecutor::DeleteExecutor(ExecutorContext* exec_ctx, std::unique_ptr<DeletePlanNode> plan)
    : Executor(exec_ctx, std::move(plan)), table_info_(nullptr), current_index_(0), is_executed_(false) {
}

void DeleteExecutor::Init() {
    // Get table info from catalog
    auto* delete_plan = GetDeletePlan();
    table_info_ = exec_ctx_->GetCatalog()->GetTable(delete_plan->GetTableName());
    
    if (table_info_ == nullptr) {
        throw ExecutionException("Table not found: " + delete_plan->GetTableName());
    }
    
    // 初始化表达式求值器
    evaluator_ = std::make_unique<ExpressionEvaluator>(table_info_->schema.get());
    
    // 扫描表找出所有需要删除的记录
    target_rids_.clear();
    current_index_ = 0;
    is_executed_ = false;
    
    auto iter = table_info_->table_heap->Begin();
    while (!iter.IsEnd()) {
        Tuple tuple = *iter;
        
        // 检查是否满足WHERE条件
        Expression* predicate = delete_plan->GetPredicate();
        if (predicate == nullptr || evaluator_->EvaluateAsBoolean(predicate, tuple)) {
            target_rids_.push_back(tuple.GetRID());
        }
        
        ++iter;
    }
}

bool DeleteExecutor::Next(Tuple* tuple, RID* rid) {
    if (is_executed_) {
        return false;  // DELETE只执行一次
    }
    
    int deleted_count = 0;
    
    // 对每个目标记录执行删除
    for (const RID& target_rid : target_rids_) {
        if (table_info_->table_heap->DeleteTuple(target_rid, exec_ctx_->GetTransaction()->GetTxnId())) {
            deleted_count++;
            
            // TODO: 更新索引
            // 这里需要根据具体的索引实现来删除相关索引项
        }
    }
    
    is_executed_ = true;
    
    // 返回一个表示删除结果的记录（包含删除的记录数）
    std::vector<Value> result_values = {Value(deleted_count)};
    *tuple = Tuple(result_values, GetOutputSchema());
    *rid = RID{INVALID_PAGE_ID, -1};  // 虚拟RID
    
    return true;
}

}  // namespace SimpleRDBMS