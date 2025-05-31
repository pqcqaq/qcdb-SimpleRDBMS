// src/execution/executor.cpp
#include "execution/executor.h"

#include "catalog/catalog.h"
#include "index/index_manager.h"
#include "catalog/table_manager.h"
#include "common/exception.h"
#include "execution/expression_cloner.h"
#include "execution/expression_evaluator.h"
#include "record/table_heap.h"
#include "record/tuple.h"

namespace SimpleRDBMS {

// SeqScanExecutor implementation
SeqScanExecutor::SeqScanExecutor(ExecutorContext* exec_ctx,
                                 std::unique_ptr<SeqScanPlanNode> plan)
    : Executor(exec_ctx, std::move(plan)), table_info_(nullptr) {}

void SeqScanExecutor::Init() {
    // Get table info from catalog
    auto* seq_scan_plan = GetSeqScanPlan();
    table_info_ =
        exec_ctx_->GetCatalog()->GetTable(seq_scan_plan->GetTableName());
    if (table_info_ == nullptr) {
        throw ExecutionException("Table not found: " +
                                 seq_scan_plan->GetTableName());
    }

    LOG_DEBUG("SeqScanExecutor::Init: table "
              << seq_scan_plan->GetTableName() << " first_page_id="
              << table_info_->table_heap->GetFirstPageId());

    // Initialize table iterator
    table_iterator_ = table_info_->table_heap->Begin();

    LOG_DEBUG("SeqScanExecutor::Init: iterator initialized, IsEnd="
              << table_iterator_.IsEnd());
}

bool SeqScanExecutor::Next(Tuple* tuple, RID* rid) {
    auto* seq_scan_plan = GetSeqScanPlan();
    
    // 创建表达式求值器（如果还没有）
    if (!evaluator_ && seq_scan_plan->GetPredicate()) {
        evaluator_ = std::make_unique<ExpressionEvaluator>(table_info_->schema.get());
    }
    
    while (!table_iterator_.IsEnd()) {
        try {
            LOG_DEBUG("SeqScanExecutor::Next: getting current tuple");
            // Get current tuple
            *tuple = *table_iterator_;
            *rid = tuple->GetRID();
            LOG_DEBUG("SeqScanExecutor::Next: got tuple with RID "
                      << rid->page_id << ":" << rid->slot_num);
            
            // Move to next tuple for next call
            ++table_iterator_;
            LOG_DEBUG("SeqScanExecutor::Next: moved to next, IsEnd="
                      << table_iterator_.IsEnd());
            
            // Apply predicate if exists
            Expression* predicate = seq_scan_plan->GetPredicate();
            if (predicate == nullptr) {
                return true; // 没有WHERE条件，返回所有记录
            }
            
            // 求值WHERE条件
            if (evaluator_->EvaluateAsBoolean(predicate, *tuple)) {
                return true; // 满足WHERE条件
            }
            // 不满足条件，继续下一个tuple
            
        } catch (const std::exception& e) {
            LOG_ERROR("SeqScanExecutor::Next: Exception during scan: " << e.what());
            return false;
        }
    }
    
    LOG_DEBUG("SeqScanExecutor::Next: iterator is at end");
    return false;
}

IndexScanExecutor::IndexScanExecutor(ExecutorContext* exec_ctx,
                                     std::unique_ptr<IndexScanPlanNode> plan)
    : Executor(exec_ctx, std::move(plan)),
      table_info_(nullptr),
      index_info_(nullptr),
      has_found_tuple_(false) {}

void IndexScanExecutor::Init() {
    auto* index_scan_plan = GetIndexScanPlan();
    
    table_info_ = exec_ctx_->GetCatalog()->GetTable(index_scan_plan->GetTableName());
    if (table_info_ == nullptr) {
        throw ExecutionException("Table not found: " + index_scan_plan->GetTableName());
    }
    
    index_info_ = exec_ctx_->GetCatalog()->GetIndex(index_scan_plan->GetIndexName());
    if (index_info_ == nullptr) {
        throw ExecutionException("Index not found: " + index_scan_plan->GetIndexName());
    }
    
    evaluator_ = std::make_unique<ExpressionEvaluator>(table_info_->schema.get());
    has_found_tuple_ = false;
    
    // 从WHERE条件中提取搜索键值
    ExtractSearchKey(index_scan_plan->GetPredicate());
}

bool IndexScanExecutor::Next(Tuple* tuple, RID* rid) {
    if (has_found_tuple_) {
        return false; // 索引查找只返回一个结果
    }
    
    auto* index_scan_plan = GetIndexScanPlan();
    TableManager* table_manager = exec_ctx_->GetTableManager();
    IndexManager* index_manager = table_manager->GetIndexManager();
    
    RID found_rid;
    bool found = index_manager->FindEntry(index_scan_plan->GetIndexName(), search_key_, &found_rid);
    
    if (found) {
        // 通过RID获取完整的tuple
        bool success = table_info_->table_heap->GetTuple(found_rid, tuple, exec_ctx_->GetTransaction()->GetTxnId());
        if (success) {
            *rid = found_rid;
            has_found_tuple_ = true;
            return true;
        }
    }
    
    return false;
}

void IndexScanExecutor::ExtractSearchKey(Expression* predicate) {
    // 这里需要实现从WHERE条件中提取搜索键的逻辑
    // 简化版本：假设是 column = value 的形式
    if (auto* binary_expr = dynamic_cast<BinaryOpExpression*>(predicate)) {
        if (binary_expr->GetOperator() == BinaryOpExpression::OpType::EQUALS) {
            if (auto* const_expr = dynamic_cast<ConstantExpression*>(binary_expr->GetRight())) {
                search_key_ = const_expr->GetValue();
            }
        }
    }
}

// InsertExecutor implementation
InsertExecutor::InsertExecutor(ExecutorContext* exec_ctx,
                               std::unique_ptr<InsertPlanNode> plan)
    : Executor(exec_ctx, std::move(plan)),
      table_info_(nullptr),
      current_index_(0) {}

void InsertExecutor::Init() {
    // Get table info from catalog
    auto* insert_plan = GetInsertPlan();
    table_info_ =
        exec_ctx_->GetCatalog()->GetTable(insert_plan->GetTableName());

    if (table_info_ == nullptr) {
        throw ExecutionException("Table not found: " +
                                 insert_plan->GetTableName());
    }

    current_index_ = 0;
}

bool InsertExecutor::Next(Tuple* tuple, RID* rid) {
    auto* insert_plan = GetInsertPlan();
    const auto& values_list = insert_plan->GetValues();
    if (current_index_ >= values_list.size()) {
        return false;
    }

    const auto& values = values_list[current_index_];
    Tuple insert_tuple(values, table_info_->schema.get());
    bool success = table_info_->table_heap->InsertTuple(
        insert_tuple, rid, exec_ctx_->GetTransaction()->GetTxnId());
    if (!success) {
        throw ExecutionException("Failed to insert tuple");
    }

    // Update indexes after successful insertion
    TableManager* table_manager = exec_ctx_->GetTableManager();
    if (table_manager) {
        bool index_success = table_manager->UpdateIndexesOnInsert(
            table_info_->table_name, insert_tuple, *rid);
        if (!index_success) {
            LOG_WARN("Failed to update indexes for insert operation");
        }
    }

    current_index_++;
    *tuple = Tuple();
    return true;
}

// UpdateExecutor implementation
UpdateExecutor::UpdateExecutor(ExecutorContext* exec_ctx,
                               std::unique_ptr<UpdatePlanNode> plan)
    : Executor(exec_ctx, std::move(plan)),
      table_info_(nullptr),
      current_index_(0),
      is_executed_(false) {}

void UpdateExecutor::Init() {
    // Get table info from catalog
    auto* update_plan = GetUpdatePlan();
    table_info_ =
        exec_ctx_->GetCatalog()->GetTable(update_plan->GetTableName());
    if (table_info_ == nullptr) {
        throw ExecutionException("Table not found: " +
                                 update_plan->GetTableName());
    }
    // 初始化表达式求值器 - 使用表的schema而不是输出schema
    evaluator_ =
        std::make_unique<ExpressionEvaluator>(table_info_->schema.get());
    // 扫描表找出所有需要更新的记录
    target_rids_.clear();
    current_index_ = 0;
    is_executed_ = false;
    auto iter = table_info_->table_heap->Begin();
    while (!iter.IsEnd()) {
        Tuple tuple = *iter;
        // 检查是否满足WHERE条件
        Expression* predicate = update_plan->GetPredicate();
        if (predicate == nullptr ||
            evaluator_->EvaluateAsBoolean(predicate, tuple)) {
            target_rids_.push_back(tuple.GetRID());
        }
        ++iter;
    }
}

bool UpdateExecutor::Next(Tuple* tuple, RID* rid) {
    if (is_executed_) {
        return false;
    }
    auto* update_plan = GetUpdatePlan();
    int updated_count = 0;
    TableManager* table_manager = exec_ctx_->GetTableManager();
    
    for (const RID& target_rid : target_rids_) {
        Tuple old_tuple;
        if (!table_info_->table_heap->GetTuple(
                target_rid, &old_tuple,
                exec_ctx_->GetTransaction()->GetTxnId())) {
            continue;
        }
        std::vector<Value> new_values;
        const auto& columns = table_info_->schema->GetColumns();
        for (size_t i = 0; i < columns.size(); ++i) {
            const std::string& column_name = columns[i].name;
            bool found_update = false;
            for (const auto& update_pair : update_plan->GetUpdates()) {
                if (update_pair.first == column_name) {
                    Value new_value = evaluator_->Evaluate(
                        update_pair.second.get(), old_tuple);
                    new_values.push_back(new_value);
                    found_update = true;
                    break;
                }
            }
            if (!found_update) {
                new_values.push_back(old_tuple.GetValue(i));
            }
        }
        Tuple new_tuple(new_values, table_info_->schema.get());
        if (table_info_->table_heap->UpdateTuple(
                new_tuple, target_rid,
                exec_ctx_->GetTransaction()->GetTxnId())) {
            
            // 修复：正确调用 TableManager 来更新索引
            if (table_manager) {
                bool index_success = table_manager->UpdateIndexesOnUpdate(
                    table_info_->table_name, old_tuple, new_tuple, target_rid);
                if (!index_success) {
                    LOG_WARN("Failed to update indexes for update operation");
                }
            }
            updated_count++;
        }
    }
    is_executed_ = true;
    std::vector<Value> result_values = {Value(updated_count)};
    *tuple = Tuple(result_values, GetOutputSchema());
    *rid = RID{INVALID_PAGE_ID, -1};
    return true;
}

DeleteExecutor::DeleteExecutor(ExecutorContext* exec_ctx,
                               std::unique_ptr<DeletePlanNode> plan)
    : Executor(exec_ctx, std::move(plan)),
      table_info_(nullptr),
      current_index_(0),
      is_executed_(false) {}

void DeleteExecutor::Init() {
    // Get table info from catalog
    auto* delete_plan = GetDeletePlan();
    table_info_ =
        exec_ctx_->GetCatalog()->GetTable(delete_plan->GetTableName());
    if (table_info_ == nullptr) {
        throw ExecutionException("Table not found: " +
                                 delete_plan->GetTableName());
    }
    // 初始化表达式求值器 - 使用表的schema
    evaluator_ =
        std::make_unique<ExpressionEvaluator>(table_info_->schema.get());
    // 扫描表找出所有需要删除的记录
    target_rids_.clear();
    current_index_ = 0;
    is_executed_ = false;
    auto iter = table_info_->table_heap->Begin();
    while (!iter.IsEnd()) {
        Tuple tuple = *iter;
        // 检查是否满足WHERE条件
        Expression* predicate = delete_plan->GetPredicate();
        if (predicate == nullptr ||
            evaluator_->EvaluateAsBoolean(predicate, tuple)) {
            target_rids_.push_back(tuple.GetRID());
        }
        ++iter;
    }
}

bool DeleteExecutor::Next(Tuple* tuple, RID* rid) {
    if (is_executed_) {
        return false;
    }
    int deleted_count = 0;
    TableManager* table_manager = exec_ctx_->GetTableManager();
    
    for (const RID& target_rid : target_rids_) {
        Tuple tuple_to_delete;
        bool got_tuple = table_info_->table_heap->GetTuple(
            target_rid, &tuple_to_delete,
            exec_ctx_->GetTransaction()->GetTxnId());
        if (table_info_->table_heap->DeleteTuple(
                target_rid, exec_ctx_->GetTransaction()->GetTxnId())) {
            if (got_tuple && table_manager) {
                // 修复：正确调用 TableManager 来更新索引
                bool index_success = table_manager->UpdateIndexesOnDelete(
                    table_info_->table_name, tuple_to_delete);
                if (!index_success) {
                    LOG_WARN("Failed to update indexes for delete operation");
                }
            }
            deleted_count++;
        }
    }
    is_executed_ = true;
    std::vector<Value> result_values = {Value(deleted_count)};
    *tuple = Tuple(result_values, GetOutputSchema());
    *rid = RID{INVALID_PAGE_ID, -1};
    return true;
}

// ProjectionExecutor implementation
ProjectionExecutor::ProjectionExecutor(ExecutorContext* exec_ctx,
                                       std::unique_ptr<ProjectionPlanNode> plan)
    : Executor(exec_ctx, std::move(plan)) {}

void ProjectionExecutor::Init() {
    auto* projection_plan = GetProjectionPlan();
    const auto* child_plan = projection_plan->GetChild(0);
    if (!child_plan) {
        throw ExecutionException("ProjectionExecutor: No child plan");
    }

    // 不要转移子计划的所有权，而是创建一个新的executor
    // 这里需要根据子计划类型创建相应的executor，但不转移plan的所有权
    if (child_plan->GetType() == PlanNodeType::SEQUENTIAL_SCAN) {
        // 创建一个新的SeqScanPlanNode，复制必要的信息
        auto* seq_scan_plan = static_cast<const SeqScanPlanNode*>(child_plan);
        auto new_seq_scan_plan = std::make_unique<SeqScanPlanNode>(
            seq_scan_plan->GetOutputSchema(),
            seq_scan_plan->GetTableName(),
            nullptr  // 暂时不处理谓词
        );
        child_executor_ = std::make_unique<SeqScanExecutor>(
            exec_ctx_, std::move(new_seq_scan_plan));
    } else {
        throw ExecutionException(
            "ProjectionExecutor: Unsupported child plan type");
    }

    child_executor_->Init();
    
    // 创建表达式求值器，使用子执行器的输出schema
    evaluator_ = std::make_unique<ExpressionEvaluator>(
        child_executor_->GetOutputSchema());
}

bool ProjectionExecutor::Next(Tuple* tuple, RID* rid) {
    auto* projection_plan = GetProjectionPlan();

    // 从子执行器获取下一个tuple
    Tuple child_tuple;
    RID child_rid;
    if (!child_executor_->Next(&child_tuple, &child_rid)) {
        return false;
    }

    // 计算投影表达式
    std::vector<Value> projected_values;
    const auto& expressions = projection_plan->GetExpressions();

    for (const auto& expr : expressions) {
        Value value = evaluator_->Evaluate(expr.get(), child_tuple);
        projected_values.push_back(value);
    }

    // 创建投影后的tuple
    *tuple = Tuple(projected_values, GetOutputSchema());
    *rid = child_rid;

    return true;
}

}  // namespace SimpleRDBMS