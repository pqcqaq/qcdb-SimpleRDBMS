/*
 * 文件: executor.cpp
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述: 查询执行器实现，包含各种SQL操作的具体执行逻辑，
 *       支持顺序扫描、索引扫描、插入、更新、删除和投影操作
 */

#include "execution/executor.h"

#include "catalog/catalog.h"
#include "catalog/table_manager.h"
#include "common/exception.h"
#include "execution/expression_cloner.h"
#include "execution/expression_evaluator.h"
#include "index/index_manager.h"
#include "record/table_heap.h"
#include "record/tuple.h"

namespace SimpleRDBMS {

/**
 * 顺序扫描执行器构造函数
 * 用于全表扫描，支持WHERE条件过滤
 */
SeqScanExecutor::SeqScanExecutor(ExecutorContext* exec_ctx,
                                 std::unique_ptr<SeqScanPlanNode> plan)
    : Executor(exec_ctx, std::move(plan)), table_info_(nullptr) {}

/**
 * 初始化顺序扫描执行器
 * 主要工作：获取表信息，初始化表迭代器
 */
void SeqScanExecutor::Init() {
    // 从catalog中获取表的元信息
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

    // 初始化表的迭代器，从第一条记录开始
    table_iterator_ = table_info_->table_heap->Begin();

    LOG_DEBUG("SeqScanExecutor::Init: iterator initialized, IsEnd="
              << table_iterator_.IsEnd());
}

/**
 * 获取下一个满足条件的tuple
 * 核心逻辑：遍历所有记录，应用WHERE条件过滤
 * @param tuple 输出参数，存储找到的记录
 * @param rid 输出参数，存储记录的RID
 * @return 是否找到满足条件的记录
 */
bool SeqScanExecutor::Next(Tuple* tuple, RID* rid) {
    auto* seq_scan_plan = GetSeqScanPlan();

    // 懒加载：在第一次需要时创建表达式求值器
    if (!evaluator_ && seq_scan_plan->GetPredicate()) {
        evaluator_ =
            std::make_unique<ExpressionEvaluator>(table_info_->schema.get());
    }

    // 循环遍历表中的每一条记录
    while (!table_iterator_.IsEnd()) {
        try {
            LOG_DEBUG("SeqScanExecutor::Next: getting current tuple");

            // 获取当前迭代器指向的记录
            *tuple = *table_iterator_;
            *rid = tuple->GetRID();
            LOG_DEBUG("SeqScanExecutor::Next: got tuple with RID "
                      << rid->page_id << ":" << rid->slot_num);

            // 移动到下一条记录，为下次调用做准备
            ++table_iterator_;
            LOG_DEBUG("SeqScanExecutor::Next: moved to next, IsEnd="
                      << table_iterator_.IsEnd());

            // 应用WHERE条件过滤
            Expression* predicate = seq_scan_plan->GetPredicate();
            if (predicate == nullptr) {
                return true;  // 没有WHERE条件，返回所有记录
            }

            // 使用表达式求值器计算WHERE条件
            if (evaluator_->EvaluateAsBoolean(predicate, *tuple)) {
                return true;  // 满足WHERE条件，返回这条记录
            }
            // 不满足条件，继续下一条记录

        } catch (const std::exception& e) {
            LOG_ERROR(
                "SeqScanExecutor::Next: Exception during scan: " << e.what());
            return false;
        }
    }

    LOG_DEBUG("SeqScanExecutor::Next: iterator is at end");
    return false;  // 遍历完所有记录，没有更多数据
}

/**
 * 索引扫描执行器构造函数
 * 用于基于索引的高效查找
 */
IndexScanExecutor::IndexScanExecutor(ExecutorContext* exec_ctx,
                                     std::unique_ptr<IndexScanPlanNode> plan)
    : Executor(exec_ctx, std::move(plan)),
      table_info_(nullptr),
      index_info_(nullptr),
      has_found_tuple_(false) {}

/**
 * 初始化索引扫描执行器
 * 主要工作：获取表和索引信息，从WHERE条件中提取搜索键
 */
void IndexScanExecutor::Init() {
    auto* index_scan_plan = GetIndexScanPlan();

    // 获取表信息
    table_info_ =
        exec_ctx_->GetCatalog()->GetTable(index_scan_plan->GetTableName());
    if (table_info_ == nullptr) {
        throw ExecutionException("Table not found: " +
                                 index_scan_plan->GetTableName());
    }

    // 获取索引信息
    index_info_ =
        exec_ctx_->GetCatalog()->GetIndex(index_scan_plan->GetIndexName());
    if (index_info_ == nullptr) {
        throw ExecutionException("Index not found: " +
                                 index_scan_plan->GetIndexName());
    }

    // 创建表达式求值器
    evaluator_ =
        std::make_unique<ExpressionEvaluator>(table_info_->schema.get());
    has_found_tuple_ = false;

    // 从WHERE条件中提取用于索引查找的键值
    ExtractSearchKey(index_scan_plan->GetPredicate());
}

/**
 * 通过索引查找记录
 * 索引扫描通常只返回一个或少数几个结果（点查询）
 * @param tuple 输出参数，存储找到的记录
 * @param rid 输出参数，存储记录的RID
 * @return 是否找到记录
 */
bool IndexScanExecutor::Next(Tuple* tuple, RID* rid) {
    if (has_found_tuple_) {
        return false;  // 已经返回过结果，索引点查询只返回一次
    }

    auto* index_scan_plan = GetIndexScanPlan();
    TableManager* table_manager = exec_ctx_->GetTableManager();
    IndexManager* index_manager = table_manager->GetIndexManager();

    // 使用索引查找记录的RID
    RID found_rid;
    bool found = index_manager->FindEntry(index_scan_plan->GetIndexName(),
                                          search_key_, &found_rid);

    if (found) {
        // 通过RID从表堆中获取完整的tuple
        bool success = table_info_->table_heap->GetTuple(
            found_rid, tuple, exec_ctx_->GetTransaction()->GetTxnId());
        if (success) {
            *rid = found_rid;
            has_found_tuple_ = true;
            return true;
        }
    }

    return false;  // 没有找到匹配的记录
}

/**
 * 从WHERE条件中提取搜索键
 * 解析等值条件（如 column = value），提取用于索引查找的键值
 * @param predicate WHERE条件表达式
 */
void IndexScanExecutor::ExtractSearchKey(Expression* predicate) {
    // 处理二元操作表达式，目前支持等值查询
    if (auto* binary_expr = dynamic_cast<BinaryOpExpression*>(predicate)) {
        if (binary_expr->GetOperator() == BinaryOpExpression::OpType::EQUALS) {
            // 提取常量值作为搜索键
            if (auto* const_expr = dynamic_cast<ConstantExpression*>(
                    binary_expr->GetRight())) {
                search_key_ = const_expr->GetValue();
            }
        }
    }
}

/**
 * 插入执行器构造函数
 * 用于向表中插入新记录
 */
InsertExecutor::InsertExecutor(ExecutorContext* exec_ctx,
                               std::unique_ptr<InsertPlanNode> plan)
    : Executor(exec_ctx, std::move(plan)),
      table_info_(nullptr),
      current_index_(0) {}

/**
 * 初始化插入执行器
 * 主要工作：获取表信息，准备插入操作
 */
void InsertExecutor::Init() {
    // 从catalog中获取目标表的信息
    auto* insert_plan = GetInsertPlan();
    table_info_ =
        exec_ctx_->GetCatalog()->GetTable(insert_plan->GetTableName());

    if (table_info_ == nullptr) {
        throw ExecutionException("Table not found: " +
                                 insert_plan->GetTableName());
    }

    current_index_ = 0;  // 重置当前处理的记录索引
}

/**
 * 执行插入操作
 * 支持批量插入，每次调用插入一条记录
 * @param tuple 输出参数（插入操作不需要返回具体数据）
 * @param rid 输出参数，存储插入记录的RID
 * @return 是否还有更多记录需要插入
 */
bool InsertExecutor::Next(Tuple* tuple, RID* rid) {
    auto* insert_plan = GetInsertPlan();
    const auto& values_list = insert_plan->GetValues();

    // 检查是否已经处理完所有待插入的记录
    if (current_index_ >= values_list.size()) {
        return false;
    }

    // 获取当前要插入的记录值
    const auto& values = values_list[current_index_];
    Tuple insert_tuple(values, table_info_->schema.get());

    // 向表堆中插入记录
    bool success = table_info_->table_heap->InsertTuple(
        insert_tuple, rid, exec_ctx_->GetTransaction()->GetTxnId());
    if (!success) {
        throw ExecutionException("Failed to insert tuple");
    }

    // 插入成功后，更新相关的索引
    TableManager* table_manager = exec_ctx_->GetTableManager();
    if (table_manager) {
        bool index_success = table_manager->UpdateIndexesOnInsert(
            table_info_->table_name, insert_tuple, *rid);
        if (!index_success) {
            LOG_WARN("Failed to update indexes for insert operation");
        }
    }

    current_index_++;  // 移动到下一条待插入的记录
    *tuple = Tuple();  // 插入操作不返回具体数据
    return true;
}

/**
 * 更新执行器构造函数
 * 用于更新表中满足条件的记录
 */
UpdateExecutor::UpdateExecutor(ExecutorContext* exec_ctx,
                               std::unique_ptr<UpdatePlanNode> plan)
    : Executor(exec_ctx, std::move(plan)),
      table_info_(nullptr),
      current_index_(0),
      is_executed_(false) {}

/**
 * 初始化更新执行器
 * 主要工作：扫描表找出所有需要更新的记录RID
 * 采用两阶段策略：先找出所有目标记录，再批量更新
 */
void UpdateExecutor::Init() {
    // 获取表信息
    auto* update_plan = GetUpdatePlan();
    table_info_ =
        exec_ctx_->GetCatalog()->GetTable(update_plan->GetTableName());
    if (table_info_ == nullptr) {
        throw ExecutionException("Table not found: " +
                                 update_plan->GetTableName());
    }

    // 创建表达式求值器，用于计算WHERE条件和SET子句
    evaluator_ =
        std::make_unique<ExpressionEvaluator>(table_info_->schema.get());

    // 第一阶段：扫描表，收集所有需要更新的记录RID
    target_rids_.clear();
    current_index_ = 0;
    is_executed_ = false;

    auto iter = table_info_->table_heap->Begin();
    while (!iter.IsEnd()) {
        Tuple tuple = *iter;
        // 检查记录是否满足WHERE条件
        Expression* predicate = update_plan->GetPredicate();
        if (predicate == nullptr ||
            evaluator_->EvaluateAsBoolean(predicate, tuple)) {
            target_rids_.push_back(tuple.GetRID());
        }
        ++iter;
    }
}

/**
 * 执行更新操作
 * 对所有目标记录进行批量更新，返回更新的记录数
 * @param tuple 输出参数，包含更新的记录数
 * @param rid 输出参数
 * @return 是否执行了更新操作
 */
bool UpdateExecutor::Next(Tuple* tuple, RID* rid) {
    if (is_executed_) {
        return false;  // 已经执行过更新操作
    }

    auto* update_plan = GetUpdatePlan();
    int updated_count = 0;
    TableManager* table_manager = exec_ctx_->GetTableManager();

    // 第二阶段：对所有目标记录执行更新
    for (const RID& target_rid : target_rids_) {
        Tuple old_tuple;
        if (!table_info_->table_heap->GetTuple(
                target_rid, &old_tuple,
                exec_ctx_->GetTransaction()->GetTxnId())) {
            continue;  // 记录可能已被删除，跳过
        }

        // 构造新的记录值：更新指定列，其他列保持不变
        std::vector<Value> new_values;
        const auto& columns = table_info_->schema->GetColumns();
        for (size_t i = 0; i < columns.size(); ++i) {
            const std::string& column_name = columns[i].name;
            bool found_update = false;

            // 检查当前列是否在SET子句中
            for (const auto& update_pair : update_plan->GetUpdates()) {
                if (update_pair.first == column_name) {
                    // 计算新值
                    Value new_value = evaluator_->Evaluate(
                        update_pair.second.get(), old_tuple);
                    new_values.push_back(new_value);
                    found_update = true;
                    break;
                }
            }

            if (!found_update) {
                // 列未在SET子句中，保持原值
                new_values.push_back(old_tuple.GetValue(i));
            }
        }

        // 执行更新操作
        Tuple new_tuple(new_values, table_info_->schema.get());
        if (table_info_->table_heap->UpdateTuple(
                new_tuple, target_rid,
                exec_ctx_->GetTransaction()->GetTxnId())) {
            // 更新相关索引
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

    // 返回更新的记录数
    std::vector<Value> result_values = {Value(updated_count)};
    *tuple = Tuple(result_values, GetOutputSchema());
    *rid = RID{INVALID_PAGE_ID, -1};
    return true;
}

/**
 * 删除执行器构造函数
 * 用于删除表中满足条件的记录
 */
DeleteExecutor::DeleteExecutor(ExecutorContext* exec_ctx,
                               std::unique_ptr<DeletePlanNode> plan)
    : Executor(exec_ctx, std::move(plan)),
      table_info_(nullptr),
      current_index_(0),
      is_executed_(false) {}

/**
 * 初始化删除执行器
 * 主要工作：扫描表找出所有需要删除的记录RID
 * 采用两阶段策略：先找出所有目标记录，再批量删除
 */
void DeleteExecutor::Init() {
    // 获取表信息
    auto* delete_plan = GetDeletePlan();
    table_info_ =
        exec_ctx_->GetCatalog()->GetTable(delete_plan->GetTableName());
    if (table_info_ == nullptr) {
        throw ExecutionException("Table not found: " +
                                 delete_plan->GetTableName());
    }

    // 创建表达式求值器，用于计算WHERE条件
    evaluator_ =
        std::make_unique<ExpressionEvaluator>(table_info_->schema.get());

    // 第一阶段：扫描表，收集所有需要删除的记录RID
    target_rids_.clear();
    current_index_ = 0;
    is_executed_ = false;

    auto iter = table_info_->table_heap->Begin();
    while (!iter.IsEnd()) {
        Tuple tuple = *iter;
        // 检查记录是否满足WHERE条件
        Expression* predicate = delete_plan->GetPredicate();
        if (predicate == nullptr ||
            evaluator_->EvaluateAsBoolean(predicate, tuple)) {
            target_rids_.push_back(tuple.GetRID());
        }
        ++iter;
    }
}

/**
 * 执行删除操作
 * 对所有目标记录进行批量删除，返回删除的记录数
 * @param tuple 输出参数，包含删除的记录数
 * @param rid 输出参数
 * @return 是否执行了删除操作
 */
bool DeleteExecutor::Next(Tuple* tuple, RID* rid) {
    if (is_executed_) {
        return false;  // 已经执行过删除操作
    }

    int deleted_count = 0;
    TableManager* table_manager = exec_ctx_->GetTableManager();

    // 第二阶段：对所有目标记录执行删除
    for (const RID& target_rid : target_rids_) {
        Tuple tuple_to_delete;

        // 先获取要删除的记录（用于更新索引）
        bool got_tuple = table_info_->table_heap->GetTuple(
            target_rid, &tuple_to_delete,
            exec_ctx_->GetTransaction()->GetTxnId());

        // 执行删除操作
        if (table_info_->table_heap->DeleteTuple(
                target_rid, exec_ctx_->GetTransaction()->GetTxnId())) {
            // 从相关索引中删除记录
            if (got_tuple && table_manager) {
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

    // 返回删除的记录数
    std::vector<Value> result_values = {Value(deleted_count)};
    *tuple = Tuple(result_values, GetOutputSchema());
    *rid = RID{INVALID_PAGE_ID, -1};
    return true;
}

/**
 * 投影执行器构造函数
 * 用于SELECT语句中的列投影操作
 */
ProjectionExecutor::ProjectionExecutor(ExecutorContext* exec_ctx,
                                       std::unique_ptr<ProjectionPlanNode> plan)
    : Executor(exec_ctx, std::move(plan)) {}

/**
 * 初始化投影执行器
 * 主要工作：创建子执行器，准备表达式求值器
 */
void ProjectionExecutor::Init() {
    auto* projection_plan = GetProjectionPlan();
    const auto* child_plan = projection_plan->GetChild(0);
    if (!child_plan) {
        throw ExecutionException("ProjectionExecutor: No child plan");
    }

    // 根据子计划类型创建相应的执行器
    // 这里采用plan复制的方式，避免所有权转移问题
    if (child_plan->GetType() == PlanNodeType::SEQUENTIAL_SCAN) {
        auto* seq_scan_plan = static_cast<const SeqScanPlanNode*>(child_plan);
        auto new_seq_scan_plan = std::make_unique<SeqScanPlanNode>(
            seq_scan_plan->GetOutputSchema(), seq_scan_plan->GetTableName(),
            nullptr  // 暂不处理复杂的WHERE条件传递
        );
        child_executor_ = std::make_unique<SeqScanExecutor>(
            exec_ctx_, std::move(new_seq_scan_plan));
    } else {
        throw ExecutionException(
            "ProjectionExecutor: Unsupported child plan type");
    }

    // 初始化子执行器
    child_executor_->Init();

    // 创建表达式求值器，用于计算投影表达式
    evaluator_ = std::make_unique<ExpressionEvaluator>(
        child_executor_->GetOutputSchema());
}

/**
 * 执行投影操作
 * 从子执行器获取记录，应用投影表达式，返回指定的列
 * @param tuple 输出参数，存储投影后的记录
 * @param rid 输出参数，存储记录的RID
 * @return 是否还有更多记录
 */
bool ProjectionExecutor::Next(Tuple* tuple, RID* rid) {
    auto* projection_plan = GetProjectionPlan();

    // 从子执行器获取下一条记录
    Tuple child_tuple;
    RID child_rid;
    if (!child_executor_->Next(&child_tuple, &child_rid)) {
        return false;  // 子执行器没有更多数据
    }

    // 计算投影表达式，生成新的列值
    std::vector<Value> projected_values;
    const auto& expressions = projection_plan->GetExpressions();

    for (const auto& expr : expressions) {
        // 对每个投影表达式求值
        Value value = evaluator_->Evaluate(expr.get(), child_tuple);
        projected_values.push_back(value);
    }

    // 创建投影后的tuple
    *tuple = Tuple(projected_values, GetOutputSchema());
    *rid = child_rid;

    return true;
}

}  // namespace SimpleRDBMS