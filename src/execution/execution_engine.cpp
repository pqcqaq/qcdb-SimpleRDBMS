#include "execution/execution_engine.h"

#include "catalog/table_manager.h"
#include "common/exception.h"
#include "execution/executor.h"
#include "execution/expression_cloner.h"
#include "parser/ast.h"
#include "recovery/log_manager.h"

namespace SimpleRDBMS {

ExecutionEngine::ExecutionEngine(BufferPoolManager* buffer_pool_manager,
                                 Catalog* catalog,
                                 TransactionManager* txn_manager,
                                 LogManager* log_manager)
    : buffer_pool_manager_(buffer_pool_manager),
      catalog_(catalog),
      txn_manager_(txn_manager),
      log_manager_(log_manager),
      table_manager_(std::make_unique<TableManager>(buffer_pool_manager, catalog)) {
    
    // 确保catalog有正确的log_manager
    if (log_manager_ && catalog_) {
        catalog_->SetLogManager(log_manager_);
    }
}

bool ExecutionEngine::Execute(Statement* statement,
                              std::vector<Tuple>* result_set,
                              Transaction* txn) {
    LOG_DEBUG("ExecutionEngine::Execute: Starting execution");
    if (!statement || !result_set || !txn) {
        LOG_ERROR("ExecutionEngine::Execute: Invalid parameters");
        if (!statement) {
            LOG_ERROR("ExecutionEngine::Execute: Statement is null");
        }
        if (!result_set) {
            LOG_ERROR("ExecutionEngine::Execute: Result set is null");
        }
        if (!txn) {
            LOG_ERROR("ExecutionEngine::Execute: Transaction is null");
        }
        return false;
    }

    LOG_DEBUG("ExecutionEngine::Execute: Statement type: "
              << static_cast<int>(statement->GetType()));
    
    // 处理DDL语句...
    switch (statement->GetType()) {
        case Statement::StmtType::CREATE_TABLE: {
            LOG_DEBUG("ExecutionEngine::Execute: Handling CREATE_TABLE");
            auto* create_stmt = static_cast<CreateTableStatement*>(statement);
            return table_manager_->CreateTable(create_stmt);
        }
        case Statement::StmtType::DROP_TABLE: {
            LOG_DEBUG("ExecutionEngine::Execute: Handling DROP_TABLE");
            auto* drop_stmt = static_cast<DropTableStatement*>(statement);
            return table_manager_->DropTable(drop_stmt->GetTableName());
        }
        case Statement::StmtType::CREATE_INDEX: {
            LOG_DEBUG("ExecutionEngine::Execute: Handling CREATE_INDEX");
            auto* create_idx_stmt =
                static_cast<CreateIndexStatement*>(statement);
            return table_manager_->CreateIndex(
                create_idx_stmt->GetIndexName(),
                create_idx_stmt->GetTableName(),
                create_idx_stmt->GetKeyColumns());
        }
        case Statement::StmtType::DROP_INDEX: {
            LOG_DEBUG("ExecutionEngine::Execute: Handling DROP_INDEX");
            auto* drop_idx_stmt = static_cast<DropIndexStatement*>(statement);
            return table_manager_->DropIndex(drop_idx_stmt->GetIndexName());
        }
        case Statement::StmtType::SHOW_TABLES: {
            return HandleShowTables(result_set);
        }
        case Statement::StmtType::BEGIN_TXN: {
            return HandleBegin(txn);
        }
        case Statement::StmtType::COMMIT_TXN: {
            return HandleCommit(txn);
        }
        case Statement::StmtType::ROLLBACK_TXN: {
            return HandleRollback(txn);
        }
        case Statement::StmtType::EXPLAIN: {
            LOG_DEBUG("ExecutionEngine::Execute: Handling EXPLAIN");
            auto* explain_stmt = static_cast<ExplainStatement*>(statement);
            return HandleExplain(explain_stmt, result_set);
        }
        default:
            LOG_DEBUG(
                "ExecutionEngine::Execute: Handling DML statement, creating "
                "plan");
            break;
    }

    LOG_DEBUG("ExecutionEngine::Execute: Creating execution plan");
    auto plan = CreatePlan(statement);
    if (!plan) {
        LOG_ERROR("ExecutionEngine::Execute: Failed to create plan");
        return false;
    }

    LOG_DEBUG("ExecutionEngine::Execute: Creating executor context");
    ExecutorContext exec_ctx(txn, catalog_, buffer_pool_manager_,
                             table_manager_.get());

    LOG_DEBUG("ExecutionEngine::Execute: Creating executor");
    auto executor = CreateExecutor(&exec_ctx, std::move(plan));
    if (!executor) {
        LOG_ERROR("ExecutionEngine::Execute: Failed to create executor");
        return false;
    }

    LOG_DEBUG("ExecutionEngine::Execute: Initializing executor");
    executor->Init();

    LOG_DEBUG("ExecutionEngine::Execute: Executing tuples");
    Tuple tuple;
    RID rid;
    int tuple_count = 0;
    // 最多百万，这里是DEBUG用的，防止死循环
    const int MAX_TUPLES = 1000000;
    
    // 添加超时保护
    auto start_time = std::chrono::steady_clock::now();
    const auto TIMEOUT_DURATION = std::chrono::seconds(10); // 10秒超时
    
    while (tuple_count < MAX_TUPLES) {
        // 检查超时
        auto current_time = std::chrono::steady_clock::now();
        if (current_time - start_time > TIMEOUT_DURATION) {
            LOG_ERROR("ExecutionEngine::Execute: Operation timed out after "
                      << std::chrono::duration_cast<std::chrono::seconds>(TIMEOUT_DURATION).count() 
                      << " seconds");
            return false;
        }
        
        bool has_next = false;
        try {
            has_next = executor->Next(&tuple, &rid);
        } catch (const std::exception& e) {
            LOG_ERROR("ExecutionEngine::Execute: Exception during tuple execution: " << e.what());
            return false;
        }
        
        if (!has_next) {
            break;
        }
        
        result_set->push_back(tuple);
        tuple_count++;
        
        if (tuple_count % 100 == 0) {
            LOG_DEBUG("ExecutionEngine::Execute: Processed " << tuple_count
                                                             << " tuples");
        }
    }

    if (tuple_count >= MAX_TUPLES) {
        LOG_ERROR("ExecutionEngine::Execute: Reached maximum tuple limit ("
                  << MAX_TUPLES << "), possible infinite loop detected");
        return false;
    }

    LOG_DEBUG("ExecutionEngine::Execute: Execution completed, processed "
              << tuple_count << " tuples");
    return true;
}

std::unique_ptr<PlanNode> ExecutionEngine::CreatePlan(Statement* statement) {
    if (!statement) {
        return nullptr;
    }

    switch (statement->GetType()) {
        case Statement::StmtType::SELECT:
            return CreateSelectPlan(static_cast<SelectStatement*>(statement));
        case Statement::StmtType::INSERT:
            return CreateInsertPlan(static_cast<InsertStatement*>(statement));
        case Statement::StmtType::UPDATE:
            return CreateUpdatePlan(static_cast<UpdateStatement*>(statement));
        case Statement::StmtType::DELETE:
            return CreateDeletePlan(static_cast<DeleteStatement*>(statement));
        default:
            return nullptr;
    }
}

std::unique_ptr<PlanNode> ExecutionEngine::CreateUpdatePlan(
    UpdateStatement* stmt) {
    if (!stmt) {
        return nullptr;
    }
    TableInfo* table_info = catalog_->GetTable(stmt->GetTableName());
    if (!table_info) {
        return nullptr;
    }

    // 创建UPDATE操作的输出schema（只包含一个整数列表示影响的行数）
    std::vector<Column> result_columns = {
        {"affected_rows", TypeId::INTEGER, 0, false, false}};
    auto result_schema = std::make_unique<Schema>(result_columns);

    // 使用表达式克隆器来拷贝更新表达式
    std::vector<std::pair<std::string, std::unique_ptr<Expression>>> updates;
    for (const auto& clause : stmt->GetUpdateClauses()) {
        auto cloned_expr = ExpressionCloner::Clone(clause.value.get());
        if (!cloned_expr) {
            throw ExecutionException("Failed to clone update expression");
        }
        updates.emplace_back(clause.column_name, std::move(cloned_expr));
    }
    // 克隆 WHERE 子句
    auto where_copy = ExpressionCloner::Clone(stmt->GetWhereClause());
    return std::make_unique<UpdatePlanNode>(
        result_schema.release(),  // 使用专门的结果schema
        stmt->GetTableName(), std::move(updates), std::move(where_copy));
}

std::unique_ptr<PlanNode> ExecutionEngine::CreateDeletePlan(
    DeleteStatement* stmt) {
    if (!stmt) {
        return nullptr;
    }
    TableInfo* table_info = catalog_->GetTable(stmt->GetTableName());
    if (!table_info) {
        return nullptr;
    }

    // 创建DELETE操作的输出schema（只包含一个整数列表示影响的行数）
    std::vector<Column> result_columns = {
        {"affected_rows", TypeId::INTEGER, 0, false, false}};
    auto result_schema = std::make_unique<Schema>(result_columns);

    // 克隆 WHERE 子句
    auto where_copy = ExpressionCloner::Clone(stmt->GetWhereClause());
    return std::make_unique<DeletePlanNode>(
        result_schema.release(),  // 使用专门的结果schema
        stmt->GetTableName(), std::move(where_copy));
}

std::unique_ptr<Executor> ExecutionEngine::CreateExecutor(
    ExecutorContext* exec_ctx, std::unique_ptr<PlanNode> plan) {
    if (!plan) {
        return nullptr;
    }

    switch (plan->GetType()) {
        case PlanNodeType::SEQUENTIAL_SCAN: {
            auto seq_scan_plan = static_cast<SeqScanPlanNode*>(plan.release());
            return std::make_unique<SeqScanExecutor>(
                exec_ctx, std::unique_ptr<SeqScanPlanNode>(seq_scan_plan));
        }
        case PlanNodeType::INDEX_SCAN: {
            auto index_scan_plan =
                static_cast<IndexScanPlanNode*>(plan.release());
            return std::make_unique<IndexScanExecutor>(
                exec_ctx, std::unique_ptr<IndexScanPlanNode>(index_scan_plan));
        }
        case PlanNodeType::PROJECTION: {
            auto projection_plan =
                static_cast<ProjectionPlanNode*>(plan.release());
            return std::make_unique<ProjectionExecutor>(
                exec_ctx, std::unique_ptr<ProjectionPlanNode>(projection_plan));
        }
        case PlanNodeType::INSERT: {
            auto insert_plan = static_cast<InsertPlanNode*>(plan.release());
            return std::make_unique<InsertExecutor>(
                exec_ctx, std::unique_ptr<InsertPlanNode>(insert_plan));
        }
        case PlanNodeType::UPDATE: {
            auto update_plan = static_cast<UpdatePlanNode*>(plan.release());
            return std::make_unique<UpdateExecutor>(
                exec_ctx, std::unique_ptr<UpdatePlanNode>(update_plan));
        }
        case PlanNodeType::DELETE: {
            auto delete_plan = static_cast<DeletePlanNode*>(plan.release());
            return std::make_unique<DeleteExecutor>(
                exec_ctx, std::unique_ptr<DeletePlanNode>(delete_plan));
        }
        default:
            return nullptr;
    }
}

std::unique_ptr<PlanNode> ExecutionEngine::CreateSelectPlan(
    SelectStatement* stmt) {
    if (!stmt) {
        LOG_ERROR("CreateSelectPlan: SelectStatement is null");
        return nullptr;
    }

    LOG_DEBUG("CreateSelectPlan: Creating plan for table "
              << stmt->GetTableName());
    TableInfo* table_info = catalog_->GetTable(stmt->GetTableName());
    if (!table_info) {
        LOG_ERROR("CreateSelectPlan: Table '" << stmt->GetTableName()
                                              << "' not found in catalog");
        return nullptr;
    }

    const auto& select_list = stmt->GetSelectList();
    bool is_select_all = false;
    if (select_list.size() == 1) {
        auto* col_ref =
            dynamic_cast<ColumnRefExpression*>(select_list[0].get());
        if (col_ref && col_ref->GetColumnName() == "*") {
            is_select_all = true;
        }
    }

    LOG_DEBUG("CreateSelectPlan: Found table "
              << stmt->GetTableName() << " with schema containing "
              << table_info->schema->GetColumnCount() << " columns");

    // 检查是否可以使用索引
    std::unique_ptr<PlanNode> scan_plan;
    if (stmt->GetWhereClause()) {
        std::string selected_index =
            SelectBestIndex(stmt->GetTableName(), stmt->GetWhereClause());
        if (!selected_index.empty()) {
            LOG_DEBUG("Using index scan with index: " << selected_index);
            auto where_copy = ExpressionCloner::Clone(stmt->GetWhereClause());
            scan_plan = std::make_unique<IndexScanPlanNode>(
                table_info->schema.get(), stmt->GetTableName(), selected_index,
                std::move(where_copy));
        }
    }

    // 如果不能使用索引，回退到顺序扫描
    if (!scan_plan) {
        LOG_DEBUG("Using sequential scan");
        auto where_copy = ExpressionCloner::Clone(stmt->GetWhereClause());
        scan_plan = std::make_unique<SeqScanPlanNode>(table_info->schema.get(),
                                                      stmt->GetTableName(),
                                                      std::move(where_copy));
    }

    if (is_select_all) {
        return std::move(scan_plan);
    } else {
        // 需要投影
        std::vector<Column> selected_columns;
        for (const auto& expr : select_list) {
            auto* col_ref = dynamic_cast<ColumnRefExpression*>(expr.get());
            if (col_ref) {
                const std::string& col_name = col_ref->GetColumnName();
                if (table_info->schema->HasColumn(col_name)) {
                    selected_columns.push_back(
                        table_info->schema->GetColumn(col_name));
                } else {
                    LOG_ERROR("CreateSelectPlan: Column '"
                              << col_name << "' not found in table");
                    return nullptr;
                }
            }
        }

        auto projection_schema = std::make_unique<Schema>(selected_columns);
        const Schema* output_schema = projection_schema.get();
        std::vector<std::unique_ptr<Expression>> expressions;
        for (const auto& expr : select_list) {
            expressions.push_back(ExpressionCloner::Clone(expr.get()));
        }

        auto projection_plan = std::make_unique<ProjectionPlanNode>(
            output_schema, std::move(expressions), std::move(scan_plan));
        projection_plan->SetOwnedSchema(std::move(projection_schema));
        return std::move(projection_plan);
    }
}

std::unique_ptr<PlanNode> ExecutionEngine::CreateInsertPlan(
    InsertStatement* stmt) {
    if (!stmt) {
        return nullptr;
    }

    TableInfo* table_info = catalog_->GetTable(stmt->GetTableName());
    if (!table_info) {
        return nullptr;
    }

    return std::make_unique<InsertPlanNode>(
        table_info->schema.get(), stmt->GetTableName(), stmt->GetValues());
}

std::string ExecutionEngine::SelectBestIndex(const std::string& table_name,
                                             Expression* where_clause) {
    if (!where_clause) {
        return "";
    }

    LOG_DEBUG("SelectBestIndex: Analyzing WHERE clause for table "
              << table_name);

    // 简单的索引选择逻辑：支持等值查询
    if (auto* binary_expr = dynamic_cast<BinaryOpExpression*>(where_clause)) {
        if (binary_expr->GetOperator() == BinaryOpExpression::OpType::EQUALS) {
            std::string column_name;

            // 检查左操作数是否为列引用
            if (auto* col_ref = dynamic_cast<ColumnRefExpression*>(
                    binary_expr->GetLeft())) {
                column_name = col_ref->GetColumnName();
            }
            // 检查右操作数是否为列引用（支持 value = column 的情况）
            else if (auto* col_ref = dynamic_cast<ColumnRefExpression*>(
                         binary_expr->GetRight())) {
                column_name = col_ref->GetColumnName();
            }

            if (!column_name.empty()) {
                // 确保另一个操作数是常量
                bool has_constant = false;
                if (dynamic_cast<ConstantExpression*>(binary_expr->GetLeft()) ||
                    dynamic_cast<ConstantExpression*>(
                        binary_expr->GetRight())) {
                    has_constant = true;
                }

                if (has_constant) {
                    LOG_DEBUG(
                        "SelectBestIndex: Found equality condition on column: "
                        << column_name);

                    // 查找该列上的索引
                    std::vector<IndexInfo*> indexes =
                        catalog_->GetTableIndexes(table_name);
                    for (auto* index_info : indexes) {
                        if (index_info->key_columns.size() == 1 &&
                            index_info->key_columns[0] == column_name) {
                            LOG_DEBUG("SelectBestIndex: Found suitable index: "
                                      << index_info->index_name);
                            return index_info->index_name;
                        }
                    }
                    LOG_DEBUG("SelectBestIndex: No index found for column: "
                              << column_name);
                }
            }
        }
        // 可以扩展支持其他操作符，如 <, >, <=, >= 等
        else {
            LOG_DEBUG("SelectBestIndex: Unsupported operator for index usage");
        }
    }
    // 可以扩展支持 AND/OR 复合条件
    else if (auto* and_expr = dynamic_cast<BinaryOpExpression*>(where_clause)) {
        if (and_expr->GetOperator() == BinaryOpExpression::OpType::AND) {
            // 递归检查左右子表达式
            std::string left_index =
                SelectBestIndex(table_name, and_expr->GetLeft());
            if (!left_index.empty()) {
                return left_index;
            }
            std::string right_index =
                SelectBestIndex(table_name, and_expr->GetRight());
            if (!right_index.empty()) {
                return right_index;
            }
        }
    }

    LOG_DEBUG("SelectBestIndex: No suitable index found for the WHERE clause");
    return "";  // 没有找到合适的索引
}

bool ExecutionEngine::HandleShowTables(std::vector<Tuple>* result_set) {
    // 创建更详细的结果schema
    std::vector<Column> columns = {
        {"table_name", TypeId::VARCHAR, 100, false, false},
        {"column_name", TypeId::VARCHAR, 100, false, false},
        {"data_type", TypeId::VARCHAR, 50, false, false},
        {"is_nullable", TypeId::VARCHAR, 10, false, false},
        {"is_primary_key", TypeId::VARCHAR, 10, false, false},
        {"column_size", TypeId::INTEGER, 0, false, false}};
    Schema result_schema(columns);

    // 清空结果集
    result_set->clear();

    try {
        // 从catalog获取所有表名
        std::vector<std::string> table_names = catalog_->GetAllTableNames();

        // 为每个表的每个字段创建一个Tuple
        for (const std::string& table_name : table_names) {
            TableInfo* table_info = catalog_->GetTable(table_name);
            if (!table_info || !table_info->schema) {
                continue;
            }

            const Schema* table_schema = table_info->schema.get();

            // 遍历表的所有列
            for (size_t i = 0; i < table_schema->GetColumnCount(); ++i) {
                const Column& col = table_schema->GetColumn(i);

                // 创建包含列信息的Value向量
                std::vector<Value> values;
                values.push_back(Value(table_name));  // table_name
                values.push_back(Value(col.name));    // column_name
                values.push_back(Value(TypeIdToString(col.type)));  // data_type
                values.push_back(
                    Value(col.nullable ? "YES" : "NO"));  // is_nullable
                values.push_back(Value(
                    col.is_primary_key ? "YES" : "NO"));  // is_primary_key
                values.push_back(
                    Value(static_cast<int32_t>(col.size)));  // column_size

                // 创建Tuple并添加到结果集
                Tuple tuple(values, &result_schema);
                result_set->push_back(tuple);
            }
        }

        LOG_DEBUG("SHOW TABLES returned " << result_set->size()
                                          << " columns from "
                                          << table_names.size() << " tables");
        return true;

    } catch (const std::exception& e) {
        LOG_ERROR("Error in HandleShowTables: " << e.what());
        return false;
    }
}

bool ExecutionEngine::HandleBegin(Transaction* txn) {
    // BEGIN语句通常在main中的ExecuteSQL里处理
    // 这里可以记录日志或执行其他必要操作
    LOG_DEBUG("BEGIN transaction executed");
    return true;
}

bool ExecutionEngine::HandleCommit(Transaction* txn) {
    // COMMIT语句通常在main中的ExecuteSQL里处理
    // 这里可以记录日志或执行其他必要操作
    LOG_DEBUG("COMMIT transaction executed");
    return true;
}

bool ExecutionEngine::HandleRollback(Transaction* txn) {
    // ROLLBACK语句通常在main中的ExecuteSQL里处理
    // 这里可以记录日志或执行其他必要操作
    LOG_DEBUG("ROLLBACK transaction executed");
    return true;
}

bool ExecutionEngine::HandleExplain(ExplainStatement* stmt,
                                    std::vector<Tuple>* result_set) {
    // 获取要解释的语句
    Statement* inner_stmt = stmt->GetStatement();

    // 创建执行计划
    auto plan = CreatePlan(inner_stmt);
    if (!plan) {
        LOG_ERROR("HandleExplain: Failed to create plan");
        return false;
    }

    // 格式化执行计划
    std::string plan_text = FormatExecutionPlan(plan.get());

    // 创建结果schema（只有一个字符串列）
    std::vector<Column> columns = {
        {"QUERY PLAN", TypeId::VARCHAR, 1000, false, false}};
    Schema result_schema(columns);

    // 将计划文本按行分割并加入结果集
    std::istringstream iss(plan_text);
    std::string line;
    while (std::getline(iss, line)) {
        std::vector<Value> values = {Value(line)};
        Tuple tuple(values, &result_schema);
        result_set->push_back(tuple);
    }

    return true;
}

std::string ExecutionEngine::FormatExecutionPlan(PlanNode* plan, int indent) {
    std::ostringstream oss;
    for (int i = 0; i < indent; i++) {
        oss << "  ";
    }
    oss << "-> " << GetPlanNodeTypeString(plan->GetType());
    switch (plan->GetType()) {
        case PlanNodeType::SEQUENTIAL_SCAN: {
            auto* seq_scan = static_cast<SeqScanPlanNode*>(plan);
            oss << " on " << seq_scan->GetTableName();
            if (seq_scan->GetPredicate()) {
                oss << " (Filter: WHERE clause)";
            }
            break;
        }
        case PlanNodeType::INDEX_SCAN: {
            auto* index_scan = static_cast<IndexScanPlanNode*>(plan);
            oss << " using " << index_scan->GetIndexName() << " on "
                << index_scan->GetTableName();
            if (index_scan->GetPredicate()) {
                oss << " (Index Cond: WHERE clause)";
            }
            break;
        }
        case PlanNodeType::INSERT: {
            auto* insert_plan = static_cast<InsertPlanNode*>(plan);
            oss << " into " << insert_plan->GetTableName();
            oss << " (" << insert_plan->GetValues().size() << " rows)";
            break;
        }
        case PlanNodeType::UPDATE: {
            auto* update_plan = static_cast<UpdatePlanNode*>(plan);
            oss << " on " << update_plan->GetTableName();
            if (update_plan->GetPredicate()) {
                oss << " (Filter: WHERE clause)";
            }
            break;
        }
        case PlanNodeType::DELETE: {
            auto* delete_plan = static_cast<DeletePlanNode*>(plan);
            oss << " from " << delete_plan->GetTableName();
            if (delete_plan->GetPredicate()) {
                oss << " (Filter: WHERE clause)";
            }
            break;
        }
        case PlanNodeType::PROJECTION: {
            auto* proj_plan = static_cast<ProjectionPlanNode*>(plan);
            oss << " (" << proj_plan->GetExpressions().size() << " columns)";
            break;
        }
        default:
            break;
    }
    oss << "\n";
    const auto& children = plan->GetChildren();
    for (const auto& child : children) {
        oss << FormatExecutionPlan(child.get(), indent + 1);
    }
    return oss.str();
}

std::string ExecutionEngine::GetPlanNodeTypeString(PlanNodeType type) {
    switch (type) {
        case PlanNodeType::SEQUENTIAL_SCAN:
            return "Seq Scan";
        case PlanNodeType::INDEX_SCAN:
            return "Index Scan";
        case PlanNodeType::INSERT:
            return "Insert";
        case PlanNodeType::UPDATE:
            return "Update";
        case PlanNodeType::DELETE:
            return "Delete";
        case PlanNodeType::PROJECTION:
            return "Projection";
        case PlanNodeType::FILTER:
            return "Filter";
        case PlanNodeType::NESTED_LOOP_JOIN:
            return "Nested Loop Join";
        case PlanNodeType::HASH_JOIN:
            return "Hash Join";
        case PlanNodeType::AGGREGATION:
            return "Aggregation";
        case PlanNodeType::SORT:
            return "Sort";
        case PlanNodeType::LIMIT:
            return "Limit";
        default:
            return "Unknown";
    }
}

}  // namespace SimpleRDBMS