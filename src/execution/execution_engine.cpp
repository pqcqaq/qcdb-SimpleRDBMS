/*
 * 文件: execution_engine.cpp
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述: SQL查询执行引擎实现，负责将解析后的SQL语句转换为执行计划并执行
 *       包含DDL语句处理、DML执行计划生成、索引选择优化等核心功能
 */

#include "execution/execution_engine.h"

#include "catalog/table_manager.h"
#include "common/exception.h"
#include "execution/executor.h"
#include "execution/expression_cloner.h"
#include "parser/ast.h"
#include "recovery/log_manager.h"

namespace SimpleRDBMS {

/**
 * 构造函数：初始化执行引擎的各个组件
 * @param buffer_pool_manager 缓冲池管理器，用于页面缓存
 * @param catalog 系统目录，存储元数据信息
 * @param txn_manager 事务管理器，处理事务相关操作
 * @param log_manager 日志管理器，用于WAL日志记录
 */
ExecutionEngine::ExecutionEngine(BufferPoolManager* buffer_pool_manager,
                                 Catalog* catalog,
                                 TransactionManager* txn_manager,
                                 LogManager* log_manager)
    : buffer_pool_manager_(buffer_pool_manager),
      catalog_(catalog),
      txn_manager_(txn_manager),
      log_manager_(log_manager),
      table_manager_(
          std::make_unique<TableManager>(buffer_pool_manager, catalog)) {
    // 让catalog持有log_manager的引用，这样catalog可以记录DDL操作日志
    if (log_manager_ && catalog_) {
        catalog_->SetLogManager(log_manager_);
    }
}

/**
 * 核心执行方法：根据SQL语句类型选择不同的执行路径
 * @param statement 解析后的SQL语句抽象语法树
 * @param result_set 查询结果集，用于存储执行结果
 * @param txn 当前事务上下文
 * @return 执行是否成功
 */
bool ExecutionEngine::Execute(Statement* statement,
                              std::vector<Tuple>* result_set,
                              Transaction* txn) {
    LOG_DEBUG("ExecutionEngine::Execute: Starting execution");

    // 参数有效性检查，确保传入的参数都不为空
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

    // DDL语句直接处理，不需要生成执行计划
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
            // SHOW TABLES是特殊命令，直接处理
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

    // 对于DML语句（SELECT、INSERT、UPDATE、DELETE），需要生成执行计划
    LOG_DEBUG("ExecutionEngine::Execute: Creating execution plan");
    auto plan = CreatePlan(statement);
    if (!plan) {
        LOG_ERROR("ExecutionEngine::Execute: Failed to create plan");
        return false;
    }

    // 创建执行器上下文，包含事务、catalog等信息
    LOG_DEBUG("ExecutionEngine::Execute: Creating executor context");
    ExecutorContext exec_ctx(txn, catalog_, buffer_pool_manager_,
                             table_manager_.get());

    // 根据执行计划创建对应的executor
    LOG_DEBUG("ExecutionEngine::Execute: Creating executor");
    auto executor = CreateExecutor(&exec_ctx, std::move(plan));
    if (!executor) {
        LOG_ERROR("ExecutionEngine::Execute: Failed to create executor");
        return false;
    }

    // 初始化executor，准备执行
    LOG_DEBUG("ExecutionEngine::Execute: Initializing executor");
    executor->Init();

    // 使用Volcano模型执行查询，逐个获取tuple
    LOG_DEBUG("ExecutionEngine::Execute: Executing tuples");
    Tuple tuple;
    RID rid;
    int tuple_count = 0;

    // 防护措施：最多处理百万条记录，避免无限循环导致系统卡死
    const int MAX_TUPLES = 1000000;

    // 超时保护：设置10秒超时，防止长时间执行
    auto start_time = std::chrono::steady_clock::now();
    const auto TIMEOUT_DURATION = std::chrono::seconds(10);

    while (tuple_count < MAX_TUPLES) {
        // 检查是否超时
        auto current_time = std::chrono::steady_clock::now();
        if (current_time - start_time > TIMEOUT_DURATION) {
            LOG_ERROR("ExecutionEngine::Execute: Operation timed out after "
                      << std::chrono::duration_cast<std::chrono::seconds>(
                             TIMEOUT_DURATION)
                             .count()
                      << " seconds");
            return false;
        }

        bool has_next = false;
        try {
            // 调用executor的Next方法获取下一个tuple
            has_next = executor->Next(&tuple, &rid);
        } catch (const std::exception& e) {
            LOG_ERROR(
                "ExecutionEngine::Execute: Exception during tuple execution: "
                << e.what());
            return false;
        }

        // 如果没有更多tuple，执行完成
        if (!has_next) {
            break;
        }

        // 将tuple添加到结果集
        result_set->push_back(tuple);
        tuple_count++;

        // 每处理100个tuple打印一次日志，便于监控执行进度
        if (tuple_count % 100 == 0) {
            LOG_DEBUG("ExecutionEngine::Execute: Processed " << tuple_count
                                                             << " tuples");
        }
    }

    // 检查是否达到最大tuple限制
    if (tuple_count >= MAX_TUPLES) {
        LOG_ERROR("ExecutionEngine::Execute: Reached maximum tuple limit ("
                  << MAX_TUPLES << "), possible infinite loop detected");
        return false;
    }

    LOG_DEBUG("ExecutionEngine::Execute: Execution completed, processed "
              << tuple_count << " tuples");
    return true;
}

/**
 * 执行计划生成器：根据SQL语句类型创建相应的执行计划
 * @param statement SQL语句
 * @return 执行计划节点
 */
std::unique_ptr<PlanNode> ExecutionEngine::CreatePlan(Statement* statement) {
    if (!statement) {
        return nullptr;
    }

    // 根据SQL语句类型创建对应的执行计划
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

/**
 * 创建UPDATE语句的执行计划
 * @param stmt UPDATE语句的AST节点
 * @return UPDATE执行计划
 */
std::unique_ptr<PlanNode> ExecutionEngine::CreateUpdatePlan(
    UpdateStatement* stmt) {
    if (!stmt) {
        return nullptr;
    }

    // 检查表是否存在
    TableInfo* table_info = catalog_->GetTable(stmt->GetTableName());
    if (!table_info) {
        return nullptr;
    }

    // UPDATE操作的返回结果是影响的行数，所以schema只包含一个INTEGER列
    std::vector<Column> result_columns = {
        {"affected_rows", TypeId::INTEGER, 0, false, false}};
    auto result_schema = std::make_unique<Schema>(result_columns);

    // 克隆更新表达式，避免多次使用时的内存问题
    std::vector<std::pair<std::string, std::unique_ptr<Expression>>> updates;
    for (const auto& clause : stmt->GetUpdateClauses()) {
        auto cloned_expr = ExpressionCloner::Clone(clause.value.get());
        if (!cloned_expr) {
            throw ExecutionException("Failed to clone update expression");
        }
        updates.emplace_back(clause.column_name, std::move(cloned_expr));
    }

    // 克隆WHERE子句表达式
    auto where_copy = ExpressionCloner::Clone(stmt->GetWhereClause());
    return std::make_unique<UpdatePlanNode>(
        std::move(result_schema), stmt->GetTableName(), std::move(updates),
        std::move(where_copy));
}

/**
 * 创建DELETE语句的执行计划
 * @param stmt DELETE语句的AST节点
 * @return DELETE执行计划
 */
std::unique_ptr<PlanNode> ExecutionEngine::CreateDeletePlan(
    DeleteStatement* stmt) {
    if (!stmt) {
        return nullptr;
    }

    // 检查表是否存在
    TableInfo* table_info = catalog_->GetTable(stmt->GetTableName());
    if (!table_info) {
        return nullptr;
    }

    // DELETE操作的返回结果是影响的行数，所以schema只包含一个INTEGER列
    std::vector<Column> result_columns = {
        {"affected_rows", TypeId::INTEGER, 0, false, false}};
    auto result_schema = std::make_unique<Schema>(result_columns);

    // 克隆WHERE子句表达式
    auto where_copy = ExpressionCloner::Clone(stmt->GetWhereClause());
    return std::make_unique<DeletePlanNode>(
        std::move(result_schema), stmt->GetTableName(), std::move(where_copy));
}

/**
 * 执行器工厂方法：根据执行计划类型创建相应的执行器
 * @param exec_ctx 执行器上下文
 * @param plan 执行计划
 * @return 对应的执行器实例
 */
std::unique_ptr<Executor> ExecutionEngine::CreateExecutor(
    ExecutorContext* exec_ctx, std::unique_ptr<PlanNode> plan) {
    if (!plan) {
        return nullptr;
    }

    // 根据plan类型创建对应的executor，这里使用了factory pattern
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

/**
 * 创建SELECT语句的执行计划，包含扫描方式选择和投影处理
 * @param stmt SELECT语句的AST节点
 * @return SELECT执行计划
 */
std::unique_ptr<PlanNode> ExecutionEngine::CreateSelectPlan(
    SelectStatement* stmt) {
    if (!stmt) {
        LOG_ERROR("CreateSelectPlan: SelectStatement is null");
        return nullptr;
    }

    LOG_DEBUG("CreateSelectPlan: Creating plan for table "
              << stmt->GetTableName());

    // 从catalog获取表信息
    TableInfo* table_info = catalog_->GetTable(stmt->GetTableName());
    if (!table_info) {
        LOG_ERROR("CreateSelectPlan: Table '" << stmt->GetTableName()
                                              << "' not found in catalog");
        return nullptr;
    }

    // 检查是否是SELECT *查询
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

    // 查询优化：检查是否可以使用索引扫描
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

    // 如果没有合适的索引，使用顺序扫描
    if (!scan_plan) {
        LOG_DEBUG("Using sequential scan");
        auto where_copy = ExpressionCloner::Clone(stmt->GetWhereClause());
        scan_plan = std::make_unique<SeqScanPlanNode>(table_info->schema.get(),
                                                      stmt->GetTableName(),
                                                      std::move(where_copy));
    }

    // 如果是SELECT *，直接返回扫描计划
    if (is_select_all) {
        return std::move(scan_plan);
    } else {
        // 需要投影操作，选择特定的列
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

        // 创建投影的输出schema
        auto projection_schema = std::make_unique<Schema>(selected_columns);
        const Schema* output_schema = projection_schema.get();

        // 克隆SELECT列表中的表达式
        std::vector<std::unique_ptr<Expression>> expressions;
        for (const auto& expr : select_list) {
            expressions.push_back(ExpressionCloner::Clone(expr.get()));
        }

        // 创建投影计划节点，将扫描计划作为子节点
        auto projection_plan = std::make_unique<ProjectionPlanNode>(
            output_schema, std::move(expressions), std::move(scan_plan));
        projection_plan->SetOwnedSchema(std::move(projection_schema));
        return std::move(projection_plan);
    }
}

/**
 * 创建INSERT语句的执行计划
 * @param stmt INSERT语句的AST节点
 * @return INSERT执行计划
 */
std::unique_ptr<PlanNode> ExecutionEngine::CreateInsertPlan(
    InsertStatement* stmt) {
    if (!stmt) {
        return nullptr;
    }

    // 检查表是否存在
    TableInfo* table_info = catalog_->GetTable(stmt->GetTableName());
    if (!table_info) {
        return nullptr;
    }

    // 创建INSERT执行计划，包含表schema、表名和要插入的值
    return std::make_unique<InsertPlanNode>(
        table_info->schema.get(), stmt->GetTableName(), stmt->GetValues());
}

/**
 * 索引选择优化器：分析WHERE条件，选择最适合的索引
 * @param table_name 表名
 * @param where_clause WHERE条件表达式
 * @return 选中的索引名，如果没有合适索引则返回空字符串
 */
std::string ExecutionEngine::SelectBestIndex(const std::string& table_name,
                                             Expression* where_clause) {
    if (!where_clause) {
        return "";
    }

    LOG_DEBUG("SelectBestIndex: Analyzing WHERE clause for table "
              << table_name);

    // 当前实现的索引选择策略：支持等值查询的索引扫描
    if (auto* binary_expr = dynamic_cast<BinaryOpExpression*>(where_clause)) {
        if (binary_expr->GetOperator() == BinaryOpExpression::OpType::EQUALS) {
            std::string column_name;

            // 检查左操作数是否为列引用
            if (auto* col_ref = dynamic_cast<ColumnRefExpression*>(
                    binary_expr->GetLeft())) {
                column_name = col_ref->GetColumnName();
            }
            // 检查右操作数是否为列引用，支持 value = column 的情况
            else if (auto* col_ref = dynamic_cast<ColumnRefExpression*>(
                         binary_expr->GetRight())) {
                column_name = col_ref->GetColumnName();
            }

            if (!column_name.empty()) {
                // 确保另一个操作数是常量，这样才能有效使用索引
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

                    // 在该列上查找索引
                    std::vector<IndexInfo*> indexes =
                        catalog_->GetTableIndexes(table_name);
                    for (auto* index_info : indexes) {
                        // 目前只支持单列索引
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
        // TODO: 扩展支持其他操作符，如 <, >, <=, >= 等范围查询
        else {
            LOG_DEBUG("SelectBestIndex: Unsupported operator for index usage");
        }
    }
    // 处理AND复合条件，递归查找可用索引
    else if (auto* and_expr = dynamic_cast<BinaryOpExpression*>(where_clause)) {
        if (and_expr->GetOperator() == BinaryOpExpression::OpType::AND) {
            // 递归检查左右子表达式，优先使用左侧找到的索引
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
    return "";
}

/**
 * 处理SHOW TABLES命令，返回数据库中所有表的详细信息
 * @param result_set 用于存储表信息的结果集
 * @return 执行是否成功
 */
bool ExecutionEngine::HandleShowTables(std::vector<Tuple>* result_set) {
    // 创建结果schema，包含表的详细信息
    std::vector<Column> columns = {
        {"table_name", TypeId::VARCHAR, 100, false, false},
        {"column_name", TypeId::VARCHAR, 100, false, false},
        {"data_type", TypeId::VARCHAR, 50, false, false},
        {"is_nullable", TypeId::VARCHAR, 10, false, false},
        {"is_primary_key", TypeId::VARCHAR, 10, false, false},
        {"column_size", TypeId::INTEGER, 0, false, false}};
    Schema result_schema(columns);

    result_set->clear();

    try {
        // 获取数据库中所有表名
        std::vector<std::string> table_names = catalog_->GetAllTableNames();

        // 为每个表的每个列创建一条记录
        for (const std::string& table_name : table_names) {
            TableInfo* table_info = catalog_->GetTable(table_name);
            if (!table_info || !table_info->schema) {
                continue;
            }

            const Schema* table_schema = table_info->schema.get();

            // 遍历表的所有列，为每列生成一条结果记录
            for (size_t i = 0; i < table_schema->GetColumnCount(); ++i) {
                const Column& col = table_schema->GetColumn(i);

                // 构造列信息的Value向量
                std::vector<Value> values;
                values.push_back(Value(table_name));
                values.push_back(Value(col.name));
                values.push_back(Value(TypeIdToString(col.type)));
                values.push_back(Value(col.nullable ? "YES" : "NO"));
                values.push_back(Value(col.is_primary_key ? "YES" : "NO"));
                values.push_back(Value(static_cast<int32_t>(col.size)));

                // 创建tuple并添加到结果集
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

/**
 * 处理BEGIN事务命令
 * @param txn 当前事务
 * @return 执行是否成功
 */
bool ExecutionEngine::HandleBegin(Transaction* txn) {
    // BEGIN语句的实际处理通常在上层的main函数中完成
    // 这里主要是记录日志和进行必要的状态检查
    LOG_DEBUG("BEGIN transaction executed");
    return true;
}

/**
 * 处理COMMIT事务命令
 * @param txn 当前事务
 * @return 执行是否成功
 */
bool ExecutionEngine::HandleCommit(Transaction* txn) {
    // COMMIT语句的实际处理通常在上层的main函数中完成
    // 这里主要是记录日志和进行必要的状态检查
    LOG_DEBUG("COMMIT transaction executed");
    return true;
}

/**
 * 处理ROLLBACK事务命令
 * @param txn 当前事务
 * @return 执行是否成功
 */
bool ExecutionEngine::HandleRollback(Transaction* txn) {
    // ROLLBACK语句的实际处理通常在上层的main函数中完成
    // 这里主要是记录日志和进行必要的状态检查
    LOG_DEBUG("ROLLBACK transaction executed");
    return true;
}

/**
 * 处理EXPLAIN命令，显示SQL语句的执行计划
 * @param stmt EXPLAIN语句
 * @param result_set 用于存储执行计划的结果集
 * @return 执行是否成功
 */
bool ExecutionEngine::HandleExplain(ExplainStatement* stmt,
                                    std::vector<Tuple>* result_set) {
    // 获取要分析的内部SQL语句
    Statement* inner_stmt = stmt->GetStatement();

    // 为内部语句创建执行计划
    auto plan = CreatePlan(inner_stmt);
    if (!plan) {
        LOG_ERROR("HandleExplain: Failed to create plan");
        return false;
    }

    // 将执行计划格式化为可读的文本
    std::string plan_text = FormatExecutionPlan(plan.get());

    // 创建结果schema，只包含一个字符串列用于显示计划
    std::vector<Column> columns = {
        {"QUERY PLAN", TypeId::VARCHAR, 1000, false, false}};
    Schema result_schema(columns);

    // 将计划文本按行分割并添加到结果集
    std::istringstream iss(plan_text);
    std::string line;
    while (std::getline(iss, line)) {
        std::vector<Value> values = {Value(line)};
        Tuple tuple(values, &result_schema);
        result_set->push_back(tuple);
    }

    return true;
}

/**
 * 格式化执行计划为树状结构的文本
 * @param plan 执行计划节点
 * @param indent 缩进级别
 * @return 格式化后的执行计划文本
 */
std::string ExecutionEngine::FormatExecutionPlan(PlanNode* plan, int indent) {
    std::ostringstream oss;

    // 添加缩进，形成树状结构
    for (int i = 0; i < indent; i++) {
        oss << "  ";
    }
    oss << "-> " << GetPlanNodeTypeString(plan->GetType());

    // 根据计划节点类型添加具体信息
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

    // 递归格式化子计划节点
    const auto& children = plan->GetChildren();
    for (const auto& child : children) {
        oss << FormatExecutionPlan(child.get(), indent + 1);
    }
    return oss.str();
}

/**
 * 将执行计划节点类型转换为可读的字符串
 * @param type 计划节点类型
 * @return 对应的字符串描述
 */
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