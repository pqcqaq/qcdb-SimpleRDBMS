#include "execution/execution_engine.h"
#include "execution/executor.h"
#include "parser/ast.h"
#include "catalog/table_manager.h"
#include "execution/expression_cloner.h"
#include "common/exception.h"

namespace SimpleRDBMS {

ExecutionEngine::ExecutionEngine(BufferPoolManager* buffer_pool_manager,
                                 Catalog* catalog,
                                 TransactionManager* txn_manager)
    : buffer_pool_manager_(buffer_pool_manager),
      catalog_(catalog),
      txn_manager_(txn_manager),
      table_manager_(std::make_unique<TableManager>(buffer_pool_manager, catalog)) {}

bool ExecutionEngine::Execute(Statement* statement, std::vector<Tuple>* result_set, Transaction* txn) {
    if (!statement || !result_set || !txn) {
        return false;
    }
    
    // Handle DDL statements directly
    switch (statement->GetType()) {
        case Statement::StmtType::CREATE_TABLE: {
            auto* create_stmt = static_cast<CreateTableStatement*>(statement);
            return table_manager_->CreateTable(create_stmt);
        }
        case Statement::StmtType::DROP_TABLE: {
            auto* drop_stmt = static_cast<DropTableStatement*>(statement);
            return table_manager_->DropTable(drop_stmt->GetTableName());
        }
        case Statement::StmtType::CREATE_INDEX: {
            auto* create_idx_stmt = static_cast<CreateIndexStatement*>(statement);
            return table_manager_->CreateIndex(
                create_idx_stmt->GetIndexName(),
                create_idx_stmt->GetTableName(),
                create_idx_stmt->GetKeyColumns()
            );
        }
        case Statement::StmtType::DROP_INDEX: {
            auto* drop_idx_stmt = static_cast<DropIndexStatement*>(statement);
            return table_manager_->DropIndex(drop_idx_stmt->GetIndexName());
        }
        default:
            // For DML statements, create plan and executor
            break;
    }
    
    auto plan = CreatePlan(statement);
    if (!plan) {
        return false;
    }
    
    ExecutorContext exec_ctx(txn, catalog_, buffer_pool_manager_);
    auto executor = CreateExecutor(&exec_ctx, std::move(plan));
    if (!executor) {
        return false;
    }
    
    executor->Init();
    
    Tuple tuple;
    RID rid;
    while (executor->Next(&tuple, &rid)) {
        result_set->push_back(tuple);
    }
    
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

std::unique_ptr<PlanNode> ExecutionEngine::CreateUpdatePlan(UpdateStatement* stmt) {
    if (!stmt) {
        return nullptr;
    }
    
    TableInfo* table_info = catalog_->GetTable(stmt->GetTableName());
    if (!table_info) {
        return nullptr;
    }
    
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
        table_info->schema.get(),
        stmt->GetTableName(),
        std::move(updates),
        std::move(where_copy)
    );
}

std::unique_ptr<PlanNode> ExecutionEngine::CreateDeletePlan(DeleteStatement* stmt) {
    if (!stmt) {
        return nullptr;
    }
    
    TableInfo* table_info = catalog_->GetTable(stmt->GetTableName());
    if (!table_info) {
        return nullptr;
    }
    
    // 克隆 WHERE 子句
    auto where_copy = ExpressionCloner::Clone(stmt->GetWhereClause());
    
    return std::make_unique<DeletePlanNode>(
        table_info->schema.get(),
        stmt->GetTableName(),
        std::move(where_copy)
    );
}

std::unique_ptr<Executor> ExecutionEngine::CreateExecutor(ExecutorContext* exec_ctx, 
                                                           std::unique_ptr<PlanNode> plan) {
    if (!plan) {
        return nullptr;
    }
    
    switch (plan->GetType()) {
        case PlanNodeType::SEQUENTIAL_SCAN: {
            auto seq_scan_plan = static_cast<SeqScanPlanNode*>(plan.release());
            return std::make_unique<SeqScanExecutor>(exec_ctx, 
                std::unique_ptr<SeqScanPlanNode>(seq_scan_plan));
        }
        case PlanNodeType::INSERT: {
            auto insert_plan = static_cast<InsertPlanNode*>(plan.release());
            return std::make_unique<InsertExecutor>(exec_ctx, 
                std::unique_ptr<InsertPlanNode>(insert_plan));
        }
        case PlanNodeType::UPDATE: {
            auto update_plan = static_cast<UpdatePlanNode*>(plan.release());
            return std::make_unique<UpdateExecutor>(exec_ctx, 
                std::unique_ptr<UpdatePlanNode>(update_plan));
        }
        case PlanNodeType::DELETE: {
            auto delete_plan = static_cast<DeletePlanNode*>(plan.release());
            return std::make_unique<DeleteExecutor>(exec_ctx, 
                std::unique_ptr<DeletePlanNode>(delete_plan));
        }
        default:
            return nullptr;
    }
}

std::unique_ptr<PlanNode> ExecutionEngine::CreateSelectPlan(SelectStatement* stmt) {
    if (!stmt) {
        return nullptr;
    }
    
    TableInfo* table_info = catalog_->GetTable(stmt->GetTableName());
    if (!table_info) {
        return nullptr;
    }
    
    return std::make_unique<SeqScanPlanNode>(
        table_info->schema.get(),
        stmt->GetTableName(),
        nullptr
    );
}

std::unique_ptr<PlanNode> ExecutionEngine::CreateInsertPlan(InsertStatement* stmt) {
    if (!stmt) {
        return nullptr;
    }
    
    TableInfo* table_info = catalog_->GetTable(stmt->GetTableName());
    if (!table_info) {
        return nullptr;
    }
    
    return std::make_unique<InsertPlanNode>(
        table_info->schema.get(),
        stmt->GetTableName(),
        stmt->GetValues()
    );
}

}  // namespace SimpleRDBMS