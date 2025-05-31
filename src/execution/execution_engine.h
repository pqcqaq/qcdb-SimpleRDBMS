#pragma once

#include <memory>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "catalog/catalog.h"
#include "catalog/table_manager.h"
#include "execution/executor.h"
#include "parser/ast.h"
#include "transaction/transaction_manager.h"

namespace SimpleRDBMS {

class UpdateStatement;
class DeleteStatement;
class UpdatePlanNode;
class DeletePlanNode;

class ExecutionEngine {
   public:
    ExecutionEngine(BufferPoolManager* buffer_pool_manager, Catalog* catalog,
                    TransactionManager* txn_manager);

    // Execute a statement
    bool Execute(Statement* statement, std::vector<Tuple>* result_set,
                 Transaction* txn);

   private:
    BufferPoolManager* buffer_pool_manager_;
    Catalog* catalog_;
    TransactionManager* txn_manager_;
    std::unique_ptr<TableManager> table_manager_;  // 添加 TableManager

    // Create execution plan
    std::unique_ptr<PlanNode> CreatePlan(Statement* statement);

    // Create executor
    std::unique_ptr<Executor> CreateExecutor(ExecutorContext* exec_ctx,
                                             std::unique_ptr<PlanNode> plan);

    // Plan creation for different statement types
    std::unique_ptr<PlanNode> CreateSelectPlan(SelectStatement* stmt);
    std::unique_ptr<PlanNode> CreateInsertPlan(InsertStatement* stmt);
    std::unique_ptr<PlanNode> CreateUpdatePlan(UpdateStatement* stmt);
    std::unique_ptr<PlanNode> CreateDeletePlan(DeleteStatement* stmt);
    bool HandleShowTables(std::vector<Tuple>* result_set);
    bool HandleBegin(Transaction* txn);
    bool HandleCommit(Transaction* txn);
    bool HandleRollback(Transaction* txn);
    bool HandleExplain(ExplainStatement* stmt, std::vector<Tuple>* result_set);
    std::string FormatExecutionPlan(PlanNode* plan, int indent = 0);
    std::string GetPlanNodeTypeString(PlanNodeType type);

    std::string TypeIdToString(TypeId type_id) {
        switch (type_id) {
            case TypeId::BOOLEAN:
                return "BOOLEAN";
            case TypeId::TINYINT:
                return "TINYINT";
            case TypeId::SMALLINT:
                return "SMALLINT";
            case TypeId::INTEGER:
                return "INTEGER";
            case TypeId::BIGINT:
                return "BIGINT";
            case TypeId::FLOAT:
                return "FLOAT";
            case TypeId::DOUBLE:
                return "DOUBLE";
            case TypeId::VARCHAR:
                return "VARCHAR";
            case TypeId::TIMESTAMP:
                return "TIMESTAMP";
            default:
                return "UNKNOWN";
        }
    }
};

}  // namespace SimpleRDBMS