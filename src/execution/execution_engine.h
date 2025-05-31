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

class ExecutionEngine {
public:
    ExecutionEngine(BufferPoolManager* buffer_pool_manager,
                    Catalog* catalog,
                    TransactionManager* txn_manager);
    
    // Execute a statement
    bool Execute(Statement* statement, std::vector<Tuple>* result_set, Transaction* txn);

private:
    BufferPoolManager* buffer_pool_manager_;
    Catalog* catalog_;
    TransactionManager* txn_manager_;
    std::unique_ptr<TableManager> table_manager_;  // 添加 TableManager
    
    // Create execution plan
    std::unique_ptr<PlanNode> CreatePlan(Statement* statement);
    
    // Create executor
    std::unique_ptr<Executor> CreateExecutor(ExecutorContext* exec_ctx, std::unique_ptr<PlanNode> plan);
    
    // Plan creation for different statement types
    std::unique_ptr<PlanNode> CreateSelectPlan(SelectStatement* stmt);
    std::unique_ptr<PlanNode> CreateInsertPlan(InsertStatement* stmt);
};

}  // namespace SimpleRDBMS