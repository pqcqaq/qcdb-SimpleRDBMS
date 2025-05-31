#pragma once

#include <memory>

#include "buffer/buffer_pool_manager.h"
#include "catalog/catalog.h"
#include "execution/plan_node.h"
#include "record/tuple.h"
#include "transaction/transaction.h"

namespace SimpleRDBMS {

// Forward declarations
class ExecutorContext;
class PlanNode;
class SeqScanPlanNode;
class InsertPlanNode;
class TableInfo;
class TableHeap;

// Executor context (moved before Executor class)
class ExecutorContext {
   public:
    ExecutorContext(Transaction* txn, Catalog* catalog,
                    BufferPoolManager* buffer_pool_manager)
        : transaction_(txn),
          catalog_(catalog),
          buffer_pool_manager_(buffer_pool_manager) {}

    Transaction* GetTransaction() { return transaction_; }
    Catalog* GetCatalog() { return catalog_; }
    BufferPoolManager* GetBufferPoolManager() { return buffer_pool_manager_; }

   private:
    Transaction* transaction_;
    Catalog* catalog_;
    BufferPoolManager* buffer_pool_manager_;
};

// Base executor class
class Executor {
   public:
    Executor(ExecutorContext* exec_ctx, std::unique_ptr<PlanNode> plan)
        : exec_ctx_(exec_ctx), plan_(std::move(plan)) {}

    virtual ~Executor() = default;

    // Initialize the executor
    virtual void Init() = 0;

    // Get the next tuple
    virtual bool Next(Tuple* tuple, RID* rid) = 0;

    // Get output schema
    const Schema* GetOutputSchema() const { return plan_->GetOutputSchema(); }

   protected:
    ExecutorContext* exec_ctx_;
    std::unique_ptr<PlanNode> plan_;
};

// Sequential scan executor
class SeqScanExecutor : public Executor {
   public:
    SeqScanExecutor(ExecutorContext* exec_ctx,
                    std::unique_ptr<SeqScanPlanNode> plan);

    void Init() override;
    bool Next(Tuple* tuple, RID* rid) override;

    // 获取具体类型的计划节点
    SeqScanPlanNode* GetSeqScanPlan() const {
        return static_cast<SeqScanPlanNode*>(plan_.get());
    }

   private:
    TableInfo* table_info_;
    TableHeap::Iterator table_iterator_;
};

// Insert executor
class InsertExecutor : public Executor {
   public:
    InsertExecutor(ExecutorContext* exec_ctx,
                   std::unique_ptr<InsertPlanNode> plan);

    void Init() override;
    bool Next(Tuple* tuple, RID* rid) override;

    // 获取具体类型的计划节点
    InsertPlanNode* GetInsertPlan() const {
        return static_cast<InsertPlanNode*>(plan_.get());
    }

   private:
    TableInfo* table_info_;
    size_t current_index_;
};

}  // namespace SimpleRDBMS