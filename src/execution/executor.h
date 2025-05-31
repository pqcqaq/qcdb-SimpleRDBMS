#pragma once

#include <memory>

#include "catalog/catalog.h"
#include "execution/expression_evaluator.h"
#include "execution/plan_node.h"
#include "parser/ast.h"
#include "record/table_heap.h"
#include "record/tuple.h"
#include "transaction/transaction.h"

namespace SimpleRDBMS {

// Forward declarations
class ExecutorContext;
class PlanNode;
class SeqScanPlanNode;
class InsertPlanNode;
class TableInfo;
class UpdatePlanNode;
class DeletePlanNode;
class TableManager;  // 新增前向声明

// Executor context (moved before Executor class)
class ExecutorContext {
   public:
    ExecutorContext(Transaction* txn, Catalog* catalog,
                    BufferPoolManager* buffer_pool_manager,
                    TableManager* table_manager)
        : transaction_(txn),
          catalog_(catalog),
          buffer_pool_manager_(buffer_pool_manager),
          table_manager_(table_manager) {}

    Transaction* GetTransaction() { return transaction_; }
    Catalog* GetCatalog() { return catalog_; }
    BufferPoolManager* GetBufferPoolManager() { return buffer_pool_manager_; }
    TableManager* GetTableManager() { return table_manager_; }

   private:
    Transaction* transaction_;
    Catalog* catalog_;
    BufferPoolManager* buffer_pool_manager_;
    TableManager* table_manager_;
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
    std::unique_ptr<ExpressionEvaluator> evaluator_;
};

class IndexScanExecutor : public Executor {
   public:
    IndexScanExecutor(ExecutorContext* exec_ctx,
                      std::unique_ptr<IndexScanPlanNode> plan);
    void Init() override;
    bool Next(Tuple* tuple, RID* rid) override;
    IndexScanPlanNode* GetIndexScanPlan() const {
        return static_cast<IndexScanPlanNode*>(plan_.get());
    }

   private:
    TableInfo* table_info_;
    IndexInfo* index_info_;
    std::unique_ptr<ExpressionEvaluator> evaluator_;
    Value search_key_;
    bool has_found_tuple_;
    RID found_rid_;
    void ExtractSearchKey(Expression* predicate);
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
// Update executor
class UpdateExecutor : public Executor {
   public:
    UpdateExecutor(ExecutorContext* exec_ctx,
                   std::unique_ptr<UpdatePlanNode> plan);
    void Init() override;
    bool Next(Tuple* tuple, RID* rid) override;

    // 获取具体类型的计划节点
    UpdatePlanNode* GetUpdatePlan() const {
        return static_cast<UpdatePlanNode*>(plan_.get());
    }

    // 获取表的schema（用于操作数据）
    const Schema* GetTableSchema() const {
        return table_info_ ? table_info_->schema.get() : nullptr;
    }

   private:
    TableInfo* table_info_;
    std::unique_ptr<ExpressionEvaluator> evaluator_;
    std::vector<RID> target_rids_;  // 需要更新的记录RID列表
    size_t current_index_;
    bool is_executed_;
};

// Delete executor
class DeleteExecutor : public Executor {
   public:
    DeleteExecutor(ExecutorContext* exec_ctx,
                   std::unique_ptr<DeletePlanNode> plan);
    void Init() override;
    bool Next(Tuple* tuple, RID* rid) override;

    // 获取具体类型的计划节点
    DeletePlanNode* GetDeletePlan() const {
        return static_cast<DeletePlanNode*>(plan_.get());
    }

    // 获取表的schema（用于操作数据）
    const Schema* GetTableSchema() const {
        return table_info_ ? table_info_->schema.get() : nullptr;
    }

   private:
    TableInfo* table_info_;
    std::unique_ptr<ExpressionEvaluator> evaluator_;
    std::vector<RID> target_rids_;  // 需要删除的记录RID列表
    size_t current_index_;
    bool is_executed_;
};

// Projection executor
class ProjectionExecutor : public Executor {
   public:
    ProjectionExecutor(ExecutorContext* exec_ctx,
                       std::unique_ptr<ProjectionPlanNode> plan);
    void Init() override;
    bool Next(Tuple* tuple, RID* rid) override;

    ProjectionPlanNode* GetProjectionPlan() const {
        return static_cast<ProjectionPlanNode*>(plan_.get());
    }

   private:
    std::unique_ptr<Executor> child_executor_;
    std::unique_ptr<ExpressionEvaluator> evaluator_;
};

}  // namespace SimpleRDBMS