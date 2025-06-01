/*
 * 文件: executor.h
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述: 查询执行器头文件，定义了各种SQL操作的执行器类，
 *       基于Volcano模型实现迭代器式的查询执行引擎
 */

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

// 前向声明，避免循环依赖
class ExecutorContext;
class PlanNode;
class SeqScanPlanNode;
class InsertPlanNode;
class TableInfo;
class UpdatePlanNode;
class DeletePlanNode;
class TableManager;

/**
 * 执行器上下文类
 * 为查询执行器提供必要的运行时环境和资源访问
 * 包含事务、元数据管理、缓冲池等核心组件的引用
 */
class ExecutorContext {
   public:
    /**
     * 构造函数
     * @param txn 当前事务对象
     * @param catalog 元数据管理器
     * @param buffer_pool_manager 缓冲池管理器
     * @param table_manager 表管理器
     */
    ExecutorContext(Transaction* txn, Catalog* catalog,
                    BufferPoolManager* buffer_pool_manager,
                    TableManager* table_manager)
        : transaction_(txn),
          catalog_(catalog),
          buffer_pool_manager_(buffer_pool_manager),
          table_manager_(table_manager) {}

    /** 获取当前事务 */
    Transaction* GetTransaction() { return transaction_; }

    /** 获取元数据管理器 */
    Catalog* GetCatalog() { return catalog_; }

    /** 获取缓冲池管理器 */
    BufferPoolManager* GetBufferPoolManager() { return buffer_pool_manager_; }

    /** 获取表管理器 */
    TableManager* GetTableManager() { return table_manager_; }

   private:
    Transaction* transaction_;                // 当前事务
    Catalog* catalog_;                        // 元数据管理器
    BufferPoolManager* buffer_pool_manager_;  // 缓冲池管理器
    TableManager* table_manager_;             // 表管理器
};

/**
 * 执行器基类
 * 所有具体执行器的抽象基类，定义了执行器的基本接口
 * 采用Volcano模型，通过Next()方法逐个返回结果tuple
 */
class Executor {
   public:
    /**
     * 构造函数
     * @param exec_ctx 执行器上下文
     * @param plan 执行计划节点
     */
    Executor(ExecutorContext* exec_ctx, std::unique_ptr<PlanNode> plan)
        : exec_ctx_(exec_ctx), plan_(std::move(plan)) {}

    virtual ~Executor() = default;

    /**
     * 初始化执行器
     * 在开始执行前进行必要的准备工作
     */
    virtual void Init() = 0;

    /**
     * 获取下一个结果tuple
     * @param tuple 输出参数，存储获取到的tuple
     * @param rid 输出参数，存储tuple的RID
     * @return 是否成功获取到tuple，false表示没有更多结果
     */
    virtual bool Next(Tuple* tuple, RID* rid) = 0;

    /** 获取输出schema */
    const Schema* GetOutputSchema() const { return plan_->GetOutputSchema(); }

   protected:
    ExecutorContext* exec_ctx_;       // 执行器上下文
    std::unique_ptr<PlanNode> plan_;  // 执行计划节点
};

/**
 * 顺序扫描执行器
 * 实现全表扫描操作，支持WHERE条件过滤
 * 按页面顺序遍历表中的所有记录
 */
class SeqScanExecutor : public Executor {
   public:
    /**
     * 构造函数
     * @param exec_ctx 执行器上下文
     * @param plan 顺序扫描计划节点
     */
    SeqScanExecutor(ExecutorContext* exec_ctx,
                    std::unique_ptr<SeqScanPlanNode> plan);

    /** 初始化扫描器，设置表迭代器 */
    void Init() override;

    /** 获取下一个满足条件的tuple */
    bool Next(Tuple* tuple, RID* rid) override;

    /** 获取顺序扫描计划节点 */
    SeqScanPlanNode* GetSeqScanPlan() const {
        return static_cast<SeqScanPlanNode*>(plan_.get());
    }

   private:
    TableInfo* table_info_;                           // 表信息
    TableHeap::Iterator table_iterator_;              // 表迭代器
    std::unique_ptr<ExpressionEvaluator> evaluator_;  // 表达式求值器
};

/**
 * 索引扫描执行器
 * 基于B+树索引进行高效的点查询
 * 适用于等值条件查询，避免全表扫描
 */
class IndexScanExecutor : public Executor {
   public:
    /**
     * 构造函数
     * @param exec_ctx 执行器上下文
     * @param plan 索引扫描计划节点
     */
    IndexScanExecutor(ExecutorContext* exec_ctx,
                      std::unique_ptr<IndexScanPlanNode> plan);

    /** 初始化索引扫描器 */
    void Init() override;

    /** 通过索引查找记录 */
    bool Next(Tuple* tuple, RID* rid) override;

    /** 获取索引扫描计划节点 */
    IndexScanPlanNode* GetIndexScanPlan() const {
        return static_cast<IndexScanPlanNode*>(plan_.get());
    }

   private:
    TableInfo* table_info_;                           // 表信息
    IndexInfo* index_info_;                           // 索引信息
    std::unique_ptr<ExpressionEvaluator> evaluator_;  // 表达式求值器
    Value search_key_;                                // 搜索键值
    bool has_found_tuple_;                            // 是否已找到记录
    RID found_rid_;                                   // 找到的记录RID

    /**
     * 从WHERE条件中提取搜索键
     * @param predicate WHERE条件表达式
     */
    void ExtractSearchKey(Expression* predicate);
};

/**
 * 插入执行器
 * 执行INSERT语句，支持单行和多行插入
 * 负责向表堆插入记录并更新相关索引
 */
class InsertExecutor : public Executor {
   public:
    /**
     * 构造函数
     * @param exec_ctx 执行器上下文
     * @param plan 插入计划节点
     */
    InsertExecutor(ExecutorContext* exec_ctx,
                   std::unique_ptr<InsertPlanNode> plan);

    /** 初始化插入器 */
    void Init() override;

    /** 执行插入操作 */
    bool Next(Tuple* tuple, RID* rid) override;

    /** 获取插入计划节点 */
    InsertPlanNode* GetInsertPlan() const {
        return static_cast<InsertPlanNode*>(plan_.get());
    }

   private:
    TableInfo* table_info_;  // 表信息
    size_t current_index_;   // 当前处理的记录索引
};

/**
 * 更新执行器
 * 执行UPDATE语句，支持WHERE条件和SET子句
 * 采用两阶段更新：先收集目标RID，再批量更新
 */
class UpdateExecutor : public Executor {
   public:
    /**
     * 构造函数
     * @param exec_ctx 执行器上下文
     * @param plan 更新计划节点
     */
    UpdateExecutor(ExecutorContext* exec_ctx,
                   std::unique_ptr<UpdatePlanNode> plan);

    /** 初始化更新器，扫描并收集目标记录 */
    void Init() override;

    /** 执行批量更新操作 */
    bool Next(Tuple* tuple, RID* rid) override;

    /** 获取更新计划节点 */
    UpdatePlanNode* GetUpdatePlan() const {
        return static_cast<UpdatePlanNode*>(plan_.get());
    }

    /** 获取表的schema，用于数据操作 */
    const Schema* GetTableSchema() const {
        return table_info_ ? table_info_->schema.get() : nullptr;
    }

   private:
    TableInfo* table_info_;                           // 表信息
    std::unique_ptr<ExpressionEvaluator> evaluator_;  // 表达式求值器
    std::vector<RID> target_rids_;                    // 需要更新的记录RID列表
    size_t current_index_;                            // 当前处理索引
    bool is_executed_;                                // 是否已执行
};

/**
 * 删除执行器
 * 执行DELETE语句，支持WHERE条件过滤
 * 采用两阶段删除：先收集目标RID，再批量删除
 */
class DeleteExecutor : public Executor {
   public:
    /**
     * 构造函数
     * @param exec_ctx 执行器上下文
     * @param plan 删除计划节点
     */
    DeleteExecutor(ExecutorContext* exec_ctx,
                   std::unique_ptr<DeletePlanNode> plan);

    /** 初始化删除器，扫描并收集目标记录 */
    void Init() override;

    /** 执行批量删除操作 */
    bool Next(Tuple* tuple, RID* rid) override;

    /** 获取删除计划节点 */
    DeletePlanNode* GetDeletePlan() const {
        return static_cast<DeletePlanNode*>(plan_.get());
    }

    /** 获取表的schema，用于数据操作 */
    const Schema* GetTableSchema() const {
        return table_info_ ? table_info_->schema.get() : nullptr;
    }

   private:
    TableInfo* table_info_;                           // 表信息
    std::unique_ptr<ExpressionEvaluator> evaluator_;  // 表达式求值器
    std::vector<RID> target_rids_;                    // 需要删除的记录RID列表
    size_t current_index_;                            // 当前处理索引
    bool is_executed_;                                // 是否已执行
};

/**
 * 投影执行器
 * 执行SELECT语句中的列投影操作
 * 从子执行器获取数据，应用投影表达式生成指定的输出列
 */
class ProjectionExecutor : public Executor {
   public:
    /**
     * 构造函数
     * @param exec_ctx 执行器上下文
     * @param plan 投影计划节点
     */
    ProjectionExecutor(ExecutorContext* exec_ctx,
                       std::unique_ptr<ProjectionPlanNode> plan);

    /** 初始化投影器和子执行器 */
    void Init() override;

    /** 获取投影后的tuple */
    bool Next(Tuple* tuple, RID* rid) override;

    /** 获取投影计划节点 */
    ProjectionPlanNode* GetProjectionPlan() const {
        return static_cast<ProjectionPlanNode*>(plan_.get());
    }

   private:
    std::unique_ptr<Executor> child_executor_;        // 子执行器
    std::unique_ptr<ExpressionEvaluator> evaluator_;  // 表达式求值器
};

}  // namespace SimpleRDBMS