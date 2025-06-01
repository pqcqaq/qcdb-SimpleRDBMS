/*
 * 文件: execution_engine.h
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述: SQL查询执行引擎头文件，定义了执行引擎的核心接口和功能
 *       负责将解析后的SQL语句转换为执行计划并执行，是整个数据库系统的核心组件
 */

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

// 前向声明，避免循环依赖问题
class UpdateStatement;
class DeleteStatement;
class UpdatePlanNode;
class DeletePlanNode;

/**
 * SQL执行引擎类
 *
 * 这是整个数据库系统的核心执行组件，主要职责包括：
 * 1. 接收解析后的SQL语句AST
 * 2. 生成相应的执行计划（PlanNode树）
 * 3. 创建对应的执行器（Executor）
 * 4. 协调各个组件完成SQL语句的执行
 * 5. 进行简单的查询优化（如索引选择）
 *
 * 支持的SQL类型：
 * - DDL: CREATE TABLE/INDEX, DROP TABLE/INDEX
 * - DML: SELECT, INSERT, UPDATE, DELETE
 * - 事务控制: BEGIN, COMMIT, ROLLBACK
 * - 管理命令: SHOW TABLES, EXPLAIN
 */
class ExecutionEngine {
   public:
    /**
     * 构造函数：初始化执行引擎的各个依赖组件
     * @param buffer_pool_manager 缓冲池管理器，负责页面缓存和I/O
     * @param catalog 系统目录，存储表和索引的元数据信息
     * @param txn_manager 事务管理器，处理事务的并发控制
     * @param log_manager 日志管理器，用于WAL日志记录（可选）
     */
    ExecutionEngine(BufferPoolManager* buffer_pool_manager, Catalog* catalog,
                    TransactionManager* txn_manager,
                    LogManager* log_manager = nullptr);

    /**
     * 核心执行方法：执行SQL语句并返回结果
     *
     * 执行流程：
     * 1. 根据statement类型判断是DDL还是DML
     * 2. DDL语句直接处理（如CREATE TABLE）
     * 3. DML语句生成执行计划，然后创建执行器执行
     * 4. 使用Volcano模型逐个获取结果tuple
     *
     * @param statement 解析后的SQL语句AST节点
     * @param result_set 用于存储查询结果的向量
     * @param txn 当前事务上下文
     * @return 执行是否成功
     */
    bool Execute(Statement* statement, std::vector<Tuple>* result_set,
                 Transaction* txn);

   private:
    // ============ 核心组件依赖 ============
    BufferPoolManager* buffer_pool_manager_;       // 缓冲池管理器，处理页面缓存
    Catalog* catalog_;                             // 系统目录，存储元数据
    TransactionManager* txn_manager_;              // 事务管理器，处理并发控制
    LogManager* log_manager_;                      // 日志管理器，用于恢复
    std::unique_ptr<TableManager> table_manager_;  // 表管理器，封装表相关操作

    // ============ 查询优化相关方法 ============

    /**
     * 索引选择优化器：分析WHERE条件，选择最佳索引
     *
     * 当前支持的优化策略：
     * - 等值查询：WHERE column = value 形式可以使用索引
     * - AND条件：递归分析左右子表达式
     * - 单列索引：优先选择匹配度最高的单列索引
     *
     * TODO: 扩展支持范围查询、复合索引、OR条件等
     *
     * @param table_name 表名
     * @param where_clause WHERE条件表达式
     * @return 选中的索引名，无合适索引则返回空字符串
     */
    std::string SelectBestIndex(const std::string& table_name,
                                Expression* where_clause);

    // ============ 执行计划生成方法 ============

    /**
     * 执行计划工厂方法：根据SQL语句类型创建相应的执行计划
     * @param statement SQL语句AST节点
     * @return 执行计划根节点，失败返回nullptr
     */
    std::unique_ptr<PlanNode> CreatePlan(Statement* statement);

    /**
     * 执行器工厂方法：根据执行计划创建对应的执行器
     *
     * 支持的执行器类型：
     * - SeqScanExecutor: 顺序扫描
     * - IndexScanExecutor: 索引扫描
     * - ProjectionExecutor: 投影操作
     * - InsertExecutor: 插入操作
     * - UpdateExecutor: 更新操作
     * - DeleteExecutor: 删除操作
     *
     * @param exec_ctx 执行器上下文，包含事务、catalog等信息
     * @param plan 执行计划节点
     * @return 对应的执行器实例
     */
    std::unique_ptr<Executor> CreateExecutor(ExecutorContext* exec_ctx,
                                             std::unique_ptr<PlanNode> plan);

    // ============ 各类型SQL的执行计划创建方法 ============

    /**
     * 创建SELECT语句的执行计划
     *
     * 处理逻辑：
     * 1. 检查表是否存在
     * 2. 分析WHERE条件，选择扫描方式（索引扫描 vs 顺序扫描）
     * 3. 处理SELECT列表（* vs 指定列）
     * 4. 如需投影，创建ProjectionPlanNode
     *
     * @param stmt SELECT语句AST节点
     * @return SELECT执行计划
     */
    std::unique_ptr<PlanNode> CreateSelectPlan(SelectStatement* stmt);

    /**
     * 创建INSERT语句的执行计划
     * @param stmt INSERT语句AST节点
     * @return INSERT执行计划
     */
    std::unique_ptr<PlanNode> CreateInsertPlan(InsertStatement* stmt);

    /**
     * 创建UPDATE语句的执行计划
     * @param stmt UPDATE语句AST节点
     * @return UPDATE执行计划
     */
    std::unique_ptr<PlanNode> CreateUpdatePlan(UpdateStatement* stmt);

    /**
     * 创建DELETE语句的执行计划
     * @param stmt DELETE语句AST节点
     * @return DELETE执行计划
     */
    std::unique_ptr<PlanNode> CreateDeletePlan(DeleteStatement* stmt);

    // ============ 特殊命令处理方法 ============

    /**
     * 处理SHOW TABLES命令
     * 返回数据库中所有表的详细信息，包括每个表的列信息
     * @param result_set 存储表信息的结果集
     * @return 执行是否成功
     */
    bool HandleShowTables(std::vector<Tuple>* result_set);

    /**
     * 处理BEGIN事务命令
     * 注意：实际的事务开始逻辑在TransactionManager中处理
     * @param txn 当前事务
     * @return 执行是否成功
     */
    bool HandleBegin(Transaction* txn);

    /**
     * 处理COMMIT事务命令
     * 注意：实际的事务提交逻辑在TransactionManager中处理
     * @param txn 当前事务
     * @return 执行是否成功
     */
    bool HandleCommit(Transaction* txn);

    /**
     * 处理ROLLBACK事务命令
     * 注意：实际的事务回滚逻辑在TransactionManager中处理
     * @param txn 当前事务
     * @return 执行是否成功
     */
    bool HandleRollback(Transaction* txn);

    /**
     * 处理EXPLAIN命令，显示SQL语句的执行计划
     * @param stmt EXPLAIN语句AST节点
     * @param result_set 存储执行计划的结果集
     * @return 执行是否成功
     */
    bool HandleExplain(ExplainStatement* stmt, std::vector<Tuple>* result_set);

    // ============ 执行计划格式化方法 ============

    /**
     * 将执行计划格式化为树状结构的可读文本
     * 用于EXPLAIN命令的输出显示
     * @param plan 执行计划节点
     * @param indent 缩进级别，用于形成树状结构
     * @return 格式化后的执行计划文本
     */
    std::string FormatExecutionPlan(PlanNode* plan, int indent = 0);

    /**
     * 将执行计划节点类型转换为可读的字符串描述
     * @param type 计划节点类型枚举
     * @return 对应的字符串描述
     */
    std::string GetPlanNodeTypeString(PlanNodeType type);

    /**
     * 数据类型ID转字符串的工具方法
     * 用于SHOW TABLES命令中显示列的数据类型
     * @param type_id 数据类型ID枚举
     * @return 对应的类型名称字符串
     */
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