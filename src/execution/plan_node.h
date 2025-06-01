/*
 * 文件: plan_node.h
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述: 查询计划节点头文件，定义了查询执行计划树的各种节点类型，
 *       实现了SQL语句到执行计划的抽象表示，支持各种数据库操作
 */

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/types.h"

// 前向声明，避免循环依赖
namespace SimpleRDBMS {
class Schema;
class Expression;

/**
 * 计划节点类型枚举
 * 定义了所有支持的查询操作类型，用于运行时类型识别和执行器分发
 */
enum class PlanNodeType {
    SEQUENTIAL_SCAN,   // 顺序扫描（全表扫描）
    INDEX_SCAN,        // 索引扫描
    INSERT,            // 插入操作
    UPDATE,            // 更新操作
    DELETE,            // 删除操作
    PROJECTION,        // 投影操作（SELECT子句）
    FILTER,            // 过滤操作（WHERE子句）
    NESTED_LOOP_JOIN,  // 嵌套循环连接
    HASH_JOIN,         // 哈希连接
    AGGREGATION,       // 聚合操作（GROUP BY）
    SORT,              // 排序操作（ORDER BY）
    LIMIT              // 限制操作（LIMIT子句）
};

/**
 * 计划节点基类
 * 所有具体计划节点的抽象基类，定义了查询计划树的基本结构
 * 采用树形结构，每个节点可以有多个子节点，形成执行管道
 *
 * 设计理念：
 * - 基于Volcano模型的迭代器执行模式
 * - 每个节点负责特定的数据库操作
 * - 通过组合不同节点构建复杂查询的执行计划
 */
class PlanNode {
   public:
    /**
     * 构造函数
     * @param output_schema 该节点输出的数据schema
     * @param children 子节点列表，形成执行计划树
     */
    PlanNode(const Schema* output_schema,
             std::vector<std::unique_ptr<PlanNode>> children)
        : output_schema_(output_schema), children_(std::move(children)) {}

    virtual ~PlanNode() = default;

    /**
     * 获取节点类型（纯虚函数）
     * 子类必须实现，用于运行时类型识别和执行器创建
     * @return 节点类型枚举值
     */
    virtual PlanNodeType GetType() const = 0;

    /** 获取输出schema，描述该节点产生的数据结构 */
    const Schema* GetOutputSchema() const { return output_schema_; }

    /** 获取所有子节点的引用 */
    const std::vector<std::unique_ptr<PlanNode>>& GetChildren() const {
        return children_;
    }

    /**
     * 获取指定索引的子节点
     * @param index 子节点索引
     * @return 子节点指针，如果索引越界返回nullptr
     */
    const PlanNode* GetChild(size_t index) const {
        return index < children_.size() ? children_[index].get() : nullptr;
    }

   protected:
    const Schema* output_schema_;                      // 输出数据的schema
    std::vector<std::unique_ptr<PlanNode>> children_;  // 子节点列表
};

/**
 * 顺序扫描计划节点
 * 对应SQL的全表扫描操作，按页面顺序遍历表中的所有记录
 * 支持WHERE条件过滤，是最基本的数据访问方式
 *
 * 适用场景：
 * - 没有合适索引的查询
 * - 小表的全表访问
 * - 范围扫描条件
 */
class SeqScanPlanNode : public PlanNode {
   public:
    /**
     * 构造函数
     * @param output_schema 输出schema
     * @param table_name 要扫描的表名
     * @param predicate WHERE条件表达式，可为空表示无条件扫描
     */
    SeqScanPlanNode(const Schema* output_schema, const std::string& table_name,
                    std::unique_ptr<Expression> predicate = nullptr)
        : PlanNode(output_schema, {}),
          table_name_(table_name),
          predicate_(std::move(predicate)) {}

    /** 返回节点类型 */
    PlanNodeType GetType() const override {
        return PlanNodeType::SEQUENTIAL_SCAN;
    }

    /** 获取扫描的表名 */
    const std::string& GetTableName() const { return table_name_; }

    /** 获取WHERE条件表达式 */
    Expression* GetPredicate() const { return predicate_.get(); }

   private:
    std::string table_name_;                 // 目标表名
    std::unique_ptr<Expression> predicate_;  // WHERE条件
};

/**
 * 插入计划节点
 * 对应SQL的INSERT语句，支持单行和多行插入
 * 负责将数据插入到表中，并维护相关索引的一致性
 *
 * 支持的插入形式：
 * - INSERT INTO table VALUES (...)
 * - INSERT INTO table VALUES (...), (...)  // 多行插入
 */
class InsertPlanNode : public PlanNode {
   public:
    /**
     * 构造函数
     * @param output_schema 输出schema（通常为影响的行数）
     * @param table_name 目标表名
     * @param values 要插入的值列表，支持多行插入
     */
    InsertPlanNode(const Schema* output_schema, const std::string& table_name,
                   std::vector<std::vector<Value>> values)
        : PlanNode(output_schema, {}),
          table_name_(table_name),
          values_(std::move(values)) {}

    /** 返回节点类型 */
    PlanNodeType GetType() const override { return PlanNodeType::INSERT; }

    /** 获取目标表名 */
    const std::string& GetTableName() const { return table_name_; }

    /** 获取要插入的所有值 */
    const std::vector<std::vector<Value>>& GetValues() const { return values_; }

   private:
    std::string table_name_;                  // 目标表名
    std::vector<std::vector<Value>> values_;  // 插入值列表
};

/**
 * 投影计划节点
 * 对应SQL的SELECT子句，负责选择和计算输出列
 * 支持列选择、表达式计算、函数调用等
 *
 * 主要功能：
 * - 列投影：SELECT col1, col2
 * - 表达式计算：SELECT col1 + col2 AS sum
 * - 常量输出：SELECT 'hello', 123
 * - 函数调用：SELECT UPPER(name)
 */
class ProjectionPlanNode : public PlanNode {
   public:
    /**
     * 构造函数
     * @param output_schema 投影后的输出schema
     * @param expressions 投影表达式列表，每个表达式对应一个输出列
     * @param child 子计划节点，提供输入数据
     */
    ProjectionPlanNode(const Schema* output_schema,
                       std::vector<std::unique_ptr<Expression>> expressions,
                       std::unique_ptr<PlanNode> child)
        : PlanNode(output_schema, {}), expressions_(std::move(expressions)) {
        children_.push_back(std::move(child));
    }

    /** 返回节点类型 */
    PlanNodeType GetType() const override { return PlanNodeType::PROJECTION; }

    /** 获取投影表达式列表 */
    const std::vector<std::unique_ptr<Expression>>& GetExpressions() const {
        return expressions_;
    }

    /**
     * 设置schema的所有权
     * 用于管理schema的生命周期，避免悬空指针
     * @param schema 要管理的schema
     */
    void SetOwnedSchema(std::unique_ptr<Schema> schema) {
        owned_schema_ = std::move(schema);
    }

   private:
    std::vector<std::unique_ptr<Expression>> expressions_;  // 投影表达式列表
    std::unique_ptr<Schema> owned_schema_;  // 管理schema生命周期
};

/**
 * 更新计划节点
 * 对应SQL的UPDATE语句，支持SET子句和WHERE条件
 * 实现记录的就地更新，并维护索引一致性
 *
 * 支持的更新形式：
 * - UPDATE table SET col1 = value WHERE condition
 * - UPDATE table SET col1 = expr, col2 = value WHERE condition
 */
class UpdatePlanNode : public PlanNode {
   public:
    /**
     * 构造函数
     * @param output_schema 输出schema（通常为更新的行数）
     * @param table_name 目标表名
     * @param updates SET子句的列名和表达式对列表
     * @param predicate WHERE条件表达式，可为空表示更新所有记录
     */
    UpdatePlanNode(
        std::unique_ptr<Schema> output_schema, const std::string& table_name,
        std::vector<std::pair<std::string, std::unique_ptr<Expression>>>
            updates,
        std::unique_ptr<Expression> predicate = nullptr)
        : PlanNode(nullptr, {}),  // 先传 nullptr，下面设置
          table_name_(table_name),
          updates_(std::move(updates)),
          predicate_(std::move(predicate)),
          owned_schema_(std::move(output_schema)) {
        // 设置 output_schema_ 指向 owned_schema_
        output_schema_ = owned_schema_.get();
    }

    /** 返回节点类型 */
    PlanNodeType GetType() const override { return PlanNodeType::UPDATE; }

    /** 获取目标表名 */
    const std::string& GetTableName() const { return table_name_; }

    /** 获取SET子句的更新列表 */
    const std::vector<std::pair<std::string, std::unique_ptr<Expression>>>&
    GetUpdates() const {
        return updates_;
    }

    /** 获取WHERE条件表达式 */
    Expression* GetPredicate() const { return predicate_.get(); }

   private:
    std::string table_name_;  // 目标表名
    std::vector<std::pair<std::string, std::unique_ptr<Expression>>>
        updates_;                            // SET子句
    std::unique_ptr<Expression> predicate_;  // WHERE条件
    std::unique_ptr<Schema> owned_schema_;  // 管理schema生命周期
};

/**
 * 删除计划节点
 * 对应SQL的DELETE语句，支持WHERE条件过滤
 * 负责从表中删除符合条件的记录，并维护索引一致性
 *
 * 支持的删除形式：
 * - DELETE FROM table WHERE condition
 * - DELETE FROM table  // 清空表
 */
class DeletePlanNode : public PlanNode {
   public:
    /**
     * 构造函数
     * @param output_schema 输出schema（通常为删除的行数）
     * @param table_name 目标表名
     * @param predicate WHERE条件表达式，可为空表示删除所有记录
     */
    DeletePlanNode(std::unique_ptr<Schema> output_schema, const std::string& table_name,
                   std::unique_ptr<Expression> predicate = nullptr)
        : PlanNode(nullptr, {}),  // 先传 nullptr，下面设置
          table_name_(table_name),
          predicate_(std::move(predicate)),
          owned_schema_(std::move(output_schema)) {
        // 设置 output_schema_ 指向 owned_schema_
        output_schema_ = owned_schema_.get();
    }

    /** 返回节点类型 */
    PlanNodeType GetType() const override { return PlanNodeType::DELETE; }

    /** 获取目标表名 */
    const std::string& GetTableName() const { return table_name_; }

    /** 获取WHERE条件表达式 */
    Expression* GetPredicate() const { return predicate_.get(); }

   private:
    std::string table_name_;                 // 目标表名
    std::unique_ptr<Expression> predicate_;  // WHERE条件
    std::unique_ptr<Schema> owned_schema_;  // 管理schema生命周期
};

/**
 * 索引扫描计划节点
 * 基于B+树索引进行高效的数据访问
 * 适用于等值查询和范围查询，避免全表扫描的开销
 *
 * 适用场景：
 * - 等值查询：WHERE indexed_col = value
 * - 范围查询：WHERE indexed_col BETWEEN a AND b
 * - 排序需求：ORDER BY indexed_col
 *
 * 性能优势：
 * - O(log n)的查找复杂度
 * - 减少磁盘I/O
 * - 支持索引覆盖查询
 */
class IndexScanPlanNode : public PlanNode {
   public:
    /**
     * 构造函数
     * @param output_schema 输出schema
     * @param table_name 目标表名
     * @param index_name 使用的索引名
     * @param predicate 查询条件表达式，用于提取索引键值
     */
    IndexScanPlanNode(const Schema* output_schema,
                      const std::string& table_name,
                      const std::string& index_name,
                      std::unique_ptr<Expression> predicate = nullptr)
        : PlanNode(output_schema, {}),
          table_name_(table_name),
          index_name_(index_name),
          predicate_(std::move(predicate)) {}

    /** 返回节点类型 */
    PlanNodeType GetType() const override { return PlanNodeType::INDEX_SCAN; }

    /** 获取目标表名 */
    const std::string& GetTableName() const { return table_name_; }

    /** 获取使用的索引名 */
    const std::string& GetIndexName() const { return index_name_; }

    /** 获取查询条件表达式 */
    Expression* GetPredicate() const { return predicate_.get(); }

   private:
    std::string table_name_;                 // 目标表名
    std::string index_name_;                 // 索引名
    std::unique_ptr<Expression> predicate_;  // 查询条件
};

}  // namespace SimpleRDBMS