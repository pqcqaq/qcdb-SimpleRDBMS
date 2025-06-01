/*
 * 文件: ast.h
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述: SQL抽象语法树(AST)节点定义，包含表达式和语句的所有节点类型
 */

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/types.h"

namespace SimpleRDBMS {

// 前向声明访问者模式的访问者类
class ASTVisitor;

/**
 * AST节点基类
 *
 * 所有AST节点的根基类，定义了访问者模式的接口
 * 使用访问者模式的好处：
 * 1. 可以在不修改AST节点的情况下添加新的操作
 * 2. 将操作逻辑从数据结构中分离出来
 * 3. 方便实现代码生成、优化、解释执行等功能
 */
class ASTNode {
   public:
    virtual ~ASTNode() = default;

    /**
     * 接受访问者访问
     * 这是访问者模式的核心方法，每个具体节点都会调用对应的Visit方法
     */
    virtual void Accept(ASTVisitor* visitor) = 0;
};

/**
 * 表达式基类
 *
 * 表达式是SQL中可以求值的部分，包括：
 * - 常量值 (123, 'hello', true)
 * - 列引用 (table.column, column)
 * - 二元操作 (a + b, a > b, a AND b)
 * - 一元操作 (NOT a, -a)
 * - 函数调用 (COUNT(*), SUM(price))
 */
class Expression : public ASTNode {
   public:
    /**
     * 表达式类型枚举
     * 用于运行时类型识别和类型安全的向下转换
     */
    enum class ExprType {
        CONSTANT,      // 常量表达式
        COLUMN_REF,    // 列引用表达式
        BINARY_OP,     // 二元操作表达式
        UNARY_OP,      // 一元操作表达式
        FUNCTION_CALL  // 函数调用表达式（暂未实现）
    };

    /**
     * 获取表达式的具体类型
     * 用于RTTI（运行时类型识别）
     */
    virtual ExprType GetType() const = 0;
};

/**
 * 常量表达式
 *
 * 表示SQL中的字面值常量，如：
 * - 数字：123, 45.67
 * - 字符串：'hello world'
 * - 布尔值：TRUE, FALSE
 * - NULL值：NULL
 *
 * 设计要点：
 * - 使用Value类型存储，支持多种数据类型
 * - 在解析阶段就确定值，执行时直接返回
 */
class ConstantExpression : public Expression {
   public:
    explicit ConstantExpression(const Value& value) : value_(value) {}

    ExprType GetType() const override { return ExprType::CONSTANT; }
    void Accept(ASTVisitor* visitor) override;

    /**
     * 获取常量值
     * @return 存储的常量值
     */
    const Value& GetValue() const { return value_; }

   private:
    Value value_;  // 存储的常量值
};

/**
 * 列引用表达式
 *
 * 表示对表中某列的引用，如：
 * - users.name (带表名的列引用)
 * - age (不带表名的列引用)
 *
 * 使用场景：
 * - SELECT子句中的列选择
 * - WHERE子句中的条件判断
 * - UPDATE子句中的列更新
 *
 * 设计要点：
 * - table_name可能为空（当只有列名时）
 * - 在查询执行时需要根据schema解析成具体的列信息
 */
class ColumnRefExpression : public Expression {
   public:
    ColumnRefExpression(const std::string& table, const std::string& column)
        : table_name_(table), column_name_(column) {}

    ExprType GetType() const override { return ExprType::COLUMN_REF; }
    void Accept(ASTVisitor* visitor) override;

    const std::string& GetTableName() const { return table_name_; }
    const std::string& GetColumnName() const { return column_name_; }

   private:
    std::string table_name_;   // 表名，可能为空
    std::string column_name_;  // 列名
};

/**
 * 语句基类
 *
 * SQL语句的基类，语句是可以执行的SQL命令单元
 * 与表达式不同，语句不返回值，而是执行某种操作
 */
class Statement : public ASTNode {
   public:
    /**
     * 语句类型枚举
     * 涵盖了数据库系统支持的所有主要SQL语句类型
     */
    enum class StmtType {
        SELECT,        // 查询语句
        INSERT,        // 插入语句
        UPDATE,        // 更新语句
        DELETE,        // 删除语句
        CREATE_TABLE,  // 创建表语句
        DROP_TABLE,    // 删除表语句
        CREATE_INDEX,  // 创建索引语句
        DROP_INDEX,    // 删除索引语句
        SHOW_TABLES,   // 显示表语句
        BEGIN_TXN,     // 开始事务
        COMMIT_TXN,    // 提交事务
        ROLLBACK_TXN,  // 回滚事务
        EXPLAIN        // 执行计划解释
    };

    /**
     * 获取语句的具体类型
     */
    virtual StmtType GetType() const = 0;
};

/**
 * SELECT查询语句
 *
 * 表示SQL的SELECT语句，支持：
 * - 列选择（SELECT列表）
 * - 表指定（FROM子句）
 * - 条件过滤（WHERE子句）
 *
 * 当前限制：
 * - 只支持单表查询，不支持JOIN
 * - 不支持GROUP BY、ORDER BY、LIMIT等子句
 *
 * 示例SQL：
 * SELECT id, name FROM users WHERE age > 18;
 */
class SelectStatement : public Statement {
   public:
    SelectStatement(std::vector<std::unique_ptr<Expression>> select_list,
                    std::string table_name,
                    std::unique_ptr<Expression> where_clause = nullptr)
        : select_list_(std::move(select_list)),
          table_name_(std::move(table_name)),
          where_clause_(std::move(where_clause)) {}

    StmtType GetType() const override { return StmtType::SELECT; }
    void Accept(ASTVisitor* visitor) override;

    const std::vector<std::unique_ptr<Expression>>& GetSelectList() const {
        return select_list_;
    }
    const std::string& GetTableName() const { return table_name_; }
    Expression* GetWhereClause() const { return where_clause_.get(); }

   private:
    std::vector<std::unique_ptr<Expression>>
        select_list_;                           // SELECT子句的列表达式
    std::string table_name_;                    // FROM子句的表名
    std::unique_ptr<Expression> where_clause_;  // WHERE子句，可选
};

/**
 * CREATE TABLE建表语句
 *
 * 用于创建新表，包含表名和列定义
 *
 * 支持的列属性：
 * - 数据类型（INT, VARCHAR, BOOLEAN等）
 * - 主键约束（PRIMARY KEY）
 * - 非空约束（NOT NULL）
 * - 长度限制（VARCHAR(50)）
 *
 * 示例SQL：
 * CREATE TABLE users (
 *     id INT PRIMARY KEY,
 *     name VARCHAR(50) NOT NULL,
 *     age INT
 * );
 */
class CreateTableStatement : public Statement {
   public:
    CreateTableStatement(const std::string& table_name,
                         std::vector<Column> columns)
        : table_name_(table_name), columns_(std::move(columns)) {}

    StmtType GetType() const override { return StmtType::CREATE_TABLE; }
    void Accept(ASTVisitor* visitor) override;

    const std::string& GetTableName() const { return table_name_; }
    const std::vector<Column>& GetColumns() const { return columns_; }

   private:
    std::string table_name_;       // 要创建的表名
    std::vector<Column> columns_;  // 列定义列表
};

/**
 * INSERT插入语句
 *
 * 用于向表中插入新记录，支持多行插入
 *
 * 当前限制：
 * - 必须为所有列提供值（不支持部分列插入）
 * - 值的顺序必须与表定义的列顺序一致
 *
 * 示例SQL：
 * INSERT INTO users VALUES
 *     (1, 'Alice', 25),
 *     (2, 'Bob', 30);
 */
class InsertStatement : public Statement {
   public:
    InsertStatement(const std::string& table_name,
                    std::vector<std::vector<Value>> values)
        : table_name_(table_name), values_(std::move(values)) {}

    StmtType GetType() const override { return StmtType::INSERT; }
    void Accept(ASTVisitor* visitor) override;

    const std::string& GetTableName() const { return table_name_; }
    const std::vector<std::vector<Value>>& GetValues() const { return values_; }

   private:
    std::string table_name_;                  // 目标表名
    std::vector<std::vector<Value>> values_;  // 要插入的数据，支持多行
};

/**
 * DROP TABLE删除表语句
 *
 * 用于删除整个表及其所有数据和相关索引
 * 这是一个危险操作，会永久删除数据
 *
 * 示例SQL：
 * DROP TABLE users;
 */
class DropTableStatement : public Statement {
   public:
    explicit DropTableStatement(const std::string& table_name)
        : table_name_(table_name) {}

    StmtType GetType() const override { return StmtType::DROP_TABLE; }
    void Accept(ASTVisitor* visitor) override;

    const std::string& GetTableName() const { return table_name_; }

   private:
    std::string table_name_;  // 要删除的表名
};

/**
 * CREATE INDEX创建索引语句
 *
 * 用于在表的指定列上创建B+树索引，提高查询性能
 *
 * 当前支持：
 * - 单列索引
 * - 唯一索引名称
 *
 * 未来扩展：
 * - 复合索引（多列）
 * - 唯一索引
 * - 部分索引
 *
 * 示例SQL：
 * CREATE INDEX idx_name ON users(name);
 */
class CreateIndexStatement : public Statement {
   public:
    CreateIndexStatement(const std::string& index_name,
                         const std::string& table_name,
                         const std::vector<std::string>& key_columns)
        : index_name_(index_name),
          table_name_(table_name),
          key_columns_(key_columns) {}

    StmtType GetType() const override { return StmtType::CREATE_INDEX; }
    void Accept(ASTVisitor* visitor) override;

    const std::string& GetIndexName() const { return index_name_; }
    const std::string& GetTableName() const { return table_name_; }
    const std::vector<std::string>& GetKeyColumns() const {
        return key_columns_;
    }

   private:
    std::string index_name_;                // 索引名称
    std::string table_name_;                // 目标表名
    std::vector<std::string> key_columns_;  // 索引列名列表
};

/**
 * DROP INDEX删除索引语句
 *
 * 用于删除指定的索引，释放存储空间
 * 删除索引不会影响表数据，但会降低查询性能
 *
 * 示例SQL：
 * DROP INDEX idx_name;
 */
class DropIndexStatement : public Statement {
   public:
    explicit DropIndexStatement(const std::string& index_name)
        : index_name_(index_name) {}

    StmtType GetType() const override { return StmtType::DROP_INDEX; }
    void Accept(ASTVisitor* visitor) override;

    const std::string& GetIndexName() const { return index_name_; }

   private:
    std::string index_name_;  // 要删除的索引名称
};

/**
 * 二元操作表达式
 *
 * 表示两个操作数之间的运算，包括：
 * - 比较操作：=, !=, <, >, <=, >=
 * - 逻辑操作：AND, OR
 * - 算术操作：+, -, *, /
 *
 * 设计思路：
 * - 左右操作数都是Expression，支持嵌套运算
 * - 在执行时会递归求值左右操作数，然后执行相应运算
 * - 比较操作返回布尔值，算术操作返回数值
 */
class BinaryOpExpression : public Expression {
   public:
    /**
     * 二元操作符类型枚举
     * 注意：这里有一些不属于二元操作的类型（如SHOW_TABLES等），
     * 可能是设计时的遗留问题，需要后续重构
     */
    enum class OpType {
        // 比较操作符
        EQUALS,          // =
        NOT_EQUALS,      // != or <>
        LESS_THAN,       //
        GREATER_THAN,    // >
        LESS_EQUALS,     // <=
        GREATER_EQUALS,  // >=

        // 逻辑操作符
        AND,  // AND
        OR,   // OR

        // 算术操作符
        PLUS,      // +
        MINUS,     // -
        MULTIPLY,  // *
        DIVIDE,    // /

    };

    BinaryOpExpression(std::unique_ptr<Expression> left, OpType op,
                       std::unique_ptr<Expression> right)
        : left_(std::move(left)), op_(op), right_(std::move(right)) {}

    ExprType GetType() const override { return ExprType::BINARY_OP; }
    void Accept(ASTVisitor* visitor) override;

    Expression* GetLeft() const { return left_.get(); }
    Expression* GetRight() const { return right_.get(); }
    OpType GetOperator() const { return op_; }

   private:
    std::unique_ptr<Expression> left_;   // 左操作数
    OpType op_;                          // 操作符
    std::unique_ptr<Expression> right_;  // 右操作数
};

/**
 * UPDATE子句结构
 *
 * 表示UPDATE语句中的单个赋值操作
 * 如：SET name = 'John', age = 25
 * 每个UpdateClause代表一个 column_name = value 对
 */
struct UpdateClause {
    std::string column_name;            // 要更新的列名
    std::unique_ptr<Expression> value;  // 新值表达式

    UpdateClause(const std::string& col, std::unique_ptr<Expression> val)
        : column_name(col), value(std::move(val)) {}
};

/**
 * UPDATE更新语句
 *
 * 用于修改表中已存在记录的数据
 *
 * 支持特性：
 * - 多列同时更新
 * - WHERE子句过滤要更新的记录
 * - 表达式作为新值（不仅仅是常量）
 *
 * 示例SQL：
 * UPDATE users SET name = 'John', age = age + 1 WHERE id = 1;
 */
class UpdateStatement : public Statement {
   public:
    UpdateStatement(const std::string& table_name,
                    std::vector<UpdateClause> update_clauses,
                    std::unique_ptr<Expression> where_clause = nullptr)
        : table_name_(table_name),
          update_clauses_(std::move(update_clauses)),
          where_clause_(std::move(where_clause)) {}

    StmtType GetType() const override { return StmtType::UPDATE; }
    void Accept(ASTVisitor* visitor) override;

    const std::string& GetTableName() const { return table_name_; }
    const std::vector<UpdateClause>& GetUpdateClauses() const {
        return update_clauses_;
    }
    Expression* GetWhereClause() const { return where_clause_.get(); }

   private:
    std::string table_name_;                    // 目标表名
    std::vector<UpdateClause> update_clauses_;  // 更新子句列表
    std::unique_ptr<Expression> where_clause_;  // WHERE过滤条件，可选
};

/**
 * DELETE删除语句
 *
 * 用于删除表中满足条件的记录
 *
 * 注意：
 * - 如果没有WHERE子句，会删除表中所有记录（危险操作）
 * - 删除操作会同时更新相关的索引
 *
 * 示例SQL：
 * DELETE FROM users WHERE age < 18;
 */
class DeleteStatement : public Statement {
   public:
    DeleteStatement(const std::string& table_name,
                    std::unique_ptr<Expression> where_clause = nullptr)
        : table_name_(table_name), where_clause_(std::move(where_clause)) {}

    StmtType GetType() const override { return StmtType::DELETE; }
    void Accept(ASTVisitor* visitor) override;

    const std::string& GetTableName() const { return table_name_; }
    Expression* GetWhereClause() const { return where_clause_.get(); }

   private:
    std::string table_name_;                    // 目标表名
    std::unique_ptr<Expression> where_clause_;  // WHERE过滤条件，可选
};

/**
 * 一元操作表达式
 *
 * 表示只有一个操作数的运算，包括：
 * - 逻辑NOT：NOT condition
 * - 数值取负：-value
 *
 * 使用场景：
 * - WHERE NOT (age > 18)
 * - SELECT -price FROM products
 */
class UnaryOpExpression : public Expression {
   public:
    /**
     * 一元操作符类型枚举
     */
    enum class OpType {
        NOT,      // 逻辑非
        NEGATIVE  // 数值取负
    };

    UnaryOpExpression(OpType op, std::unique_ptr<Expression> operand)
        : op_(op), operand_(std::move(operand)) {}

    ExprType GetType() const override { return ExprType::UNARY_OP; }
    void Accept(ASTVisitor* visitor) override;

    OpType GetOperator() const { return op_; }
    Expression* GetOperand() const { return operand_.get(); }

   private:
    OpType op_;                            // 操作符
    std::unique_ptr<Expression> operand_;  // 操作数
};

/**
 * SHOW TABLES语句
 *
 * 用于显示数据库中所有表的信息
 * 这是一个管理命令，不需要额外参数
 *
 * 示例SQL：
 * SHOW TABLES;
 */
class ShowTablesStatement : public Statement {
   public:
    ShowTablesStatement() = default;
    StmtType GetType() const override { return StmtType::SHOW_TABLES; }
    void Accept(ASTVisitor* visitor) override;
};

/**
 * BEGIN事务开始语句
 *
 * 开启一个新的数据库事务
 * 在事务中的所有操作要么全部成功，要么全部回滚
 *
 * 示例SQL：
 * BEGIN;
 */
class BeginStatement : public Statement {
   public:
    BeginStatement() = default;
    StmtType GetType() const override { return StmtType::BEGIN_TXN; }
    void Accept(ASTVisitor* visitor) override;
};

/**
 * COMMIT事务提交语句
 *
 * 提交当前事务，使所有变更永久生效
 *
 * 示例SQL：
 * COMMIT;
 */
class CommitStatement : public Statement {
   public:
    CommitStatement() = default;
    StmtType GetType() const override { return StmtType::COMMIT_TXN; }
    void Accept(ASTVisitor* visitor) override;
};

/**
 * ROLLBACK事务回滚语句
 *
 * 回滚当前事务，撤销所有未提交的变更
 *
 * 示例SQL：
 * ROLLBACK;
 */
class RollbackStatement : public Statement {
   public:
    RollbackStatement() = default;
    StmtType GetType() const override { return StmtType::ROLLBACK_TXN; }
    void Accept(ASTVisitor* visitor) override;
};

/**
 * EXPLAIN执行计划解释语句
 *
 * 显示SQL语句的执行计划，用于查询优化和性能分析
 * 包装了一个要解释的SQL语句
 *
 * 示例SQL：
 * EXPLAIN SELECT * FROM users WHERE age > 18;
 */
class ExplainStatement : public Statement {
   public:
    explicit ExplainStatement(std::unique_ptr<Statement> stmt)
        : statement_(std::move(stmt)) {}

    StmtType GetType() const override { return StmtType::EXPLAIN; }
    void Accept(ASTVisitor* visitor) override;

    Statement* GetStatement() const { return statement_.get(); }

   private:
    std::unique_ptr<Statement> statement_;  // 要解释的语句
};

/**
 * AST访问者接口
 *
 * 访问者模式的核心接口，定义了访问每种AST节点的方法
 *
 * 使用访问者模式的优势：
 * 1. 在不修改AST节点类的情况下添加新操作
 * 2. 将算法从数据结构中分离
 * 3. 可以轻松实现多种遍历策略
 *
 * 常见的访问者实现：
 * - 代码生成器：将AST转换为执行计划
 * - 优化器：对AST进行查询优化
 * - 类型检查器：验证表达式类型
 * - 打印器：将AST转换为可读格式
 */
class ASTVisitor {
   public:
    virtual ~ASTVisitor() = default;

    // 表达式节点访问方法
    virtual void Visit(ConstantExpression* expr) = 0;
    virtual void Visit(ColumnRefExpression* expr) = 0;
    virtual void Visit(BinaryOpExpression* expr) = 0;
    virtual void Visit(UnaryOpExpression* expr) = 0;

    // 语句节点访问方法
    virtual void Visit(SelectStatement* stmt) = 0;
    virtual void Visit(InsertStatement* stmt) = 0;
    virtual void Visit(UpdateStatement* stmt) = 0;
    virtual void Visit(DeleteStatement* stmt) = 0;
    virtual void Visit(CreateTableStatement* stmt) = 0;
    virtual void Visit(DropTableStatement* stmt) = 0;
    virtual void Visit(CreateIndexStatement* stmt) = 0;
    virtual void Visit(DropIndexStatement* stmt) = 0;
    virtual void Visit(ShowTablesStatement* stmt) = 0;
    virtual void Visit(BeginStatement* stmt) = 0;
    virtual void Visit(CommitStatement* stmt) = 0;
    virtual void Visit(RollbackStatement* stmt) = 0;
    virtual void Visit(ExplainStatement* stmt) = 0;
};

}  // namespace SimpleRDBMS