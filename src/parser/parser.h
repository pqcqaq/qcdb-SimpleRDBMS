/*
 * 文件: parser.h
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述: SQL语法分析器头文件，使用递归下降方法将token序列转换为抽象语法树
 */

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "parser/ast.h"
#include "parser/lexer.h"

namespace SimpleRDBMS {

/**
 * SQL语法分析器类
 *
 * 这是SQL编译前端的第二个阶段，负责将词法分析器产生的token序列
 * 转换成抽象语法树(AST)。
 *
 * 核心设计思路：
 * 1. 使用递归下降解析法 - 简单直观，易于理解和维护
 * 2. 每个语法规则对应一个解析方法
 * 3. 按照操作符优先级组织表达式解析方法
 * 4. 支持错误恢复和详细的错误报告
 * 5. 生成类型化的AST节点，便于后续分析
 *
 * 解析策略：
 * - 自顶向下的语法分析
 * - 支持SQL的主要语句类型（DDL/DML/TCL）
 * - 表达式解析遵循标准的运算符优先级
 * - 使用前看一个token的LL(1)解析
 *
 * 当前支持的SQL语法：
 * - SELECT语句（单表查询，支持WHERE子句）
 * - INSERT语句（VALUES子句，支持多行插入）
 * - UPDATE语句（支持SET多列，WHERE子句）
 * - DELETE语句（支持WHERE子句）
 * - CREATE/DROP TABLE语句
 * - CREATE/DROP INDEX语句
 * - 事务控制语句（BEGIN/COMMIT/ROLLBACK）
 * - 管理命令（SHOW TABLES, EXPLAIN）
 */
class Parser {
   public:
    /**
     * 构造函数
     * @param sql 要解析的SQL文本
     *
     * 初始化词法分析器并读取第一个token
     * 这样后续的解析方法就可以直接使用current_token_
     */
    explicit Parser(const std::string& sql);

    /**
     * 解析SQL语句的主入口方法
     * @return 解析得到的Statement AST节点
     * @throws Exception 如果遇到语法错误
     *
     * 工作流程：
     * 1. 调用ParseStatement()解析具体的语句类型
     * 2. 检查语句结束条件（EOF或分号）
     * 3. 返回完整的AST树
     *
     * 错误处理：
     * - 语法错误时抛出Exception并包含错误位置信息
     * - 提供详细的错误消息帮助调试
     */
    std::unique_ptr<Statement> Parse();

    /**
     * 重新设置要解析的SQL文本
     * @param sql 新的SQL文本
     *
     * 这个方法允许复用同一个Parser实例解析多个SQL语句
     * 主要用于交互式SQL环境或批处理场景
     */
    void SetQuery(const std::string& sql) {
        lexer_ = Lexer(sql);
        Advance();  // 读取第一个token
    }

   private:
    Lexer lexer_;          // 词法分析器实例
    Token current_token_;  // 当前正在处理的token

    // ==================== 基础解析工具方法 ====================

    /**
     * 前进到下一个token
     * 更新current_token_为下一个token
     * 这是解析过程中最基本的操作
     */
    void Advance();

    /**
     * 尝试匹配指定类型的token
     * @param type 期望的token类型
     * @return 如果匹配成功返回true并自动前进，否则返回false
     *
     * 这是可选匹配，不会在失败时抛出异常
     * 常用于处理可选的语法元素
     */
    bool Match(TokenType type);

    /**
     * 期望特定类型的token，如果不匹配则抛出异常
     * @param type 期望的token类型
     * @throws Exception 如果当前token类型不匹配
     *
     * 这是强制匹配，用于处理必需的语法元素
     */
    void Expect(TokenType type);

    // ==================== 语句解析方法 ====================
    // 每个方法对应一种SQL语句类型的解析

    /**
     * 解析SQL语句（顶层分发方法）
     * @return Statement AST节点
     *
     * 根据当前token的类型决定调用哪个具体的语句解析方法
     * 支持的语句类型：
     * - SELECT, INSERT, UPDATE, DELETE
     * - CREATE TABLE/INDEX, DROP TABLE/INDEX
     * - BEGIN, COMMIT, ROLLBACK
     * - SHOW TABLES, EXPLAIN
     */
    std::unique_ptr<Statement> ParseStatement();

    /**
     * 解析SELECT查询语句
     * 语法：SELECT column_list FROM table_name [WHERE condition]
     * @return SelectStatement AST节点
     *
     * 当前限制：
     * - 只支持单表查询，不支持JOIN
     * - 不支持GROUP BY, ORDER BY, LIMIT等子句
     * - 支持SELECT *和指定列列表
     */
    std::unique_ptr<Statement> ParseSelectStatement();

    /**
     * 解析CREATE TABLE语句
     * 语法：CREATE TABLE table_name (column_definitions)
     * @return CreateTableStatement AST节点
     *
     * 支持的列定义：
     * - 基本数据类型（INT, VARCHAR, FLOAT等）
     * - NOT NULL约束
     * - PRIMARY KEY约束
     * - VARCHAR长度限制
     */
    std::unique_ptr<Statement> ParseCreateTableStatement();

    /**
     * 解析DROP TABLE语句
     * 语法：DROP TABLE table_name
     * @return DropTableStatement AST节点
     */
    std::unique_ptr<Statement> ParseDropTableStatement();

    /**
     * 解析INSERT语句
     * 语法：INSERT INTO table_name [(column_list)] VALUES (value_list) [, ...]
     * @return InsertStatement AST节点
     *
     * 支持特性：
     * - 多行插入
     * - 可选的列名列表（当前解析但不使用）
     * - 只支持常量值，不支持表达式
     */
    std::unique_ptr<Statement> ParseInsertStatement();

    /**
     * 解析UPDATE语句
     * 语法：UPDATE table_name SET column=value [, ...] [WHERE condition]
     * @return UpdateStatement AST节点
     */
    std::unique_ptr<Statement> ParseUpdateStatement();

    /**
     * 解析DELETE语句
     * 语法：DELETE FROM table_name [WHERE condition]
     * @return DeleteStatement AST节点
     */
    std::unique_ptr<Statement> ParseDeleteStatement();

    /**
     * 解析SHOW TABLES语句
     * 语法：SHOW TABLES
     * @return ShowTablesStatement AST节点
     */
    std::unique_ptr<Statement> ParseShowTablesStatement();

    /**
     * 解析BEGIN事务开始语句
     * 语法：BEGIN
     * @return BeginStatement AST节点
     */
    std::unique_ptr<Statement> ParseBeginStatement();

    /**
     * 解析COMMIT事务提交语句
     * 语法：COMMIT
     * @return CommitStatement AST节点
     */
    std::unique_ptr<Statement> ParseCommitStatement();

    /**
     * 解析ROLLBACK事务回滚语句
     * 语法：ROLLBACK
     * @return RollbackStatement AST节点
     */
    std::unique_ptr<Statement> ParseRollbackStatement();

    /**
     * 解析EXPLAIN执行计划语句
     * 语法：EXPLAIN statement
     * @return ExplainStatement AST节点
     */
    std::unique_ptr<Statement> ParseExplainStatement();

    /**
     * 解析CREATE INDEX语句
     * 语法：CREATE INDEX index_name ON table_name (column_list)
     * @return CreateIndexStatement AST节点
     */
    std::unique_ptr<Statement> ParseCreateIndexStatement();

    /**
     * 解析DROP INDEX语句
     * 语法：DROP INDEX index_name [ON table_name]
     * @return DropIndexStatement AST节点
     */
    std::unique_ptr<Statement> ParseDropIndexStatement();

    // ==================== 表达式解析方法 ====================
    // 按照操作符优先级从低到高排列，使用递归下降解析

    /**
     * 解析表达式（入口方法）
     * @return Expression AST节点
     *
     * 从最低优先级的OR表达式开始解析
     * 这是表达式解析的统一入口点
     */
    std::unique_ptr<Expression> ParseExpression();

    /**
     * 解析OR逻辑表达式
     * 语法：and_expr [OR and_expr]*
     * @return Expression AST节点
     *
     * OR是优先级最低的逻辑操作符，所以在最顶层
     */
    std::unique_ptr<Expression> ParseOrExpression();

    /**
     * 解析AND逻辑表达式
     * 语法：comparison_expr [AND comparison_expr]*
     * @return Expression AST节点
     *
     * AND的优先级高于OR，低于比较操作符
     */
    std::unique_ptr<Expression> ParseAndExpression();

    /**
     * 解析比较表达式
     * 语法：arithmetic_expr [comparison_op arithmetic_expr]
     * @return Expression AST节点
     *
     * 比较操作符：=, !=, <, >, <=, >=
     * 注意：比较操作不支持连续比较（如 a < b < c）
     */
    std::unique_ptr<Expression> ParseComparisonExpression();

    /**
     * 解析算术表达式（加法减法层级）
     * 语法：term_expr [(+|-) term_expr]*
     * @return Expression AST节点
     *
     * 处理加法和减法，它们是左结合的
     * 优先级低于乘除法，高于比较操作
     */
    std::unique_ptr<Expression> ParseArithmeticExpression();

    /**
     * 解析项表达式（乘法除法层级）
     * 语法：unary_expr [(*|/) unary_expr]*
     * @return Expression AST节点
     *
     * 处理乘法和除法，优先级高于加减法
     * 同样是左结合的操作符
     */
    std::unique_ptr<Expression> ParseTermExpression();

    /**
     * 解析一元表达式
     * 语法：[NOT|'-'] primary_expr
     * @return Expression AST节点
     *
     * 处理一元操作符：
     * - NOT：逻辑否定
     * - -：数值取负
     * 一元操作符的优先级最高
     */
    std::unique_ptr<Expression> ParseUnaryExpression();

    /**
     * 解析基本表达式
     * @return Expression AST节点
     *
     * 解析最基本的表达式元素：
     * - 字面量常量（数字、字符串、布尔值）
     * - 列引用（table.column 或 column）
     * - 括号表达式 (expression)
     *
     * 这是表达式递归的终点
     */
    std::unique_ptr<Expression> ParsePrimaryExpression();

    // ==================== 辅助解析方法 ====================

    /**
     * 解析列定义列表
     * 语法：(column_name data_type [constraints], ...)
     * @return Column对象的vector
     *
     * 用于CREATE TABLE语句中的列定义解析
     * 支持的约束：NOT NULL, PRIMARY KEY
     * 支持的数据类型：INT, VARCHAR, FLOAT等
     */
    std::vector<Column> ParseColumnDefinitions();

    /**
     * 解析数据类型
     * @return TypeId枚举值
     * @throws Exception 如果数据类型无效
     *
     * 支持的数据类型：
     * - INT/INTEGER：32位整数
     * - BIGINT：64位整数
     * - FLOAT：单精度浮点数
     * - DOUBLE：双精度浮点数
     * - VARCHAR：变长字符串
     * - BOOLEAN/BOOL：布尔值
     */
    TypeId ParseDataType();
};

}  // namespace SimpleRDBMS