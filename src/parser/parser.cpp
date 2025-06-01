/*
 * 文件: parser.cpp
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述: SQL语法分析器实现，包含词法分析和语法分析功能，将SQL文本转换为AST
 */

#include "parser/parser.h"

#include <algorithm>
#include <cctype>
#include <unordered_map>

#include "common/exception.h"

namespace SimpleRDBMS {

// ==================== AST节点Accept方法实现 ====================
// 这些方法实现了访问者模式，每个AST节点都调用对应的Visit方法

void ConstantExpression::Accept(ASTVisitor* visitor) { visitor->Visit(this); }

void ColumnRefExpression::Accept(ASTVisitor* visitor) { visitor->Visit(this); }

void SelectStatement::Accept(ASTVisitor* visitor) { visitor->Visit(this); }

void InsertStatement::Accept(ASTVisitor* visitor) { visitor->Visit(this); }

void CreateTableStatement::Accept(ASTVisitor* visitor) { visitor->Visit(this); }

void DropTableStatement::Accept(ASTVisitor* visitor) { visitor->Visit(this); }

void CreateIndexStatement::Accept(ASTVisitor* visitor) { visitor->Visit(this); }

void DropIndexStatement::Accept(ASTVisitor* visitor) { visitor->Visit(this); }

void BinaryOpExpression::Accept(ASTVisitor* visitor) { visitor->Visit(this); }

void UpdateStatement::Accept(ASTVisitor* visitor) { visitor->Visit(this); }

void DeleteStatement::Accept(ASTVisitor* visitor) { visitor->Visit(this); }

void UnaryOpExpression::Accept(ASTVisitor* visitor) { visitor->Visit(this); }

// ==================== 词法分析器实现 ====================

/**
 * SQL关键字映射表
 *
 * 将SQL关键字字符串映射到对应的Token类型
 * 使用static确保只初始化一次，提高性能
 *
 * 设计要点：
 * - 支持常见的数据类型别名（如INTEGER -> INT, BOOL -> BOOLEAN）
 * - 包含布尔字面量TRUE/FALSE
 * - 涵盖了基本的DDL/DML/TCL命令
 */
static std::unordered_map<std::string, TokenType> keywords = {
    // DML查询相关
    {"SELECT", TokenType::SELECT},
    {"FROM", TokenType::FROM},
    {"WHERE", TokenType::WHERE},

    // DML数据操作
    {"INSERT", TokenType::INSERT},
    {"INTO", TokenType::INTO},
    {"VALUES", TokenType::VALUES},
    {"UPDATE", TokenType::UPDATE},
    {"SET", TokenType::SET},
    {"DELETE", TokenType::DELETE},

    // DDL表结构操作
    {"CREATE", TokenType::CREATE},
    {"TABLE", TokenType::TABLE},
    {"DROP", TokenType::DROP},
    {"INDEX", TokenType::INDEX},
    {"ON", TokenType::ON},

    // 约束和属性
    {"PRIMARY", TokenType::PRIMARY},
    {"KEY", TokenType::KEY},
    {"NOT", TokenType::NOT},
    {"NULL", TokenType::_NULL},

    // 数据类型（包含别名）
    {"INT", TokenType::INT},
    {"INTEGER", TokenType::INT},  // INTEGER是INT的别名
    {"BIGINT", TokenType::BIGINT},
    {"VARCHAR", TokenType::VARCHAR},
    {"FLOAT", TokenType::FLOAT},
    {"DOUBLE", TokenType::DOUBLE},
    {"BOOLEAN", TokenType::BOOLEAN},
    {"BOOL", TokenType::BOOLEAN},  // BOOL是BOOLEAN的别名

    // 逻辑操作符
    {"AND", TokenType::AND},
    {"OR", TokenType::OR},

    // 布尔字面量
    {"TRUE", TokenType::BOOLEAN_LITERAL},
    {"FALSE", TokenType::BOOLEAN_LITERAL},

    // 管理命令
    {"SHOW", TokenType::SHOW},
    {"TABLES", TokenType::TABLES},

    // 事务控制
    {"BEGIN", TokenType::BEGIN},
    {"COMMIT", TokenType::COMMIT},
    {"ROLLBACK", TokenType::ROLLBACK},

    // 查询计划
    {"EXPLAIN", TokenType::EXPLAIN},
};

/**
 * 词法分析器构造函数
 * 初始化扫描状态为起始位置
 */
Lexer::Lexer(const std::string& input)
    : input_(input), position_(0), line_(1), column_(1) {}

/**
 * 查看当前字符但不前进位置
 * @return 当前字符，如果已到文件末尾返回'\0'
 */
char Lexer::Peek() {
    if (position_ >= input_.size()) {
        return '\0';
    }
    return input_[position_];
}

/**
 * 读取当前字符并前进到下一个位置
 * 同时维护行号和列号信息，用于错误报告
 * @return 读取的字符
 */
char Lexer::Advance() {
    if (position_ >= input_.size()) {
        return '\0';
    }
    char ch = input_[position_++];

    // 遇到换行符时更新行号，重置列号
    if (ch == '\n') {
        line_++;
        column_ = 1;
    } else {
        column_++;
    }
    return ch;
}

/**
 * 跳过空白字符
 * 空白字符包括空格、制表符、换行符等，在SQL中只起分隔作用
 */
void Lexer::SkipWhitespace() {
    while (std::isspace(Peek())) {
        Advance();
    }
}

/**
 * 扫描数字字面量
 * 支持整数和浮点数的识别
 * @return 数字token（INTEGER_LITERAL或FLOAT_LITERAL）
 *
 * 实现逻辑：
 * 1. 扫描连续的数字字符
 * 2. 如果遇到小数点，标记为浮点数并继续扫描
 * 3. 最多只允许一个小数点
 */
Token Lexer::ScanNumber() {
    Token token;
    token.line = line_;
    token.column = column_;

    std::string value;
    bool has_dot = false;  // 是否已经遇到小数点

    // 扫描数字和小数点
    while (std::isdigit(Peek()) || Peek() == '.') {
        if (Peek() == '.') {
            // 如果已经有小数点了，就停止扫描
            if (has_dot) break;
            has_dot = true;
        }
        value += Advance();
    }

    token.value = value;
    // 根据是否有小数点决定token类型
    token.type =
        has_dot ? TokenType::FLOAT_LITERAL : TokenType::INTEGER_LITERAL;
    return token;
}

/**
 * 扫描字符串字面量
 * 支持单引号和双引号，以及转义字符处理
 * @return 字符串token
 *
 * 转义字符支持：
 * \n -> 换行符
 * \t -> 制表符
 * \r -> 回车符
 * \\ -> 反斜杠
 * \' -> 单引号
 * \" -> 双引号
 */
Token Lexer::ScanString() {
    Token token;
    token.line = line_;
    token.column = column_;
    token.type = TokenType::STRING_LITERAL;

    char quote = Advance();  // 记住开始的引号类型
    std::string value;

    // 扫描直到遇到匹配的结束引号
    while (Peek() != quote && Peek() != '\0') {
        if (Peek() == '\\') {
            // 处理转义字符
            Advance();  // 跳过反斜杠
            char ch = Advance();
            switch (ch) {
                case 'n':
                    value += '\n';
                    break;
                case 't':
                    value += '\t';
                    break;
                case 'r':
                    value += '\r';
                    break;
                case '\\':
                    value += '\\';
                    break;
                case '\'':
                    value += '\'';
                    break;
                case '"':
                    value += '"';
                    break;
                default:
                    // 其他字符直接添加
                    value += ch;
                    break;
            }
        } else {
            value += Advance();
        }
    }

    // 如果找到了结束引号，跳过它
    if (Peek() == quote) {
        Advance();
    }
    // 注意：如果没有找到结束引号，这里应该报错，但当前实现比较宽松

    token.value = value;
    return token;
}

/**
 * 扫描标识符或关键字
 * 标识符由字母、数字、下划线组成，但必须以字母或下划线开头
 * @return 标识符token或关键字token
 *
 * 处理流程：
 * 1. 扫描连续的字母、数字、下划线字符
 * 2. 将结果转换为大写并查找关键字表
 * 3. 如果是关键字，返回对应的关键字token
 * 4. 否则返回IDENTIFIER token
 */
Token Lexer::ScanIdentifier() {
    Token token;
    token.line = line_;
    token.column = column_;

    std::string value;
    // 扫描标识符字符（字母、数字、下划线）
    while (std::isalnum(Peek()) || Peek() == '_') {
        value += Advance();
    }

    // 转换为大写用于关键字查找（SQL关键字不区分大小写）
    std::string upper_value = value;
    std::transform(upper_value.begin(), upper_value.end(), upper_value.begin(),
                   ::toupper);

    // 查找是否为关键字
    auto it = keywords.find(upper_value);
    if (it != keywords.end()) {
        token.type = it->second;
        // 布尔字面量使用大写值，其他关键字保持原始大小写
        if (token.type == TokenType::BOOLEAN_LITERAL) {
            token.value = upper_value;
        } else {
            token.value = value;
        }
    } else {
        // 不是关键字，就是普通标识符
        token.type = TokenType::IDENTIFIER;
        token.value = value;
    }

    return token;
}

/**
 * 获取下一个token
 * 词法分析器的核心方法，实现token的识别和分类
 * @return 下一个token
 *
 * 扫描策略：
 * 1. 跳过空白字符
 * 2. 根据第一个字符判断token类型
 * 3. 调用相应的扫描方法
 * 4. 处理多字符操作符（如<=、>=、!=、<>）
 */
Token Lexer::NextToken() {
    SkipWhitespace();
    Token token;
    token.line = line_;
    token.column = column_;

    char ch = Peek();

    // 文件结束
    if (ch == '\0') {
        token.type = TokenType::EOF_TOKEN;
        return token;
    }

    // 数字字面量
    if (std::isdigit(ch)) {
        return ScanNumber();
    }

    // 标识符或关键字
    if (std::isalpha(ch) || ch == '_') {
        return ScanIdentifier();
    }

    // 字符串字面量
    if (ch == '\'' || ch == '"') {
        return ScanString();
    }

    // 其他单字符或多字符token
    Advance();
    switch (ch) {
        case '(':
            token.type = TokenType::LPAREN;
            token.value = "(";
            break;
        case ')':
            token.type = TokenType::RPAREN;
            token.value = ")";
            break;
        case ',':
            token.type = TokenType::COMMA;
            token.value = ",";
            break;
        case ';':
            token.type = TokenType::SEMICOLON;
            token.value = ";";
            break;
        case '*':
            // 乘法操作符（也用于SELECT *）
            token.type = TokenType::MULTIPLY;
            token.value = "*";
            break;
        case '+':
            token.type = TokenType::PLUS;
            token.value = "+";
            break;
        case '-':
            token.type = TokenType::MINUS;
            token.value = "-";
            break;
        case '/':
            token.type = TokenType::DIVIDE;
            token.value = "/";
            break;
        case '=':
            token.type = TokenType::EQUALS;
            token.value = "=";
            break;
        case '<':
            // 处理 <、<=、<> 三种情况
            if (Peek() == '=') {
                Advance();
                token.type = TokenType::LESS_EQUALS;
                token.value = "<=";
            } else if (Peek() == '>') {
                Advance();
                token.type = TokenType::NOT_EQUALS;
                token.value = "<>";
            } else {
                token.type = TokenType::LESS_THAN;
                token.value = "<";
            }
            break;
        case '>':
            // 处理 >、>= 两种情况
            if (Peek() == '=') {
                Advance();
                token.type = TokenType::GREATER_EQUALS;
                token.value = ">=";
            } else {
                token.type = TokenType::GREATER_THAN;
                token.value = ">";
            }
            break;
        case '!':
            // 处理 != 不等于操作符
            if (Peek() == '=') {
                Advance();
                token.type = TokenType::NOT_EQUALS;
                token.value = "!=";
            } else {
                // 单独的!不是有效的SQL操作符
                token.type = TokenType::INVALID;
                token.value = "!";
            }
            break;
        default:
            // 未识别的字符
            token.type = TokenType::INVALID;
            token.value = std::string(1, ch);
            break;
    }
    return token;
}

// ==================== 语法分析器实现 ====================

/**
 * 语法分析器构造函数
 * 初始化词法分析器并读取第一个token
 */
Parser::Parser(const std::string& sql) : lexer_(sql) { Advance(); }

/**
 * 读取下一个token
 * 更新current_token_为下一个token
 */
void Parser::Advance() { current_token_ = lexer_.NextToken(); }

/**
 * 尝试匹配指定类型的token
 * 如果匹配成功，自动前进到下一个token
 * @param type 期望的token类型
 * @return 是否匹配成功
 */
bool Parser::Match(TokenType type) {
    if (current_token_.type == type) {
        Advance();
        return true;
    }
    return false;
}

/**
 * 期望特定类型的token，如果不匹配则抛出异常
 * @param type 期望的token类型
 * @throws Exception 如果token不匹配
 */
void Parser::Expect(TokenType type) {
    if (!Match(type)) {
        throw Exception("Unexpected token: " + current_token_.value);
    }
}

/**
 * 解析SQL语句的入口方法
 * @return 解析得到的Statement AST节点
 * @throws Exception 如果语法错误
 */
std::unique_ptr<Statement> Parser::Parse() {
    auto stmt = ParseStatement();

    // 确保语句结束时到达文件末尾或分号
    if (current_token_.type != TokenType::EOF_TOKEN &&
        current_token_.type != TokenType::SEMICOLON) {
        throw Exception("Expected end of statement");
    }
    return stmt;
}

/**
 * 解析SELECT查询语句
 * 语法：SELECT column_list FROM table_name [WHERE condition]
 * @return SelectStatement AST节点
 */
std::unique_ptr<Statement> Parser::ParseSelectStatement() {
    Expect(TokenType::SELECT);

    // 解析SELECT列表
    std::vector<std::unique_ptr<Expression>> select_list;
    if (current_token_.type == TokenType::MULTIPLY) {
        // SELECT * 的情况
        Advance();
        select_list.push_back(std::make_unique<ColumnRefExpression>("", "*"));
    } else {
        // SELECT col1, col2, ... 的情况
        do {
            auto expr = ParseExpression();
            select_list.push_back(std::move(expr));
        } while (Match(TokenType::COMMA));
    }

    Expect(TokenType::FROM);

    // 解析表名
    if (current_token_.type != TokenType::IDENTIFIER) {
        throw Exception("Expected table name");
    }
    std::string table_name = current_token_.value;
    Advance();

    // 解析可选的WHERE子句
    std::unique_ptr<Expression> where_clause = nullptr;
    if (Match(TokenType::WHERE)) {
        where_clause = ParseExpression();
    }

    return std::make_unique<SelectStatement>(std::move(select_list), table_name,
                                             std::move(where_clause));
}

/**
 * 解析CREATE TABLE语句
 * 语法：CREATE TABLE table_name (column_definitions)
 * @return CreateTableStatement AST节点
 */
std::unique_ptr<Statement> Parser::ParseCreateTableStatement() {
    Expect(TokenType::TABLE);

    // 解析表名
    if (current_token_.type != TokenType::IDENTIFIER) {
        throw Exception("Expected table name");
    }
    std::string table_name = current_token_.value;
    Advance();

    // 解析列定义
    auto columns = ParseColumnDefinitions();

    return std::make_unique<CreateTableStatement>(table_name,
                                                  std::move(columns));
}

/**
 * 解析INSERT语句
 * 语法：INSERT INTO table_name [(column_list)] VALUES (value_list) [,
 * (value_list)]...
 * @return InsertStatement AST节点
 *
 * 当前限制：
 * - 只支持VALUES子句，不支持INSERT INTO ... SELECT
 * - 如果指定列列表，暂时解析但不使用
 */
std::unique_ptr<Statement> Parser::ParseInsertStatement() {
    LOG_DEBUG("ParseInsertStatement: Starting INSERT statement parsing");

    Expect(TokenType::INSERT);
    Expect(TokenType::INTO);

    // 解析表名
    if (current_token_.type != TokenType::IDENTIFIER) {
        LOG_ERROR("ParseInsertStatement: Expected table name");
        throw Exception("Expected table name");
    }
    std::string table_name = current_token_.value;
    LOG_DEBUG("ParseInsertStatement: Table name: " << table_name);
    Advance();

    // 可选的列名列表（暂时解析但不使用）
    std::vector<std::string> column_names;
    if (Match(TokenType::LPAREN)) {
        LOG_DEBUG("ParseInsertStatement: Parsing column list");
        do {
            if (current_token_.type != TokenType::IDENTIFIER) {
                throw Exception("Expected column name");
            }
            column_names.push_back(current_token_.value);
            Advance();
        } while (Match(TokenType::COMMA));
        Expect(TokenType::RPAREN);
        LOG_DEBUG("ParseInsertStatement: Column list parsed, "
                  << column_names.size() << " columns");
    }

    Expect(TokenType::VALUES);
    LOG_DEBUG("ParseInsertStatement: Parsing VALUES clause");

    // 解析多个值列表：VALUES (1,2,3), (4,5,6), ...
    std::vector<std::vector<Value>> values_list;
    int value_list_count = 0;
    do {
        LOG_DEBUG("ParseInsertStatement: Parsing value list "
                  << value_list_count);
        Expect(TokenType::LPAREN);
        std::vector<Value> values;
        int value_count = 0;
        do {
            LOG_DEBUG("ParseInsertStatement: Parsing value "
                      << value_count << " in list " << value_list_count);
            auto expr = ParsePrimaryExpression();

            // 将表达式转换为值（当前只支持常量）
            if (auto* const_expr =
                    dynamic_cast<ConstantExpression*>(expr.get())) {
                values.push_back(const_expr->GetValue());
                LOG_DEBUG("ParseInsertStatement: Added constant value");
            } else {
                LOG_ERROR(
                    "ParseInsertStatement: Only constant values are supported "
                    "in INSERT");
                throw Exception("Only constant values are supported in INSERT");
            }
            value_count++;
        } while (Match(TokenType::COMMA));
        Expect(TokenType::RPAREN);
        LOG_DEBUG("ParseInsertStatement: Completed value list "
                  << value_list_count << " with " << value_count << " values");
        values_list.push_back(std::move(values));
        value_list_count++;
    } while (Match(TokenType::COMMA));

    LOG_DEBUG("ParseInsertStatement: Completed parsing, " << values_list.size()
                                                          << " value lists");
    return std::make_unique<InsertStatement>(table_name,
                                             std::move(values_list));
}

/**
 * 解析列定义列表
 * 语法：(column_name data_type [constraints], ...)
 * @return 列定义的vector
 *
 * 支持的约束：
 * - NOT NULL
 * - PRIMARY KEY
 *
 * 支持的数据类型：
 * - INT/INTEGER, BIGINT, FLOAT, DOUBLE, BOOLEAN/BOOL
 * - VARCHAR(size)
 */
std::vector<Column> Parser::ParseColumnDefinitions() {
    Expect(TokenType::LPAREN);

    std::vector<Column> columns;
    bool has_primary_key = false;  // 确保只有一个主键

    do {
        Column col;

        // 解析列名
        if (current_token_.type != TokenType::IDENTIFIER) {
            throw Exception("Expected column name");
        }
        col.name = current_token_.value;
        Advance();

        // 解析数据类型
        col.type = ParseDataType();

        // 如果是VARCHAR，解析长度限制
        if (col.type == TypeId::VARCHAR) {
            Expect(TokenType::LPAREN);
            if (current_token_.type != TokenType::INTEGER_LITERAL) {
                throw Exception("Expected varchar size");
            }
            col.size = std::stoi(current_token_.value);
            Advance();
            Expect(TokenType::RPAREN);
        } else {
            col.size = 0;
        }

        // 设置默认属性
        col.nullable = true;
        col.is_primary_key = false;

        // 解析列约束
        while (current_token_.type == TokenType::NOT ||
               current_token_.type == TokenType::PRIMARY) {
            if (Match(TokenType::NOT)) {
                Expect(TokenType::_NULL);
                col.nullable = false;
            } else if (Match(TokenType::PRIMARY)) {
                Expect(TokenType::KEY);
                if (has_primary_key) {
                    throw Exception("Multiple primary keys not allowed");
                }
                col.is_primary_key = true;
                col.nullable = false;  // 主键自动变成NOT NULL
                has_primary_key = true;
            }
        }

        columns.push_back(col);
    } while (Match(TokenType::COMMA));

    Expect(TokenType::RPAREN);
    return columns;
}

/**
 * 解析UPDATE语句
 * 语法：UPDATE table_name SET column=value [, column=value] [WHERE condition]
 * @return UpdateStatement AST节点
 */
std::unique_ptr<Statement> Parser::ParseUpdateStatement() {
    Expect(TokenType::UPDATE);

    // 解析表名
    if (current_token_.type != TokenType::IDENTIFIER) {
        throw Exception("Expected table name");
    }
    std::string table_name = current_token_.value;
    Advance();

    Expect(TokenType::SET);

    // 解析SET子句列表
    std::vector<UpdateClause> update_clauses;
    do {
        // 解析列名
        if (current_token_.type != TokenType::IDENTIFIER) {
            throw Exception("Expected column name");
        }
        std::string column_name = current_token_.value;
        Advance();

        Expect(TokenType::EQUALS);

        // 解析新值表达式
        auto value_expr = ParseExpression();

        update_clauses.emplace_back(column_name, std::move(value_expr));

    } while (Match(TokenType::COMMA));

    // 解析可选的WHERE子句
    std::unique_ptr<Expression> where_clause = nullptr;
    if (Match(TokenType::WHERE)) {
        where_clause = ParseExpression();
    }

    return std::make_unique<UpdateStatement>(
        table_name, std::move(update_clauses), std::move(where_clause));
}

/**
 * 解析DELETE语句
 * 语法：DELETE FROM table_name [WHERE condition]
 * @return DeleteStatement AST节点
 */
std::unique_ptr<Statement> Parser::ParseDeleteStatement() {
    Expect(TokenType::DELETE);
    Expect(TokenType::FROM);

    // 解析表名
    if (current_token_.type != TokenType::IDENTIFIER) {
        throw Exception("Expected table name");
    }
    std::string table_name = current_token_.value;
    Advance();

    // 解析可选的WHERE子句
    std::unique_ptr<Expression> where_clause = nullptr;
    if (Match(TokenType::WHERE)) {
        where_clause = ParseExpression();
    }

    return std::make_unique<DeleteStatement>(table_name,
                                             std::move(where_clause));
}

/**
 * 解析数据类型
 * @return 对应的TypeId枚举值
 * @throws Exception 如果数据类型无效
 */
TypeId Parser::ParseDataType() {
    TypeId type = TypeId::INVALID;

    switch (current_token_.type) {
        case TokenType::INT:
            Advance();
            type = TypeId::INTEGER;
            break;
        case TokenType::BIGINT:
            Advance();
            type = TypeId::BIGINT;
            break;
        case TokenType::VARCHAR:
            Advance();
            type = TypeId::VARCHAR;
            break;
        case TokenType::FLOAT:
            Advance();
            type = TypeId::FLOAT;
            break;
        case TokenType::DOUBLE:
            Advance();
            type = TypeId::DOUBLE;
            break;
        case TokenType::BOOLEAN:
            Advance();
            type = TypeId::BOOLEAN;
            break;
        default:
            throw Exception("Invalid data type");
    }

    return type;
}

// ==================== 表达式解析方法 ====================
// 使用递归下降解析，按照操作符优先级组织

/**
 * 解析表达式（最高层级）
 * 从OR表达式开始，因为OR优先级最低
 */
std::unique_ptr<Expression> Parser::ParseExpression() {
    return ParseOrExpression();
}

/**
 * 解析OR表达式
 * 语法：and_expr [OR and_expr]*
 * OR是优先级最低的逻辑操作符
 */
std::unique_ptr<Expression> Parser::ParseOrExpression() {
    auto left = ParseAndExpression();
    while (Match(TokenType::OR)) {
        auto right = ParseAndExpression();
        left = std::make_unique<BinaryOpExpression>(
            std::move(left), BinaryOpExpression::OpType::OR, std::move(right));
    }
    return left;
}

/**
 * 解析AND表达式
 * 语法：comparison_expr [AND comparison_expr]*
 * AND的优先级高于OR，低于比较操作符
 */
std::unique_ptr<Expression> Parser::ParseAndExpression() {
    auto left = ParseComparisonExpression();
    while (Match(TokenType::AND)) {
        auto right = ParseComparisonExpression();
        left = std::make_unique<BinaryOpExpression>(
            std::move(left), BinaryOpExpression::OpType::AND, std::move(right));
    }
    return left;
}

/**
 * 解析语句（顶层方法）
 * 根据第一个token确定语句类型并调用相应的解析方法
 * @return Statement AST节点
 */
std::unique_ptr<Statement> Parser::ParseStatement() {
    switch (current_token_.type) {
        case TokenType::SELECT:
            return ParseSelectStatement();
        case TokenType::CREATE: {
            Advance();  // 跳过CREATE
            if (current_token_.type == TokenType::TABLE) {
                return ParseCreateTableStatement();
            } else if (current_token_.type == TokenType::INDEX) {
                return ParseCreateIndexStatement();
            } else {
                throw Exception("Expected TABLE or INDEX after CREATE");
            }
        }
        case TokenType::DROP: {
            Advance();
            if (current_token_.type == TokenType::TABLE) {
                return ParseDropTableStatement();
            } else if (current_token_.type == TokenType::INDEX) {
                return ParseDropIndexStatement();
            } else {
                throw Exception("Expected TABLE or INDEX after DROP");
            }
        }
        case TokenType::INSERT:
            return ParseInsertStatement();
        case TokenType::UPDATE:
            return ParseUpdateStatement();
        case TokenType::DELETE:
            return ParseDeleteStatement();
        case TokenType::SHOW:
            return ParseShowTablesStatement();
        case TokenType::BEGIN:
            return ParseBeginStatement();
        case TokenType::COMMIT:
            return ParseCommitStatement();
        case TokenType::ROLLBACK:
            return ParseRollbackStatement();
        case TokenType::EXPLAIN:
            return ParseExplainStatement();
        default:
            throw Exception("Unsupported statement type");
    }
}

/**
 * 解析比较表达式
 * 语法：arithmetic_expr [comparison_op arithmetic_expr]
 * 比较操作符：=, !=, <, >, <=, >=
 */
std::unique_ptr<Expression> Parser::ParseComparisonExpression() {
    auto left = ParseArithmeticExpression();

    // 检查是否有比较操作符
    if (current_token_.type == TokenType::EQUALS ||
        current_token_.type == TokenType::NOT_EQUALS ||
        current_token_.type == TokenType::LESS_THAN ||
        current_token_.type == TokenType::GREATER_THAN ||
        current_token_.type == TokenType::LESS_EQUALS ||
        current_token_.type == TokenType::GREATER_EQUALS) {
        BinaryOpExpression::OpType op;
        switch (current_token_.type) {
            case TokenType::EQUALS:
                op = BinaryOpExpression::OpType::EQUALS;
                break;
            case TokenType::NOT_EQUALS:
                op = BinaryOpExpression::OpType::NOT_EQUALS;
                break;
            case TokenType::LESS_THAN:
                op = BinaryOpExpression::OpType::LESS_THAN;
                break;
            case TokenType::GREATER_THAN:
                op = BinaryOpExpression::OpType::GREATER_THAN;
                break;
            case TokenType::LESS_EQUALS:
                op = BinaryOpExpression::OpType::LESS_EQUALS;
                break;
            case TokenType::GREATER_EQUALS:
                op = BinaryOpExpression::OpType::GREATER_EQUALS;
                break;
            default:
                op = BinaryOpExpression::OpType::EQUALS;
                break;
        }
        Advance();
        auto right = ParseArithmeticExpression();
        return std::make_unique<BinaryOpExpression>(std::move(left), op,
                                                    std::move(right));
    }
    return left;
}

/**
 * 解析算术表达式（加法和减法）
 * 语法：term_expr [(+|-) term_expr]*
 * 处理加法和减法，它们是左结合的
 */
std::unique_ptr<Expression> Parser::ParseArithmeticExpression() {
    auto left = ParseTermExpression();
    while (current_token_.type == TokenType::PLUS ||
           current_token_.type == TokenType::MINUS) {
        BinaryOpExpression::OpType op = (current_token_.type == TokenType::PLUS)
                                            ? BinaryOpExpression::OpType::PLUS
                                            : BinaryOpExpression::OpType::MINUS;
        Advance();
        auto right = ParseTermExpression();
        left = std::make_unique<BinaryOpExpression>(std::move(left), op,
                                                    std::move(right));
    }
    return left;
}

/**
 * 解析项表达式（乘法和除法）
 * 语法：unary_expr [(*|/) unary_expr]*
 * 处理乘法和除法，优先级高于加减法
 */
std::unique_ptr<Expression> Parser::ParseTermExpression() {
    auto left = ParseUnaryExpression();
    while (current_token_.type == TokenType::MULTIPLY ||
           current_token_.type == TokenType::DIVIDE) {
        BinaryOpExpression::OpType op =
            (current_token_.type == TokenType::MULTIPLY)
                ? BinaryOpExpression::OpType::MULTIPLY
                : BinaryOpExpression::OpType::DIVIDE;
        Advance();
        auto right = ParseUnaryExpression();
        left = std::make_unique<BinaryOpExpression>(std::move(left), op,
                                                    std::move(right));
    }
    return left;
}

/**
 * 解析一元表达式
 * 语法：[NOT|'-'] primary_expr
 * 处理NOT逻辑否定和数值取负
 */
std::unique_ptr<Expression> Parser::ParseUnaryExpression() {
    // 处理NOT操作符
    if (Match(TokenType::NOT)) {
        auto operand = ParseUnaryExpression();
        return std::make_unique<UnaryOpExpression>(
            UnaryOpExpression::OpType::NOT, std::move(operand));
    }
    // 处理负号
    if (Match(TokenType::MINUS)) {
        auto operand = ParseUnaryExpression();
        return std::make_unique<UnaryOpExpression>(
            UnaryOpExpression::OpType::NEGATIVE, std::move(operand));
    }
    return ParsePrimaryExpression();
}

/**
 * 解析基本表达式
 * 包括：字面量、列引用、括号表达式
 * 这是表达式解析的最底层
 */
std::unique_ptr<Expression> Parser::ParsePrimaryExpression() {
    // 括号表达式：(expression)
    if (Match(TokenType::LPAREN)) {
        auto expr = ParseExpression();
        Expect(TokenType::RPAREN);
        return expr;
    }

    // 整数字面量
    if (current_token_.type == TokenType::INTEGER_LITERAL) {
        int32_t value = std::stoi(current_token_.value);
        Advance();
        return std::make_unique<ConstantExpression>(Value(value));
    }

    // 浮点数字面量
    if (current_token_.type == TokenType::FLOAT_LITERAL) {
        double value = std::stod(current_token_.value);
        Advance();
        return std::make_unique<ConstantExpression>(Value(value));
    }

    // 字符串字面量
    if (current_token_.type == TokenType::STRING_LITERAL) {
        std::string value = current_token_.value;
        Advance();
        return std::make_unique<ConstantExpression>(Value(value));
    }

    // 布尔字面量
    if (current_token_.type == TokenType::BOOLEAN_LITERAL) {
        bool value = (current_token_.value == "TRUE");
        Advance();
        return std::make_unique<ConstantExpression>(Value(value));
    }

    // 标识符（列引用）
    if (current_token_.type == TokenType::IDENTIFIER) {
        std::string name = current_token_.value;
        Advance();

        // 检查是否是table.column格式
        if (current_token_.type == TokenType::IDENTIFIER &&
            current_token_.value == ".") {
            Advance();
            if (current_token_.type != TokenType::IDENTIFIER) {
                throw Exception("Expected column name after .");
            }
            std::string col_name = current_token_.value;
            Advance();
            return std::make_unique<ColumnRefExpression>(name, col_name);
        }
        // 普通的列引用
        return std::make_unique<ColumnRefExpression>("", name);
    }

    throw Exception("Expected expression");
}

/**
 * 解析DROP TABLE语句
 * 语法：DROP TABLE table_name
 */
std::unique_ptr<Statement> Parser::ParseDropTableStatement() {
    Expect(TokenType::TABLE);
    if (current_token_.type != TokenType::IDENTIFIER) {
        throw Exception("Expected table name");
    }
    std::string table_name = current_token_.value;
    Advance();
    return std::make_unique<DropTableStatement>(table_name);
}

/**
 * 解析DROP INDEX语句
 * 语法：DROP INDEX index_name [ON table_name]
 * 注意：ON table_name部分是可选的，因为索引名在数据库中是唯一的
 */
std::unique_ptr<Statement> Parser::ParseDropIndexStatement() {
    Expect(TokenType::INDEX);
    if (current_token_.type != TokenType::IDENTIFIER) {
        throw Exception("Expected index name");
    }
    std::string index_name = current_token_.value;
    Advance();

    // 可选的 ON table_name 部分
    if (Match(TokenType::ON)) {
        if (current_token_.type != TokenType::IDENTIFIER) {
            throw Exception("Expected table name after ON");
        }
        // 解析表名但不使用（因为索引名在数据库中是唯一的）
        std::string table_name = current_token_.value;
        Advance();
        LOG_DEBUG("ParseDropIndexStatement: Parsed table name "
                  << table_name << " but ignoring it");
    }

    return std::make_unique<DropIndexStatement>(index_name);
}

/**
 * 解析SHOW TABLES语句
 * 语法：SHOW TABLES
 */
std::unique_ptr<Statement> Parser::ParseShowTablesStatement() {
    Expect(TokenType::SHOW);
    Expect(TokenType::TABLES);
    return std::make_unique<ShowTablesStatement>();
}

/**
 * 解析BEGIN语句
 * 语法：BEGIN
 */
std::unique_ptr<Statement> Parser::ParseBeginStatement() {
    Expect(TokenType::BEGIN);
    return std::make_unique<BeginStatement>();
}

/**
 * 解析COMMIT语句
 * 语法：COMMIT
 */
std::unique_ptr<Statement> Parser::ParseCommitStatement() {
    Expect(TokenType::COMMIT);
    return std::make_unique<CommitStatement>();
}

/**
 * 解析ROLLBACK语句
 * 语法：ROLLBACK
 */
std::unique_ptr<Statement> Parser::ParseRollbackStatement() {
    Expect(TokenType::ROLLBACK);
    return std::make_unique<RollbackStatement>();
}

/**
 * 解析EXPLAIN语句
 * 语法：EXPLAIN statement
 * 用于显示SQL语句的执行计划
 */
std::unique_ptr<Statement> Parser::ParseExplainStatement() {
    Expect(TokenType::EXPLAIN);

    // 解析要EXPLAIN的语句
    auto stmt = ParseStatement();
    if (!stmt) {
        throw Exception("Expected statement after EXPLAIN");
    }

    return std::make_unique<ExplainStatement>(std::move(stmt));
}

/**
 * 解析CREATE INDEX语句
 * 语法：CREATE INDEX index_name ON table_name (column_list)
 */
std::unique_ptr<Statement> Parser::ParseCreateIndexStatement() {
    Expect(TokenType::INDEX);

    // 解析索引名
    if (current_token_.type != TokenType::IDENTIFIER) {
        throw Exception("Expected index name");
    }
    std::string index_name = current_token_.value;
    Advance();

    Expect(TokenType::ON);

    // 解析表名
    if (current_token_.type != TokenType::IDENTIFIER) {
        throw Exception("Expected table name");
    }
    std::string table_name = current_token_.value;
    Advance();

    Expect(TokenType::LPAREN);

    // 解析列名列表
    std::vector<std::string> key_columns;
    do {
        if (current_token_.type != TokenType::IDENTIFIER) {
            throw Exception("Expected column name");
        }
        key_columns.push_back(current_token_.value);
        Advance();
    } while (Match(TokenType::COMMA));

    Expect(TokenType::RPAREN);

    return std::make_unique<CreateIndexStatement>(index_name, table_name,
                                                  key_columns);
}

// AST节点的Accept方法实现（剩余部分）
void ShowTablesStatement::Accept(ASTVisitor* visitor) { visitor->Visit(this); }
void BeginStatement::Accept(ASTVisitor* visitor) { visitor->Visit(this); }
void CommitStatement::Accept(ASTVisitor* visitor) { visitor->Visit(this); }
void RollbackStatement::Accept(ASTVisitor* visitor) { visitor->Visit(this); }
void ExplainStatement::Accept(ASTVisitor* visitor) { visitor->Visit(this); }

}  // namespace SimpleRDBMS