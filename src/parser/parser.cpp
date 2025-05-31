// src/parser/parser.cpp
#include "parser/parser.h"

#include <algorithm>
#include <cctype>
#include <unordered_map>

#include "common/exception.h"

namespace SimpleRDBMS {

// AST Accept implementations
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

// Lexer implementation
static std::unordered_map<std::string, TokenType> keywords = {
    {"SELECT", TokenType::SELECT},
    {"FROM", TokenType::FROM},
    {"WHERE", TokenType::WHERE},
    {"INSERT", TokenType::INSERT},
    {"INTO", TokenType::INTO},
    {"VALUES", TokenType::VALUES},
    {"UPDATE", TokenType::UPDATE},
    {"SET", TokenType::SET},
    {"DELETE", TokenType::DELETE},
    {"CREATE", TokenType::CREATE},
    {"TABLE", TokenType::TABLE},
    {"DROP", TokenType::DROP},
    {"INDEX", TokenType::INDEX},
    {"ON", TokenType::ON},
    {"PRIMARY", TokenType::PRIMARY},
    {"KEY", TokenType::KEY},
    {"NOT", TokenType::NOT},
    {"NULL", TokenType::_NULL},
    {"INT", TokenType::INT},
    {"INTEGER", TokenType::INT},
    {"VARCHAR", TokenType::VARCHAR},
    {"FLOAT", TokenType::FLOAT},
    {"DOUBLE", TokenType::DOUBLE},
    {"BOOLEAN", TokenType::BOOLEAN},
    {"BOOL", TokenType::BOOLEAN},
    {"AND", TokenType::AND},
    {"OR", TokenType::OR},
    {"TRUE", TokenType::BOOLEAN_LITERAL},
    {"FALSE", TokenType::BOOLEAN_LITERAL}};

Lexer::Lexer(const std::string& input)
    : input_(input), position_(0), line_(1), column_(1) {}

char Lexer::Peek() {
    if (position_ >= input_.size()) {
        return '\0';
    }
    return input_[position_];
}

char Lexer::Advance() {
    if (position_ >= input_.size()) {
        return '\0';
    }
    char ch = input_[position_++];
    if (ch == '\n') {
        line_++;
        column_ = 1;
    } else {
        column_++;
    }
    return ch;
}

void Lexer::SkipWhitespace() {
    while (std::isspace(Peek())) {
        Advance();
    }
}

Token Lexer::ScanNumber() {
    Token token;
    token.line = line_;
    token.column = column_;

    std::string value;
    bool has_dot = false;

    while (std::isdigit(Peek()) || Peek() == '.') {
        if (Peek() == '.') {
            if (has_dot) break;
            has_dot = true;
        }
        value += Advance();
    }

    token.value = value;
    token.type =
        has_dot ? TokenType::FLOAT_LITERAL : TokenType::INTEGER_LITERAL;
    return token;
}

Token Lexer::ScanString() {
    Token token;
    token.line = line_;
    token.column = column_;
    token.type = TokenType::STRING_LITERAL;

    char quote = Advance();
    std::string value;

    while (Peek() != quote && Peek() != '\0') {
        if (Peek() == '\\') {
            Advance();
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
                    value += ch;
                    break;
            }
        } else {
            value += Advance();
        }
    }

    if (Peek() == quote) {
        Advance();
    }

    token.value = value;
    return token;
}

Token Lexer::ScanIdentifier() {
    Token token;
    token.line = line_;
    token.column = column_;

    std::string value;
    while (std::isalnum(Peek()) || Peek() == '_') {
        value += Advance();
    }

    std::string upper_value = value;
    std::transform(upper_value.begin(), upper_value.end(), upper_value.begin(),
                   ::toupper);

    auto it = keywords.find(upper_value);
    if (it != keywords.end()) {
        token.type = it->second;
        if (token.type == TokenType::BOOLEAN_LITERAL) {
            token.value = upper_value;
        } else {
            token.value = value;
        }
    } else {
        token.type = TokenType::IDENTIFIER;
        token.value = value;
    }

    return token;
}

Token Lexer::NextToken() {
    SkipWhitespace();
    Token token;
    token.line = line_;
    token.column = column_;
    char ch = Peek();
    if (ch == '\0') {
        token.type = TokenType::EOF_TOKEN;
        return token;
    }
    if (std::isdigit(ch)) {
        return ScanNumber();
    }
    if (std::isalpha(ch) || ch == '_') {
        return ScanIdentifier();
    }
    if (ch == '\'' || ch == '"') {
        return ScanString();
    }
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
            token.type = TokenType::MULTIPLY;  // 修改这里，原来是 STAR
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
            if (Peek() == '=') {
                Advance();
                token.type = TokenType::NOT_EQUALS;
                token.value = "!=";
            } else {
                token.type = TokenType::INVALID;
                token.value = "!";
            }
            break;
        default:
            token.type = TokenType::INVALID;
            token.value = std::string(1, ch);
            break;
    }
    return token;
}

// Parser implementation
Parser::Parser(const std::string& sql) : lexer_(sql) { Advance(); }

void Parser::Advance() { current_token_ = lexer_.NextToken(); }

bool Parser::Match(TokenType type) {
    if (current_token_.type == type) {
        Advance();
        return true;
    }
    return false;
}

void Parser::Expect(TokenType type) {
    if (!Match(type)) {
        throw Exception("Unexpected token: " + current_token_.value);
    }
}

std::unique_ptr<Statement> Parser::Parse() {
    auto stmt = ParseStatement();
    if (current_token_.type != TokenType::EOF_TOKEN &&
        current_token_.type != TokenType::SEMICOLON) {
        throw Exception("Expected end of statement");
    }
    return stmt;
}

std::unique_ptr<Statement> Parser::ParseSelectStatement() {
    Expect(TokenType::SELECT);
    std::vector<std::unique_ptr<Expression>> select_list;
    if (current_token_.type == TokenType::MULTIPLY) {  // 修改这里
        Advance();  // 消费 * token
        select_list.push_back(std::make_unique<ColumnRefExpression>("", "*"));
    } else {
        do {
            auto expr = ParseExpression();
            select_list.push_back(std::move(expr));
        } while (Match(TokenType::COMMA));
    }

    Expect(TokenType::FROM);

    if (current_token_.type != TokenType::IDENTIFIER) {
        throw Exception("Expected table name");
    }
    std::string table_name = current_token_.value;
    Advance();

    std::unique_ptr<Expression> where_clause = nullptr;
    if (Match(TokenType::WHERE)) {
        where_clause = ParseExpression();
    }

    return std::make_unique<SelectStatement>(std::move(select_list), table_name,
                                             std::move(where_clause));
}

std::unique_ptr<Statement> Parser::ParseCreateTableStatement() {
    Expect(TokenType::CREATE);
    Expect(TokenType::TABLE);

    if (current_token_.type != TokenType::IDENTIFIER) {
        throw Exception("Expected table name");
    }
    std::string table_name = current_token_.value;
    Advance();

    auto columns = ParseColumnDefinitions();

    return std::make_unique<CreateTableStatement>(table_name,
                                                  std::move(columns));
}

std::unique_ptr<Statement> Parser::ParseInsertStatement() {
    Expect(TokenType::INSERT);
    Expect(TokenType::INTO);

    if (current_token_.type != TokenType::IDENTIFIER) {
        throw Exception("Expected table name");
    }
    std::string table_name = current_token_.value;
    Advance();

    // Optional column list
    std::vector<std::string> column_names;
    if (Match(TokenType::LPAREN)) {
        do {
            if (current_token_.type != TokenType::IDENTIFIER) {
                throw Exception("Expected column name");
            }
            column_names.push_back(current_token_.value);
            Advance();
        } while (Match(TokenType::COMMA));

        Expect(TokenType::RPAREN);
    }

    Expect(TokenType::VALUES);

    std::vector<std::vector<Value>> values_list;

    // Parse multiple value lists
    do {
        Expect(TokenType::LPAREN);

        std::vector<Value> values;
        do {
            auto expr = ParsePrimaryExpression();

            // Convert expression to value
            if (auto* const_expr =
                    dynamic_cast<ConstantExpression*>(expr.get())) {
                values.push_back(const_expr->GetValue());
            } else {
                throw Exception("Only constant values are supported in INSERT");
            }
        } while (Match(TokenType::COMMA));

        Expect(TokenType::RPAREN);

        values_list.push_back(std::move(values));
    } while (Match(TokenType::COMMA));

    return std::make_unique<InsertStatement>(table_name,
                                             std::move(values_list));
}

std::vector<Column> Parser::ParseColumnDefinitions() {
    Expect(TokenType::LPAREN);

    std::vector<Column> columns;
    bool has_primary_key = false;

    do {
        Column col;

        if (current_token_.type != TokenType::IDENTIFIER) {
            throw Exception("Expected column name");
        }
        col.name = current_token_.value;
        Advance();

        col.type = ParseDataType();

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

        col.nullable = true;
        col.is_primary_key = false;

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
                col.nullable = false;
                has_primary_key = true;
            }
        }

        columns.push_back(col);
    } while (Match(TokenType::COMMA));

    Expect(TokenType::RPAREN);

    return columns;
}

std::unique_ptr<Statement> Parser::ParseUpdateStatement() {
    Expect(TokenType::UPDATE);

    // 解析表名
    if (current_token_.type != TokenType::IDENTIFIER) {
        throw Exception("Expected table name");
    }
    std::string table_name = current_token_.value;
    Advance();

    Expect(TokenType::SET);

    // 解析 SET 子句
    std::vector<UpdateClause> update_clauses;

    do {
        // 解析列名
        if (current_token_.type != TokenType::IDENTIFIER) {
            throw Exception("Expected column name");
        }
        std::string column_name = current_token_.value;
        Advance();

        Expect(TokenType::EQUALS);

        // 解析值表达式
        auto value_expr = ParseExpression();

        update_clauses.emplace_back(column_name, std::move(value_expr));

    } while (Match(TokenType::COMMA));

    // 解析可选的 WHERE 子句
    std::unique_ptr<Expression> where_clause = nullptr;
    if (Match(TokenType::WHERE)) {
        where_clause = ParseExpression();
    }

    return std::make_unique<UpdateStatement>(
        table_name, std::move(update_clauses), std::move(where_clause));
}

std::unique_ptr<Statement> Parser::ParseDeleteStatement() {
    Expect(TokenType::DELETE);
    Expect(TokenType::FROM);

    // 解析表名
    if (current_token_.type != TokenType::IDENTIFIER) {
        throw Exception("Expected table name");
    }
    std::string table_name = current_token_.value;
    Advance();

    // 解析可选的 WHERE 子句
    std::unique_ptr<Expression> where_clause = nullptr;
    if (Match(TokenType::WHERE)) {
        where_clause = ParseExpression();
    }

    return std::make_unique<DeleteStatement>(table_name,
                                             std::move(where_clause));
}

TypeId Parser::ParseDataType() {
    TypeId type = TypeId::INVALID;

    switch (current_token_.type) {
        case TokenType::INT:
            Advance();
            type = TypeId::INTEGER;
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

std::unique_ptr<Expression> Parser::ParseExpression() {
    return ParseOrExpression();
}

std::unique_ptr<Expression> Parser::ParseOrExpression() {
    auto left = ParseAndExpression();
    while (Match(TokenType::OR)) {
        auto right = ParseAndExpression();
        left = std::make_unique<BinaryOpExpression>(
            std::move(left), BinaryOpExpression::OpType::OR, std::move(right));
    }
    return left;
}

std::unique_ptr<Expression> Parser::ParseAndExpression() {
    auto left = ParseComparisonExpression();
    while (Match(TokenType::AND)) {
        auto right = ParseComparisonExpression();
        left = std::make_unique<BinaryOpExpression>(
            std::move(left), BinaryOpExpression::OpType::AND, std::move(right));
    }
    return left;
}

std::unique_ptr<Statement> Parser::ParseStatement() {
    switch (current_token_.type) {
        case TokenType::SELECT:
            return ParseSelectStatement();
        case TokenType::CREATE:
            return ParseCreateTableStatement();
        case TokenType::INSERT:
            return ParseInsertStatement();
        case TokenType::UPDATE:
            return ParseUpdateStatement();
        case TokenType::DELETE:
            return ParseDeleteStatement();
        default:
            throw Exception("Unsupported statement type");
    }
}

std::unique_ptr<Expression> Parser::ParseComparisonExpression() {
    auto left = ParseArithmeticExpression();  // 修改这里，先解析算术表达式
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

std::unique_ptr<Expression> Parser::ParseArithmeticExpression() {
    auto left = ParseTermExpression();
    while (current_token_.type == TokenType::PLUS || current_token_.type == TokenType::MINUS) {
        BinaryOpExpression::OpType op = (current_token_.type == TokenType::PLUS) ? 
            BinaryOpExpression::OpType::PLUS : BinaryOpExpression::OpType::MINUS;
        Advance();
        auto right = ParseTermExpression();
        left = std::make_unique<BinaryOpExpression>(std::move(left), op, std::move(right));
    }
    return left;
}

std::unique_ptr<Expression> Parser::ParseTermExpression() {
    auto left = ParseUnaryExpression();
    while (current_token_.type == TokenType::MULTIPLY || current_token_.type == TokenType::DIVIDE) {
        BinaryOpExpression::OpType op = (current_token_.type == TokenType::MULTIPLY) ? 
            BinaryOpExpression::OpType::MULTIPLY : BinaryOpExpression::OpType::DIVIDE;
        Advance();
        auto right = ParseUnaryExpression();
        left = std::make_unique<BinaryOpExpression>(std::move(left), op, std::move(right));
    }
    return left;
}

std::unique_ptr<Expression> Parser::ParseUnaryExpression() {
    // 处理 NOT 操作符
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

std::unique_ptr<Expression> Parser::ParsePrimaryExpression() {
    // 添加对括号表达式的支持
    if (Match(TokenType::LPAREN)) {
        auto expr = ParseExpression();
        Expect(TokenType::RPAREN);
        return expr;
    }

    if (current_token_.type == TokenType::INTEGER_LITERAL) {
        int32_t value = std::stoi(current_token_.value);
        Advance();
        return std::make_unique<ConstantExpression>(Value(value));
    }
    if (current_token_.type == TokenType::FLOAT_LITERAL) {
        double value = std::stod(current_token_.value);
        Advance();
        return std::make_unique<ConstantExpression>(Value(value));
    }
    if (current_token_.type == TokenType::STRING_LITERAL) {
        std::string value = current_token_.value;
        Advance();
        return std::make_unique<ConstantExpression>(Value(value));
    }
    if (current_token_.type == TokenType::BOOLEAN_LITERAL) {
        bool value = (current_token_.value == "TRUE");
        Advance();
        return std::make_unique<ConstantExpression>(Value(value));
    }
    if (current_token_.type == TokenType::IDENTIFIER) {
        std::string name = current_token_.value;
        Advance();
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
        return std::make_unique<ColumnRefExpression>("", name);
    }
    throw Exception("Expected expression");
}

}  // namespace SimpleRDBMS