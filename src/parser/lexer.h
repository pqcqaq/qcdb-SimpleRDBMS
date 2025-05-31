// src/parser/lexer.h
#pragma once

#include <string>
#include <vector>

namespace SimpleRDBMS {

enum class TokenType {
    // Keywords
    SELECT,
    FROM,
    WHERE,
    INSERT,
    INTO,
    VALUES,
    UPDATE,
    SET,
    DELETE,
    CREATE,
    TABLE,
    DROP,
    INDEX,
    ON,
    PRIMARY,
    KEY,
    NOT,
    _NULL,
    INT,
    BIGINT,
    VARCHAR,
    FLOAT,
    DOUBLE,
    BOOLEAN,
    AND,
    OR,
    // Operators
    EQUALS,
    NOT_EQUALS,
    LESS_THAN,
    GREATER_THAN,
    LESS_EQUALS,
    GREATER_EQUALS,
    PLUS,
    MINUS,
    MULTIPLY,
    DIVIDE,  // 添加算术运算符
    // Literals
    INTEGER_LITERAL,
    FLOAT_LITERAL,
    STRING_LITERAL,
    BOOLEAN_LITERAL,
    // Identifiers
    IDENTIFIER,
    // Punctuation
    LPAREN,
    RPAREN,
    COMMA,
    SEMICOLON,
    STAR,
    // Special
    EOF_TOKEN,
    INVALID,
    // DDL Commands
    SHOW,
    TABLES,
    BEGIN,
    COMMIT,
    ROLLBACK,
    // explain
    EXPLAIN,
};

struct Token {
    TokenType type;
    std::string value;
    size_t line;
    size_t column;
};

class Lexer {
   public:
    explicit Lexer(const std::string& input);
    Token NextToken();

   private:
    std::string input_;
    size_t position_;
    size_t line_;
    size_t column_;

    char Peek();
    char Advance();
    void SkipWhitespace();
    Token ScanNumber();
    Token ScanString();
    Token ScanIdentifier();
};

}  // namespace SimpleRDBMS