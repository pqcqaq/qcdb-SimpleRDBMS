#pragma once

#include <memory>
#include <string>
#include <vector>

#include "parser/ast.h"
#include "parser/lexer.h"

namespace SimpleRDBMS {

class Parser {
   public:
    explicit Parser(const std::string& sql);
    std::unique_ptr<Statement> Parse();

   private:
    Lexer lexer_;
    Token current_token_;

    void Advance();
    bool Match(TokenType type);
    void Expect(TokenType type);

    // Statement parsers
    std::unique_ptr<Statement> ParseStatement();
    std::unique_ptr<Statement> ParseSelectStatement();
    std::unique_ptr<Statement> ParseCreateTableStatement();
    std::unique_ptr<Statement> ParseInsertStatement();

    // Expression parsers
    std::unique_ptr<Expression> ParseExpression();
    std::unique_ptr<Expression> ParsePrimaryExpression();

    std::unique_ptr<Expression> ParseOrExpression();
    std::unique_ptr<Expression> ParseAndExpression();
    std::unique_ptr<Expression> ParseComparisonExpression();
    std::unique_ptr<Expression> ParseArithmeticExpression();
    std::unique_ptr<Expression> ParseTermExpression();

    std::unique_ptr<Statement> ParseUpdateStatement();
    std::unique_ptr<Statement> ParseDeleteStatement();
    std::unique_ptr<Expression> ParseUnaryExpression();

    std::unique_ptr<Statement> ParseShowTablesStatement();
    std::unique_ptr<Statement> ParseBeginStatement();
    std::unique_ptr<Statement> ParseCommitStatement();
    std::unique_ptr<Statement> ParseRollbackStatement();

    // Helper functions
    std::vector<Column> ParseColumnDefinitions();
    TypeId ParseDataType();
};

}  // namespace SimpleRDBMS