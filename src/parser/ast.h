#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/types.h"

namespace SimpleRDBMS {

// Forward declarations
class ASTVisitor;

// Base AST Node
class ASTNode {
   public:
    virtual ~ASTNode() = default;
    virtual void Accept(ASTVisitor* visitor) = 0;
};

// Expression nodes
class Expression : public ASTNode {
   public:
    enum class ExprType {
        CONSTANT,
        COLUMN_REF,
        BINARY_OP,
        UNARY_OP,
        FUNCTION_CALL
    };

    virtual ExprType GetType() const = 0;
};

class ConstantExpression : public Expression {
   public:
    explicit ConstantExpression(const Value& value) : value_(value) {}
    ExprType GetType() const override { return ExprType::CONSTANT; }
    void Accept(ASTVisitor* visitor) override;
    const Value& GetValue() const { return value_; }

   private:
    Value value_;
};

class ColumnRefExpression : public Expression {
   public:
    ColumnRefExpression(const std::string& table, const std::string& column)
        : table_name_(table), column_name_(column) {}
    ExprType GetType() const override { return ExprType::COLUMN_REF; }
    void Accept(ASTVisitor* visitor) override;

    const std::string& GetTableName() const { return table_name_; }
    const std::string& GetColumnName() const { return column_name_; }

   private:
    std::string table_name_;
    std::string column_name_;
};

// Statement nodes
class Statement : public ASTNode {
   public:
    enum class StmtType {
        SELECT,
        INSERT,
        UPDATE,
        DELETE,
        CREATE_TABLE,
        DROP_TABLE,
        CREATE_INDEX,
        DROP_INDEX
    };

    virtual StmtType GetType() const = 0;
};

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
    std::vector<std::unique_ptr<Expression>> select_list_;
    std::string table_name_;
    std::unique_ptr<Expression> where_clause_;
};

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
    std::string table_name_;
    std::vector<Column> columns_;
};

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
    std::string table_name_;
    std::vector<std::vector<Value>> values_;
};

class DropTableStatement : public Statement {
   public:
    explicit DropTableStatement(const std::string& table_name)
        : table_name_(table_name) {}

    StmtType GetType() const override { return StmtType::DROP_TABLE; }
    void Accept(ASTVisitor* visitor) override;

    const std::string& GetTableName() const { return table_name_; }

   private:
    std::string table_name_;
};

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
    std::string index_name_;
    std::string table_name_;
    std::vector<std::string> key_columns_;
};

class DropIndexStatement : public Statement {
   public:
    explicit DropIndexStatement(const std::string& index_name)
        : index_name_(index_name) {}

    StmtType GetType() const override { return StmtType::DROP_INDEX; }
    void Accept(ASTVisitor* visitor) override;

    const std::string& GetIndexName() const { return index_name_; }

   private:
    std::string index_name_;
};

class BinaryOpExpression : public Expression {
public:
    enum class OpType {
        EQUALS,
        NOT_EQUALS,
        LESS_THAN,
        GREATER_THAN,
        LESS_EQUALS,
        GREATER_EQUALS,
        AND,
        OR
    };
    
    BinaryOpExpression(std::unique_ptr<Expression> left,
                       OpType op,
                       std::unique_ptr<Expression> right)
        : left_(std::move(left)), op_(op), right_(std::move(right)) {}
    
    ExprType GetType() const override { return ExprType::BINARY_OP; }
    void Accept(ASTVisitor* visitor) override;
    
    Expression* GetLeft() const { return left_.get(); }
    Expression* GetRight() const { return right_.get(); }
    OpType GetOperator() const { return op_; }

private:
    std::unique_ptr<Expression> left_;
    OpType op_;
    std::unique_ptr<Expression> right_;
};

// Visitor pattern for AST traversal
class ASTVisitor {
   public:
    virtual ~ASTVisitor() = default;
    virtual void Visit(ConstantExpression* expr) = 0;
    virtual void Visit(ColumnRefExpression* expr) = 0;
    virtual void Visit(SelectStatement* stmt) = 0;
    virtual void Visit(CreateTableStatement* stmt) = 0;
    virtual void Visit(InsertStatement* stmt) = 0;
    virtual void Visit(DropTableStatement* stmt) = 0;
    virtual void Visit(CreateIndexStatement* stmt) = 0;
    virtual void Visit(DropIndexStatement* stmt) = 0;
    virtual void Visit(BinaryOpExpression* expr) = 0;
};

}  // namespace SimpleRDBMS