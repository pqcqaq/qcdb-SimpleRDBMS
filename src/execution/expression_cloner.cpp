#include "execution/expression_cloner.h"

namespace SimpleRDBMS {

std::unique_ptr<Expression> ExpressionCloner::Clone(const Expression* expr) {
    if (!expr) {
        return nullptr;
    }
    
    switch (expr->GetType()) {
        case Expression::ExprType::CONSTANT:
            return CloneConstant(static_cast<const ConstantExpression*>(expr));
        case Expression::ExprType::COLUMN_REF:
            return CloneColumnRef(static_cast<const ColumnRefExpression*>(expr));
        case Expression::ExprType::BINARY_OP:
            return CloneBinaryOp(static_cast<const BinaryOpExpression*>(expr));
        case Expression::ExprType::UNARY_OP:
            return CloneUnaryOp(static_cast<const UnaryOpExpression*>(expr));
        default:
            return nullptr;
    }
}

std::unique_ptr<Expression> ExpressionCloner::CloneConstant(const ConstantExpression* expr) {
    return std::make_unique<ConstantExpression>(expr->GetValue());
}

std::unique_ptr<Expression> ExpressionCloner::CloneColumnRef(const ColumnRefExpression* expr) {
    return std::make_unique<ColumnRefExpression>(expr->GetTableName(), expr->GetColumnName());
}

std::unique_ptr<Expression> ExpressionCloner::CloneBinaryOp(const BinaryOpExpression* expr) {
    auto left = Clone(expr->GetLeft());
    auto right = Clone(expr->GetRight());
    return std::make_unique<BinaryOpExpression>(std::move(left), expr->GetOperator(), std::move(right));
}

std::unique_ptr<Expression> ExpressionCloner::CloneUnaryOp(const UnaryOpExpression* expr) {
    auto operand = Clone(expr->GetOperand());
    return std::make_unique<UnaryOpExpression>(expr->GetOperator(), std::move(operand));
}

}  // namespace SimpleRDBMS