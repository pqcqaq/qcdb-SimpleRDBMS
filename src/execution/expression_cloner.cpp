/*
 * 文件: expression_cloner.cpp
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述: 表达式克隆器实现，用于深度复制各种类型的表达式对象，
 *       支持常量、列引用、二元操作和一元操作表达式的递归克隆
 */

#include "execution/expression_cloner.h"

namespace SimpleRDBMS {

/**
 * 克隆表达式的主入口方法
 * 根据表达式类型分发到具体的克隆方法
 * @param expr 要克隆的原始表达式
 * @return 克隆后的新表达式，如果输入为空则返回nullptr
 */
std::unique_ptr<Expression> ExpressionCloner::Clone(const Expression* expr) {
    if (!expr) {
        return nullptr;  // 空指针直接返回
    }

    // 根据表达式类型进行分发克隆
    switch (expr->GetType()) {
        case Expression::ExprType::CONSTANT:
            // 常量表达式：克隆值
            return CloneConstant(static_cast<const ConstantExpression*>(expr));

        case Expression::ExprType::COLUMN_REF:
            // 列引用表达式：克隆表名和列名
            return CloneColumnRef(
                static_cast<const ColumnRefExpression*>(expr));

        case Expression::ExprType::BINARY_OP:
            // 二元操作表达式：递归克隆左右操作数
            return CloneBinaryOp(static_cast<const BinaryOpExpression*>(expr));

        case Expression::ExprType::UNARY_OP:
            // 一元操作表达式：递归克隆操作数
            return CloneUnaryOp(static_cast<const UnaryOpExpression*>(expr));

        default:
            // 未知类型，返回空指针
            return nullptr;
    }
}

/**
 * 克隆常量表达式
 * 直接复制常量值创建新的表达式对象
 * @param expr 原始常量表达式
 * @return 新的常量表达式
 */
std::unique_ptr<Expression> ExpressionCloner::CloneConstant(
    const ConstantExpression* expr) {
    // 复制Value对象，创建新的常量表达式
    return std::make_unique<ConstantExpression>(expr->GetValue());
}

/**
 * 克隆列引用表达式
 * 复制表名和列名信息创建新的列引用
 * @param expr 原始列引用表达式
 * @return 新的列引用表达式
 */
std::unique_ptr<Expression> ExpressionCloner::CloneColumnRef(
    const ColumnRefExpression* expr) {
    // 复制表名和列名字符串，创建新的列引用表达式
    return std::make_unique<ColumnRefExpression>(expr->GetTableName(),
                                                 expr->GetColumnName());
}

/**
 * 克隆二元操作表达式
 * 递归克隆左右操作数，保持操作符不变
 * @param expr 原始二元操作表达式
 * @return 新的二元操作表达式
 */
std::unique_ptr<Expression> ExpressionCloner::CloneBinaryOp(
    const BinaryOpExpression* expr) {
    // 递归克隆左操作数和右操作数
    auto left = Clone(expr->GetLeft());
    auto right = Clone(expr->GetRight());

    // 保持原有的操作符，创建新的二元操作表达式
    return std::make_unique<BinaryOpExpression>(
        std::move(left), expr->GetOperator(), std::move(right));
}

/**
 * 克隆一元操作表达式
 * 递归克隆操作数，保持操作符不变
 * @param expr 原始一元操作表达式
 * @return 新的一元操作表达式
 */
std::unique_ptr<Expression> ExpressionCloner::CloneUnaryOp(
    const UnaryOpExpression* expr) {
    // 递归克隆操作数
    auto operand = Clone(expr->GetOperand());

    // 保持原有的操作符，创建新的一元操作表达式
    return std::make_unique<UnaryOpExpression>(expr->GetOperator(),
                                               std::move(operand));
}

}  // namespace SimpleRDBMS