// src/execution/expression_evaluator.h
#pragma once

#include <variant>
#include "catalog/schema.h"
#include "common/types.h"
#include "parser/ast.h"
#include "record/tuple.h"

namespace SimpleRDBMS {

// 表达式求值器
class ExpressionEvaluator {
public:
    ExpressionEvaluator(const Schema* schema) : schema_(schema) {}
    
    // 求值表达式，返回Value
    Value Evaluate(const Expression* expr, const Tuple& tuple);
    
    // 求值表达式，返回布尔值（用于WHERE子句）
    bool EvaluateAsBoolean(const Expression* expr, const Tuple& tuple);

private:
    const Schema* schema_;

    // 具体的求值方法
    Value EvaluateConstant(const ConstantExpression* expr, const Tuple& tuple);
    Value EvaluateColumnRef(const ColumnRefExpression* expr, const Tuple& tuple);
    Value EvaluateBinaryOp(const BinaryOpExpression* expr, const Tuple& tuple);
    Value EvaluateUnaryOp(const UnaryOpExpression* expr, const Tuple& tuple);
    Value EvaluateArithmeticOp(const Value& left, const Value& right, BinaryOpExpression::OpType op);

    // 比较操作辅助函数
    bool CompareValues(const Value& left, const Value& right, BinaryOpExpression::OpType op);
    bool IsValueTrue(const Value& value);

    // 类型转换辅助函数
    template<typename T>
    bool CompareNumeric(const T& left, const T& right, BinaryOpExpression::OpType op);
};

}  // namespace SimpleRDBMS
