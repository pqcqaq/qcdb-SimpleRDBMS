// src/execution/expression_evaluator.cpp
#include "execution/expression_evaluator.h"

#include <stdexcept>

#include "common/exception.h"

namespace SimpleRDBMS {

Value ExpressionEvaluator::Evaluate(const Expression* expr,
                                    const Tuple& tuple) {
    if (!expr) {
        throw ExecutionException("Null expression");
    }

    switch (expr->GetType()) {
        case Expression::ExprType::CONSTANT:
            return EvaluateConstant(
                static_cast<const ConstantExpression*>(expr), tuple);
        case Expression::ExprType::COLUMN_REF:
            return EvaluateColumnRef(
                static_cast<const ColumnRefExpression*>(expr), tuple);
        case Expression::ExprType::BINARY_OP:
            return EvaluateBinaryOp(
                static_cast<const BinaryOpExpression*>(expr), tuple);
        case Expression::ExprType::UNARY_OP:
            return EvaluateUnaryOp(static_cast<const UnaryOpExpression*>(expr),
                                   tuple);
        default:
            throw ExecutionException("Unsupported expression type");
    }
}

bool ExpressionEvaluator::EvaluateAsBoolean(const Expression* expr,
                                            const Tuple& tuple) {
    Value result = Evaluate(expr, tuple);
    bool bool_result = IsValueTrue(result);

    // 添加调试输出
    LOG_DEBUG("EvaluateAsBoolean: result = " << bool_result);

    return bool_result;
}

Value ExpressionEvaluator::EvaluateConstant(const ConstantExpression* expr,
                                            const Tuple& tuple) {
    (void)tuple;  // Unused parameter
    return expr->GetValue();
}

Value ExpressionEvaluator::EvaluateColumnRef(const ColumnRefExpression* expr,
                                             const Tuple& tuple) {
    const std::string& column_name = expr->GetColumnName();
    try {
        if (!schema_) {
            throw ExecutionException("Schema is null in expression evaluator");
        }

        size_t column_idx = schema_->GetColumnIdx(column_name);
        const auto& tuple_values = tuple.GetValues();

        if (column_idx >= tuple_values.size()) {
            LOG_ERROR("EvaluateColumnRef: Column index "
                      << column_idx << " out of range, tuple has "
                      << tuple_values.size() << " values");
            throw ExecutionException("Column index out of range: " +
                                     column_name);
        }

        return tuple.GetValue(column_idx);
    } catch (const std::exception& e) {
        throw ExecutionException("Column not found or invalid: " + column_name +
                                 " - " + e.what());
    }
}

Value ExpressionEvaluator::EvaluateBinaryOp(const BinaryOpExpression* expr,
                                            const Tuple& tuple) {
    Value left_val = Evaluate(expr->GetLeft(), tuple);
    BinaryOpExpression::OpType op = expr->GetOperator();

    // 对于逻辑操作符，支持短路求值
    if (op == BinaryOpExpression::OpType::AND) {
        if (!IsValueTrue(left_val)) {
            return Value(false);  // 短路：左操作数为false，整个AND表达式为false
        }
        Value right_val = Evaluate(expr->GetRight(), tuple);
        return Value(IsValueTrue(right_val));
    }
    if (op == BinaryOpExpression::OpType::OR) {
        if (IsValueTrue(left_val)) {
            return Value(true);  // 短路：左操作数为true，整个OR表达式为true
        }
        Value right_val = Evaluate(expr->GetRight(), tuple);
        return Value(IsValueTrue(right_val));
    }

    // 对于其他操作符，需要计算右操作数
    Value right_val = Evaluate(expr->GetRight(), tuple);

    // 处理算术运算
    if (op == BinaryOpExpression::OpType::PLUS ||
        op == BinaryOpExpression::OpType::MINUS ||
        op == BinaryOpExpression::OpType::MULTIPLY ||
        op == BinaryOpExpression::OpType::DIVIDE) {
        return EvaluateArithmeticOp(left_val, right_val, op);
    }

    // 执行比较操作
    bool result = CompareValues(left_val, right_val, op);
    return Value(result);
}

Value ExpressionEvaluator::EvaluateArithmeticOp(const Value& left,
                                                const Value& right,
                                                BinaryOpExpression::OpType op) {
    // 简化实现：将所有数值转换为double进行计算
    auto to_double = [](const Value& val) -> double {
        if (std::holds_alternative<int8_t>(val))
            return static_cast<double>(std::get<int8_t>(val));
        if (std::holds_alternative<int16_t>(val))
            return static_cast<double>(std::get<int16_t>(val));
        if (std::holds_alternative<int32_t>(val))
            return static_cast<double>(std::get<int32_t>(val));
        if (std::holds_alternative<int64_t>(val))
            return static_cast<double>(std::get<int64_t>(val));
        if (std::holds_alternative<float>(val))
            return static_cast<double>(std::get<float>(val));
        if (std::holds_alternative<double>(val)) return std::get<double>(val);
        throw ExecutionException(
            "Cannot convert value to numeric for arithmetic operation");
    };

    try {
        double left_num = to_double(left);
        double right_num = to_double(right);
        double result;

        switch (op) {
            case BinaryOpExpression::OpType::PLUS:
                result = left_num + right_num;
                break;
            case BinaryOpExpression::OpType::MINUS:
                result = left_num - right_num;
                break;
            case BinaryOpExpression::OpType::MULTIPLY:
                result = left_num * right_num;
                break;
            case BinaryOpExpression::OpType::DIVIDE:
                if (right_num == 0) {
                    throw ExecutionException("Division by zero");
                }
                result = left_num / right_num;
                break;
            default:
                throw ExecutionException("Unsupported arithmetic operator");
        }

        // 如果原来都是整数，尝试返回整数结果
        if ((std::holds_alternative<int32_t>(left) ||
             std::holds_alternative<int64_t>(left)) &&
            (std::holds_alternative<int32_t>(right) ||
             std::holds_alternative<int64_t>(right)) &&
            op != BinaryOpExpression::OpType::DIVIDE) {
            return Value(static_cast<int32_t>(result));
        } else {
            return Value(result);
        }
    } catch (const std::exception&) {
        throw ExecutionException("Type mismatch in arithmetic operation");
    }
}

Value ExpressionEvaluator::EvaluateUnaryOp(const UnaryOpExpression* expr,
                                           const Tuple& tuple) {
    Value operand_val = Evaluate(expr->GetOperand(), tuple);

    switch (expr->GetOperator()) {
        case UnaryOpExpression::OpType::NOT:
            return Value(!IsValueTrue(operand_val));
        case UnaryOpExpression::OpType::NEGATIVE:
            // 处理数值取负
            if (std::holds_alternative<int32_t>(operand_val)) {
                return Value(-std::get<int32_t>(operand_val));
            } else if (std::holds_alternative<int64_t>(operand_val)) {
                return Value(-std::get<int64_t>(operand_val));
            } else if (std::holds_alternative<float>(operand_val)) {
                return Value(-std::get<float>(operand_val));
            } else if (std::holds_alternative<double>(operand_val)) {
                return Value(-std::get<double>(operand_val));
            } else {
                throw ExecutionException(
                    "Cannot apply negative operator to non-numeric value");
            }
        default:
            throw ExecutionException("Unsupported unary operator");
    }
}

bool ExpressionEvaluator::CompareValues(const Value& left, const Value& right,
                                        BinaryOpExpression::OpType op) {
    // 处理相同类型的比较
    if (left.index() == right.index()) {
        switch (left.index()) {
            case 0:  // bool
                return CompareNumeric(std::get<bool>(left),
                                      std::get<bool>(right), op);
            case 1:  // int8_t
                return CompareNumeric(std::get<int8_t>(left),
                                      std::get<int8_t>(right), op);
            case 2:  // int16_t
                return CompareNumeric(std::get<int16_t>(left),
                                      std::get<int16_t>(right), op);
            case 3:  // int32_t
                return CompareNumeric(std::get<int32_t>(left),
                                      std::get<int32_t>(right), op);
            case 4:  // int64_t
                return CompareNumeric(std::get<int64_t>(left),
                                      std::get<int64_t>(right), op);
            case 5:  // float
                return CompareNumeric(std::get<float>(left),
                                      std::get<float>(right), op);
            case 6:  // double
                return CompareNumeric(std::get<double>(left),
                                      std::get<double>(right), op);
            case 7:  // string
                return CompareNumeric(std::get<std::string>(left),
                                      std::get<std::string>(right), op);
        }
    }

    // 处理数值类型间的转换比较
    auto safe_to_double = [](const Value& val) -> std::pair<bool, double> {
        try {
            if (std::holds_alternative<int8_t>(val))
                return {true, static_cast<double>(std::get<int8_t>(val))};
            if (std::holds_alternative<int16_t>(val))
                return {true, static_cast<double>(std::get<int16_t>(val))};
            if (std::holds_alternative<int32_t>(val))
                return {true, static_cast<double>(std::get<int32_t>(val))};
            if (std::holds_alternative<int64_t>(val))
                return {true, static_cast<double>(std::get<int64_t>(val))};
            if (std::holds_alternative<float>(val))
                return {true, static_cast<double>(std::get<float>(val))};
            if (std::holds_alternative<double>(val))
                return {true, std::get<double>(val)};
            return {false, 0.0};
        } catch (const std::bad_variant_access&) {
            return {false, 0.0};
        }
    };

    auto left_result = safe_to_double(left);
    auto right_result = safe_to_double(right);

    if (left_result.first && right_result.first) {
        return CompareNumeric(left_result.second, right_result.second, op);
    }

    throw ExecutionException(
        "Type mismatch in comparison - cannot convert operands to comparable "
        "types");
}

template <typename T>
bool ExpressionEvaluator::CompareNumeric(const T& left, const T& right,
                                         BinaryOpExpression::OpType op) {
    switch (op) {
        case BinaryOpExpression::OpType::EQUALS:
            return left == right;
        case BinaryOpExpression::OpType::NOT_EQUALS:
            return left != right;
        case BinaryOpExpression::OpType::LESS_THAN:
            return left < right;
        case BinaryOpExpression::OpType::LESS_EQUALS:
            return left <= right;
        case BinaryOpExpression::OpType::GREATER_THAN:
            return left > right;
        case BinaryOpExpression::OpType::GREATER_EQUALS:
            return left >= right;
        default:
            throw ExecutionException("Unsupported comparison operator");
    }
}

bool ExpressionEvaluator::IsValueTrue(const Value& value) {
    if (std::holds_alternative<bool>(value)) {
        return std::get<bool>(value);
    }
    // 非布尔值的真值判断：非零数值为true，非空字符串为true
    if (std::holds_alternative<int32_t>(value)) {
        return std::get<int32_t>(value) != 0;
    }
    if (std::holds_alternative<int64_t>(value)) {
        return std::get<int64_t>(value) != 0;
    }
    if (std::holds_alternative<float>(value)) {
        return std::get<float>(value) != 0.0f;
    }
    if (std::holds_alternative<double>(value)) {
        return std::get<double>(value) != 0.0;
    }
    if (std::holds_alternative<std::string>(value)) {
        return !std::get<std::string>(value).empty();
    }
    return false;
}

}  // namespace SimpleRDBMS