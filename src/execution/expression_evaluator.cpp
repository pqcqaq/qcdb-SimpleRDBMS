/*
 * 文件: expression_evaluator.cpp
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述: 表达式求值器实现，负责在查询执行过程中计算各种表达式的值，
 *       支持常量、列引用、算术运算、比较运算和逻辑运算的求值
 */

#include "execution/expression_evaluator.h"

#include <stdexcept>

#include "common/exception.h"

namespace SimpleRDBMS {

/**
 * 表达式求值的主入口方法
 * 根据表达式类型分发到具体的求值方法
 * @param expr 要求值的表达式
 * @param tuple 当前记录，用于获取列值
 * @return 表达式的计算结果
 */
Value ExpressionEvaluator::Evaluate(const Expression* expr,
                                    const Tuple& tuple) {
    if (!expr) {
        throw ExecutionException("Null expression");
    }

    // 根据表达式类型分发求值
    switch (expr->GetType()) {
        case Expression::ExprType::CONSTANT:
            // 常量表达式：直接返回常量值
            return EvaluateConstant(
                static_cast<const ConstantExpression*>(expr), tuple);

        case Expression::ExprType::COLUMN_REF:
            // 列引用表达式：从tuple中获取对应列的值
            return EvaluateColumnRef(
                static_cast<const ColumnRefExpression*>(expr), tuple);

        case Expression::ExprType::BINARY_OP:
            // 二元操作表达式：递归计算左右操作数，然后应用操作符
            return EvaluateBinaryOp(
                static_cast<const BinaryOpExpression*>(expr), tuple);

        case Expression::ExprType::UNARY_OP:
            // 一元操作表达式：计算操作数，然后应用操作符
            return EvaluateUnaryOp(static_cast<const UnaryOpExpression*>(expr),
                                   tuple);

        default:
            throw ExecutionException("Unsupported expression type");
    }
}

/**
 * 将表达式求值结果转换为布尔值
 * 主要用于WHERE子句的条件判断
 * @param expr 要求值的表达式
 * @param tuple 当前记录
 * @return 表达式的布尔值结果
 */
bool ExpressionEvaluator::EvaluateAsBoolean(const Expression* expr,
                                            const Tuple& tuple) {
    Value result = Evaluate(expr, tuple);
    bool bool_result = IsValueTrue(result);

    // 记录调试信息，便于troubleshooting
    LOG_DEBUG("EvaluateAsBoolean: result = " << bool_result);

    return bool_result;
}

/**
 * 求值常量表达式
 * 直接返回常量的值，不依赖tuple
 * @param expr 常量表达式
 * @param tuple 当前记录（未使用）
 * @return 常量值
 */
Value ExpressionEvaluator::EvaluateConstant(const ConstantExpression* expr,
                                            const Tuple& tuple) {
    (void)tuple;  // 明确标记未使用的参数
    return expr->GetValue();
}

/**
 * 求值列引用表达式
 * 从tuple中根据列名获取对应的值
 * @param expr 列引用表达式
 * @param tuple 当前记录
 * @return 列的值
 */
Value ExpressionEvaluator::EvaluateColumnRef(const ColumnRefExpression* expr,
                                             const Tuple& tuple) {
    const std::string& column_name = expr->GetColumnName();
    try {
        if (!schema_) {
            throw ExecutionException("Schema is null in expression evaluator");
        }

        // 通过schema查找列名对应的索引
        size_t column_idx = schema_->GetColumnIdx(column_name);
        const auto& tuple_values = tuple.GetValues();

        // 检查列索引是否超出范围
        if (column_idx >= tuple_values.size()) {
            LOG_ERROR("EvaluateColumnRef: Column index "
                      << column_idx << " out of range, tuple has "
                      << tuple_values.size() << " values");
            throw ExecutionException("Column index out of range: " +
                                     column_name);
        }

        // 从tuple中获取指定列的值
        return tuple.GetValue(column_idx);
    } catch (const std::exception& e) {
        throw ExecutionException("Column not found or invalid: " + column_name +
                                 " - " + e.what());
    }
}

/**
 * 求值二元操作表达式
 * 处理算术运算、比较运算和逻辑运算
 * 对逻辑运算实现短路求值优化
 * @param expr 二元操作表达式
 * @param tuple 当前记录
 * @return 运算结果
 */
Value ExpressionEvaluator::EvaluateBinaryOp(const BinaryOpExpression* expr,
                                            const Tuple& tuple) {
    // 先计算左操作数
    Value left_val = Evaluate(expr->GetLeft(), tuple);
    BinaryOpExpression::OpType op = expr->GetOperator();

    // 逻辑AND运算：实现短路求值
    if (op == BinaryOpExpression::OpType::AND) {
        if (!IsValueTrue(left_val)) {
            return Value(false);  // 左操作数为false，整个AND为false
        }
        Value right_val = Evaluate(expr->GetRight(), tuple);
        return Value(IsValueTrue(right_val));
    }

    // 逻辑OR运算：实现短路求值
    if (op == BinaryOpExpression::OpType::OR) {
        if (IsValueTrue(left_val)) {
            return Value(true);  // 左操作数为true，整个OR为true
        }
        Value right_val = Evaluate(expr->GetRight(), tuple);
        return Value(IsValueTrue(right_val));
    }

    // 对于其他操作符，需要计算右操作数
    Value right_val = Evaluate(expr->GetRight(), tuple);

    // 处理算术运算：+、-、*、/
    if (op == BinaryOpExpression::OpType::PLUS ||
        op == BinaryOpExpression::OpType::MINUS ||
        op == BinaryOpExpression::OpType::MULTIPLY ||
        op == BinaryOpExpression::OpType::DIVIDE) {
        return EvaluateArithmeticOp(left_val, right_val, op);
    }

    // 处理比较运算：=、!=、<、<=、>、>=
    bool result = CompareValues(left_val, right_val, op);
    return Value(result);
}

/**
 * 执行算术运算
 * 将操作数转换为double类型进行计算，避免整数溢出
 * @param left 左操作数
 * @param right 右操作数
 * @param op 算术操作符
 * @return 运算结果
 */
Value ExpressionEvaluator::EvaluateArithmeticOp(const Value& left,
                                                const Value& right,
                                                BinaryOpExpression::OpType op) {
    // Lambda函数：将Value转换为double进行计算
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

        // 根据操作符执行相应的算术运算
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

        // 智能类型推断：如果原来都是整数且不是除法，返回整数结果
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

/**
 * 求值一元操作表达式
 * 支持逻辑NOT和数值取负操作
 * @param expr 一元操作表达式
 * @param tuple 当前记录
 * @return 运算结果
 */
Value ExpressionEvaluator::EvaluateUnaryOp(const UnaryOpExpression* expr,
                                           const Tuple& tuple) {
    Value operand_val = Evaluate(expr->GetOperand(), tuple);

    switch (expr->GetOperator()) {
        case UnaryOpExpression::OpType::NOT:
            // 逻辑NOT：对操作数取反
            return Value(!IsValueTrue(operand_val));

        case UnaryOpExpression::OpType::NEGATIVE:
            // 数值取负：根据数值类型分别处理
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

/**
 * 比较两个Value的值
 * 支持同类型比较和数值类型间的自动转换比较
 * @param left 左操作数
 * @param right 右操作数
 * @param op 比较操作符
 * @return 比较结果
 */
bool ExpressionEvaluator::CompareValues(const Value& left, const Value& right,
                                        BinaryOpExpression::OpType op) {
    // 优先处理相同类型的比较，性能更好
    if (left.index() == right.index()) {
        switch (left.index()) {
            case 0:  // bool类型
                return CompareNumeric(std::get<bool>(left),
                                      std::get<bool>(right), op);
            case 1:  // int8_t类型
                return CompareNumeric(std::get<int8_t>(left),
                                      std::get<int8_t>(right), op);
            case 2:  // int16_t类型
                return CompareNumeric(std::get<int16_t>(left),
                                      std::get<int16_t>(right), op);
            case 3:  // int32_t类型
                return CompareNumeric(std::get<int32_t>(left),
                                      std::get<int32_t>(right), op);
            case 4:  // int64_t类型
                return CompareNumeric(std::get<int64_t>(left),
                                      std::get<int64_t>(right), op);
            case 5:  // float类型
                return CompareNumeric(std::get<float>(left),
                                      std::get<float>(right), op);
            case 6:  // double类型
                return CompareNumeric(std::get<double>(left),
                                      std::get<double>(right), op);
            case 7:  // string类型
                return CompareNumeric(std::get<std::string>(left),
                                      std::get<std::string>(right), op);
        }
    }

    // 处理不同数值类型间的转换比较
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

    // 如果两个操作数都能转换为数值，则进行数值比较
    if (left_result.first && right_result.first) {
        return CompareNumeric(left_result.second, right_result.second, op);
    }

    throw ExecutionException(
        "Type mismatch in comparison - cannot convert operands to comparable "
        "types");
}

/**
 * 模板方法：执行具体的数值比较
 * 支持所有可比较类型的比较操作
 * @param left 左操作数
 * @param right 右操作数
 * @param op 比较操作符
 * @return 比较结果
 */
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

/**
 * 判断Value的真值
 * 实现SQL中的真值语义：
 * - 布尔值：直接返回
 * - 数值：非零为true
 * - 字符串：非空为true
 * - 其他：false
 * @param value 要判断的值
 * @return 真值结果
 */
bool ExpressionEvaluator::IsValueTrue(const Value& value) {
    if (std::holds_alternative<bool>(value)) {
        return std::get<bool>(value);
    }

    // 数值类型的真值判断：非零为true
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

    // 字符串类型：非空为true
    if (std::holds_alternative<std::string>(value)) {
        return !std::get<std::string>(value).empty();
    }

    // 其他类型默认为false
    return false;
}

}  // namespace SimpleRDBMS