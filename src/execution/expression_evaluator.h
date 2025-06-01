/*
 * 文件: expression_evaluator.h
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述: 表达式求值器头文件，定义了查询执行过程中各种表达式的求值接口，
 *       支持常量、列引用、算术运算、比较运算和逻辑运算的计算
 */

#pragma once

#include <variant>

#include "catalog/schema.h"
#include "common/types.h"
#include "parser/ast.h"
#include "record/tuple.h"

namespace SimpleRDBMS {

/**
 * 表达式求值器类
 * 核心功能是在查询执行过程中计算各种表达式的值
 * 主要用于WHERE子句条件判断、SELECT投影列计算、UPDATE的SET子句等场景
 *
 * 设计思路：
 * 1. 基于访问者模式，根据表达式类型分发到具体的求值方法
 * 2. 支持多种数据类型的自动转换和比较
 * 3. 实现短路求值优化（AND/OR逻辑运算）
 * 4. 提供统一的Value类型作为计算结果
 */
class ExpressionEvaluator {
   public:
    /**
     * 构造函数
     * @param schema 表的schema信息，用于解析列引用和获取列索引
     */
    ExpressionEvaluator(const Schema* schema) : schema_(schema) {}

    /**
     * 表达式求值主接口
     * 计算表达式在给定tuple上下文中的值
     * @param expr 要计算的表达式AST节点
     * @param tuple 当前记录，提供列值的上下文
     * @return 表达式的计算结果，封装在Value中
     */
    Value Evaluate(const Expression* expr, const Tuple& tuple);

    /**
     * 布尔值求值接口
     * 专门用于WHERE子句的条件判断，将求值结果转换为布尔值
     * 实现SQL的真值语义：非零数值、非空字符串等都视为true
     * @param expr 要计算的表达式
     * @param tuple 当前记录
     * @return 表达式的布尔值结果
     */
    bool EvaluateAsBoolean(const Expression* expr, const Tuple& tuple);

   private:
    const Schema* schema_;  // 表schema，用于列名到索引的映射

    /**
     * 常量表达式求值
     * 最简单的情况，直接返回常量值，不依赖tuple
     * @param expr 常量表达式节点
     * @param tuple 当前记录（未使用）
     * @return 常量值
     */
    Value EvaluateConstant(const ConstantExpression* expr, const Tuple& tuple);

    /**
     * 列引用表达式求值
     * 根据列名从schema中找到列索引，然后从tuple中获取对应的值
     * 这是连接表达式和实际数据的关键桥梁
     * @param expr 列引用表达式节点
     * @param tuple 当前记录
     * @return 列的值
     */
    Value EvaluateColumnRef(const ColumnRefExpression* expr,
                            const Tuple& tuple);

    /**
     * 二元操作表达式求值
     * 递归计算左右操作数，然后应用相应的操作符
     * 支持算术运算（+、-、*、/）、比较运算（=、!=、<、>等）、逻辑运算（AND、OR）
     * 对逻辑运算实现短路求值优化
     * @param expr 二元操作表达式节点
     * @param tuple 当前记录
     * @return 运算结果
     */
    Value EvaluateBinaryOp(const BinaryOpExpression* expr, const Tuple& tuple);

    /**
     * 一元操作表达式求值
     * 计算操作数，然后应用一元操作符
     * 支持逻辑NOT和数值取负操作
     * @param expr 一元操作表达式节点
     * @param tuple 当前记录
     * @return 运算结果
     */
    Value EvaluateUnaryOp(const UnaryOpExpression* expr, const Tuple& tuple);

    /**
     * 算术运算专用求值方法
     * 处理数值类型的加减乘除运算
     * 自动进行类型转换，避免整数溢出问题
     * @param left 左操作数
     * @param right 右操作数
     * @param op 算术操作符
     * @return 运算结果
     */
    Value EvaluateArithmeticOp(const Value& left, const Value& right,
                               BinaryOpExpression::OpType op);

    /**
     * 值比较功能
     * 支持同类型直接比较和不同数值类型间的自动转换比较
     * 实现SQL的比较语义，处理类型兼容性
     * @param left 左操作数
     * @param right 右操作数
     * @param op 比较操作符
     * @return 比较结果
     */
    bool CompareValues(const Value& left, const Value& right,
                       BinaryOpExpression::OpType op);

    /**
     * Value真值判断
     * 实现SQL标准的真值语义：
     * - 布尔值：直接返回
     * - 数值：非零为true
     * - 字符串：非空为true
     * - NULL：false
     * @param value 要判断的值
     * @return 真值结果
     */
    bool IsValueTrue(const Value& value);

    /**
     * 模板化的数值比较方法
     * 对于可比较的类型，统一实现各种比较操作符
     * 支持所有标准的比较操作：=、!=、<、<=、>、>=
     * @param left 左操作数
     * @param right 右操作数
     * @param op 比较操作符
     * @return 比较结果
     */
    template <typename T>
    bool CompareNumeric(const T& left, const T& right,
                        BinaryOpExpression::OpType op);
};

}  // namespace SimpleRDBMS