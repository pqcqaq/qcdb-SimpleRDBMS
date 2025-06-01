/*
 * 文件: expression_cloner.h
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述: 表达式克隆器头文件，提供各种表达式类型的深度复制功能，
 *       用于查询计划优化和表达式树的复制操作
 */

#pragma once

#include <memory>

#include "parser/ast.h"

namespace SimpleRDBMS {

/**
 * 表达式克隆器工具类
 * 提供深度复制表达式树的静态方法，支持所有表达式类型的递归克隆
 * 主要用于查询优化器中需要复制表达式的场景，比如plan node的复制
 */
class ExpressionCloner {
   public:
    /**
     * 克隆表达式的主入口方法
     * 根据表达式的具体类型自动选择合适的克隆策略
     * 对于复合表达式会递归克隆所有子表达式
     * @param expr 要克隆的原始表达式指针
     * @return 克隆后的新表达式智能指针，如果输入为nullptr则返回nullptr
     */
    static std::unique_ptr<Expression> Clone(const Expression* expr);

   private:
    /**
     * 克隆常量表达式
     * 复制Value对象，创建新的ConstantExpression实例
     * @param expr 原始常量表达式
     * @return 新的常量表达式
     */
    static std::unique_ptr<Expression> CloneConstant(
        const ConstantExpression* expr);

    /**
     * 克隆列引用表达式
     * 复制表名和列名信息，创建新的ColumnRefExpression实例
     * @param expr 原始列引用表达式
     * @return 新的列引用表达式
     */
    static std::unique_ptr<Expression> CloneColumnRef(
        const ColumnRefExpression* expr);

    /**
     * 克隆二元操作表达式
     * 递归克隆左右两个操作数，保持操作符类型不变
     * 支持所有二元操作：算术运算、比较运算、逻辑运算等
     * @param expr 原始二元操作表达式
     * @return 新的二元操作表达式
     */
    static std::unique_ptr<Expression> CloneBinaryOp(
        const BinaryOpExpression* expr);

    /**
     * 克隆一元操作表达式
     * 递归克隆操作数，保持操作符类型不变
     * 支持所有一元操作：NOT、负号等
     * @param expr 原始一元操作表达式
     * @return 新的一元操作表达式
     */
    static std::unique_ptr<Expression> CloneUnaryOp(
        const UnaryOpExpression* expr);
};

}  // namespace SimpleRDBMS