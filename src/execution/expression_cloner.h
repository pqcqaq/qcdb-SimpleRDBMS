#pragma once

#include "parser/ast.h"
#include <memory>

namespace SimpleRDBMS {

class ExpressionCloner {
public:
    static std::unique_ptr<Expression> Clone(const Expression* expr);

private:
    static std::unique_ptr<Expression> CloneConstant(const ConstantExpression* expr);
    static std::unique_ptr<Expression> CloneColumnRef(const ColumnRefExpression* expr);
    static std::unique_ptr<Expression> CloneBinaryOp(const BinaryOpExpression* expr);
    static std::unique_ptr<Expression> CloneUnaryOp(const UnaryOpExpression* expr);
};

}  // namespace SimpleRDBMS