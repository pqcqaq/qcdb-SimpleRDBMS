#pragma once

#include <memory>
#include <vector>
#include "catalog/schema.h"
#include "common/types.h"

namespace SimpleRDBMS {

enum class PlanNodeType {
    SEQUENTIAL_SCAN,
    INDEX_SCAN,
    INSERT,
    UPDATE,
    DELETE,
    PROJECTION,
    FILTER,
    NESTED_LOOP_JOIN,
    HASH_JOIN,
    AGGREGATION,
    SORT,
    LIMIT
};

class PlanNode {
public:
    PlanNode(const Schema* output_schema, std::vector<std::unique_ptr<PlanNode>> children)
        : output_schema_(output_schema), children_(std::move(children)) {}
    
    virtual ~PlanNode() = default;
    
    virtual PlanNodeType GetType() const = 0;
    
    const Schema* GetOutputSchema() const { return output_schema_; }
    
    const std::vector<std::unique_ptr<PlanNode>>& GetChildren() const { return children_; }
    
    const PlanNode* GetChild(size_t index) const { 
        return index < children_.size() ? children_[index].get() : nullptr; 
    }

protected:
    const Schema* output_schema_;
    std::vector<std::unique_ptr<PlanNode>> children_;
};

// Sequential scan plan node
class SeqScanPlanNode : public PlanNode {
public:
    SeqScanPlanNode(const Schema* output_schema, 
                    const std::string& table_name,
                    std::unique_ptr<Expression> predicate = nullptr)
        : PlanNode(output_schema, {}),
          table_name_(table_name),
          predicate_(std::move(predicate)) {}
    
    PlanNodeType GetType() const override { return PlanNodeType::SEQUENTIAL_SCAN; }
    
    const std::string& GetTableName() const { return table_name_; }
    Expression* GetPredicate() const { return predicate_.get(); }

private:
    std::string table_name_;
    std::unique_ptr<Expression> predicate_;
};

// Insert plan node
class InsertPlanNode : public PlanNode {
public:
    InsertPlanNode(const Schema* output_schema,
                   const std::string& table_name,
                   std::vector<std::vector<Value>> values)
        : PlanNode(output_schema, {}),
          table_name_(table_name),
          values_(std::move(values)) {}
    
    PlanNodeType GetType() const override { return PlanNodeType::INSERT; }
    
    const std::string& GetTableName() const { return table_name_; }
    const std::vector<std::vector<Value>>& GetValues() const { return values_; }

private:
    std::string table_name_;
    std::vector<std::vector<Value>> values_;
};

// Projection plan node
class ProjectionPlanNode : public PlanNode {
public:
    ProjectionPlanNode(const Schema* output_schema,
                       std::vector<std::unique_ptr<Expression>> expressions,
                       std::unique_ptr<PlanNode> child)
        : PlanNode(output_schema, {}),
          expressions_(std::move(expressions)) {
        children_.push_back(std::move(child));
    }
    
    PlanNodeType GetType() const override { return PlanNodeType::PROJECTION; }
    
    const std::vector<std::unique_ptr<Expression>>& GetExpressions() const { return expressions_; }

private:
    std::vector<std::unique_ptr<Expression>> expressions_;
};

}  // namespace SimpleRDBMS