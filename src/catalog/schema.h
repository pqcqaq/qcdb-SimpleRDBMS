#pragma once

#include <vector>
#include <string>
#include <unordered_map>
#include "common/types.h"

namespace SimpleRDBMS {

class Schema {
public:
    explicit Schema(const std::vector<Column>& columns);
    
    // Get column by index
    const Column& GetColumn(size_t index) const { return columns_[index]; }
    
    // Get column by name
    const Column& GetColumn(const std::string& name) const;
    
    // Get column index by name
    size_t GetColumnIdx(const std::string& name) const;
    
    // Get all columns
    const std::vector<Column>& GetColumns() const { return columns_; }
    
    // Get column count
    size_t GetColumnCount() const { return columns_.size(); }
    
    // Get tuple size
    size_t GetTupleSize() const;
    
    // Check if column exists
    bool HasColumn(const std::string& name) const;

private:
    std::vector<Column> columns_;
    std::unordered_map<std::string, size_t> column_indices_;
};

}  // namespace SimpleRDBMS