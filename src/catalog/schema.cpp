// src/catalog/schema.cpp
#include "catalog/schema.h"
#include "common/exception.h"

namespace SimpleRDBMS {

Schema::Schema(const std::vector<Column>& columns) : columns_(columns) {
    for (size_t i = 0; i < columns_.size(); i++) {
        column_indices_[columns_[i].name] = i;
    }
}

const Column& Schema::GetColumn(const std::string& name) const {
    auto it = column_indices_.find(name);
    if (it == column_indices_.end()) {
        throw Exception("Column not found: " + name);
    }
    return columns_[it->second];
}

size_t Schema::GetColumnIdx(const std::string& name) const {
    auto it = column_indices_.find(name);
    if (it == column_indices_.end()) {
        throw Exception("Column not found: " + name);
    }
    return it->second;
}

size_t Schema::GetTupleSize() const {
    size_t size = 0;
    for (const auto& column : columns_) {
        switch (column.type) {
            case TypeId::BOOLEAN:
                size += sizeof(bool);
                break;
            case TypeId::TINYINT:
                size += sizeof(int8_t);
                break;
            case TypeId::SMALLINT:
                size += sizeof(int16_t);
                break;
            case TypeId::INTEGER:
                size += sizeof(int32_t);
                break;
            case TypeId::BIGINT:
                size += sizeof(int64_t);
                break;
            case TypeId::FLOAT:
                size += sizeof(float);
                break;
            case TypeId::DOUBLE:
                size += sizeof(double);
                break;
            case TypeId::VARCHAR:
                size += column.size;
                break;
            default:
                break;
        }
    }
    return size;
}

bool Schema::HasColumn(const std::string& name) const {
    return column_indices_.find(name) != column_indices_.end();
}

}  // namespace SimpleRDBMS