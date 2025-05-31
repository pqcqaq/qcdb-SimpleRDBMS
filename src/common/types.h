#pragma once

#include <string>
#include <vector>
#include <variant>
#include <memory>
#include "common/config.h"
#include <functional>  // 添加这一行

namespace SimpleRDBMS {

// SQL data types
enum class TypeId {
    INVALID = 0,
    BOOLEAN,
    TINYINT,
    SMALLINT,
    INTEGER,
    BIGINT,
    DECIMAL,
    FLOAT,
    DOUBLE,
    VARCHAR,
    TIMESTAMP
};

// Value type for storing actual data
using Value = std::variant<
    bool,
    int8_t,
    int16_t,
    int32_t,
    int64_t,
    float,
    double,
    std::string
>;

// Column definition
struct Column {
    std::string name;
    TypeId type;
    size_t size;  // For VARCHAR
    bool nullable;
    bool is_primary_key;
};

// RID (Record Identifier)
struct RID {
    page_id_t page_id;
    slot_offset_t slot_num;
    
    bool operator==(const RID& other) const {
        return page_id == other.page_id && slot_num == other.slot_num;
    }
};

}  // namespace SimpleRDBMS

namespace std {
    template <>
    struct hash<SimpleRDBMS::RID> {
        size_t operator()(const SimpleRDBMS::RID& rid) const {
            return hash<SimpleRDBMS::page_id_t>()(rid.page_id) ^ 
                   (hash<SimpleRDBMS::slot_offset_t>()(rid.slot_num) << 1);
        }
    };
}