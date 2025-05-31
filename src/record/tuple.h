#pragma once

#include <vector>
#include <memory>
#include "common/types.h"
#include "catalog/schema.h"

namespace SimpleRDBMS {

class Tuple {
public:
    Tuple() = default;
    Tuple(std::vector<Value> values, const Schema* schema);
    
    // Get value at index
    Value GetValue(size_t index) const;
    
    // Get all values
    const std::vector<Value>& GetValues() const { return values_; }
    
    // Serialize/Deserialize
    void SerializeTo(char* data) const;
    void DeserializeFrom(const char* data, const Schema* schema);
    
    // Get serialized size
    size_t GetSerializedSize() const;
    
    // Get/Set RID
    RID GetRID() const { return rid_; }
    void SetRID(const RID& rid) { rid_ = rid; }

private:
    std::vector<Value> values_;
    RID rid_;
    size_t serialized_size_;
};

}  // namespace SimpleRDBMS