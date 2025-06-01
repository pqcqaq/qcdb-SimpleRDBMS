#include "record/tuple.h"

#include <cstring>
#include <stdexcept>

#include "common/debug.h"

namespace SimpleRDBMS {

Tuple::Tuple(std::vector<Value> values, const Schema* schema)
    : values_(std::move(values)) {
    serialized_size_ = 0;
    
    if (!schema) {
        throw std::runtime_error("Schema cannot be null");
    }
    
    LOG_DEBUG("Tuple construction: values count=" << values_.size() 
              << ", schema columns=" << schema->GetColumnCount());
    
    if (values_.size() != schema->GetColumnCount()) {
        LOG_ERROR("Tuple construction: Value count "
                  << values_.size() << " doesn't match schema column count "
                  << schema->GetColumnCount());
        throw std::runtime_error(
            "Value count doesn't match schema column count");
    }
    
    for (size_t i = 0; i < values_.size(); i++) {
        if (i >= schema->GetColumnCount()) {
            throw std::runtime_error("Column index exceeds schema size");
        }
        
        const auto& column = schema->GetColumn(i);
        Value& value = values_[i];
        
        LOG_DEBUG("Tuple construction: Processing column " << i 
                  << " (" << column.name << ") type=" << static_cast<int>(column.type));
        
        try {
            switch (column.type) {
                case TypeId::BOOLEAN: {
                    if (!std::holds_alternative<bool>(value)) {
                        if (std::holds_alternative<int32_t>(value)) {
                            bool converted = std::get<int32_t>(value) != 0;
                            value = Value(converted);
                            LOG_DEBUG("Converted int32_t to bool: " << converted);
                        } else if (std::holds_alternative<std::string>(value)) {
                            const std::string& str = std::get<std::string>(value);
                            bool converted = (str == "TRUE" || str == "true" || str == "1");
                            value = Value(converted);
                            LOG_DEBUG("Converted string '" << str << "' to bool: " << converted);
                        } else {
                            throw std::runtime_error("Cannot convert value to BOOLEAN");
                        }
                    }
                    serialized_size_ += sizeof(bool);
                    break;
                }
                case TypeId::TINYINT: {
                    if (!std::holds_alternative<int8_t>(value)) {
                        if (std::holds_alternative<int32_t>(value)) {
                            int8_t converted = static_cast<int8_t>(std::get<int32_t>(value));
                            value = Value(converted);
                            LOG_DEBUG("Converted int32_t to int8_t: " << static_cast<int>(converted));
                        } else {
                            throw std::runtime_error("Cannot convert value to TINYINT");
                        }
                    }
                    serialized_size_ += sizeof(int8_t);
                    break;
                }
                case TypeId::SMALLINT: {
                    if (!std::holds_alternative<int16_t>(value)) {
                        if (std::holds_alternative<int32_t>(value)) {
                            int16_t converted = static_cast<int16_t>(std::get<int32_t>(value));
                            value = Value(converted);
                            LOG_DEBUG("Converted int32_t to int16_t: " << converted);
                        } else {
                            throw std::runtime_error("Cannot convert value to SMALLINT");
                        }
                    }
                    serialized_size_ += sizeof(int16_t);
                    break;
                }
                case TypeId::INTEGER: {
                    if (!std::holds_alternative<int32_t>(value)) {
                        if (std::holds_alternative<int64_t>(value)) {
                            int32_t converted = static_cast<int32_t>(std::get<int64_t>(value));
                            value = Value(converted);
                            LOG_DEBUG("Converted int64_t to int32_t: " << converted);
                        } else if (std::holds_alternative<int16_t>(value)) {
                            int32_t converted = static_cast<int32_t>(std::get<int16_t>(value));
                            value = Value(converted);
                            LOG_DEBUG("Converted int16_t to int32_t: " << converted);
                        } else if (std::holds_alternative<int8_t>(value)) {
                            int32_t converted = static_cast<int32_t>(std::get<int8_t>(value));
                            value = Value(converted);
                            LOG_DEBUG("Converted int8_t to int32_t: " << converted);
                        } else {
                            throw std::runtime_error("Cannot convert value to INTEGER");
                        }
                    }
                    serialized_size_ += sizeof(int32_t);
                    break;
                }
                case TypeId::BIGINT: {
                    if (!std::holds_alternative<int64_t>(value)) {
                        if (std::holds_alternative<int32_t>(value)) {
                            int64_t converted = static_cast<int64_t>(std::get<int32_t>(value));
                            value = Value(converted);
                            LOG_DEBUG("Converted int32_t to int64_t: " << converted);
                        } else {
                            throw std::runtime_error("Cannot convert value to BIGINT");
                        }
                    }
                    serialized_size_ += sizeof(int64_t);
                    break;
                }
                case TypeId::FLOAT: {
                    if (!std::holds_alternative<float>(value)) {
                        if (std::holds_alternative<double>(value)) {
                            float converted = static_cast<float>(std::get<double>(value));
                            value = Value(converted);
                            LOG_DEBUG("Converted double to float: " << converted);
                        } else if (std::holds_alternative<int32_t>(value)) {
                            float converted = static_cast<float>(std::get<int32_t>(value));
                            value = Value(converted);
                            LOG_DEBUG("Converted int32_t to float: " << converted);
                        } else {
                            throw std::runtime_error("Cannot convert value to FLOAT");
                        }
                    }
                    serialized_size_ += sizeof(float);
                    break;
                }
                case TypeId::DOUBLE: {
                    if (!std::holds_alternative<double>(value)) {
                        if (std::holds_alternative<float>(value)) {
                            double converted = static_cast<double>(std::get<float>(value));
                            value = Value(converted);
                            LOG_DEBUG("Converted float to double: " << converted);
                        } else if (std::holds_alternative<int32_t>(value)) {
                            double converted = static_cast<double>(std::get<int32_t>(value));
                            value = Value(converted);
                            LOG_DEBUG("Converted int32_t to double: " << converted);
                        } else {
                            throw std::runtime_error("Cannot convert value to DOUBLE");
                        }
                    }
                    serialized_size_ += sizeof(double);
                    break;
                }
                case TypeId::VARCHAR: {
                    if (!std::holds_alternative<std::string>(value)) {
                        throw std::runtime_error(
                            "Cannot convert value to VARCHAR for column " + column.name);
                    }
                    const std::string& str = std::get<std::string>(value);
                    serialized_size_ += sizeof(uint32_t) + str.size();
                    LOG_DEBUG("VARCHAR column '" << column.name << "' length: " << str.size());
                    break;
                }
                default:
                    throw std::runtime_error(
                        "Unsupported column type for column " + column.name);
            }
        } catch (const std::exception& e) {
            LOG_ERROR("Tuple construction failed at column "
                      << i << " (" << column.name << "): " << e.what());
            throw;
        }
    }
    
    LOG_DEBUG("Tuple construction completed: " << values_.size() 
              << " values, serialized_size=" << serialized_size_);
}

void Tuple::SerializeTo(char* data) const {
    size_t offset = 0;
    for (const auto& value : values_) {
        if (std::holds_alternative<bool>(value)) {
            bool v = std::get<bool>(value);
            std::memcpy(data + offset, &v, sizeof(bool));
            offset += sizeof(bool);
        } else if (std::holds_alternative<int8_t>(value)) {
            int8_t v = std::get<int8_t>(value);
            std::memcpy(data + offset, &v, sizeof(int8_t));
            offset += sizeof(int8_t);
        } else if (std::holds_alternative<int16_t>(value)) {
            int16_t v = std::get<int16_t>(value);
            std::memcpy(data + offset, &v, sizeof(int16_t));
            offset += sizeof(int16_t);
        } else if (std::holds_alternative<int32_t>(value)) {
            int32_t v = std::get<int32_t>(value);
            std::memcpy(data + offset, &v, sizeof(int32_t));
            offset += sizeof(int32_t);
        } else if (std::holds_alternative<int64_t>(value)) {
            int64_t v = std::get<int64_t>(value);
            std::memcpy(data + offset, &v, sizeof(int64_t));
            offset += sizeof(int64_t);
        } else if (std::holds_alternative<float>(value)) {
            float v = std::get<float>(value);
            std::memcpy(data + offset, &v, sizeof(float));
            offset += sizeof(float);
        } else if (std::holds_alternative<double>(value)) {
            double v = std::get<double>(value);
            std::memcpy(data + offset, &v, sizeof(double));
            offset += sizeof(double);
        } else if (std::holds_alternative<std::string>(value)) {
            const std::string& str = std::get<std::string>(value);
            uint32_t len = static_cast<uint32_t>(str.size());
            std::memcpy(data + offset, &len, sizeof(uint32_t));
            offset += sizeof(uint32_t);
            std::memcpy(data + offset, str.data(), len);
            offset += len;
        }
    }
}

void Tuple::DeserializeFrom(const char* data, const Schema* schema) {
    values_.clear();
    serialized_size_ = 0;
    
    if (!data || !schema) {
        LOG_ERROR("Tuple::DeserializeFrom: null data or schema");
        return;
    }
    
    size_t offset = 0;
    
    try {
        for (size_t i = 0; i < schema->GetColumnCount(); i++) {
            const auto& column = schema->GetColumn(i);
            
            switch (column.type) {
                case TypeId::BOOLEAN: {
                    bool v;
                    std::memcpy(&v, data + offset, sizeof(bool));
                    values_.emplace_back(v);
                    offset += sizeof(bool);
                    serialized_size_ += sizeof(bool);
                    break;
                }
                case TypeId::TINYINT: {
                    int8_t v;
                    std::memcpy(&v, data + offset, sizeof(int8_t));
                    values_.emplace_back(v);
                    offset += sizeof(int8_t);
                    serialized_size_ += sizeof(int8_t);
                    break;
                }
                case TypeId::SMALLINT: {
                    int16_t v;
                    std::memcpy(&v, data + offset, sizeof(int16_t));
                    values_.emplace_back(v);
                    offset += sizeof(int16_t);
                    serialized_size_ += sizeof(int16_t);
                    break;
                }
                case TypeId::INTEGER: {
                    int32_t v;
                    std::memcpy(&v, data + offset, sizeof(int32_t));
                    values_.emplace_back(v);
                    offset += sizeof(int32_t);
                    serialized_size_ += sizeof(int32_t);
                    break;
                }
                case TypeId::BIGINT: {
                    int64_t v;
                    std::memcpy(&v, data + offset, sizeof(int64_t));
                    values_.emplace_back(v);
                    offset += sizeof(int64_t);
                    serialized_size_ += sizeof(int64_t);
                    break;
                }
                case TypeId::FLOAT: {
                    float v;
                    std::memcpy(&v, data + offset, sizeof(float));
                    values_.emplace_back(v);
                    offset += sizeof(float);
                    serialized_size_ += sizeof(float);
                    break;
                }
                case TypeId::DOUBLE: {
                    double v;
                    std::memcpy(&v, data + offset, sizeof(double));
                    values_.emplace_back(v);
                    offset += sizeof(double);
                    serialized_size_ += sizeof(double);
                    break;
                }
                case TypeId::VARCHAR: {
                    uint32_t len;
                    std::memcpy(&len, data + offset, sizeof(uint32_t));
                    offset += sizeof(uint32_t);
                    
                    // 更合理的长度检查
                    if (len > MAX_TUPLE_SIZE) {
                        LOG_ERROR("Tuple::DeserializeFrom: VARCHAR length " << len << " exceeds maximum");
                        values_.clear();
                        serialized_size_ = 0;
                        return;
                    }
                    
                    std::string str(data + offset, len);
                    values_.emplace_back(std::move(str));
                    offset += len;
                    serialized_size_ += sizeof(uint32_t) + len;
                    break;
                }
                default:
                    LOG_ERROR("Tuple::DeserializeFrom: Unsupported column type: " 
                              << static_cast<int>(column.type));
                    values_.clear();
                    serialized_size_ = 0;
                    return;
            }
        }
        
        LOG_DEBUG("Tuple::DeserializeFrom: Successfully deserialized " 
                  << values_.size() << " values, total size: " << serialized_size_);
                  
    } catch (const std::exception& e) {
        LOG_ERROR("Tuple::DeserializeFrom: Exception during deserialization: " << e.what());
        values_.clear();
        serialized_size_ = 0;
        throw;
    }
}

size_t Tuple::GetSerializedSize() const { return serialized_size_; }

Value Tuple::GetValue(size_t index) const {
    if (index >= values_.size()) {
        LOG_ERROR("Tuple::GetValue: Index "
                  << index << " out of range, size=" << values_.size());
        throw std::out_of_range("Index out of range");
    }
    return values_[index];
}

}  // namespace SimpleRDBMS