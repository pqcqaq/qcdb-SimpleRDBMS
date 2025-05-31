#include "record/tuple.h"
#include <cstring>

namespace SimpleRDBMS {

Tuple::Tuple(std::vector<Value> values, const Schema* schema) 
    : values_(std::move(values)) {
    serialized_size_ = 0;
    for (size_t i = 0; i < values_.size(); i++) {
        const auto& column = schema->GetColumn(i);
        switch (column.type) {
            case TypeId::BOOLEAN:
                serialized_size_ += sizeof(bool);
                break;
            case TypeId::TINYINT:
                serialized_size_ += sizeof(int8_t);
                break;
            case TypeId::SMALLINT:
                serialized_size_ += sizeof(int16_t);
                break;
            case TypeId::INTEGER:
                serialized_size_ += sizeof(int32_t);
                break;
            case TypeId::BIGINT:
                serialized_size_ += sizeof(int64_t);
                break;
            case TypeId::FLOAT:
                serialized_size_ += sizeof(float);
                break;
            case TypeId::DOUBLE:
                serialized_size_ += sizeof(double);
                break;
            case TypeId::VARCHAR:
                serialized_size_ += sizeof(uint32_t) + std::get<std::string>(values_[i]).size();
                break;
            default:
                break;
        }
    }
}

Value Tuple::GetValue(size_t index) const {
    return values_[index];
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
    size_t offset = 0;
    
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
                std::string str(data + offset, len);
                values_.emplace_back(std::move(str));
                offset += len;
                serialized_size_ += sizeof(uint32_t) + len;
                break;
            }
            default:
                break;
        }
    }
}

size_t Tuple::GetSerializedSize() const {
    return serialized_size_;
}

}  // namespace SimpleRDBMS