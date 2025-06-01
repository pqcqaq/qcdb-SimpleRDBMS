/*
 * 文件: tuple.cpp
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述: Tuple类的实现，负责数据库记录的存储、序列化和反序列化操作
 */

#include "record/tuple.h"

#include <cstring>
#include <stdexcept>

#include "common/debug.h"

namespace SimpleRDBMS {

/**
 * Tuple构造函数
 * @param values 要存储的值列表
 * @param schema 表的Schema信息，用于类型检查和转换
 *
 * 这个构造函数的核心任务：
 * 1. 验证输入参数的有效性
 * 2. 检查值的数量和schema的列数是否匹配
 * 3. 对每个值进行类型检查和必要的类型转换
 * 4. 计算序列化后的总大小
 */
Tuple::Tuple(std::vector<Value> values, const Schema* schema)
    : values_(std::move(values)) {
    serialized_size_ = 0;

    // 基本参数校验 - schema不能为空
    if (!schema) {
        throw std::runtime_error("Schema cannot be null");
    }

    LOG_DEBUG("Tuple construction: values count=" << values_.size()
                                                  << ", schema columns="
                                                  << schema->GetColumnCount());

    // 检查值的数量是否和schema定义的列数匹配
    if (values_.size() != schema->GetColumnCount()) {
        LOG_ERROR("Tuple construction: Value count "
                  << values_.size() << " doesn't match schema column count "
                  << schema->GetColumnCount());
        throw std::runtime_error(
            "Value count doesn't match schema column count");
    }

    // 遍历每个值，进行类型检查和转换
    for (size_t i = 0; i < values_.size(); i++) {
        // 防御性编程 - 再次检查索引有效性
        if (i >= schema->GetColumnCount()) {
            throw std::runtime_error("Column index exceeds schema size");
        }

        const auto& column = schema->GetColumn(i);
        Value& value = values_[i];

        LOG_DEBUG("Tuple construction: Processing column "
                  << i << " (" << column.name
                  << ") type=" << static_cast<int>(column.type));

        try {
            // 根据schema中定义的类型进行类型检查和转换
            switch (column.type) {
                case TypeId::BOOLEAN: {
                    // 如果不是bool类型，尝试从其他类型转换
                    if (!std::holds_alternative<bool>(value)) {
                        if (std::holds_alternative<int32_t>(value)) {
                            // int转bool：0为false，非0为true
                            bool converted = std::get<int32_t>(value) != 0;
                            value = Value(converted);
                            LOG_DEBUG(
                                "Converted int32_t to bool: " << converted);
                        } else if (std::holds_alternative<std::string>(value)) {
                            // 字符串转bool：支持常见的真值表示
                            const std::string& str =
                                std::get<std::string>(value);
                            bool converted =
                                (str == "TRUE" || str == "true" || str == "1");
                            value = Value(converted);
                            LOG_DEBUG("Converted string '"
                                      << str << "' to bool: " << converted);
                        } else {
                            throw std::runtime_error(
                                "Cannot convert value to BOOLEAN");
                        }
                    }
                    serialized_size_ += sizeof(bool);
                    break;
                }
                case TypeId::TINYINT: {
                    // TINYINT类型处理 - 8位有符号整数
                    if (!std::holds_alternative<int8_t>(value)) {
                        if (std::holds_alternative<int32_t>(value)) {
                            // 从int32强制转换，可能会丢失精度
                            int8_t converted =
                                static_cast<int8_t>(std::get<int32_t>(value));
                            value = Value(converted);
                            LOG_DEBUG("Converted int32_t to int8_t: "
                                      << static_cast<int>(converted));
                        } else {
                            throw std::runtime_error(
                                "Cannot convert value to TINYINT");
                        }
                    }
                    serialized_size_ += sizeof(int8_t);
                    break;
                }
                case TypeId::SMALLINT: {
                    // SMALLINT类型处理 - 16位有符号整数
                    if (!std::holds_alternative<int16_t>(value)) {
                        if (std::holds_alternative<int32_t>(value)) {
                            // 从int32强制转换，可能会丢失精度
                            int16_t converted =
                                static_cast<int16_t>(std::get<int32_t>(value));
                            value = Value(converted);
                            LOG_DEBUG(
                                "Converted int32_t to int16_t: " << converted);
                        } else {
                            throw std::runtime_error(
                                "Cannot convert value to SMALLINT");
                        }
                    }
                    serialized_size_ += sizeof(int16_t);
                    break;
                }
                case TypeId::INTEGER: {
                    // INTEGER类型处理 -
                    // 32位有符号整数，支持多种类型的向上/向下转换
                    if (!std::holds_alternative<int32_t>(value)) {
                        if (std::holds_alternative<int64_t>(value)) {
                            // 从int64向下转换，可能丢失精度
                            int32_t converted =
                                static_cast<int32_t>(std::get<int64_t>(value));
                            value = Value(converted);
                            LOG_DEBUG(
                                "Converted int64_t to int32_t: " << converted);
                        } else if (std::holds_alternative<int16_t>(value)) {
                            // 从int16向上转换，安全操作
                            int32_t converted =
                                static_cast<int32_t>(std::get<int16_t>(value));
                            value = Value(converted);
                            LOG_DEBUG(
                                "Converted int16_t to int32_t: " << converted);
                        } else if (std::holds_alternative<int8_t>(value)) {
                            // 从int8向上转换，安全操作
                            int32_t converted =
                                static_cast<int32_t>(std::get<int8_t>(value));
                            value = Value(converted);
                            LOG_DEBUG(
                                "Converted int8_t to int32_t: " << converted);
                        } else {
                            throw std::runtime_error(
                                "Cannot convert value to INTEGER");
                        }
                    }
                    serialized_size_ += sizeof(int32_t);
                    break;
                }
                case TypeId::BIGINT: {
                    // BIGINT类型处理 - 64位有符号整数
                    if (!std::holds_alternative<int64_t>(value)) {
                        if (std::holds_alternative<int32_t>(value)) {
                            // 从int32向上转换，安全操作
                            int64_t converted =
                                static_cast<int64_t>(std::get<int32_t>(value));
                            value = Value(converted);
                            LOG_DEBUG(
                                "Converted int32_t to int64_t: " << converted);
                        } else {
                            throw std::runtime_error(
                                "Cannot convert value to BIGINT");
                        }
                    }
                    serialized_size_ += sizeof(int64_t);
                    break;
                }
                case TypeId::FLOAT: {
                    // FLOAT类型处理 - 32位浮点数
                    if (!std::holds_alternative<float>(value)) {
                        if (std::holds_alternative<double>(value)) {
                            // 从double向下转换，可能丢失精度
                            float converted =
                                static_cast<float>(std::get<double>(value));
                            value = Value(converted);
                            LOG_DEBUG(
                                "Converted double to float: " << converted);
                        } else if (std::holds_alternative<int32_t>(value)) {
                            // 从整数转换为浮点数
                            float converted =
                                static_cast<float>(std::get<int32_t>(value));
                            value = Value(converted);
                            LOG_DEBUG(
                                "Converted int32_t to float: " << converted);
                        } else {
                            throw std::runtime_error(
                                "Cannot convert value to FLOAT");
                        }
                    }
                    serialized_size_ += sizeof(float);
                    break;
                }
                case TypeId::DOUBLE: {
                    // DOUBLE类型处理 - 64位浮点数
                    if (!std::holds_alternative<double>(value)) {
                        if (std::holds_alternative<float>(value)) {
                            // 从float向上转换，安全操作
                            double converted =
                                static_cast<double>(std::get<float>(value));
                            value = Value(converted);
                            LOG_DEBUG(
                                "Converted float to double: " << converted);
                        } else if (std::holds_alternative<int32_t>(value)) {
                            // 从整数转换为浮点数
                            double converted =
                                static_cast<double>(std::get<int32_t>(value));
                            value = Value(converted);
                            LOG_DEBUG(
                                "Converted int32_t to double: " << converted);
                        } else {
                            throw std::runtime_error(
                                "Cannot convert value to DOUBLE");
                        }
                    }
                    serialized_size_ += sizeof(double);
                    break;
                }
                case TypeId::VARCHAR: {
                    // VARCHAR类型处理 - 变长字符串，不支持自动转换
                    if (!std::holds_alternative<std::string>(value)) {
                        throw std::runtime_error(
                            "Cannot convert value to VARCHAR for column " +
                            column.name);
                    }
                    const std::string& str = std::get<std::string>(value);
                    // VARCHAR的序列化格式：4字节长度 + 实际字符串内容
                    serialized_size_ += sizeof(uint32_t) + str.size();
                    LOG_DEBUG("VARCHAR column '" << column.name
                                                 << "' length: " << str.size());
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
                                               << " values, serialized_size="
                                               << serialized_size_);
}

/**
 * 将Tuple序列化到指定的内存缓冲区
 * @param data 目标缓冲区指针，调用者需要确保有足够空间
 *
 * 序列化的核心思路：
 * 1. 按照values_的顺序逐个序列化每个值
 * 2. 对于固定长度类型，直接memcpy到缓冲区
 * 3. 对于VARCHAR，先写入长度(4字节)，再写入内容
 * 4. 通过offset追踪当前写入位置
 */
void Tuple::SerializeTo(char* data) const {
    size_t offset = 0;

    // 遍历所有值进行序列化
    for (const auto& value : values_) {
        // 根据Value的实际类型进行序列化
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
            // VARCHAR的序列化：长度(uint32_t) + 字符串内容
            const std::string& str = std::get<std::string>(value);
            uint32_t len = static_cast<uint32_t>(str.size());
            // 先写入长度
            std::memcpy(data + offset, &len, sizeof(uint32_t));
            offset += sizeof(uint32_t);
            // 再写入字符串内容
            std::memcpy(data + offset, str.data(), len);
            offset += len;
        }
    }
}

/**
 * 从内存缓冲区反序列化生成Tuple
 * @param data 源数据缓冲区
 * @param schema 表Schema，用于确定数据类型和列数
 *
 * 反序列化的核心思路：
 * 1. 清空当前的values_并重置serialized_size_
 * 2. 根据schema的列定义，按顺序从缓冲区读取数据
 * 3. 对于固定长度类型，直接memcpy
 * 4. 对于VARCHAR，先读长度再读内容，并进行边界检查
 * 5. 发生任何错误时清空数据并抛出异常
 */
void Tuple::DeserializeFrom(const char* data, const Schema* schema) {
    values_.clear();
    serialized_size_ = 0;

    // 基本参数校验
    if (!data || !schema) {
        LOG_ERROR("Tuple::DeserializeFrom: null data or schema");
        return;
    }

    size_t offset = 0;

    try {
        // 按照schema定义的列顺序进行反序列化
        for (size_t i = 0; i < schema->GetColumnCount(); i++) {
            const auto& column = schema->GetColumn(i);

            // 根据列类型进行相应的反序列化操作
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
                    // VARCHAR反序列化：先读长度，再读内容
                    uint32_t len;
                    std::memcpy(&len, data + offset, sizeof(uint32_t));
                    offset += sizeof(uint32_t);

                    // 防止恶意数据导致内存分配过大
                    if (len > MAX_TUPLE_SIZE) {
                        LOG_ERROR("Tuple::DeserializeFrom: VARCHAR length "
                                  << len << " exceeds maximum");
                        values_.clear();
                        serialized_size_ = 0;
                        return;
                    }

                    // 构造字符串并移动到values_中
                    std::string str(data + offset, len);
                    values_.emplace_back(std::move(str));
                    offset += len;
                    serialized_size_ += sizeof(uint32_t) + len;
                    break;
                }
                default:
                    LOG_ERROR(
                        "Tuple::DeserializeFrom: Unsupported column type: "
                        << static_cast<int>(column.type));
                    values_.clear();
                    serialized_size_ = 0;
                    return;
            }
        }

        LOG_DEBUG("Tuple::DeserializeFrom: Successfully deserialized "
                  << values_.size()
                  << " values, total size: " << serialized_size_);

    } catch (const std::exception& e) {
        LOG_ERROR("Tuple::DeserializeFrom: Exception during deserialization: "
                  << e.what());
        values_.clear();
        serialized_size_ = 0;
        throw;
    }
}

/**
 * 获取Tuple序列化后的总字节数
 * @return 序列化后的字节数
 */
size_t Tuple::GetSerializedSize() const { return serialized_size_; }

/**
 * 根据索引获取指定位置的值
 * @param index 值的索引位置
 * @return 对应位置的Value对象
 * @throws std::out_of_range 当索引超出范围时抛出异常
 */
Value Tuple::GetValue(size_t index) const {
    // 边界检查
    if (index >= values_.size()) {
        LOG_ERROR("Tuple::GetValue: Index "
                  << index << " out of range, size=" << values_.size());
        throw std::out_of_range("Index out of range");
    }
    return values_[index];
}

}  // namespace SimpleRDBMS