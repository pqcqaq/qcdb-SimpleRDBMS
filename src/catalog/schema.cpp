/*
 * 文件: schema.cpp
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述: 数据库表结构定义的实现，包含列信息管理、类型检查和元组大小计算等功能
 */

#include "catalog/schema.h"

#include "common/exception.h"

namespace SimpleRDBMS {

/**
 * Schema构造函数
 *
 * 思路：
 * 1. 保存列定义的vector
 * 2. 构建列名到索引的映射表，用于快速查找
 * 这样既保持了列的顺序（用vector），又能快速按名称查找（用map）
 */
Schema::Schema(const std::vector<Column>& columns) : columns_(columns) {
    // 遍历所有列，建立列名到索引的映射关系
    for (size_t i = 0; i < columns_.size(); i++) {
        column_indices_[columns_[i].name] = i;
    }
}

/**
 * 根据列名获取列定义
 *
 * 实现思路：
 * 1. 先在映射表中查找列名对应的索引
 * 2. 如果找到就返回对应的Column对象
 * 3. 找不到就抛异常，避免返回无效引用
 */
const Column& Schema::GetColumn(const std::string& name) const {
    auto it = column_indices_.find(name);
    if (it == column_indices_.end()) {
        throw Exception("Column not found: " + name);
    }
    return columns_[it->second];
}

/**
 * 根据列名获取列的索引位置
 *
 * 这个方法很有用，因为很多操作需要知道列在tuple中的位置
 * 比如读取tuple的某个字段值时需要知道字段的偏移位置
 */
size_t Schema::GetColumnIdx(const std::string& name) const {
    auto it = column_indices_.find(name);
    if (it == column_indices_.end()) {
        throw Exception("Column not found: " + name);
    }
    return it->second;
}

/**
 * 计算根据此schema创建的tuple的总大小
 *
 * 计算思路：
 * 1. 遍历所有列，根据数据类型累加每列占用的字节数
 * 2. 固定长度类型直接加sizeof
 * 3. VARCHAR类型加上指定的最大长度
 *
 * 注意：这里计算的是最大可能大小，实际存储时VARCHAR可能更小
 * 但为了简化实现，我们按最大长度分配空间
 */
size_t Schema::GetTupleSize() const {
    size_t size = 0;

    for (const auto& column : columns_) {
        switch (column.type) {
            case TypeId::BOOLEAN:
                size += sizeof(bool);  // 1字节
                break;
            case TypeId::TINYINT:
                size += sizeof(int8_t);  // 1字节
                break;
            case TypeId::SMALLINT:
                size += sizeof(int16_t);  // 2字节
                break;
            case TypeId::INTEGER:
                size += sizeof(int32_t);  // 4字节
                break;
            case TypeId::BIGINT:
                size += sizeof(int64_t);  // 8字节
                break;
            case TypeId::FLOAT:
                size += sizeof(float);  // 4字节
                break;
            case TypeId::DOUBLE:
                size += sizeof(double);  // 8字节
                break;
            case TypeId::VARCHAR:
                // VARCHAR按声明的最大长度计算空间
                // 实际实现中可能需要额外的长度信息存储
                size += column.size;
                break;
            default:
                // 遇到未知类型，不加大小，但应该记录warning
                LOG_WARN(
                    "Unknown column type: " << static_cast<int>(column.type));
                break;
        }
    }
    return size;
}

/**
 * 检查schema中是否包含指定名称的列
 *
 * 这是一个便捷方法，避免调用者需要捕获异常来判断列是否存在
 * 在SQL解析和验证阶段经常用到
 */
bool Schema::HasColumn(const std::string& name) const {
    return column_indices_.find(name) != column_indices_.end();
}

}  // namespace SimpleRDBMS