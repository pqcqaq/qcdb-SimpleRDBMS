/*
 * 文件: schema.h
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述:
 * 数据库表结构定义，管理表的列信息、数据类型和约束，提供列查找和元组大小计算功能
 */

#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "common/types.h"

namespace SimpleRDBMS {

/**
 * 数据库表的Schema定义类
 *
 * 这个类是数据库表结构的核心，负责：
 * 1. 管理表的所有列信息（名称、类型、约束等）
 * 2. 提供高效的列查找功能（按名称和索引）
 * 3. 计算基于此schema的tuple所需的存储空间
 * 4. 验证列的存在性和合法性
 *
 * 设计思路：
 * - 使用vector存储列定义，保持列的顺序性
 * - 使用unordered_map建立列名到索引的映射，提供O(1)的名称查找
 * - 提供多种访问方式，满足不同使用场景的需求
 */
class Schema {
   public:
    /**
     * 构造函数
     * @param columns 列定义的vector，定义了表的所有列
     *
     * 会自动构建列名到索引的映射关系，用于快速按名称查找列
     */
    explicit Schema(const std::vector<Column>& columns);

    // ======================== 列访问接口 ========================

    /**
     * 根据索引获取列定义
     * @param index 列的索引位置
     * @return 列定义的常量引用
     *
     * 这是最高效的访问方式，直接通过数组索引访问
     * 适用于已知列位置的场景，如tuple的序列化/反序列化
     */
    const Column& GetColumn(size_t index) const { return columns_[index]; }

    /**
     * 根据列名获取列定义
     * @param name 列名
     * @return 列定义的常量引用
     * @throws Exception 如果列不存在
     *
     * 通过列名查找，适用于SQL解析等需要按名称访问的场景
     */
    const Column& GetColumn(const std::string& name) const;

    /**
     * 根据列名获取列的索引位置
     * @param name 列名
     * @return 列在schema中的索引位置
     * @throws Exception 如果列不存在
     *
     * 用于获取列在tuple中的位置，便于后续的数据读写操作
     */
    size_t GetColumnIdx(const std::string& name) const;

    /**
     * 获取所有列定义
     * @return 列定义vector的常量引用
     *
     * 用于遍历所有列，如表结构的显示、schema的序列化等
     */
    const std::vector<Column>& GetColumns() const { return columns_; }

    // ======================== Schema信息查询 ========================

    /**
     * 获取列的数量
     * @return 表中列的总数
     *
     * 常用于循环遍历、内存分配、验证等场景
     */
    size_t GetColumnCount() const { return columns_.size(); }

    /**
     * 计算基于此schema的tuple所需的存储大小
     * @return 元组的字节大小
     *
     * 根据所有列的数据类型计算总的存储空间需求
     * 对于VARCHAR类型，按声明的最大长度计算
     */
    size_t GetTupleSize() const;

    /**
     * 检查是否包含指定名称的列
     * @param name 要检查的列名
     * @return 存在返回true，不存在返回false
     *
     * 这是一个便捷方法，避免调用者需要通过异常来判断列是否存在
     * 在SQL验证、查询优化等阶段经常使用
     */
    bool HasColumn(const std::string& name) const;

   private:
    // ======================== 内部数据结构 ========================

    /**
     * 列定义的有序列表
     * 保持列的顺序，这个顺序决定了tuple中字段的布局
     */
    std::vector<Column> columns_;

    /**
     * 列名到索引的映射表
     * 提供O(1)时间复杂度的按名称查找功能
     * key: 列名, value: 在columns_中的索引位置
     */
    std::unordered_map<std::string, size_t> column_indices_;
};

}  // namespace SimpleRDBMS