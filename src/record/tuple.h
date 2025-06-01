/*
 * 文件: tuple.h
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述: Tuple类的定义，表示数据库中的一行记录，支持序列化/反序列化和RID管理
 */

#pragma once

#include <memory>
#include <vector>

#include "catalog/schema.h"
#include "common/types.h"

namespace SimpleRDBMS {

/**
 * Tuple类 - 数据库记录的抽象表示
 *
 * 这个类是数据库系统中非常核心的组件，主要职责包括：
 * 1. 存储一行记录的所有列值
 * 2. 提供序列化/反序列化功能，用于磁盘存储
 * 3. 管理记录标识符(RID)，用于定位记录在存储中的位置
 * 4. 支持类型安全的值访问和操作
 *
 * 设计思路：
 * - 使用std::vector<Value>存储异构类型的列值
 * - 通过Schema进行类型检查和转换
 * - 支持高效的内存序列化，便于缓存和持久化
 * - RID用于在Buffer Pool和Storage层之间建立映射关系
 */
class Tuple {
   public:
    /**
     * 默认构造函数
     * 创建一个空的Tuple对象，通常用于后续的反序列化操作
     */
    Tuple() : serialized_size_(0) {}  // 显式初始化serialized_size_

    /**
     * 参数化构造函数
     * @param values 列值的vector，包含这一行记录的所有数据
     * @param schema 表的Schema指针，用于类型检查和转换
     *
     * 构造过程会进行以下操作：
     * 1. 检查values数量和schema列数是否匹配
     * 2. 对每个值进行类型检查和必要的类型转换
     * 3. 计算序列化后的总大小
     * 4. 初始化内部状态
     */
    Tuple(std::vector<Value> values, const Schema* schema);

    /**
     * 根据索引获取指定列的值
     * @param index 列的索引位置，从0开始
     * @return 对应位置的Value对象
     * @throws std::out_of_range 当索引超出范围时抛出异常
     *
     * 这是访问Tuple数据的主要接口，提供类型安全的访问方式
     */
    Value GetValue(size_t index) const;

    /**
     * 获取所有列值的引用
     * @return values_的const引用
     *
     * 提供对整个values_容器的只读访问，主要用于：
     * 1. 批量处理所有列值
     * 2. 在执行引擎中进行投影操作
     * 3. Debug和测试场景
     */
    const std::vector<Value>& GetValues() const { return values_; }

    /**
     * 将Tuple序列化到指定的内存缓冲区
     * @param data 目标缓冲区指针，调用者需要确保有足够的空间
     *
     * 序列化格式说明：
     * - 固定长度类型：直接按字节序列化
     * - VARCHAR类型：4字节长度 + 字符串内容
     * - 所有数据按照values_的顺序连续存储
     *
     * 注意：调用前需要通过GetSerializedSize()确保缓冲区足够大
     */
    void SerializeTo(char* data) const;

    /**
     * 从内存缓冲区反序列化生成Tuple
     * @param data 源数据缓冲区指针
     * @param schema 表Schema，用于确定数据类型和列数
     *
     * 反序列化过程：
     * 1. 清空当前数据
     * 2. 根据schema按列顺序读取数据
     * 3. 进行必要的边界检查和类型验证
     * 4. 重新计算serialized_size_
     *
     * 注意：失败时会清空所有数据并抛出异常
     */
    void DeserializeFrom(const char* data, const Schema* schema);

    /**
     * 获取Tuple序列化后的总字节数
     * @return 序列化后占用的字节数
     *
     * 这个值在以下场景中很重要：
     * 1. 分配序列化缓冲区
     * 2. 计算页面空间使用情况
     * 3. Buffer Pool的内存管理
     */
    size_t GetSerializedSize() const;

    /**
     * 获取当前Tuple的记录标识符
     * @return RID对象，包含页面ID和slot编号
     *
     * RID是记录在存储层的唯一标识符，用于：
     * 1. 在Buffer Pool中定位具体的记录
     * 2. 建立索引项和实际数据的映射关系
     * 3. 支持记录的删除和更新操作
     */
    RID GetRID() const { return rid_; }

    /**
     * 设置Tuple的记录标识符
     * @param rid 新的RID值
     *
     * 通常在以下情况下调用：
     * 1. 将Tuple插入到页面后设置其RID
     * 2. 从页面读取Tuple时恢复其RID
     * 3. 记录位置发生变化时更新RID
     */
    void SetRID(const RID& rid) { rid_ = rid; }

   private:
    /**
     * 存储所有列值的容器
     * 使用std::vector支持动态大小和高效的随机访问
     * 每个Value对象使用std::variant支持多种数据类型
     */
    std::vector<Value> values_;

    /**
     * 记录标识符，用于在存储层定位这条记录
     * 包含页面ID和在页面内的slot编号
     * 默认值为无效RID，表示记录尚未持久化
     */
    RID rid_;

    /**
     * 序列化后的总字节数
     * 在构造函数中计算，用于优化内存分配和空间管理
     * 对于VARCHAR类型会根据实际字符串长度动态计算
     */
    size_t serialized_size_;
};

}  // namespace SimpleRDBMS