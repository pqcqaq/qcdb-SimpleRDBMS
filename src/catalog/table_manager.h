/*
 * 文件: table_manager.h
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述:
 * 表管理器头文件，负责表和索引的创建删除、数据操作时的索引维护，是catalog和index
 * manager的协调者
 */

#pragma once

#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "common/types.h"

namespace SimpleRDBMS {

// 前向声明，避免头文件循环依赖
class BufferPoolManager;
class Catalog;
class CreateTableStatement;
class IndexManager;
class TableInfo;
class Tuple;

/**
 * 表管理器类
 *
 * TableManager是数据库系统中表和索引管理的核心协调者，负责：
 * 1. 表的创建和删除，包括主键索引的自动创建
 * 2. 索引的创建和删除，包括物理索引结构的管理
 * 3. 数据变更时的索引维护（INSERT、UPDATE、DELETE时的索引同步）
 * 4. 系统启动时的索引重建，确保数据和索引的一致性
 * 5. 协调catalog（元数据）和IndexManager（物理索引）的操作
 *
 * 设计思路：
 * - 作为catalog和IndexManager之间的桥梁，统一管理表和索引的生命周期
 * - 确保索引和表数据的一致性，特别是在系统崩溃恢复后
 * - 提供高层的表操作接口，隐藏底层的索引管理复杂性
 * - 支持事务操作时的索引维护，保证ACID特性
 */
class TableManager {
   public:
    /**
     * 构造函数
     * @param buffer_pool_manager 缓冲池管理器，用于页面I/O
     * @param catalog catalog对象，管理元数据
     *
     * 初始化时会：
     * 1. 创建IndexManager实例
     * 2. 重建所有现有的索引，确保系统启动后的一致性
     */
    TableManager(BufferPoolManager* buffer_pool_manager, Catalog* catalog);

    /**
     * 析构函数
     * 确保IndexManager被正确销毁，释放相关资源
     */
    ~TableManager();

    // 禁止拷贝和赋值，避免资源管理问题
    TableManager(const TableManager&) = delete;
    TableManager& operator=(const TableManager&) = delete;

    // ======================== 表操作接口 ========================

    /**
     * 创建表
     * @param stmt CREATE TABLE语句的AST节点
     * @return 创建是否成功
     *
     * 创建流程：
     * 1. 解析CREATE TABLE语句，提取表名和列定义
     * 2. 验证列定义的合法性
     * 3. 在catalog中创建表的元数据
     * 4. 如果定义了主键，自动创建主键索引
     * 5. 验证创建结果的正确性
     */
    bool CreateTable(const CreateTableStatement* stmt);

    /**
     * 删除表
     * @param table_name 要删除的表名
     * @return 删除是否成功
     *
     * 删除流程：
     * 1. 检查表是否存在
     * 2. 删除该表的所有索引（包括主键索引）
     * 3. 从catalog中删除表的元数据
     *
     * 注意：必须先删除索引再删除表，保证引用完整性
     */
    bool DropTable(const std::string& table_name);

    // ======================== 索引操作接口 ========================

    /**
     * 创建索引
     * @param index_name 索引名称，必须在系统中唯一
     * @param table_name 索引所属的表名
     * @param key_columns 索引的键列名列表
     * @return 创建是否成功
     *
     * 创建流程：
     * 1. 验证索引名唯一性和表的存在性
     * 2. 验证索引列在表中存在且无重复
     * 3. 在catalog中创建索引元数据
     * 4. 在IndexManager中创建物理索引结构
     * 5. 用表中现有数据填充索引
     * 6. 保存catalog信息到磁盘
     */
    bool CreateIndex(const std::string& index_name,
                     const std::string& table_name,
                     const std::vector<std::string>& key_columns);

    /**
     * 删除索引
     * @param index_name 要删除的索引名称
     * @return 删除是否成功
     *
     * 删除流程：
     * 1. 验证索引存在性
     * 2. 从IndexManager中删除物理索引结构
     * 3. 从catalog中删除索引元数据
     */
    bool DropIndex(const std::string& index_name);

    // ======================== 索引维护接口 ========================

    /**
     * 插入记录时更新相关索引
     * @param table_name 表名
     * @param tuple 新插入的记录
     * @param rid 记录的标识符
     * @return 所有索引更新是否都成功
     *
     * 维护流程：
     * 1. 获取该表的所有索引
     * 2. 对每个索引，提取新记录的键值
     * 3. 将键值和RID插入到对应的索引中
     * 4. 提供超时保护，避免索引操作阻塞
     *
     * 这个方法在INSERT操作时调用，保持索引和数据的一致性
     */
    bool UpdateIndexesOnInsert(const std::string& table_name,
                               const Tuple& tuple, const RID& rid);

    /**
     * 删除记录时更新相关索引
     * @param table_name 表名
     * @param tuple 被删除的记录
     * @return 所有索引更新是否都成功
     *
     * 维护流程：
     * 1. 获取该表的所有索引
     * 2. 对每个索引，提取被删除记录的键值
     * 3. 从对应的索引中删除该键值
     *
     * 这个方法在DELETE操作时调用
     */
    bool UpdateIndexesOnDelete(const std::string& table_name,
                               const Tuple& tuple);

    /**
     * 更新记录时更新相关索引
     * @param table_name 表名
     * @param old_tuple 更新前的记录
     * @param new_tuple 更新后的记录
     * @param rid 记录的标识符
     * @return 所有索引更新是否都成功
     *
     * 维护流程：
     * 1. 获取该表的所有索引
     * 2. 对每个索引，比较新旧记录的键值
     * 3. 如果键值发生变化：
     *    a. 从索引中删除旧键值
     *    b. 向索引中插入新键值
     *    c. 如果插入失败，尝试回滚（恢复旧键值）
     * 4. 如果键值没变化，跳过该索引
     *
     * 这个方法在UPDATE操作时调用
     */
    bool UpdateIndexesOnUpdate(const std::string& table_name,
                               const Tuple& old_tuple, const Tuple& new_tuple,
                               const RID& rid);

    // ======================== 访问器接口 ========================

    /**
     * 获取catalog对象指针
     * @return catalog对象指针
     *
     * 供其他组件访问catalog进行元数据查询
     */
    Catalog* GetCatalog() { return catalog_; }

    /**
     * 获取索引管理器指针
     * @return IndexManager对象指针
     *
     * 供查询执行器等组件访问IndexManager进行索引查找
     * 主要用于SELECT操作中的索引扫描
     */
    IndexManager* GetIndexManager();

   private:
    // ======================== 成员变量 ========================

    BufferPoolManager* buffer_pool_manager_;  // 缓冲池管理器，用于页面I/O
    Catalog* catalog_;                        // catalog对象，管理元数据
    std::unique_ptr<IndexManager>
        index_manager_;  // 索引管理器，管理物理索引结构

    // ======================== 内部辅助方法 ========================

    /**
     * 使用表中现有数据填充新创建的索引
     * @param index_name 索引名称
     * @param table_info 表信息，包含schema和table_heap
     * @param key_columns 索引的键列名列表
     * @return 填充是否成功
     *
     * 填充流程：
     * 1. 验证表信息和索引列的有效性
     * 2. 遍历表中的所有记录
     * 3. 提取每条记录的索引键值
     * 4. 将键值和RID插入到索引中
     * 5. 统计处理结果并记录日志
     *
     * 注意：目前只支持单列索引，多列索引是未来的扩展方向
     * 这个方法在创建索引和系统启动时的索引重建中使用
     */
    bool PopulateIndexWithExistingData(
        const std::string& index_name, TableInfo* table_info,
        const std::vector<std::string>& key_columns);

    /**
     * 重建所有索引
     *
     * 重建流程：
     * 1. 获取系统中所有的表
     * 2. 对每个表，获取其所有索引定义
     * 3. 重新创建索引的物理结构
     * 4. 用现有数据填充索引
     *
     * 这个方法主要在系统启动时调用，确保：
     * - 索引和数据的一致性
     * - 系统崩溃后的恢复正确性
     * - 新增的索引定义被正确创建
     *
     * 注意：这是一个耗时操作，大表可能需要较长时间
     */
    void RebuildAllIndexes();
};

}  // namespace SimpleRDBMS