/*
 * 文件: index_manager.h
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述: 索引管理器头文件，负责B+树索引的创建、删除和操作管理
 */

#pragma once

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "catalog/catalog.h"
#include "common/types.h"

namespace SimpleRDBMS {

class BufferPoolManager;
class Schema;
template <typename KeyType, typename ValueType>
class BPlusTree;

// 前向声明实现类，使用PIMPL模式隐藏实现细节
class IndexManagerImpl;

/**
 * 索引管理器类
 *
 * 这个类是整个索引系统的核心管理器，负责：
 * 1. 管理数据库中所有的B+树索引
 * 2. 提供统一的索引操作接口
 * 3. 处理不同数据类型的索引创建和操作
 * 4. 与catalog系统配合维护索引元信息
 *
 * 设计思路：
 * - 使用PIMPL模式隐藏复杂的实现细节，降低编译依赖
 * - 支持多种数据类型的键值（int32_t, int64_t, float, double, string）
 * - 每个索引都是一个独立的B+树实例
 * - 通过template方法支持类型安全的索引访问
 */
class IndexManager {
   public:
    /**
     * 构造函数
     * @param buffer_pool_manager 缓冲池管理器指针，用于页面管理
     * @param catalog catalog管理器指针，用于元数据管理，可以为nullptr
     */
    explicit IndexManager(BufferPoolManager* buffer_pool_manager,
                          Catalog* catalog = nullptr);

    /**
     * 析构函数
     * 负责清理所有索引资源和PIMPL实现对象
     */
    ~IndexManager();

    // 禁止拷贝和赋值，避免资源管理问题
    IndexManager(const IndexManager&) = delete;
    IndexManager& operator=(const IndexManager&) = delete;

    /**
     * 创建新索引
     *
     * 根据指定的列创建B+树索引，支持单列和复合索引（当前主要支持单列）
     *
     * @param index_name 索引名称，必须在数据库中唯一
     * @param table_name 目标表名
     * @param key_columns 索引列名列表，当前主要支持单列索引
     * @param table_schema 表结构信息，用于获取列类型和约束
     * @return true表示创建成功，false表示失败（如索引已存在、列不存在等）
     *
     * 实现要点：
     * - 根据列的数据类型创建对应的B+树实例
     * - 将索引信息注册到catalog系统
     * - 初始化B+树的root页面
     */
    bool CreateIndex(const std::string& index_name,
                     const std::string& table_name,
                     const std::vector<std::string>& key_columns,
                     const Schema* table_schema);

    /**
     * 删除指定索引
     *
     * @param index_name 要删除的索引名称
     * @return true表示删除成功，false表示失败（如索引不存在）
     *
     * 实现要点：
     * - 释放B+树占用的所有页面
     * - 从内存中移除索引实例
     * - 从catalog中删除索引元信息
     */
    bool DropIndex(const std::string& index_name);

    /**
     * 向索引中插入键值对
     *
     * @param index_name 目标索引名称
     * @param key 要插入的键值
     * @param rid 对应的记录标识符
     * @return true表示插入成功，false表示失败
     *
     * 使用场景：
     * - 当向表中insert新记录时，需要更新所有相关索引
     * - 支持重复键值的插入（B+树允许重复键）
     */
    bool InsertEntry(const std::string& index_name, const Value& key,
                     const RID& rid);

    /**
     * 从索引中删除键值对
     *
     * @param index_name 目标索引名称
     * @param key 要删除的键值
     * @return true表示删除成功，false表示失败（如键不存在）
     *
     * 使用场景：
     * - 当从表中delete记录时，需要从所有相关索引中删除对应条目
     * - 当update记录的索引列时，需要删除旧值、插入新值
     */
    bool DeleteEntry(const std::string& index_name, const Value& key);

    /**
     * 在索引中查找键值
     *
     * @param index_name 目标索引名称
     * @param key 要查找的键值
     * @param rid 输出参数，如果找到则存储对应的记录标识符
     * @return true表示找到，false表示未找到
     *
     * 这是索引最重要的功能，用于：
     * - WHERE子句中的等值查询优化
     * - 主键查找
     * - JOIN操作中的键值匹配
     */
    bool FindEntry(const std::string& index_name, const Value& key, RID* rid);

    /**
     * 获取指定类型的索引实例
     *
     * 模板方法，提供类型安全的索引访问
     *
     * @tparam KeyType 键的C++类型（如int32_t, std::string等）
     * @param index_name 索引名称
     * @return 索引实例指针，如果不存在或类型不匹配则返回nullptr
     *
     * 使用示例：
     * auto* tree = GetIndex<int32_t>("idx_id");
     * if (tree != nullptr) {
     *     // 可以直接操作B+树
     * }
     */
    template <typename KeyType>
    BPlusTree<KeyType, RID>* GetIndex(const std::string& index_name);

    /**
     * 获取数据库中所有索引的名称
     *
     * @return 所有索引名称的vector
     *
     * 用于：
     * - SHOW INDEXES命令的实现
     * - 系统管理和调试
     */
    std::vector<std::string> GetAllIndexNames() const;

    /**
     * 获取指定表的所有索引
     *
     * @param table_name 表名
     * @return 该表上所有索引名称的vector
     *
     * 用于：
     * - 查询优化器选择最佳索引
     * - 表删除时清理相关索引
     * - SHOW INDEX FROM table_name命令
     */
    std::vector<std::string> GetTableIndexes(
        const std::string& table_name) const;

   private:
    // PIMPL指针，指向实际的实现类
    // 这样做的好处：
    // 1. 隐藏复杂的实现细节和依赖
    // 2. 减少头文件的编译依赖
    // 3. 保持ABI稳定性
    std::unique_ptr<IndexManagerImpl> impl_;
};

}  // namespace SimpleRDBMS