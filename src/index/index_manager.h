// src/index/index_manager.h
#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <functional>
#include "common/types.h"
#include "catalog/catalog.h"

namespace SimpleRDBMS {

class BufferPoolManager;
class Schema;
template <typename KeyType, typename ValueType>
class BPlusTree;

// 前向声明实现类
class IndexManagerImpl;

class IndexManager {
public:
    explicit IndexManager(BufferPoolManager* buffer_pool_manager, Catalog* catalog = nullptr);
    ~IndexManager();
    
    // 禁止拷贝和赋值
    IndexManager(const IndexManager&) = delete;
    IndexManager& operator=(const IndexManager&) = delete;
    
    /**
     * 创建索引
     * @param index_name 索引名称
     * @param table_name 表名
     * @param key_columns 索引列名列表
     * @param table_schema 表结构
     * @return 创建是否成功
     */
    bool CreateIndex(const std::string& index_name, 
                    const std::string& table_name,
                    const std::vector<std::string>& key_columns,
                    const Schema* table_schema);
    
    /**
     * 删除索引
     * @param index_name 索引名称
     * @return 删除是否成功
     */
    bool DropIndex(const std::string& index_name);
    
    /**
     * 向索引中插入键值对
     * @param index_name 索引名称
     * @param key 键值
     * @param rid 记录标识符
     * @return 插入是否成功
     */
    bool InsertEntry(const std::string& index_name, const Value& key, const RID& rid);
    
    /**
     * 从索引中删除键值对
     * @param index_name 索引名称
     * @param key 键值
     * @return 删除是否成功
     */
    bool DeleteEntry(const std::string& index_name, const Value& key);
    
    /**
     * 在索引中查找键值
     * @param index_name 索引名称
     * @param key 键值
     * @param rid 输出参数，存储找到的记录标识符
     * @return 查找是否成功
     */
    bool FindEntry(const std::string& index_name, const Value& key, RID* rid);
    
    /**
     * 获取指定类型的索引实例
     * @tparam KeyType 键类型
     * @param index_name 索引名称
     * @return 索引实例指针，如果不存在则返回nullptr
     */
    template <typename KeyType>
    BPlusTree<KeyType, RID>* GetIndex(const std::string& index_name);
    
    /**
     * 获取所有索引名称
     * @return 索引名称列表
     */
    std::vector<std::string> GetAllIndexNames() const;
    
    /**
     * 获取指定表的所有索引
     * @param table_name 表名
     * @return 索引名称列表
     */
    std::vector<std::string> GetTableIndexes(const std::string& table_name) const;

private:
    std::unique_ptr<IndexManagerImpl> impl_;
};

} // namespace SimpleRDBMS