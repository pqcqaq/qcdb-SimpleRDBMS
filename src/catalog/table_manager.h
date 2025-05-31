// namespace SimpleRDBMS// src/catalog/table_manager.h
#pragma once

#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "common/types.h"

namespace SimpleRDBMS {

class BufferPoolManager;
class Catalog;
class CreateTableStatement;
class IndexManager;
class TableInfo;
class Tuple;

class TableManager {
   public:
    TableManager(BufferPoolManager* buffer_pool_manager, Catalog* catalog);
    ~TableManager();

    // 禁止拷贝和赋值
    TableManager(const TableManager&) = delete;
    TableManager& operator=(const TableManager&) = delete;

    /**
     * 创建表
     * @param stmt CREATE TABLE 语句
     * @return 创建是否成功
     */
    bool CreateTable(const CreateTableStatement* stmt);

    /**
     * 删除表
     * @param table_name 表名
     * @return 删除是否成功
     */
    bool DropTable(const std::string& table_name);

    /**
     * 创建索引
     * @param index_name 索引名称
     * @param table_name 表名
     * @param key_columns 索引列名列表
     * @return 创建是否成功
     */
    bool CreateIndex(const std::string& index_name,
                     const std::string& table_name,
                     const std::vector<std::string>& key_columns);

    /**
     * 删除索引
     * @param index_name 索引名称
     * @return 删除是否成功
     */
    bool DropIndex(const std::string& index_name);

    /**
     * 在插入记录时更新相关索引
     * @param table_name 表名
     * @param tuple 插入的记录
     * @param rid 记录标识符
     * @return 更新是否成功
     */
    bool UpdateIndexesOnInsert(const std::string& table_name,
                               const Tuple& tuple, const RID& rid);

    /**
     * 在删除记录时更新相关索引
     * @param table_name 表名
     * @param tuple 被删除的记录
     * @return 更新是否成功
     */
    bool UpdateIndexesOnDelete(const std::string& table_name,
                               const Tuple& tuple);

    /**
     * 在更新记录时更新相关索引
     * @param table_name 表名
     * @param old_tuple 更新前的记录
     * @param new_tuple 更新后的记录
     * @param rid 记录标识符
     * @return 更新是否成功
     */
    bool UpdateIndexesOnUpdate(const std::string& table_name,
                               const Tuple& old_tuple, const Tuple& new_tuple,
                               const RID& rid);

    /**
     * 获取目录对象
     * @return 目录对象指针
     */
    Catalog* GetCatalog() { return catalog_; }

    /**
     * 获取索引管理器
     * @return 索引管理器指针
     */
    IndexManager* GetIndexManager();

   private:
    BufferPoolManager* buffer_pool_manager_;
    Catalog* catalog_;
    std::unique_ptr<IndexManager> index_manager_;

    /**
     * 使用现有数据填充索引
     * @param index_name 索引名称
     * @param table_info 表信息
     * @param key_columns 索引列名列表
     * @return 填充是否成功
     */
    bool PopulateIndexWithExistingData(
        const std::string& index_name, TableInfo* table_info,
        const std::vector<std::string>& key_columns);

    /**
     * 重建所有索引
     * 在初始化时调用，确保所有表的索引都被正确创建
     */
    void RebuildAllIndexes();
};

}  // namespace SimpleRDBMS