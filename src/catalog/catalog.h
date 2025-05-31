// src/catalog/catalog.h
#pragma once

#include "common/config.h"
#include "common/types.h"
#include "common/debug.h"
#include <unordered_map>
#include <memory>
#include <string>
#include <vector>
#include <mutex>
#include <atomic>

// 前向声明
namespace SimpleRDBMS {
class BufferPoolManager;
class Schema;
class TableHeap;

struct TableInfo {
    std::unique_ptr<Schema> schema;
    std::string table_name;
    std::unique_ptr<TableHeap> table_heap;
    oid_t table_oid;
    page_id_t first_page_id;
};

struct IndexInfo {
    std::string index_name;
    std::string table_name;
    std::vector<std::string> key_columns;
    oid_t index_oid;
};

class Catalog {
   public:
    explicit Catalog(BufferPoolManager* buffer_pool_manager);

    // Table operations
    bool CreateTable(const std::string& table_name, const Schema& schema);
    bool DropTable(const std::string& table_name);
    TableInfo* GetTable(const std::string& table_name);
    TableInfo* GetTable(oid_t table_oid);

    // Index operations
    bool CreateIndex(const std::string& index_name,
                     const std::string& table_name,
                     const std::vector<std::string>& key_columns);
    bool DropIndex(const std::string& index_name);
    IndexInfo* GetIndex(const std::string& index_name);
    IndexInfo* GetIndex(oid_t index_oid);
    std::vector<IndexInfo*> GetTableIndexes(const std::string& table_name);

    // 持久化相关方法
    void LoadCatalogFromDisk();
    void SaveCatalogToDisk();

    // 调试方法：列出所有表（将实现移到 .cpp 文件中）
    void DebugPrintTables() const;

   private:
    BufferPoolManager* buffer_pool_manager_;
    mutable std::mutex save_mutex_;
    std::atomic<bool> save_in_progress_ = {false};

    // Table name -> TableInfo
    std::unordered_map<std::string, std::unique_ptr<TableInfo>> tables_;
    // Table OID -> table name
    std::unordered_map<oid_t, std::string> table_oid_map_;
    // Index name -> IndexInfo
    std::unordered_map<std::string, std::unique_ptr<IndexInfo>> indexes_;
    // Index OID -> index name
    std::unordered_map<oid_t, std::string> index_oid_map_;

    // Next OID
    oid_t next_table_oid_;
    oid_t next_index_oid_;

    // 序列化辅助方法
    void SerializeSchema(const Schema& schema, char* buffer, size_t& offset);
    std::unique_ptr<Schema> DeserializeSchema(const char* buffer,
                                              size_t& offset);
};

}  // namespace SimpleRDBMS