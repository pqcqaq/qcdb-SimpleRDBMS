#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include "catalog/schema.h"
#include "catalog/table_manager.h"
#include "record/table_heap.h"

namespace SimpleRDBMS {

struct TableInfo {
    std::unique_ptr<Schema> schema;
    std::string table_name;
    std::unique_ptr<TableHeap> table_heap;
    oid_t table_oid;
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

private:
    BufferPoolManager* buffer_pool_manager_;
    
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
};

}  // namespace SimpleRDBMS