// src/catalog/catalog.cpp
#include "catalog/catalog.h"
#include "common/exception.h"

namespace SimpleRDBMS {

Catalog::Catalog(BufferPoolManager* buffer_pool_manager)
    : buffer_pool_manager_(buffer_pool_manager),
      next_table_oid_(1),
      next_index_oid_(1) {
}

bool Catalog::CreateTable(const std::string& table_name, const Schema& schema) {
    // Check if table already exists
    if (tables_.find(table_name) != tables_.end()) {
        return false;
    }
    
    // Create table info
    auto table_info = std::make_unique<TableInfo>();
    table_info->table_name = table_name;
    table_info->schema = std::make_unique<Schema>(schema);
    table_info->table_oid = next_table_oid_++;
    table_info->table_heap = std::make_unique<TableHeap>(buffer_pool_manager_, table_info->schema.get());
    
    // Store table info
    oid_t table_oid = table_info->table_oid;
    tables_[table_name] = std::move(table_info);
    table_oid_map_[table_oid] = table_name;
    
    return true;
}

bool Catalog::DropTable(const std::string& table_name) {
    auto it = tables_.find(table_name);
    if (it == tables_.end()) {
        return false;
    }
    
    oid_t table_oid = it->second->table_oid;
    table_oid_map_.erase(table_oid);
    tables_.erase(it);
    
    return true;
}

TableInfo* Catalog::GetTable(const std::string& table_name) {
    auto it = tables_.find(table_name);
    if (it == tables_.end()) {
        return nullptr;
    }
    return it->second.get();
}

TableInfo* Catalog::GetTable(oid_t table_oid) {
    auto it = table_oid_map_.find(table_oid);
    if (it == table_oid_map_.end()) {
        return nullptr;
    }
    return GetTable(it->second);
}

bool Catalog::CreateIndex(const std::string& index_name,
                         const std::string& table_name,
                         const std::vector<std::string>& key_columns) {
    // Check if index already exists
    if (indexes_.find(index_name) != indexes_.end()) {
        return false;
    }
    
    // Check if table exists
    if (tables_.find(table_name) == tables_.end()) {
        return false;
    }
    
    // Create index info
    auto index_info = std::make_unique<IndexInfo>();
    index_info->index_name = index_name;
    index_info->table_name = table_name;
    index_info->key_columns = key_columns;
    index_info->index_oid = next_index_oid_++;
    
    // Store index info
    oid_t index_oid = index_info->index_oid;
    indexes_[index_name] = std::move(index_info);
    index_oid_map_[index_oid] = index_name;
    
    return true;
}

bool Catalog::DropIndex(const std::string& index_name) {
    auto it = indexes_.find(index_name);
    if (it == indexes_.end()) {
        return false;
    }
    
    oid_t index_oid = it->second->index_oid;
    index_oid_map_.erase(index_oid);
    indexes_.erase(it);
    
    return true;
}

IndexInfo* Catalog::GetIndex(const std::string& index_name) {
    auto it = indexes_.find(index_name);
    if (it == indexes_.end()) {
        return nullptr;
    }
    return it->second.get();
}

IndexInfo* Catalog::GetIndex(oid_t index_oid) {
    auto it = index_oid_map_.find(index_oid);
    if (it == index_oid_map_.end()) {
        return nullptr;
    }
    return GetIndex(it->second);
}

std::vector<IndexInfo*> Catalog::GetTableIndexes(const std::string& table_name) {
    std::vector<IndexInfo*> result;
    
    for (const auto& [index_name, index_info] : indexes_) {
        if (index_info->table_name == table_name) {
            result.push_back(index_info.get());
        }
    }
    
    return result;
}

}  // namespace SimpleRDBMS