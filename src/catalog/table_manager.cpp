// src/catalog/table_manager.cpp
#include "catalog/table_manager.h"

#include <algorithm>
#include <unordered_set>

#include "catalog/catalog.h"  // 添加完整的 catalog.h 包含
#include "catalog/schema.h"
#include "common/exception.h"
#include "record/table_heap.h"

namespace SimpleRDBMS {

TableManager::TableManager(BufferPoolManager* buffer_pool_manager,
                           Catalog* catalog)
    : buffer_pool_manager_(buffer_pool_manager), catalog_(catalog) {}

bool TableManager::CreateTable(const CreateTableStatement* stmt) {
    if (stmt == nullptr) {
        return false;
    }
    
    const std::string& table_name = stmt->GetTableName();
    const std::vector<Column>& columns = stmt->GetColumns();
    
    LOG_DEBUG("TableManager::CreateTable: Creating table " << table_name);
    
    // Check if table already exists
    if (catalog_->GetTable(table_name) != nullptr) {
        LOG_WARN("TableManager::CreateTable: Table " << table_name << " already exists");
        return false;  // Table already exists
    }
    
    // Validate columns
    if (columns.empty()) {
        LOG_ERROR("TableManager::CreateTable: No columns defined for table " << table_name);
        return false;  // No columns defined
    }
    
    // Check for duplicate column names
    std::unordered_set<std::string> column_names;
    int primary_key_count = 0;
    for (const auto& column : columns) {
        // Check duplicate column name
        if (column_names.count(column.name) > 0) {
            LOG_ERROR("TableManager::CreateTable: Duplicate column name: " << column.name);
            return false;  // Duplicate column name
        }
        column_names.insert(column.name);
        
        // Count primary keys
        if (column.is_primary_key) {
            primary_key_count++;
        }
        
        // Validate column type
        if (column.type == TypeId::INVALID) {
            LOG_ERROR("TableManager::CreateTable: Invalid column type for: " << column.name);
            return false;  // Invalid column type
        }
        
        // Validate VARCHAR size
        if (column.type == TypeId::VARCHAR && column.size == 0) {
            LOG_ERROR("TableManager::CreateTable: VARCHAR column must have size: " << column.name);
            return false;  // VARCHAR must have size
        }
    }
    
    // Currently only support single primary key
    if (primary_key_count > 1) {
        LOG_ERROR("TableManager::CreateTable: Multiple primary keys not supported");
        return false;  // Multiple primary keys not supported
    }
    
    // Create schema
    Schema schema(columns);
    
    LOG_DEBUG("TableManager::CreateTable: Creating table through catalog");
    
    // Create table through catalog
    bool success = catalog_->CreateTable(table_name, schema);
    if (!success) {
        LOG_ERROR("TableManager::CreateTable: Failed to create table " << table_name);
        return false;
    }
    
    LOG_DEBUG("TableManager::CreateTable: Table " << table_name << " created successfully");
    
    // 如果有主键，创建主键索引
    if (success && primary_key_count == 1) {
        LOG_DEBUG("TableManager::CreateTable: Creating primary key index for " << table_name);
        
        // Find primary key column
        std::vector<std::string> key_columns;
        for (const auto& column : columns) {
            if (column.is_primary_key) {
                key_columns.push_back(column.name);
                break;
            }
        }
        
        // Create primary key index
        std::string index_name = table_name + "_pk";
        bool index_success = CreateIndex(index_name, table_name, key_columns);
        
        if (index_success) {
            LOG_DEBUG("TableManager::CreateTable: Primary key index created successfully");
            // 只在最后统一保存一次catalog
            try {
                catalog_->SaveCatalogToDisk();
                LOG_DEBUG("TableManager::CreateTable: Catalog saved after index creation");
            } catch (const std::exception& e) {
                LOG_WARN("TableManager::CreateTable: Failed to save catalog after index creation: " << e.what());
                // 不影响表创建的成功
            }
        } else {
            LOG_WARN("TableManager::CreateTable: Failed to create primary key index, but table creation succeeded");
        }
    }
    
    LOG_DEBUG("TableManager::CreateTable: Table " << table_name << " creation process completed");
    return success;
}

bool TableManager::DropTable(const std::string& table_name) {
    // Check if table exists
    TableInfo* table_info = catalog_->GetTable(table_name);
    if (table_info == nullptr) {
        return false;  // Table doesn't exist
    }

    // Drop all indexes on this table
    std::vector<IndexInfo*> indexes = catalog_->GetTableIndexes(table_name);
    for (auto* index_info : indexes) {
        DropIndex(index_info->index_name);
    }

    // Drop the table
    return catalog_->DropTable(table_name);
}

bool TableManager::CreateIndex(const std::string& index_name,
                               const std::string& table_name,
                               const std::vector<std::string>& key_columns) {
    // Check if index already exists
    if (catalog_->GetIndex(index_name) != nullptr) {
        return false;  // Index already exists
    }
    
    // Check if table exists
    TableInfo* table_info = catalog_->GetTable(table_name);
    if (table_info == nullptr) {
        return false;  // Table doesn't exist
    }
    
    // Validate key columns
    if (key_columns.empty()) {
        return false;  // No key columns specified
    }
    
    const Schema* schema = table_info->schema.get();
    
    // Check if all key columns exist in the table
    for (const auto& key_column : key_columns) {
        if (!schema->HasColumn(key_column)) {
            return false;  // Column doesn't exist
        }
    }
    
    // Check for duplicate key columns
    std::unordered_set<std::string> key_column_set(key_columns.begin(), key_columns.end());
    if (key_column_set.size() != key_columns.size()) {
        return false;  // Duplicate key columns
    }
    
    // Create index through catalog
    bool success = catalog_->CreateIndex(index_name, table_name, key_columns);
    if (success) {
        // 为了避免对空表的迭代器问题，我们暂时跳过索引数据的填充
        // 在实际系统中，这部分应该在插入数据时动态维护
        LOG_DEBUG("CreateIndex: Index " << index_name << " created successfully, skipping data population for now");
        
        // TODO: 以下代码在有数据时才执行，避免空表的迭代器问题
        /*
        auto* table_heap = table_info->table_heap.get();
        const Schema* schema = table_info->schema.get();
        
        // Get column indices for key columns
        std::vector<size_t> key_column_indices;
        for (const auto& col_name : key_columns) {
            key_column_indices.push_back(schema->GetColumnIdx(col_name));
        }
        
        // 只有在表不为空时才扫描
        try {
            auto iter = table_heap->Begin();
            int count = 0;
            while (!iter.IsEnd() && count < 1000) { // 添加安全计数器
                Tuple tuple = *iter;
                // Extract key from tuple based on key columns
                if (key_columns.size() == 1) {
                    Value key_value = tuple.GetValue(key_column_indices[0]);
                    RID rid = tuple.GetRID();
                    // TODO: Insert into B+ tree index when IndexManager is available
                }
                ++iter;
                count++;
            }
            LOG_DEBUG("CreateIndex: Processed " << count << " existing records");
        } catch (const std::exception& e) {
            LOG_WARN("CreateIndex: Failed to populate index with existing data: " << e.what());
            // 不影响索引创建的成功
        }
        */
    }
    
    return success;
}

bool TableManager::DropIndex(const std::string& index_name) {
    // Check if index exists
    IndexInfo* index_info = catalog_->GetIndex(index_name);
    if (index_info == nullptr) {
        return false;  // Index doesn't exist
    }

    // Drop the index
    return catalog_->DropIndex(index_name);
}

}  // namespace SimpleRDBMS