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

    // Check if table already exists
    if (catalog_->GetTable(table_name) != nullptr) {
        return false;  // Table already exists
    }

    // Validate columns
    if (columns.empty()) {
        return false;  // No columns defined
    }

    // Check for duplicate column names
    std::unordered_set<std::string> column_names;
    int primary_key_count = 0;

    for (const auto& column : columns) {
        // Check duplicate column name
        if (column_names.count(column.name) > 0) {
            return false;  // Duplicate column name
        }
        column_names.insert(column.name);

        // Count primary keys
        if (column.is_primary_key) {
            primary_key_count++;
        }

        // Validate column type
        if (column.type == TypeId::INVALID) {
            return false;  // Invalid column type
        }

        // Validate VARCHAR size
        if (column.type == TypeId::VARCHAR && column.size == 0) {
            return false;  // VARCHAR must have size
        }
    }

    // Currently only support single primary key
    if (primary_key_count > 1) {
        return false;  // Multiple primary keys not supported
    }

    // Create schema
    Schema schema(columns);

    // Create table through catalog
    bool success = catalog_->CreateTable(table_name, schema);

    if (success && primary_key_count == 1) {
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
        CreateIndex(index_name, table_name, key_columns);
    }

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
    std::unordered_set<std::string> key_column_set(key_columns.begin(),
                                                   key_columns.end());
    if (key_column_set.size() != key_columns.size()) {
        return false;  // Duplicate key columns
    }

    // Create index through catalog
    bool success = catalog_->CreateIndex(index_name, table_name, key_columns);

    if (success) {
        // Populate the index with existing data from the table
        auto* table_heap = table_info->table_heap.get();
        const Schema* schema = table_info->schema.get();

        // Get column indices for key columns
        std::vector<size_t> key_column_indices;
        for (const auto& col_name : key_columns) {
            key_column_indices.push_back(schema->GetColumnIdx(col_name));
        }

        // Scan the table and insert keys into index
        auto iter = table_heap->Begin();
        while (!iter.IsEnd()) {
            Tuple tuple = *iter;

            // Extract key from tuple based on key columns
            // For simplicity, we'll assume single column index
            if (key_columns.size() == 1) {
                Value key_value = tuple.GetValue(key_column_indices[0]);
                RID rid = tuple.GetRID();

                (void)rid;  // Ensure key_value is used

                // Insert into B+ tree index
                // Note: This requires index manager support which we'll need to
                // add For now, we'll leave this as a placeholder
                // index_manager_->InsertEntry(index_name, key_value, rid);
            }

            ++iter;
        }
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