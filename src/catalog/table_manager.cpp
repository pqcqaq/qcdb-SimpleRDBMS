// src/catalog/table_manager.cpp
#include "catalog/table_manager.h"

#include <algorithm>
#include <unordered_set>

#include "catalog/catalog.h"  // 添加完整的 catalog.h 包含
#include "catalog/schema.h"
#include "common/debug.h"
#include "common/exception.h"
#include "common/types.h"
#include "index/index_manager.h"
#include "parser/ast.h"
#include "record/table_heap.h"

namespace SimpleRDBMS {

TableManager::TableManager(BufferPoolManager* buffer_pool_manager,
                           Catalog* catalog)
    : buffer_pool_manager_(buffer_pool_manager), catalog_(catalog) {
    index_manager_ =
        std::make_unique<IndexManager>(buffer_pool_manager, catalog);

    // 重建所有索引
    RebuildAllIndexes();
}

TableManager::~TableManager() {
    // 确保在析构时正确清理索引管理器
    if (index_manager_) {
        LOG_DEBUG("TableManager::~TableManager: Destroying IndexManager");
        index_manager_.reset();  // 显式重置
    }
    LOG_DEBUG("TableManager::~TableManager: TableManager destroyed");
}

void TableManager::RebuildAllIndexes() {
    LOG_DEBUG("TableManager::RebuildAllIndexes: Starting index rebuild");
    
    // 获取所有表名
    std::vector<std::string> table_names = catalog_->GetAllTableNames();
    
    for (const auto& table_name : table_names) {
        // 获取该表的所有索引
        std::vector<IndexInfo*> indexes = catalog_->GetTableIndexes(table_name);
        
        for (auto* index_info : indexes) {
            LOG_DEBUG("TableManager::RebuildAllIndexes: Rebuilding index " << index_info->index_name);
            
            TableInfo* table_info = catalog_->GetTable(table_name);
            if (!table_info) {
                LOG_ERROR("TableManager::RebuildAllIndexes: Table " << table_name << " not found");
                continue;
            }
            
            // 重新创建物理索引
            bool success = index_manager_->CreateIndex(
                index_info->index_name,
                table_name,
                index_info->key_columns,
                table_info->schema.get()
            );
            
            if (!success) {
                LOG_ERROR("TableManager::RebuildAllIndexes: Failed to rebuild index " << index_info->index_name);
                continue;
            }
            
            // 用现有数据填充索引
            PopulateIndexWithExistingData(index_info->index_name, table_info, index_info->key_columns);
        }
    }
    
    LOG_DEBUG("TableManager::RebuildAllIndexes: Index rebuild completed");
}

bool TableManager::CreateIndex(const std::string& index_name,
                               const std::string& table_name,
                               const std::vector<std::string>& key_columns) {
    LOG_DEBUG("TableManager::CreateIndex: Creating index "
              << index_name << " on table " << table_name);

    // 检查索引是否已存在
    if (catalog_->GetIndex(index_name) != nullptr) {
        LOG_WARN("TableManager::CreateIndex: Index " << index_name
                                                     << " already exists");
        return false;
    }

    TableInfo* table_info = catalog_->GetTable(table_name);
    if (table_info == nullptr) {
        LOG_ERROR("TableManager::CreateIndex: Table " << table_name
                                                      << " not found");
        return false;
    }

    if (key_columns.empty()) {
        LOG_ERROR("TableManager::CreateIndex: Key columns cannot be empty");
        return false;
    }

    const Schema* schema = table_info->schema.get();
    for (const auto& key_column : key_columns) {
        if (!schema->HasColumn(key_column)) {
            LOG_ERROR("TableManager::CreateIndex: Column "
                      << key_column << " not found in table " << table_name);
            return false;
        }
    }

    std::unordered_set<std::string> key_column_set(key_columns.begin(),
                                                   key_columns.end());
    if (key_column_set.size() != key_columns.size()) {
        LOG_ERROR("TableManager::CreateIndex: Duplicate columns in key");
        return false;
    }

    // 先在目录中创建索引信息
    bool catalog_success =
        catalog_->CreateIndex(index_name, table_name, key_columns);
    if (!catalog_success) {
        LOG_ERROR(
            "TableManager::CreateIndex: Failed to create index in catalog");
        return false;
    }

    // 然后在索引管理器中创建物理索引
    bool index_success = index_manager_->CreateIndex(index_name, table_name,
                                                     key_columns, schema);
    if (!index_success) {
        LOG_ERROR("TableManager::CreateIndex: Failed to create physical index");
        catalog_->DropIndex(index_name);  // 清理目录中的索引信息
        return false;
    }

    LOG_DEBUG(
        "TableManager::CreateIndex: Physical index created, now populating "
        "with existing data");
    bool populate_success =
        PopulateIndexWithExistingData(index_name, table_info, key_columns);
    if (!populate_success) {
        LOG_WARN(
            "TableManager::CreateIndex: Failed to populate index with existing "
            "data");
    }

    LOG_DEBUG("TableManager::CreateIndex: Index " << index_name
                                                  << " created successfully");
    try {
        catalog_->SaveCatalogToDisk();
        LOG_DEBUG(
            "TableManager::CreateIndex: Catalog saved after index creation");
    } catch (const std::exception& e) {
        LOG_WARN(
            "TableManager::CreateIndex: Failed to save catalog after index "
            "creation: "
            << e.what());
    }

    return true;
}

bool TableManager::PopulateIndexWithExistingData(
    const std::string& index_name, TableInfo* table_info,
    const std::vector<std::string>& key_columns) {
    LOG_DEBUG("TableManager::PopulateIndexWithExistingData: Populating index "
              << index_name);

    if (!table_info || !table_info->table_heap || !table_info->schema) {
        LOG_ERROR(
            "TableManager::PopulateIndexWithExistingData: Invalid table info");
        return false;
    }

    // 目前只支持单列索引
    if (key_columns.size() != 1) {
        LOG_ERROR(
            "TableManager::PopulateIndexWithExistingData: Multi-column indexes "
            "not supported");
        return false;
    }

    const std::string& column_name = key_columns[0];
    const Schema* schema = table_info->schema.get();

    try {
        size_t column_idx = schema->GetColumnIdx(column_name);
        const Column& column = schema->GetColumn(column_idx);

        LOG_DEBUG(
            "TableManager::PopulateIndexWithExistingData: Processing column "
            << column_name << " of type " << static_cast<int>(column.type));

        // 遍历表中的所有记录
        auto iter = table_info->table_heap->Begin();
        auto end_iter = table_info->table_heap->End();

        int processed_count = 0;
        int success_count = 0;
        int error_count = 0;

        while (!iter.IsEnd()) {
            try {
                Tuple tuple = *iter;
                RID rid = tuple.GetRID();

                // 提取索引键值
                Value key_value = tuple.GetValue(column_idx);

                // 插入到索引中
                bool insert_success =
                    index_manager_->InsertEntry(index_name, key_value, rid);
                if (insert_success) {
                    success_count++;
                } else {
                    error_count++;
                    LOG_WARN(
                        "TableManager::PopulateIndexWithExistingData: Failed "
                        "to insert key into index");
                }

                processed_count++;

                // 每处理1000条记录记录一次日志
                if (processed_count % 1000 == 0) {
                    LOG_DEBUG(
                        "TableManager::PopulateIndexWithExistingData: "
                        "Processed "
                        << processed_count << " records");
                }

                ++iter;
            } catch (const std::exception& e) {
                LOG_ERROR(
                    "TableManager::PopulateIndexWithExistingData: Exception "
                    "processing record: "
                    << e.what());
                error_count++;
                ++iter;  // 继续处理下一条记录
            }
        }

        LOG_INFO(
            "TableManager::PopulateIndexWithExistingData: Finished populating "
            "index "
            << index_name << ". Processed: " << processed_count
            << ", Success: " << success_count << ", Errors: " << error_count);

        return error_count == 0;  // 只有所有记录都成功插入才返回true

    } catch (const std::exception& e) {
        LOG_ERROR("TableManager::PopulateIndexWithExistingData: Exception: "
                  << e.what());
        return false;
    }
}

bool TableManager::DropIndex(const std::string& index_name) {
    LOG_DEBUG("TableManager::DropIndex: Dropping index " << index_name);

    // 检查索引是否存在
    IndexInfo* index_info = catalog_->GetIndex(index_name);
    if (index_info == nullptr) {
        LOG_WARN("TableManager::DropIndex: Index " << index_name
                                                   << " not found");
        return false;
    }

    // 从索引管理器中删除物理索引
    bool index_success = index_manager_->DropIndex(index_name);
    if (!index_success) {
        LOG_WARN("TableManager::DropIndex: Failed to drop physical index "
                 << index_name);
        // 继续删除目录记录，因为可能索引管理器中没有这个索引
    }

    // 从目录中删除索引记录
    bool catalog_success = catalog_->DropIndex(index_name);
    if (!catalog_success) {
        LOG_ERROR("TableManager::DropIndex: Failed to drop index from catalog");
        return false;
    }

    LOG_DEBUG("TableManager::DropIndex: Index " << index_name
                                                << " dropped successfully");
    return true;
}

bool TableManager::UpdateIndexesOnInsert(const std::string& table_name,
                                         const Tuple& tuple, const RID& rid) {
    LOG_TRACE("TableManager::UpdateIndexesOnInsert: Updating indexes for table "
              << table_name);
    
    if (!index_manager_) {
        LOG_WARN("TableManager::UpdateIndexesOnInsert: IndexManager is null");
        return true; // 没有索引管理器，认为成功
    }
    
    std::vector<std::string> table_indexes;
    try {
        table_indexes = index_manager_->GetTableIndexes(table_name);
    } catch (const std::exception& e) {
        LOG_ERROR("TableManager::UpdateIndexesOnInsert: Failed to get table indexes: " << e.what());
        return false;
    }
    
    bool all_success = true;
    for (const auto& index_name : table_indexes) {
        IndexInfo* index_info = catalog_->GetIndex(index_name);
        if (!index_info) {
            LOG_WARN(
                "TableManager::UpdateIndexesOnInsert: Index info not found for "
                << index_name);
            continue;
        }
        
        if (index_info->key_columns.size() != 1) {
            LOG_WARN(
                "TableManager::UpdateIndexesOnInsert: Multi-column index not "
                "supported: "
                << index_name);
            continue;
        }

        try {
            TableInfo* table_info = catalog_->GetTable(table_name);
            if (!table_info) {
                LOG_ERROR(
                    "TableManager::UpdateIndexesOnInsert: Table info not "
                    "found");
                all_success = false;
                continue;
            }

            const std::string& column_name = index_info->key_columns[0];
            size_t column_idx = table_info->schema->GetColumnIdx(column_name);
            Value key_value = tuple.GetValue(column_idx);

            // 添加超时保护
            auto start_time = std::chrono::steady_clock::now();
            const auto TIMEOUT_DURATION = std::chrono::seconds(5);
            
            bool success = false;
            try {
                success = index_manager_->InsertEntry(index_name, key_value, rid);
                
                auto current_time = std::chrono::steady_clock::now();
                if (current_time - start_time > TIMEOUT_DURATION) {
                    LOG_ERROR("TableManager::UpdateIndexesOnInsert: Index insert timed out for " << index_name);
                    success = false;
                }
            } catch (const std::exception& e) {
                LOG_ERROR("TableManager::UpdateIndexesOnInsert: Exception during index insert: " << e.what());
                success = false;
            }
            
            if (!success) {
                LOG_WARN(
                    "TableManager::UpdateIndexesOnInsert: Failed to insert "
                    "into index "
                    << index_name);
                all_success = false;
            } else {
                LOG_TRACE(
                    "TableManager::UpdateIndexesOnInsert: Successfully "
                    "inserted "
                    << "into index " << index_name);
            }
        } catch (const std::exception& e) {
            LOG_ERROR(
                "TableManager::UpdateIndexesOnInsert: Exception updating index "
                << index_name << ": " << e.what());
            all_success = false;
        }
    }
    return all_success;
}

bool TableManager::UpdateIndexesOnDelete(const std::string& table_name,
                                         const Tuple& tuple) {
    LOG_TRACE("TableManager::UpdateIndexesOnDelete: Updating indexes for table "
              << table_name);
    std::vector<std::string> table_indexes =
        index_manager_->GetTableIndexes(table_name);
    bool all_success = true;
    for (const auto& index_name : table_indexes) {
        IndexInfo* index_info = catalog_->GetIndex(index_name);
        if (!index_info) {
            LOG_WARN(
                "TableManager::UpdateIndexesOnDelete: Index info not found for "
                << index_name);
            continue;
        }
        if (index_info->key_columns.size() != 1) {
            LOG_WARN(
                "TableManager::UpdateIndexesOnDelete: Multi-column index not "
                "supported: "
                << index_name);
            continue;
        }
        try {
            TableInfo* table_info = catalog_->GetTable(table_name);
            if (!table_info) {
                LOG_ERROR(
                    "TableManager::UpdateIndexesOnDelete: Table info not "
                    "found");
                all_success = false;
                continue;
            }
            const std::string& column_name = index_info->key_columns[0];
            size_t column_idx = table_info->schema->GetColumnIdx(column_name);
            Value key_value = tuple.GetValue(column_idx);

            // 修复：真正调用 IndexManager 的 DeleteEntry 方法
            bool success = index_manager_->DeleteEntry(index_name, key_value);
            if (!success) {
                LOG_WARN(
                    "TableManager::UpdateIndexesOnDelete: Failed to delete "
                    "from index "
                    << index_name);
                all_success = false;
            } else {
                LOG_TRACE(
                    "TableManager::UpdateIndexesOnDelete: Successfully deleted "
                    << "from index " << index_name);
            }
        } catch (const std::exception& e) {
            LOG_ERROR(
                "TableManager::UpdateIndexesOnDelete: Exception updating index "
                << index_name << ": " << e.what());
            all_success = false;
        }
    }
    return all_success;
}

bool TableManager::UpdateIndexesOnUpdate(const std::string& table_name,
                                         const Tuple& old_tuple,
                                         const Tuple& new_tuple,
                                         const RID& rid) {
    LOG_TRACE("TableManager::UpdateIndexesOnUpdate: Updating indexes for table "
              << table_name);
    std::vector<std::string> table_indexes =
        index_manager_->GetTableIndexes(table_name);
    bool all_success = true;
    for (const auto& index_name : table_indexes) {
        IndexInfo* index_info = catalog_->GetIndex(index_name);
        if (!index_info) {
            LOG_WARN(
                "TableManager::UpdateIndexesOnUpdate: Index info not found for "
                << index_name);
            continue;
        }
        if (index_info->key_columns.size() != 1) {
            LOG_WARN(
                "TableManager::UpdateIndexesOnUpdate: Multi-column index not "
                "supported: "
                << index_name);
            continue;
        }
        try {
            TableInfo* table_info = catalog_->GetTable(table_name);
            if (!table_info) {
                LOG_ERROR(
                    "TableManager::UpdateIndexesOnUpdate: Table info not "
                    "found");
                all_success = false;
                continue;
            }
            const std::string& column_name = index_info->key_columns[0];
            size_t column_idx = table_info->schema->GetColumnIdx(column_name);
            Value old_key_value = old_tuple.GetValue(column_idx);
            Value new_key_value = new_tuple.GetValue(column_idx);

            // 检查键值是否真的改变了
            if (old_key_value.index() == new_key_value.index()) {
                bool keys_equal = false;
                std::visit(
                    [&](const auto& old_val) {
                        using T = std::decay_t<decltype(old_val)>;
                        if (std::holds_alternative<T>(new_key_value)) {
                            keys_equal =
                                (old_val == std::get<T>(new_key_value));
                        }
                    },
                    old_key_value);
                if (keys_equal) {
                    continue;  // 键值没有改变，跳过
                }
            }

            // 修复：真正调用 IndexManager 的方法
            bool delete_success =
                index_manager_->DeleteEntry(index_name, old_key_value);
            if (!delete_success) {
                LOG_WARN(
                    "TableManager::UpdateIndexesOnUpdate: Failed to delete old "
                    "key from index "
                    << index_name);
                all_success = false;
            }
            bool insert_success =
                index_manager_->InsertEntry(index_name, new_key_value, rid);
            if (!insert_success) {
                LOG_WARN(
                    "TableManager::UpdateIndexesOnUpdate: Failed to insert new "
                    "key into index "
                    << index_name);
                all_success = false;
                // 尝试回滚
                index_manager_->InsertEntry(index_name, old_key_value, rid);
            }
        } catch (const std::exception& e) {
            LOG_ERROR(
                "TableManager::UpdateIndexesOnUpdate: Exception updating index "
                << index_name << ": " << e.what());
            all_success = false;
        }
    }
    return all_success;
}

IndexManager* TableManager::GetIndexManager() { return index_manager_.get(); }

bool TableManager::CreateTable(const CreateTableStatement* stmt) {
    if (!stmt) {
        LOG_ERROR("TableManager::CreateTable: CreateTableStatement is null");
        return false;
    }

    LOG_DEBUG("TableManager::CreateTable: Creating table "
              << stmt->GetTableName());
    const std::string& table_name = stmt->GetTableName();
    const std::vector<Column>& columns = stmt->GetColumns();

    if (columns.empty()) {
        LOG_ERROR("TableManager::CreateTable: No columns defined for table "
                  << table_name);
        return false;
    }

    Schema schema(columns);
    bool success = catalog_->CreateTable(table_name, schema);
    if (!success) {
        LOG_ERROR("TableManager::CreateTable: Failed to create table "
                  << table_name);
        return false;
    }

    LOG_DEBUG("TableManager::CreateTable: Table " << table_name
                                                  << " created successfully");

    // Find primary key column
    std::string primary_key_column;
    for (const auto& column : columns) {
        if (column.is_primary_key) {
            primary_key_column = column.name;
            LOG_DEBUG("TableManager::CreateTable: Found primary key column: "
                      << primary_key_column);
            break;
        }
    }

    if (!primary_key_column.empty()) {
        std::string primary_key_index_name = table_name + "_pk";
        std::vector<std::string> key_columns = {primary_key_column};

        LOG_DEBUG("TableManager::CreateTable: Creating primary key index "
                  << primary_key_index_name << " on column "
                  << primary_key_column);

        // First create in catalog
        bool catalog_success = catalog_->CreateIndex(primary_key_index_name,
                                                     table_name, key_columns);
        if (!catalog_success) {
            LOG_ERROR(
                "TableManager::CreateTable: Failed to create primary key index "
                "in catalog");
            // Clean up the table since we couldn't create the required primary
            // key index
            catalog_->DropTable(table_name);
            return false;
        }

        // Then create the physical index
        TableInfo* table_info = catalog_->GetTable(table_name);
        if (!table_info) {
            LOG_ERROR(
                "TableManager::CreateTable: Table info not found after "
                "creation");
            catalog_->DropTable(table_name);
            return false;
        }

        bool index_success =
            index_manager_->CreateIndex(primary_key_index_name, table_name,
                                        key_columns, table_info->schema.get());

        if (!index_success) {
            LOG_ERROR(
                "TableManager::CreateTable: Failed to create physical primary "
                "key index");
            catalog_->DropIndex(primary_key_index_name);
            catalog_->DropTable(table_name);
            return false;
        }

        LOG_DEBUG("TableManager::CreateTable: Primary key index "
                  << primary_key_index_name << " created successfully");

        // Verify the index was created
        auto all_indexes = index_manager_->GetAllIndexNames();
        bool found_pk_index = false;
        for (const auto& idx_name : all_indexes) {
            if (idx_name == primary_key_index_name) {
                found_pk_index = true;
                break;
            }
        }

        if (!found_pk_index) {
            LOG_ERROR(
                "TableManager::CreateTable: Primary key index not found after "
                "creation");
            catalog_->DropTable(table_name);
            return false;
        }

        LOG_DEBUG(
            "TableManager::CreateTable: Verified primary key index exists");
    } else {
        LOG_DEBUG(
            "TableManager::CreateTable: No primary key column found for table "
            << table_name);
    }

    LOG_DEBUG(
        "TableManager::CreateTable: Successfully completed table creation for "
        << table_name);
    return true;
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

}  // namespace SimpleRDBMS