/*
 * 文件: table_manager.cpp
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述: 表管理器的实现，负责表的创建删除、索引管理和数据操作时的索引维护
 */

#include "catalog/table_manager.h"

#include <algorithm>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "common/debug.h"
#include "common/exception.h"
#include "common/types.h"
#include "index/index_manager.h"
#include "parser/ast.h"
#include "record/table_heap.h"

namespace SimpleRDBMS {

/**
 * TableManager构造函数
 *
 * 初始化思路：
 * 1. 保存buffer pool和catalog的引用
 * 2. 创建IndexManager实例
 * 3. 重建所有现有的索引（恢复时需要）
 */
TableManager::TableManager(BufferPoolManager* buffer_pool_manager,
                           Catalog* catalog)
    : buffer_pool_manager_(buffer_pool_manager), catalog_(catalog) {
    // 创建索引管理器，传入必要的依赖
    index_manager_ =
        std::make_unique<IndexManager>(buffer_pool_manager, catalog);

    // 系统启动时重建所有索引，确保索引和数据的一致性
    RebuildAllIndexes();
}

/**
 * TableManager析构函数
 *
 * 清理思路：
 * 1. 确保IndexManager被正确销毁
 * 2. 记录清理过程，便于调试
 */
TableManager::~TableManager() {
    if (index_manager_) {
        LOG_DEBUG("TableManager::~TableManager: Destroying IndexManager");
        index_manager_.reset();  // 显式重置，确保资源释放
    }
    LOG_DEBUG("TableManager::~TableManager: TableManager destroyed");
}

/**
 * 重建所有索引
 *
 * 重建流程：
 * 1. 获取所有表名
 * 2. 对每个表，获取其所有索引信息
 * 3. 重新创建索引的物理结构
 * 4. 用现有数据填充索引
 *
 * 这个方法主要在系统启动时调用，确保索引和数据的一致性
 */
void TableManager::RebuildAllIndexes() {
    LOG_DEBUG("TableManager::RebuildAllIndexes: Starting index rebuild");

    // 获取系统中所有的表
    std::vector<std::string> table_names = catalog_->GetAllTableNames();

    for (const auto& table_name : table_names) {
        // 获取该表的所有索引定义
        std::vector<IndexInfo*> indexes = catalog_->GetTableIndexes(table_name);

        for (auto* index_info : indexes) {
            LOG_DEBUG("TableManager::RebuildAllIndexes: Rebuilding index "
                      << index_info->index_name);

            TableInfo* table_info = catalog_->GetTable(table_name);
            if (!table_info) {
                LOG_ERROR("TableManager::RebuildAllIndexes: Table "
                          << table_name << " not found");
                continue;
            }

            // 重新创建索引的物理结构（B+树等）
            bool success = index_manager_->CreateIndex(
                index_info->index_name, table_name, index_info->key_columns,
                table_info->schema.get());

            if (!success) {
                LOG_ERROR(
                    "TableManager::RebuildAllIndexes: Failed to rebuild index "
                    << index_info->index_name);
                continue;
            }

            // 用表中现有的数据填充索引
            PopulateIndexWithExistingData(index_info->index_name, table_info,
                                          index_info->key_columns);
        }
    }

    LOG_DEBUG("TableManager::RebuildAllIndexes: Index rebuild completed");
}

/**
 * 创建索引
 *
 * 创建流程：
 * 1. 验证输入参数的合法性（索引名、表名、列名等）
 * 2. 在catalog中创建索引元数据
 * 3. 在索引管理器中创建物理索引结构
 * 4. 用现有数据填充索引
 * 5. 保存catalog到磁盘
 */
bool TableManager::CreateIndex(const std::string& index_name,
                               const std::string& table_name,
                               const std::vector<std::string>& key_columns) {
    LOG_DEBUG("TableManager::CreateIndex: Creating index "
              << index_name << " on table " << table_name);

    // 检查索引名是否已经存在
    if (catalog_->GetIndex(index_name) != nullptr) {
        LOG_WARN("TableManager::CreateIndex: Index " << index_name
                                                     << " already exists");
        return false;
    }

    // 检查表是否存在
    TableInfo* table_info = catalog_->GetTable(table_name);
    if (table_info == nullptr) {
        LOG_ERROR("TableManager::CreateIndex: Table " << table_name
                                                      << " not found");
        return false;
    }

    // 验证索引列不能为空
    if (key_columns.empty()) {
        LOG_ERROR("TableManager::CreateIndex: Key columns cannot be empty");
        return false;
    }

    // 验证所有索引列都存在于表中
    const Schema* schema = table_info->schema.get();
    for (const auto& key_column : key_columns) {
        if (!schema->HasColumn(key_column)) {
            LOG_ERROR("TableManager::CreateIndex: Column "
                      << key_column << " not found in table " << table_name);
            return false;
        }
    }

    // 检查索引列是否有重复
    std::unordered_set<std::string> key_column_set(key_columns.begin(),
                                                   key_columns.end());
    if (key_column_set.size() != key_columns.size()) {
        LOG_ERROR("TableManager::CreateIndex: Duplicate columns in key");
        return false;
    }

    // 先在catalog中创建索引元数据
    bool catalog_success =
        catalog_->CreateIndex(index_name, table_name, key_columns);
    if (!catalog_success) {
        LOG_ERROR(
            "TableManager::CreateIndex: Failed to create index in catalog");
        return false;
    }

    // 然后在索引管理器中创建物理索引结构
    bool index_success = index_manager_->CreateIndex(index_name, table_name,
                                                     key_columns, schema);
    if (!index_success) {
        LOG_ERROR("TableManager::CreateIndex: Failed to create physical index");
        catalog_->DropIndex(index_name);  // 失败时清理catalog中的记录
        return false;
    }

    LOG_DEBUG(
        "TableManager::CreateIndex: Physical index created, now populating "
        "with existing data");

    // 用表中现有的数据填充新创建的索引
    bool populate_success =
        PopulateIndexWithExistingData(index_name, table_info, key_columns);
    if (!populate_success) {
        LOG_WARN(
            "TableManager::CreateIndex: Failed to populate index with existing "
            "data");
    }

    LOG_DEBUG("TableManager::CreateIndex: Index " << index_name
                                                  << " created successfully");

    // 保存catalog信息到磁盘
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

/**
 * 用现有数据填充索引
 *
 * 填充流程：
 * 1. 验证表信息和索引列的有效性
 * 2. 遍历表中的所有记录
 * 3. 提取每条记录的索引键值
 * 4. 将键值和RID插入到索引中
 *
 * 注意：目前只支持单列索引，多列索引需要后续实现
 */
bool TableManager::PopulateIndexWithExistingData(
    const std::string& index_name, TableInfo* table_info,
    const std::vector<std::string>& key_columns) {
    LOG_DEBUG("TableManager::PopulateIndexWithExistingData: Populating index "
              << index_name);

    // 验证输入参数的有效性
    if (!table_info || !table_info->table_heap || !table_info->schema) {
        LOG_ERROR(
            "TableManager::PopulateIndexWithExistingData: Invalid table info");
        return false;
    }

    // 目前只支持单列索引，多列索引是未来的扩展方向
    if (key_columns.size() != 1) {
        LOG_ERROR(
            "TableManager::PopulateIndexWithExistingData: Multi-column indexes "
            "not supported");
        return false;
    }

    const std::string& column_name = key_columns[0];
    const Schema* schema = table_info->schema.get();

    try {
        // 获取索引列在schema中的位置和类型信息
        size_t column_idx = schema->GetColumnIdx(column_name);
        const Column& column = schema->GetColumn(column_idx);

        LOG_DEBUG(
            "TableManager::PopulateIndexWithExistingData: Processing column "
            << column_name << " of type " << static_cast<int>(column.type));

        // 获取表的迭代器，准备遍历所有记录
        auto iter = table_info->table_heap->Begin();
        auto end_iter = table_info->table_heap->End();

        // 统计处理结果
        int processed_count = 0;
        int success_count = 0;
        int error_count = 0;

        // 遍历表中的每一条记录
        while (!iter.IsEnd()) {
            try {
                Tuple tuple = *iter;
                RID rid = tuple.GetRID();

                // 从tuple中提取索引键值
                Value key_value = tuple.GetValue(column_idx);

                // 将键值和RID插入到索引中
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

                // 每处理1000条记录输出一次进度日志
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
                ++iter;  // 即使出错也继续处理下一条记录
            }
        }

        LOG_INFO(
            "TableManager::PopulateIndexWithExistingData: Finished populating "
            "index "
            << index_name << ". Processed: " << processed_count
            << ", Success: " << success_count << ", Errors: " << error_count);

        // 只有所有记录都成功插入才返回true
        return error_count == 0;

    } catch (const std::exception& e) {
        LOG_ERROR("TableManager::PopulateIndexWithExistingData: Exception: "
                  << e.what());
        return false;
    }
}

/**
 * 删除索引
 *
 * 删除流程：
 * 1. 检查索引是否存在
 * 2. 从索引管理器中删除物理索引结构
 * 3. 从catalog中删除索引元数据
 */
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
        // 继续删除catalog记录，因为可能索引管理器中没有这个索引
    }

    // 从catalog中删除索引记录
    bool catalog_success = catalog_->DropIndex(index_name);
    if (!catalog_success) {
        LOG_ERROR("TableManager::DropIndex: Failed to drop index from catalog");
        return false;
    }

    LOG_DEBUG("TableManager::DropIndex: Index " << index_name
                                                << " dropped successfully");
    return true;
}

/**
 * 插入记录时更新索引
 *
 * 更新流程：
 * 1. 获取表的所有索引
 * 2. 对每个索引，提取新记录的键值
 * 3. 将键值和RID插入到对应的索引中
 *
 * 这个方法在INSERT操作时被调用，保持索引和数据的一致性
 */
bool TableManager::UpdateIndexesOnInsert(const std::string& table_name,
                                         const Tuple& tuple, const RID& rid) {
    LOG_TRACE("TableManager::UpdateIndexesOnInsert: Updating indexes for table "
              << table_name);

    if (!index_manager_) {
        LOG_WARN("TableManager::UpdateIndexesOnInsert: IndexManager is null");
        return true;  // 没有索引管理器，认为成功
    }

    // 获取该表的所有索引
    std::vector<std::string> table_indexes;
    try {
        table_indexes = index_manager_->GetTableIndexes(table_name);
    } catch (const std::exception& e) {
        LOG_ERROR(
            "TableManager::UpdateIndexesOnInsert: Failed to get table indexes: "
            << e.what());
        return false;
    }

    bool all_success = true;

    // 遍历每个索引，将新记录的键值插入
    for (const auto& index_name : table_indexes) {
        IndexInfo* index_info = catalog_->GetIndex(index_name);
        if (!index_info) {
            LOG_WARN(
                "TableManager::UpdateIndexesOnInsert: Index info not found for "
                << index_name);
            continue;
        }

        // 目前只支持单列索引
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

            // 提取索引键值
            const std::string& column_name = index_info->key_columns[0];
            size_t column_idx = table_info->schema->GetColumnIdx(column_name);
            Value key_value = tuple.GetValue(column_idx);

            // 添加超时保护，避免索引操作阻塞太久
            auto start_time = std::chrono::steady_clock::now();
            const auto TIMEOUT_DURATION = std::chrono::seconds(5);

            bool success = false;
            try {
                success =
                    index_manager_->InsertEntry(index_name, key_value, rid);

                auto current_time = std::chrono::steady_clock::now();
                if (current_time - start_time > TIMEOUT_DURATION) {
                    LOG_ERROR(
                        "TableManager::UpdateIndexesOnInsert: Index insert "
                        "timed out for "
                        << index_name);
                    success = false;
                }
            } catch (const std::exception& e) {
                LOG_ERROR(
                    "TableManager::UpdateIndexesOnInsert: Exception during "
                    "index insert: "
                    << e.what());
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

/**
 * 删除记录时更新索引
 *
 * 更新流程：
 * 1. 获取表的所有索引
 * 2. 对每个索引，提取被删除记录的键值
 * 3. 从对应的索引中删除该键值
 *
 * 这个方法在DELETE操作时被调用
 */
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

        // 目前只支持单列索引
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

            // 提取要删除的键值
            const std::string& column_name = index_info->key_columns[0];
            size_t column_idx = table_info->schema->GetColumnIdx(column_name);
            Value key_value = tuple.GetValue(column_idx);

            // 从索引中删除该键值
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

/**
 * 更新记录时更新索引
 *
 * 更新流程：
 * 1. 获取表的所有索引
 * 2. 对每个索引，比较新旧记录的键值
 * 3. 如果键值发生变化，先删除旧键值，再插入新键值
 * 4. 如果键值没变化，跳过该索引
 *
 * 这个方法在UPDATE操作时被调用
 */
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

        // 目前只支持单列索引
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

            // 提取新旧记录的键值
            const std::string& column_name = index_info->key_columns[0];
            size_t column_idx = table_info->schema->GetColumnIdx(column_name);
            Value old_key_value = old_tuple.GetValue(column_idx);
            Value new_key_value = new_tuple.GetValue(column_idx);

            // 检查键值是否真的改变了
            // 只有键值发生变化时才需要更新索引
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
                    continue;  // 键值没有改变，跳过该索引
                }
            }

            // 先删除旧的键值，再插入新的键值
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
                // 尝试回滚，恢复旧的键值
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

/**
 * 获取索引管理器指针
 *
 * 这个方法主要供其他组件访问IndexManager，
 * 比如查询执行器需要使用索引进行查找
 */
IndexManager* TableManager::GetIndexManager() { return index_manager_.get(); }

/**
 * 创建表
 *
 * 创建流程：
 * 1. 验证CreateTableStatement的有效性
 * 2. 提取表名和列定义
 * 3. 创建Schema并在catalog中创建表
 * 4. 如果有主键列，自动创建主键索引
 * 5. 验证创建结果
 */
bool TableManager::CreateTable(const CreateTableStatement* stmt) {
    if (!stmt) {
        LOG_ERROR("TableManager::CreateTable: CreateTableStatement is null");
        return false;
    }

    LOG_DEBUG("TableManager::CreateTable: Creating table "
              << stmt->GetTableName());

    const std::string& table_name = stmt->GetTableName();
    const std::vector<Column>& columns = stmt->GetColumns();

    // 验证表至少有一列
    if (columns.empty()) {
        LOG_ERROR("TableManager::CreateTable: No columns defined for table "
                  << table_name);
        return false;
    }

    // 创建schema并在catalog中创建表
    Schema schema(columns);
    bool success = catalog_->CreateTable(table_name, schema);
    if (!success) {
        LOG_ERROR("TableManager::CreateTable: Failed to create table "
                  << table_name);
        return false;
    }

    LOG_DEBUG("TableManager::CreateTable: Table " << table_name
                                                  << " created successfully");

    // 查找主键列，主键列需要自动创建唯一索引
    std::string primary_key_column;
    for (const auto& column : columns) {
        if (column.is_primary_key) {
            primary_key_column = column.name;
            LOG_DEBUG("TableManager::CreateTable: Found primary key column: "
                      << primary_key_column);
            break;
        }
    }

    // 如果有主键列，创建主键索引
    if (!primary_key_column.empty()) {
        std::string primary_key_index_name = table_name + "_pk";
        std::vector<std::string> key_columns = {primary_key_column};

        LOG_DEBUG("TableManager::CreateTable: Creating primary key index "
                  << primary_key_index_name << " on column "
                  << primary_key_column);

        // 先在catalog中创建索引信息
        bool catalog_success = catalog_->CreateIndex(primary_key_index_name,
                                                     table_name, key_columns);
        if (!catalog_success) {
            LOG_ERROR(
                "TableManager::CreateTable: Failed to create primary key index "
                "in catalog");
            // 创建主键索引失败，需要清理已创建的表
            catalog_->DropTable(table_name);
            return false;
        }

        // 然后创建物理索引结构
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

        // 验证索引是否真的创建成功
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

/**
 * 删除表
 *
 * 删除流程：
 * 1. 检查表是否存在
 * 2. 删除该表的所有索引
 * 3. 从catalog中删除表
 *
 * 注意：必须先删除索引再删除表，保证数据一致性
 */
bool TableManager::DropTable(const std::string& table_name) {
    LOG_DEBUG("TableManager::DropTable: Dropping table " << table_name);

    // 检查表是否存在
    TableInfo* table_info = catalog_->GetTable(table_name);
    if (table_info == nullptr) {
        LOG_WARN("TableManager::DropTable: Table " << table_name
                                                   << " not found");
        return false;
    }

    // 首先删除该表的所有索引
    // 必须先删除索引，因为索引依赖于表的存在
    std::vector<IndexInfo*> indexes = catalog_->GetTableIndexes(table_name);
    for (auto* index_info : indexes) {
        LOG_DEBUG("TableManager::DropTable: Dropping index "
                  << index_info->index_name);
        bool index_dropped = DropIndex(index_info->index_name);
        if (!index_dropped) {
            LOG_WARN("TableManager::DropTable: Failed to drop index "
                     << index_info->index_name);
        }
    }

    // 然后从catalog中删除表本身
    bool success = catalog_->DropTable(table_name);
    if (success) {
        LOG_DEBUG("TableManager::DropTable: Successfully dropped table "
                  << table_name);
    } else {
        LOG_ERROR("TableManager::DropTable: Failed to drop table "
                  << table_name);
    }

    return success;
}

}  // namespace SimpleRDBMS