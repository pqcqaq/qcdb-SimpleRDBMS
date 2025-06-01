/*
 * 文件: catalog.cpp
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述: 数据库目录管理器实现，负责表和索引的元数据管理与持久化
 */

#include "catalog/catalog.h"

#include <cstring>

#include "buffer/buffer_pool_manager.h"
#include "catalog/schema.h"
#include "common/debug.h"
#include "common/exception.h"
#include "record/table_heap.h"
#include "recovery/log_manager.h"

namespace SimpleRDBMS {

/**
 * 构造函数 - 初始化目录管理器
 * @param buffer_pool_manager 缓冲池管理器指针
 * @param log_manager 日志管理器指针
 *
 * 实现思路：
 * 1. 初始化成员变量和OID计数器
 * 2. 尝试从磁盘加载已有的catalog信息
 * 3. 如果加载失败则从空catalog开始
 */
Catalog::Catalog(BufferPoolManager* buffer_pool_manager,
                 LogManager* log_manager)
    : buffer_pool_manager_(buffer_pool_manager),
      log_manager_(log_manager),
      next_table_oid_(1),  // 从1开始分配table OID
      next_index_oid_(1),  // 从1开始分配index OID
      save_in_progress_(false) {
    LOG_DEBUG("Catalog: Initializing catalog");

    // 尝试从磁盘加载catalog元数据
    try {
        LoadCatalogFromDisk();
        LOG_DEBUG("Catalog: Successfully loaded catalog from disk");
    } catch (const std::exception& e) {
        LOG_WARN("Catalog: Failed to load catalog from disk: " << e.what());
        LOG_DEBUG("Catalog: Starting with empty catalog");
    }
    LOG_DEBUG("Catalog: Catalog initialization completed");
}

/**
 * 析构函数 - 安全清理catalog
 *
 * 实现思路：
 * - 在析构前尝试保存catalog到磁盘
 * - 异常安全处理，避免析构过程中抛出异常
 */
Catalog::~Catalog() {
    // 析构时安全保存catalog，如果buffer pool还可用
    if (buffer_pool_manager_) {
        try {
            SaveCatalogToDisk();
        } catch (const std::exception& e) {
            LOG_WARN(
                "Catalog::~Catalog: Failed to save catalog during destruction: "
                << e.what());
        } catch (...) {
            LOG_WARN(
                "Catalog::~Catalog: Unknown error saving catalog during "
                "destruction");
        }
    }
    LOG_DEBUG("Catalog destructor completed");
}

/**
 * 创建新表
 * @param table_name 表名
 * @param schema 表的schema定义
 * @return 是否创建成功
 *
 * 实现思路：
 * 1. 检查表名是否已存在
 * 2. 创建TableInfo对象，分配OID
 * 3. 为表分配第一个数据页面
 * 4. 创建TableHeap对象管理表数据
 * 5. 更新内存中的catalog映射
 * 6. 持久化catalog到磁盘
 */
bool Catalog::CreateTable(const std::string& table_name, const Schema& schema) {
    LOG_DEBUG("CreateTable: Starting to create table " << table_name);

    // 检查表名是否已存在
    if (tables_.find(table_name) != tables_.end()) {
        LOG_WARN("CreateTable: Table " << table_name << " already exists");
        return false;
    }

    LOG_DEBUG("CreateTable: Creating table info for " << table_name);
    auto table_info = std::make_unique<TableInfo>();
    table_info->table_name = table_name;
    table_info->schema = std::make_unique<Schema>(schema);
    table_info->table_oid = next_table_oid_++;  // 分配唯一的table OID

    LOG_DEBUG("CreateTable: Allocating first page for table " << table_name);

    // 为表分配第一个数据页面
    page_id_t first_page_id;
    Page* first_page = buffer_pool_manager_->NewPage(&first_page_id);
    if (first_page == nullptr) {
        LOG_ERROR("CreateTable: Failed to allocate first page for table "
                  << table_name);
        return false;
    }

    LOG_DEBUG("CreateTable: Allocated first page "
              << first_page_id << " for table " << table_name);

    // 初始化表页面
    first_page->WLatch();
    auto* table_page = reinterpret_cast<TablePage*>(first_page);
    table_page->Init(first_page_id, INVALID_PAGE_ID);
    first_page->WUnlatch();

    table_info->first_page_id = first_page_id;

    LOG_DEBUG("CreateTable: Creating TableHeap for table " << table_name);

    // 创建TableHeap管理表数据
    table_info->table_heap = std::make_unique<TableHeap>(
        buffer_pool_manager_, table_info->schema.get(), first_page_id);

    // 设置LogManager用于事务日志
    if (log_manager_) {
        table_info->table_heap->SetLogManager(log_manager_);
    }

    buffer_pool_manager_->UnpinPage(first_page_id, true);

    LOG_DEBUG("CreateTable: Adding table to catalog maps");

    // 更新catalog的内存映射
    oid_t table_oid = table_info->table_oid;
    tables_[table_name] = std::move(table_info);
    table_oid_map_[table_oid] = table_name;

    LOG_DEBUG("CreateTable: Saving catalog to disk");

    // 持久化catalog到磁盘
    try {
        SaveCatalogToDisk();
        LOG_DEBUG("CreateTable: Successfully saved catalog to disk");
    } catch (const std::exception& e) {
        LOG_ERROR("CreateTable: Failed to save catalog to disk: " << e.what());
        // 回滚内存中的更改
        tables_.erase(table_name);
        table_oid_map_.erase(table_oid);
        return false;
    }

    LOG_DEBUG("CreateTable: Table " << table_name << " created successfully");
    return true;
}

/**
 * 删除表
 * @param table_name 要删除的表名
 * @return 是否删除成功
 *
 * 实现思路：
 * 1. 查找表是否存在
 * 2. 从内存映射中移除
 * 3. 保存catalog到磁盘
 */
bool Catalog::DropTable(const std::string& table_name) {
    auto it = tables_.find(table_name);
    if (it == tables_.end()) {
        return false;
    }

    // 从映射中移除表信息
    oid_t table_oid = it->second->table_oid;
    table_oid_map_.erase(table_oid);
    tables_.erase(it);

    // 保存到磁盘
    SaveCatalogToDisk();
    return true;
}

/**
 * 根据表名获取表信息
 * @param table_name 表名
 * @return 表信息指针，不存在返回nullptr
 */
TableInfo* Catalog::GetTable(const std::string& table_name) {
    auto it = tables_.find(table_name);
    if (it == tables_.end()) {
        return nullptr;
    }
    return it->second.get();
}

/**
 * 根据表OID获取表信息
 * @param table_oid 表的OID
 * @return 表信息指针，不存在返回nullptr
 */
TableInfo* Catalog::GetTable(oid_t table_oid) {
    auto it = table_oid_map_.find(table_oid);
    if (it == table_oid_map_.end()) {
        return nullptr;
    }
    return GetTable(it->second);
}

/**
 * 创建索引
 * @param index_name 索引名
 * @param table_name 表名
 * @param key_columns 索引键列名列表
 * @return 是否创建成功
 *
 * 实现思路：
 * 1. 检查索引名和表名的有效性
 * 2. 创建IndexInfo对象
 * 3. 更新内存映射
 * 4. 延迟保存让上层决定何时持久化
 */
bool Catalog::CreateIndex(const std::string& index_name,
                          const std::string& table_name,
                          const std::vector<std::string>& key_columns) {
    // 检查索引是否已存在
    if (indexes_.find(index_name) != indexes_.end()) {
        return false;
    }

    // 检查表是否存在
    if (tables_.find(table_name) == tables_.end()) {
        return false;
    }

    // 创建索引信息
    auto index_info = std::make_unique<IndexInfo>();
    index_info->index_name = index_name;
    index_info->table_name = table_name;
    index_info->key_columns = key_columns;
    index_info->index_oid = next_index_oid_++;  // 分配唯一的index OID

    // 存储索引信息到内存映射
    oid_t index_oid = index_info->index_oid;
    indexes_[index_name] = std::move(index_info);
    index_oid_map_[index_oid] = index_name;

    LOG_DEBUG("CreateIndex: Index " << index_name << " created in memory");

    // 延迟保存，让上层调用者决定何时保存
    return true;
}

/**
 * 删除索引
 * @param index_name 索引名
 * @return 是否删除成功
 */
bool Catalog::DropIndex(const std::string& index_name) {
    auto it = indexes_.find(index_name);
    if (it == indexes_.end()) {
        return false;
    }

    // 从映射中移除索引信息
    oid_t index_oid = it->second->index_oid;
    index_oid_map_.erase(index_oid);
    indexes_.erase(it);

    // 保存到磁盘
    SaveCatalogToDisk();
    return true;
}

/**
 * 根据索引名获取索引信息
 */
IndexInfo* Catalog::GetIndex(const std::string& index_name) {
    auto it = indexes_.find(index_name);
    if (it == indexes_.end()) {
        return nullptr;
    }
    return it->second.get();
}

/**
 * 根据索引OID获取索引信息
 */
IndexInfo* Catalog::GetIndex(oid_t index_oid) {
    auto it = index_oid_map_.find(index_oid);
    if (it == index_oid_map_.end()) {
        return nullptr;
    }
    return GetIndex(it->second);
}

/**
 * 获取表的所有索引
 * @param table_name 表名
 * @return 该表的所有索引信息列表
 */
std::vector<IndexInfo*> Catalog::GetTableIndexes(
    const std::string& table_name) {
    std::vector<IndexInfo*> result;
    for (const auto& [index_name, index_info] : indexes_) {
        if (index_info->table_name == table_name) {
            result.push_back(index_info.get());
        }
    }
    return result;
}

/**
 * 从磁盘加载catalog元数据
 *
 * 实现思路：
 * 1. 读取catalog页面（固定为页面1）
 * 2. 验证magic number确保数据有效性
 * 3. 反序列化表和索引的元数据
 * 4. 重建内存中的映射关系
 * 5. 为每个表重新创建TableHeap对象
 */
void Catalog::LoadCatalogFromDisk() {
    LOG_DEBUG("LoadCatalogFromDisk: Starting catalog load");
    if (buffer_pool_manager_ == nullptr) {
        LOG_ERROR("LoadCatalogFromDisk: BufferPoolManager is null");
        return;
    }

    LOG_DEBUG("LoadCatalogFromDisk: Attempting to fetch catalog page 0");
    Page* catalog_page = nullptr;
    try {
        // 使用GetSpecificPage而不是FetchPage，确保可以访问页面0
        catalog_page = buffer_pool_manager_->GetSpecificPage(0);
    } catch (const std::exception& e) {
        LOG_WARN(
            "LoadCatalogFromDisk: Exception when getting page 0: " << e.what());
        return;
    }

    if (catalog_page == nullptr) {
        LOG_DEBUG(
            "LoadCatalogFromDisk: Page 0 does not exist, assuming new "
            "database");
        return;
    }

    LOG_DEBUG("LoadCatalogFromDisk: Successfully got catalog page 0");
    const char* data = catalog_page->GetData();
    size_t offset = 0;

    uint32_t magic_number;
    std::memcpy(&magic_number, data + offset, sizeof(uint32_t));
    offset += sizeof(uint32_t);

    LOG_DEBUG("LoadCatalogFromDisk: Read magic number: 0x"
              << std::hex << magic_number << std::dec
              << " (expected: 0x12345678)");

    if (magic_number != 0x12345678) {
        LOG_WARN(
            "LoadCatalogFromDisk: Invalid magic number, not a valid catalog "
            "page");
        buffer_pool_manager_->UnpinPage(0, false);
        return;
    }

    LOG_DEBUG(
        "LoadCatalogFromDisk: Valid catalog page found, loading metadata");

    // 读取OID计数器
    std::memcpy(&next_table_oid_, data + offset, sizeof(oid_t));
    offset += sizeof(oid_t);
    std::memcpy(&next_index_oid_, data + offset, sizeof(oid_t));
    offset += sizeof(oid_t);
    LOG_DEBUG("LoadCatalogFromDisk: Next table OID: "
              << next_table_oid_ << ", Next index OID: " << next_index_oid_);

    // 读取表的数量和信息
    uint32_t table_count;
    std::memcpy(&table_count, data + offset, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    LOG_DEBUG("LoadCatalogFromDisk: Loading " << table_count << " tables");

    // 加载每个表的信息
    for (uint32_t i = 0; i < table_count; ++i) {
        LOG_DEBUG("LoadCatalogFromDisk: Loading table " << i);
        auto table_info = std::make_unique<TableInfo>();

        // 检查数据边界
        if (offset + sizeof(oid_t) + sizeof(page_id_t) + sizeof(uint32_t) >
            PAGE_SIZE) {
            LOG_ERROR("LoadCatalogFromDisk: Not enough data to read table "
                      << i);
            break;
        }

        // 读取表的基本信息
        std::memcpy(&table_info->table_oid, data + offset, sizeof(oid_t));
        offset += sizeof(oid_t);
        std::memcpy(&table_info->first_page_id, data + offset,
                    sizeof(page_id_t));
        offset += sizeof(page_id_t);

        // 读取表名
        uint32_t name_len;
        std::memcpy(&name_len, data + offset, sizeof(uint32_t));
        offset += sizeof(uint32_t);
        if (name_len > 256 || offset + name_len > PAGE_SIZE) {
            LOG_ERROR(
                "LoadCatalogFromDisk: Invalid table name length: " << name_len);
            break;
        }
        table_info->table_name = std::string(data + offset, name_len);
        offset += name_len;

        LOG_DEBUG("LoadCatalogFromDisk: Loading table '"
                  << table_info->table_name << "' with OID "
                  << table_info->table_oid << " and first page "
                  << table_info->first_page_id);

        // 反序列化schema
        try {
            table_info->schema = DeserializeSchema(data, offset);
        } catch (const std::exception& e) {
            LOG_ERROR(
                "LoadCatalogFromDisk: Failed to deserialize schema for table "
                << table_info->table_name << ": " << e.what());
            break;
        }

        // 重新创建TableHeap
        try {
            table_info->table_heap = std::make_unique<TableHeap>(
                buffer_pool_manager_, table_info->schema.get(),
                table_info->first_page_id);
            // 设置LogManager
            if (log_manager_) {
                table_info->table_heap->SetLogManager(log_manager_);
            }
        } catch (const std::exception& e) {
            LOG_ERROR(
                "LoadCatalogFromDisk: Failed to create TableHeap for table "
                << table_info->table_name << ": " << e.what());
            break;
        }

        // 更新内存映射
        std::string table_name = table_info->table_name;
        oid_t table_oid = table_info->table_oid;
        tables_[table_name] = std::move(table_info);
        table_oid_map_[table_oid] = table_name;
        LOG_DEBUG("LoadCatalogFromDisk: Successfully loaded table "
                  << table_name);
    }

    // 加载索引信息
    if (offset + sizeof(uint32_t) <= PAGE_SIZE) {
        uint32_t index_count;
        std::memcpy(&index_count, data + offset, sizeof(uint32_t));
        offset += sizeof(uint32_t);
        LOG_DEBUG("LoadCatalogFromDisk: Loading " << index_count << " indexes");

        for (uint32_t i = 0; i < index_count; ++i) {
            LOG_DEBUG("LoadCatalogFromDisk: Loading index " << i);
            auto index_info = std::make_unique<IndexInfo>();

            if (offset + sizeof(oid_t) + sizeof(uint32_t) > PAGE_SIZE) {
                LOG_ERROR("LoadCatalogFromDisk: Not enough data to read index "
                          << i);
                break;
            }

            // 读取索引OID
            std::memcpy(&index_info->index_oid, data + offset, sizeof(oid_t));
            offset += sizeof(oid_t);

            // 读取索引名称
            uint32_t index_name_len;
            std::memcpy(&index_name_len, data + offset, sizeof(uint32_t));
            offset += sizeof(uint32_t);
            if (index_name_len > 256 || offset + index_name_len > PAGE_SIZE) {
                LOG_ERROR("LoadCatalogFromDisk: Invalid index name length: "
                          << index_name_len);
                break;
            }
            index_info->index_name = std::string(data + offset, index_name_len);
            offset += index_name_len;

            // 读取表名称
            uint32_t table_name_len;
            std::memcpy(&table_name_len, data + offset, sizeof(uint32_t));
            offset += sizeof(uint32_t);
            if (table_name_len > 256 || offset + table_name_len > PAGE_SIZE) {
                LOG_ERROR("LoadCatalogFromDisk: Invalid table name length: "
                          << table_name_len);
                break;
            }
            index_info->table_name = std::string(data + offset, table_name_len);
            offset += table_name_len;

            // 读取键列信息
            uint32_t key_column_count;
            std::memcpy(&key_column_count, data + offset, sizeof(uint32_t));
            offset += sizeof(uint32_t);

            for (uint32_t j = 0; j < key_column_count; ++j) {
                uint32_t column_len;
                std::memcpy(&column_len, data + offset, sizeof(uint32_t));
                offset += sizeof(uint32_t);
                if (column_len > 256 || offset + column_len > PAGE_SIZE) {
                    LOG_ERROR(
                        "LoadCatalogFromDisk: Invalid column name length: "
                        << column_len);
                    break;
                }
                std::string column_name =
                    std::string(data + offset, column_len);
                offset += column_len;
                index_info->key_columns.push_back(column_name);
            }

            // 更新索引映射
            std::string index_name = index_info->index_name;
            oid_t index_oid = index_info->index_oid;
            indexes_[index_name] = std::move(index_info);
            index_oid_map_[index_oid] = index_name;
            LOG_DEBUG("LoadCatalogFromDisk: Successfully loaded index "
                      << index_name);
        }
    } else {
        LOG_DEBUG("LoadCatalogFromDisk: No space left to read indexes");
    }

    buffer_pool_manager_->UnpinPage(0, false);
    LOG_DEBUG(
        "LoadCatalogFromDisk: Catalog load completed successfully, loaded "
        << tables_.size() << " tables and " << indexes_.size() << " indexes");
}

/**
 * 保存catalog元数据到磁盘
 *
 * 实现思路：
 * 1. 获取或创建catalog页面（页面1）
 * 2. 序列化magic number和OID计数器
 * 3. 序列化所有表的元数据（包括schema）
 * 4. 序列化所有索引的元数据
 * 5. 强制刷新到磁盘确保持久化
 * 6. 使用原子标志防止并发保存
 */
void Catalog::SaveCatalogToDisk() {
    std::lock_guard<std::mutex> save_lock(save_mutex_);
    if (save_in_progress_.load()) {
        LOG_DEBUG("SaveCatalogToDisk: Save already in progress, skipping");
        return;
    }
    if (!buffer_pool_manager_) {
        LOG_DEBUG(
            "SaveCatalogToDisk: BufferPoolManager is null, skipping save");
        return;
    }
    save_in_progress_.store(true);

    try {
        LOG_DEBUG("SaveCatalogToDisk: Starting catalog save");
        const page_id_t CATALOG_PAGE_ID = 0;

        // 使用新的GetSpecificPage方法直接获取页面0
        Page* catalog_page =
            buffer_pool_manager_->GetSpecificPage(CATALOG_PAGE_ID);
        if (catalog_page == nullptr) {
            LOG_ERROR("SaveCatalogToDisk: Failed to get catalog page 0");
            save_in_progress_.store(false);
            return;
        }

        if (catalog_page->GetPageId() != CATALOG_PAGE_ID) {
            LOG_ERROR("SaveCatalogToDisk: Page ID mismatch, expected 0 but got "
                      << catalog_page->GetPageId());
            buffer_pool_manager_->UnpinPage(catalog_page->GetPageId(), false);
            save_in_progress_.store(false);
            return;
        }

        char* data = catalog_page->GetData();
        std::memset(data, 0, PAGE_SIZE);
        size_t offset = 0;

        LOG_DEBUG("SaveCatalogToDisk: Writing catalog data");

        // 写入魔数
        uint32_t magic_number = 0x12345678;
        std::memcpy(data + offset, &magic_number, sizeof(uint32_t));
        offset += sizeof(uint32_t);
        LOG_DEBUG("SaveCatalogToDisk: Written magic number 0x"
                  << std::hex << magic_number << std::dec);

        // 写入下一个表OID和索引OID
        std::memcpy(data + offset, &next_table_oid_, sizeof(oid_t));
        offset += sizeof(oid_t);
        std::memcpy(data + offset, &next_index_oid_, sizeof(oid_t));
        offset += sizeof(oid_t);

        // 写入表数量和表信息
        uint32_t table_count = static_cast<uint32_t>(tables_.size());
        std::memcpy(data + offset, &table_count, sizeof(uint32_t));
        offset += sizeof(uint32_t);
        LOG_DEBUG("SaveCatalogToDisk: Writing " << table_count << " tables");

        for (const auto& [table_name, table_info] : tables_) {
            LOG_DEBUG("SaveCatalogToDisk: Writing table " << table_name);

            // 检查空间是否足够
            size_t required_space = sizeof(oid_t) + sizeof(page_id_t) +
                                    sizeof(uint32_t) + table_name.length() +
                                    200;  // 为schema预留空间
            if (offset + required_space > PAGE_SIZE) {
                LOG_ERROR(
                    "SaveCatalogToDisk: Not enough space in catalog page for "
                    "table "
                    << table_name);
                break;
            }

            // 写入表信息
            std::memcpy(data + offset, &table_info->table_oid, sizeof(oid_t));
            offset += sizeof(oid_t);
            std::memcpy(data + offset, &table_info->first_page_id,
                        sizeof(page_id_t));
            offset += sizeof(page_id_t);

            uint32_t name_len = static_cast<uint32_t>(table_name.length());
            std::memcpy(data + offset, &name_len, sizeof(uint32_t));
            offset += sizeof(uint32_t);
            std::memcpy(data + offset, table_name.c_str(), name_len);
            offset += name_len;

            // 序列化schema
            try {
                SerializeSchema(*table_info->schema, data, offset);
                LOG_DEBUG(
                    "SaveCatalogToDisk: Successfully serialized schema for "
                    "table "
                    << table_name);
            } catch (const std::exception& e) {
                LOG_ERROR(
                    "SaveCatalogToDisk: Failed to serialize schema for table "
                    << table_name << ": " << e.what());
                break;
            }
        }

        // 写入索引信息
        uint32_t index_count = static_cast<uint32_t>(indexes_.size());
        if (offset + sizeof(uint32_t) <= PAGE_SIZE) {
            std::memcpy(data + offset, &index_count, sizeof(uint32_t));
            offset += sizeof(uint32_t);
            LOG_DEBUG("SaveCatalogToDisk: Writing " << index_count
                                                    << " indexes");

            for (const auto& [index_name, index_info] : indexes_) {
                LOG_DEBUG("SaveCatalogToDisk: Writing index " << index_name);

                // 检查空间是否足够
                size_t required_space = sizeof(oid_t) + sizeof(uint32_t) +
                                        index_name.length() + sizeof(uint32_t) +
                                        index_info->table_name.length() +
                                        sizeof(uint32_t);
                for (const auto& column : index_info->key_columns) {
                    required_space += sizeof(uint32_t) + column.length();
                }
                if (offset + required_space > PAGE_SIZE) {
                    LOG_ERROR("SaveCatalogToDisk: Not enough space for index "
                              << index_name);
                    break;
                }

                // 写入索引信息
                std::memcpy(data + offset, &index_info->index_oid,
                            sizeof(oid_t));
                offset += sizeof(oid_t);

                uint32_t index_name_len =
                    static_cast<uint32_t>(index_name.length());
                std::memcpy(data + offset, &index_name_len, sizeof(uint32_t));
                offset += sizeof(uint32_t);
                std::memcpy(data + offset, index_name.c_str(), index_name_len);
                offset += index_name_len;

                uint32_t table_name_len =
                    static_cast<uint32_t>(index_info->table_name.length());
                std::memcpy(data + offset, &table_name_len, sizeof(uint32_t));
                offset += sizeof(uint32_t);
                std::memcpy(data + offset, index_info->table_name.c_str(),
                            table_name_len);
                offset += table_name_len;

                uint32_t key_column_count =
                    static_cast<uint32_t>(index_info->key_columns.size());
                std::memcpy(data + offset, &key_column_count, sizeof(uint32_t));
                offset += sizeof(uint32_t);

                for (const auto& column : index_info->key_columns) {
                    uint32_t column_len =
                        static_cast<uint32_t>(column.length());
                    std::memcpy(data + offset, &column_len, sizeof(uint32_t));
                    offset += sizeof(uint32_t);
                    std::memcpy(data + offset, column.c_str(), column_len);
                    offset += column_len;
                }

                LOG_DEBUG("SaveCatalogToDisk: Successfully wrote index "
                          << index_name);
            }
        } else {
            LOG_WARN("SaveCatalogToDisk: No space left for indexes");
            uint32_t zero_count = 0;
            if (offset + sizeof(uint32_t) <= PAGE_SIZE) {
                std::memcpy(data + offset, &zero_count, sizeof(uint32_t));
                offset += sizeof(uint32_t);
            }
        }

        LOG_DEBUG("SaveCatalogToDisk: Final offset: " << offset << " bytes");

        // 标记页面为脏页并解除固定
        catalog_page->SetDirty(true);
        buffer_pool_manager_->UnpinPage(CATALOG_PAGE_ID, true);

        // 强制刷新catalog页面到磁盘
        bool flush_success = buffer_pool_manager_->FlushPage(CATALOG_PAGE_ID);
        LOG_DEBUG("SaveCatalogToDisk: Force flush result: "
                  << (flush_success ? "success" : "failed"));

        LOG_DEBUG("SaveCatalogToDisk: Catalog save completed successfully");
    } catch (const std::exception& e) {
        LOG_ERROR("SaveCatalogToDisk: Exception during save: " << e.what());
    }

    save_in_progress_.store(false);
}

/**
 * 序列化Schema到缓冲区
 * @param schema 要序列化的schema对象
 * @param buffer 目标缓冲区
 * @param offset 当前偏移位置（会被更新）
 *
 * 实现思路：
 * 1. 先写入列的数量
 * 2. 对每个列写入：列名长度+列名+数据类型+大小+约束信息
 * 3. 检查缓冲区空间防止溢出
 */
void Catalog::SerializeSchema(const Schema& schema, char* buffer,
                              size_t& offset) {
    LOG_DEBUG("SerializeSchema: Starting schema serialization at offset "
              << offset);

    // 估算所需空间防止溢出
    size_t estimated_size = sizeof(uint32_t);  // column_count
    for (size_t i = 0; i < schema.GetColumnCount(); ++i) {
        const Column& column = schema.GetColumn(i);
        estimated_size += sizeof(uint32_t) + column.name.length();  // name
        estimated_size += sizeof(TypeId) + sizeof(size_t) + sizeof(bool) +
                          sizeof(bool);  // other fields
    }

    if (offset + estimated_size > PAGE_SIZE) {
        throw Exception("Not enough space to serialize schema");
    }

    // 写入列数量
    uint32_t column_count = static_cast<uint32_t>(schema.GetColumnCount());
    std::memcpy(buffer + offset, &column_count, sizeof(uint32_t));
    offset += sizeof(uint32_t);

    LOG_DEBUG("SerializeSchema: Serializing " << column_count << " columns");

    // 序列化每个列的信息
    for (size_t i = 0; i < schema.GetColumnCount(); ++i) {
        const Column& column = schema.GetColumn(i);

        LOG_DEBUG("SerializeSchema: Serializing column " << i << ": "
                                                         << column.name);

        // 检查剩余空间
        size_t required_space = sizeof(uint32_t) + column.name.length() +
                                sizeof(TypeId) + sizeof(size_t) + sizeof(bool) +
                                sizeof(bool);
        if (offset + required_space > PAGE_SIZE) {
            throw Exception("Not enough space to serialize column " +
                            column.name);
        }

        // 写入列名长度和内容
        uint32_t name_len = static_cast<uint32_t>(column.name.length());
        std::memcpy(buffer + offset, &name_len, sizeof(uint32_t));
        offset += sizeof(uint32_t);
        std::memcpy(buffer + offset, column.name.c_str(), name_len);
        offset += name_len;

        // 写入列的属性信息
        std::memcpy(buffer + offset, &column.type, sizeof(TypeId));
        offset += sizeof(TypeId);
        std::memcpy(buffer + offset, &column.size, sizeof(size_t));
        offset += sizeof(size_t);
        std::memcpy(buffer + offset, &column.nullable, sizeof(bool));
        offset += sizeof(bool);
        std::memcpy(buffer + offset, &column.is_primary_key, sizeof(bool));
        offset += sizeof(bool);
    }

    LOG_DEBUG("SerializeSchema: Schema serialization completed, final offset: "
              << offset);
}

/**
 * 从缓冲区反序列化Schema
 * @param buffer 源缓冲区
 * @param offset 当前偏移位置（会被更新）
 * @return 反序列化得到的Schema对象
 *
 * 实现思路：
 * 1. 读取列的数量
 * 2. 对每个列读取：列名长度+列名+数据类型+大小+约束信息
 * 3. 构造Column对象并添加到列表中
 * 4. 创建Schema对象返回
 */
std::unique_ptr<Schema> Catalog::DeserializeSchema(const char* buffer,
                                                   size_t& offset) {
    // 读取列数量
    uint32_t column_count;
    std::memcpy(&column_count, buffer + offset, sizeof(uint32_t));
    offset += sizeof(uint32_t);

    std::vector<Column> columns;
    columns.reserve(column_count);

    // 反序列化每个列的信息
    for (uint32_t i = 0; i < column_count; ++i) {
        Column column;

        // 读取列名长度和内容
        uint32_t name_len;
        std::memcpy(&name_len, buffer + offset, sizeof(uint32_t));
        offset += sizeof(uint32_t);

        column.name = std::string(buffer + offset, name_len);
        offset += name_len;

        // 读取列的属性信息
        std::memcpy(&column.type, buffer + offset, sizeof(TypeId));
        offset += sizeof(TypeId);
        std::memcpy(&column.size, buffer + offset, sizeof(size_t));
        offset += sizeof(size_t);
        std::memcpy(&column.nullable, buffer + offset, sizeof(bool));
        offset += sizeof(bool);
        std::memcpy(&column.is_primary_key, buffer + offset, sizeof(bool));
        offset += sizeof(bool);

        columns.push_back(column);
    }

    return std::make_unique<Schema>(columns);
}

/**
 * 调试用：打印所有表的信息
 */
void Catalog::DebugPrintTables() const {
    LOG_DEBUG("=== Catalog Debug: All Tables ===");
    LOG_DEBUG("Total tables: " << tables_.size());
    for (const auto& [table_name, table_info] : tables_) {
        LOG_DEBUG("Table: " << table_name << ", OID: " << table_info->table_oid
                            << ", FirstPage: " << table_info->first_page_id
                            << ", Columns: "
                            << table_info->schema->GetColumnCount());
    }
    LOG_DEBUG("=== End Catalog Debug ===");
}

/**
 * 获取所有表名的列表
 * @return 排序后的表名列表
 */
std::vector<std::string> Catalog::GetAllTableNames() const {
    std::vector<std::string> table_names;
    table_names.reserve(tables_.size());

    // 收集所有表名
    for (const auto& [name, info] : tables_) {
        table_names.push_back(name);
    }

    // 对表名进行排序，使输出更有序
    std::sort(table_names.begin(), table_names.end());

    return table_names;
}

}  // namespace SimpleRDBMS