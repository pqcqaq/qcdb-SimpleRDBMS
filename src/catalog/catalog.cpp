// src/catalog/catalog.cpp

#include "catalog/catalog.h"

#include <cstring>

#include "buffer/buffer_pool_manager.h"
#include "catalog/schema.h"
#include "common/debug.h"
#include "common/exception.h"
#include "record/table_heap.h"

namespace SimpleRDBMS {

Catalog::Catalog(BufferPoolManager* buffer_pool_manager)
    : buffer_pool_manager_(buffer_pool_manager),
      next_table_oid_(1),
      next_index_oid_(1),
      save_in_progress_(false) {  // 初始化原子变量

    LOG_DEBUG("Catalog: Initializing catalog");

    // 从磁盘加载catalog信息
    try {
        LoadCatalogFromDisk();
        LOG_DEBUG("Catalog: Successfully loaded catalog from disk");
    } catch (const std::exception& e) {
        LOG_WARN("Catalog: Failed to load catalog from disk: " << e.what());
        LOG_DEBUG("Catalog: Starting with empty catalog");
    }

    LOG_DEBUG("Catalog: Catalog initialization completed");
}

bool Catalog::CreateTable(const std::string& table_name, const Schema& schema) {
    LOG_DEBUG("CreateTable: Starting to create table " << table_name);
    // Check if table already exists
    if (tables_.find(table_name) != tables_.end()) {
        LOG_WARN("CreateTable: Table " << table_name << " already exists");
        return false;
    }

    LOG_DEBUG("CreateTable: Creating table info for " << table_name);
    // Create table info
    auto table_info = std::make_unique<TableInfo>();
    table_info->table_name = table_name;
    table_info->schema = std::make_unique<Schema>(schema);
    table_info->table_oid = next_table_oid_++;

    LOG_DEBUG("CreateTable: Allocating first page for table " << table_name);
    // 创建TableHeap时先分配first_page_id
    page_id_t first_page_id;
    Page* first_page = buffer_pool_manager_->NewPage(&first_page_id);
    if (first_page == nullptr) {
        LOG_ERROR("CreateTable: Failed to allocate first page for table "
                  << table_name);
        return false;
    }
    LOG_DEBUG("CreateTable: Allocated first page "
              << first_page_id << " for table " << table_name);

    // 重要：需要先初始化页面！
    first_page->WLatch();
    auto* table_page = reinterpret_cast<TablePage*>(first_page);
    table_page->Init(first_page_id, INVALID_PAGE_ID);
    first_page->WUnlatch();

    table_info->first_page_id = first_page_id;

    LOG_DEBUG("CreateTable: Creating TableHeap for table " << table_name);
    // 使用已分配并初始化的page_id创建TableHeap
    table_info->table_heap = std::make_unique<TableHeap>(
        buffer_pool_manager_, table_info->schema.get(), first_page_id);
    
    buffer_pool_manager_->UnpinPage(first_page_id, true);

    LOG_DEBUG("CreateTable: Adding table to catalog maps");

    // Store table info
    oid_t table_oid = table_info->table_oid;
    tables_[table_name] = std::move(table_info);
    table_oid_map_[table_oid] = table_name;

    LOG_DEBUG("CreateTable: Saving catalog to disk");

    // 保存到磁盘
    try {
        SaveCatalogToDisk();
        LOG_DEBUG("CreateTable: Successfully saved catalog to disk");
    } catch (const std::exception& e) {
        LOG_ERROR("CreateTable: Failed to save catalog to disk: " << e.what());
        // 回滚操作
        tables_.erase(table_name);
        table_oid_map_.erase(table_oid);
        return false;
    }

    LOG_DEBUG("CreateTable: Table " << table_name << " created successfully");
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

    // 保存到磁盘
    SaveCatalogToDisk();
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

    LOG_DEBUG("CreateIndex: Index " << index_name << " created in memory");

    // 延迟保存，让上层调用者决定何时保存
    // SaveCatalogToDisk();  // 注释掉自动保存

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

    // 保存到磁盘
    SaveCatalogToDisk();
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

void Catalog::LoadCatalogFromDisk() {
    LOG_DEBUG("LoadCatalogFromDisk: Starting catalog load");

    // 首先检查磁盘是否有页面
    if (buffer_pool_manager_ == nullptr) {
        LOG_ERROR("LoadCatalogFromDisk: BufferPoolManager is null");
        return;
    }

    LOG_DEBUG("LoadCatalogFromDisk: Attempting to fetch catalog page 1");

    // 尝试从页面1加载catalog信息
    Page* catalog_page = nullptr;
    try {
        catalog_page = buffer_pool_manager_->FetchPage(1);
    } catch (const std::exception& e) {
        LOG_WARN("LoadCatalogFromDisk: Exception when fetching page 1: "
                 << e.what());
        return;
    }

    if (catalog_page == nullptr) {
        LOG_DEBUG(
            "LoadCatalogFromDisk: Page 1 does not exist, assuming new "
            "database");
        return;
    }

    LOG_DEBUG("LoadCatalogFromDisk: Successfully fetched catalog page 1");

    const char* data = catalog_page->GetData();
    size_t offset = 0;

    // 先打印前几个字节进行调试
    LOG_DEBUG("LoadCatalogFromDisk: First 16 bytes: "
              << std::hex << static_cast<unsigned>(data[0]) << " "
              << static_cast<unsigned>(data[1]) << " "
              << static_cast<unsigned>(data[2]) << " "
              << static_cast<unsigned>(data[3]) << " "
              << static_cast<unsigned>(data[4]) << " "
              << static_cast<unsigned>(data[5]) << " "
              << static_cast<unsigned>(data[6]) << " "
              << static_cast<unsigned>(data[7]) << " "
              << static_cast<unsigned>(data[8]) << " "
              << static_cast<unsigned>(data[9]) << " "
              << static_cast<unsigned>(data[10]) << " "
              << static_cast<unsigned>(data[11]) << " "
              << static_cast<unsigned>(data[12]) << " "
              << static_cast<unsigned>(data[13]) << " "
              << static_cast<unsigned>(data[14]) << " "
              << static_cast<unsigned>(data[15]) << std::dec);

    // 读取magic number检查
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
        buffer_pool_manager_->UnpinPage(1, false);
        return;
    }

    LOG_DEBUG(
        "LoadCatalogFromDisk: Valid catalog page found, loading metadata");

    // 读取next_table_oid和next_index_oid
    std::memcpy(&next_table_oid_, data + offset, sizeof(oid_t));
    offset += sizeof(oid_t);
    std::memcpy(&next_index_oid_, data + offset, sizeof(oid_t));
    offset += sizeof(oid_t);

    LOG_DEBUG("LoadCatalogFromDisk: Next table OID: "
              << next_table_oid_ << ", Next index OID: " << next_index_oid_);

    // 读取表的数量
    uint32_t table_count;
    std::memcpy(&table_count, data + offset, sizeof(uint32_t));
    offset += sizeof(uint32_t);

    LOG_DEBUG("LoadCatalogFromDisk: Loading " << table_count << " tables");

    // 读取每个表的信息
    for (uint32_t i = 0; i < table_count; ++i) {
        LOG_DEBUG("LoadCatalogFromDisk: Loading table " << i);

        auto table_info = std::make_unique<TableInfo>();

        // 检查剩余空间
        if (offset + sizeof(oid_t) + sizeof(page_id_t) + sizeof(uint32_t) >
            PAGE_SIZE) {
            LOG_ERROR("LoadCatalogFromDisk: Not enough data to read table "
                      << i);
            break;
        }

        // 读取table_oid
        std::memcpy(&table_info->table_oid, data + offset, sizeof(oid_t));
        offset += sizeof(oid_t);

        // 读取first_page_id
        std::memcpy(&table_info->first_page_id, data + offset,
                    sizeof(page_id_t));
        offset += sizeof(page_id_t);

        // 读取table_name长度和内容
        uint32_t name_len;
        std::memcpy(&name_len, data + offset, sizeof(uint32_t));
        offset += sizeof(uint32_t);

        if (name_len > 256 || offset + name_len > PAGE_SIZE) {  // 合理性检查
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

        // 读取schema
        try {
            table_info->schema = DeserializeSchema(data, offset);
        } catch (const std::exception& e) {
            LOG_ERROR(
                "LoadCatalogFromDisk: Failed to deserialize schema for table "
                << table_info->table_name << ": " << e.what());
            break;
        }

        // 使用已存在的first_page_id重建TableHeap
        try {
            table_info->table_heap = std::make_unique<TableHeap>(
                buffer_pool_manager_, table_info->schema.get(),
                table_info->first_page_id);
        } catch (const std::exception& e) {
            LOG_ERROR(
                "LoadCatalogFromDisk: Failed to create TableHeap for table "
                << table_info->table_name << ": " << e.what());
            break;
        }

        // 添加到内存结构
        std::string table_name = table_info->table_name;
        oid_t table_oid = table_info->table_oid;
        tables_[table_name] = std::move(table_info);
        table_oid_map_[table_oid] = table_name;

        LOG_DEBUG("LoadCatalogFromDisk: Successfully loaded table "
                  << table_name);
    }

    buffer_pool_manager_->UnpinPage(1, false);

    LOG_DEBUG(
        "LoadCatalogFromDisk: Catalog load completed successfully, loaded "
        << tables_.size() << " tables");
}

void Catalog::SaveCatalogToDisk() {
    // 防止并发保存
    std::lock_guard<std::mutex> save_lock(save_mutex_);
    // 如果正在保存，直接返回
    if (save_in_progress_.load()) {
        LOG_DEBUG("SaveCatalogToDisk: Save already in progress, skipping");
        return;
    }
    save_in_progress_.store(true);

    try {
        LOG_DEBUG("SaveCatalogToDisk: Starting catalog save");

        // 获取或创建页面1作为catalog页面
        Page* catalog_page = buffer_pool_manager_->FetchPage(1);
        if (catalog_page == nullptr) {
            LOG_DEBUG(
                "SaveCatalogToDisk: Page 1 does not exist, creating new page");
            page_id_t page_id;
            catalog_page = buffer_pool_manager_->NewPage(&page_id);
            if (catalog_page == nullptr) {
                LOG_ERROR("SaveCatalogToDisk: Failed to create new page");
                save_in_progress_.store(false);
                return;
            }
            if (page_id != 1) {
                LOG_WARN("SaveCatalogToDisk: Expected page 1 but got page "
                         << page_id);
            }
            LOG_DEBUG("SaveCatalogToDisk: Created new catalog page with ID: "
                      << page_id);
        } else {
            LOG_DEBUG("SaveCatalogToDisk: Found existing catalog page 1");
        }

        // 确保页面被pin住
        catalog_page->IncreasePinCount();

        char* data = catalog_page->GetData();
        // 先清零整个页面，确保干净的开始
        std::memset(data, 0, PAGE_SIZE);
        size_t offset = 0;

        LOG_DEBUG("SaveCatalogToDisk: Writing catalog data");

        // 写入magic number
        uint32_t magic_number = 0x12345678;
        std::memcpy(data + offset, &magic_number, sizeof(uint32_t));
        offset += sizeof(uint32_t);
        LOG_DEBUG("SaveCatalogToDisk: Written magic number 0x"
                  << std::hex << magic_number << std::dec);

        // 写入next_table_oid和next_index_oid
        std::memcpy(data + offset, &next_table_oid_, sizeof(oid_t));
        offset += sizeof(oid_t);
        std::memcpy(data + offset, &next_index_oid_, sizeof(oid_t));
        offset += sizeof(oid_t);

        // 写入表的数量
        uint32_t table_count = static_cast<uint32_t>(tables_.size());
        std::memcpy(data + offset, &table_count, sizeof(uint32_t));
        offset += sizeof(uint32_t);
        LOG_DEBUG("SaveCatalogToDisk: Writing " << table_count << " tables");

        // 写入每个表的信息
        for (const auto& [table_name, table_info] : tables_) {
            LOG_DEBUG("SaveCatalogToDisk: Writing table " << table_name);

            // 检查缓冲区空间
            size_t required_space = sizeof(oid_t) + sizeof(page_id_t) +
                                    sizeof(uint32_t) + table_name.length() +
                                    200;  // 额外的schema空间
            if (offset + required_space > PAGE_SIZE) {
                LOG_ERROR(
                    "SaveCatalogToDisk: Not enough space in catalog page for "
                    "table "
                    << table_name);
                break;
            }

            // 写入table_oid
            std::memcpy(data + offset, &table_info->table_oid, sizeof(oid_t));
            offset += sizeof(oid_t);

            // 写入first_page_id
            std::memcpy(data + offset, &table_info->first_page_id,
                        sizeof(page_id_t));
            offset += sizeof(page_id_t);

            // 写入table_name长度和内容
            uint32_t name_len = static_cast<uint32_t>(table_name.length());
            std::memcpy(data + offset, &name_len, sizeof(uint32_t));
            offset += sizeof(uint32_t);
            std::memcpy(data + offset, table_name.c_str(), name_len);
            offset += name_len;

            // 写入schema
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

        LOG_DEBUG("SaveCatalogToDisk: Final offset: " << offset << " bytes");

        // 确保页面被标记为dirty
        catalog_page->SetDirty(true);
        page_id_t catalog_page_id = catalog_page->GetPageId();

        // 多次unpin以确保正确释放
        while (catalog_page->GetPinCount() > 0) {
            buffer_pool_manager_->UnpinPage(catalog_page_id, true);
        }

        // 立即强制刷新到磁盘
        bool flush_success = buffer_pool_manager_->FlushPage(catalog_page_id);
        LOG_DEBUG("SaveCatalogToDisk: Force flush result: "
                  << (flush_success ? "success" : "failed"));

        // 额外确保：再次强制刷新所有页面
        buffer_pool_manager_->FlushAllPages();

        LOG_DEBUG("SaveCatalogToDisk: Catalog save completed successfully");

    } catch (const std::exception& e) {
        LOG_ERROR("SaveCatalogToDisk: Exception during save: " << e.what());
    }

    save_in_progress_.store(false);
}

void Catalog::SerializeSchema(const Schema& schema, char* buffer,
                              size_t& offset) {
    LOG_DEBUG("SerializeSchema: Starting schema serialization at offset "
              << offset);

    // 检查空间是否足够
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

    // 写入每个列的信息
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

        // 写入列属性
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

std::unique_ptr<Schema> Catalog::DeserializeSchema(const char* buffer,
                                                   size_t& offset) {
    // 读取列数量
    uint32_t column_count;
    std::memcpy(&column_count, buffer + offset, sizeof(uint32_t));
    offset += sizeof(uint32_t);

    std::vector<Column> columns;
    columns.reserve(column_count);

    // 读取每个列的信息
    for (uint32_t i = 0; i < column_count; ++i) {
        Column column;

        // 读取列名长度和内容
        uint32_t name_len;
        std::memcpy(&name_len, buffer + offset, sizeof(uint32_t));
        offset += sizeof(uint32_t);

        column.name = std::string(buffer + offset, name_len);
        offset += name_len;

        // 读取列属性
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

}  // namespace SimpleRDBMS