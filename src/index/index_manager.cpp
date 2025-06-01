/*
 * 文件: index_manager.cpp
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述:
 * 索引管理器实现，负责B+树索引的创建、删除和操作管理，支持多种数据类型的索引
 */

#include "index/index_manager.h"

#include "buffer/buffer_pool_manager.h"
#include "catalog/catalog.h"
#include "catalog/catalog.h"  // 为了 Catalog, TableInfo
#include "catalog/schema.h"
#include "catalog/schema.h"  // 为了 Schema
#include "catalog/table_manager.h"
#include "common/config.h"
#include "common/debug.h"  // 为了 LOG_* 宏
#include "common/exception.h"
#include "common/types.h"
#include "index/b_plus_tree.h"
#include "index/index_manager.h"  // 为了 IndexManager
#include "parser/ast.h"           // 为了 CreateTableStatement
#include "record/table_heap.h"    // 为了 TableHeap

namespace SimpleRDBMS {

/**
 * 索引键类型枚举
 * 用来标识B+树索引支持的不同数据类型
 */
enum class IndexKeyType {
    INVALID = 0,  // 无效类型
    INT32,        // 32位整数
    INT64,        // 64位整数
    FLOAT,        // 单精度浮点数
    DOUBLE,       // 双精度浮点数
    STRING        // 字符串类型
};

/**
 * 索引元数据结构
 * 保存每个索引的基本信息和B+树实例
 */
struct IndexMetadata {
    IndexKeyType key_type;                 // 索引键的数据类型
    std::string index_name;                // 索引名称
    std::string table_name;                // 对应的表名
    std::vector<std::string> key_columns;  // 索引列名（目前只支持单列）
    std::unique_ptr<void, std::function<void(void*)>>
        index_instance;  // B+树实例指针

    IndexMetadata(IndexKeyType type, const std::string& idx_name,
                  const std::string& tbl_name,
                  const std::vector<std::string>& columns)
        : key_type(type),
          index_name(idx_name),
          table_name(tbl_name),
          key_columns(columns),
          index_instance(nullptr, [](void*) {}) {}
};

/**
 * IndexManager的内部实现类
 * 使用Pimpl模式隐藏实现细节，提供线程安全的索引管理
 */
class IndexManagerImpl {
   public:
    explicit IndexManagerImpl(BufferPoolManager* buffer_pool_manager,
                              Catalog* catalog)
        : buffer_pool_manager_(buffer_pool_manager), catalog_(catalog) {
        LOG_DEBUG("IndexManager: Initializing IndexManager");
    }

    ~IndexManagerImpl() {
        LOG_DEBUG("IndexManager: Destroying IndexManager with "
                  << indexes_.size() << " indexes");

        // 安全地清理所有索引，防止析构时出现异常
        try {
            std::lock_guard<std::mutex> lock(latch_);
            indexes_.clear();  // 会自动调用每个索引的自定义删除器
        } catch (const std::exception& e) {
            LOG_ERROR(
                "IndexManagerImpl::~IndexManagerImpl: Exception during "
                "cleanup: "
                << e.what());
        } catch (...) {
            LOG_ERROR(
                "IndexManagerImpl::~IndexManagerImpl: Unknown exception during "
                "cleanup");
        }

        LOG_DEBUG("IndexManager: IndexManager destroyed successfully");
    }

    /**
     * 根据表列的数据类型确定对应的索引键类型
     * @param column 表列信息，包含数据类型
     * @return 对应的IndexKeyType枚举值
     */
    IndexKeyType DetermineKeyType(const Column& column) {
        switch (column.type) {
            case TypeId::INTEGER:
                return IndexKeyType::INT32;
            case TypeId::BIGINT:
                return IndexKeyType::INT64;
            case TypeId::FLOAT:
                return IndexKeyType::FLOAT;
            case TypeId::DOUBLE:
                return IndexKeyType::DOUBLE;
            case TypeId::VARCHAR:
                return IndexKeyType::STRING;
            default:
                LOG_ERROR("IndexManager: Unsupported column type for index: "
                          << static_cast<int>(column.type));
                return IndexKeyType::INVALID;
        }
    }

    /**
     * 创建新的B+树索引
     * 目前只支持单列索引，多列索引需要后续扩展
     * @param index_name 索引名称
     * @param table_name 表名
     * @param key_columns 索引列名列表
     * @param table_schema 表的schema信息
     * @return 成功返回true，失败返回false
     */
    bool CreateIndex(const std::string& index_name,
                     const std::string& table_name,
                     const std::vector<std::string>& key_columns,
                     const Schema* table_schema) {
        std::lock_guard<std::mutex> lock(latch_);
        LOG_DEBUG("IndexManager: Creating index "
                  << index_name << " on table " << table_name
                  << " (currently have " << indexes_.size() << " indexes)");

        // 检查索引是否已存在
        if (indexes_.find(index_name) != indexes_.end()) {
            LOG_WARN("IndexManager: Index " << index_name << " already exists");
            return false;
        }

        // 目前只支持单列索引
        if (key_columns.size() != 1) {
            LOG_ERROR("IndexManager: Multi-column indexes not supported yet");
            return false;
        }

        if (table_schema == nullptr) {
            LOG_ERROR("IndexManager: Table "
                      << table_name << " does not exist or schema is null");
            return false;
        }

        // 验证表是否存在于catalog中
        if (catalog_ != nullptr) {
            TableInfo* table_info = catalog_->GetTable(table_name);
            if (table_info == nullptr) {
                LOG_ERROR("IndexManager: Table "
                          << table_name << " does not exist in catalog");
                return false;
            }
            // 确保传入的schema和catalog中的一致
            if (table_info->schema.get() != table_schema) {
                LOG_ERROR("IndexManager: Schema mismatch for table "
                          << table_name);
                return false;
            }
        }

        // 验证索引列是否存在
        const std::string& column_name = key_columns[0];
        if (!table_schema->HasColumn(column_name)) {
            LOG_ERROR("IndexManager: Column "
                      << column_name << " not found in table " << table_name);
            return false;
        }

        // 获取列信息并确定索引键类型
        const Column& column = table_schema->GetColumn(column_name);
        IndexKeyType key_type = DetermineKeyType(column);
        if (key_type == IndexKeyType::INVALID) {
            LOG_ERROR(
                "IndexManager: Cannot create index on column with unsupported "
                "type");
            return false;
        }

        // 创建索引元数据
        auto metadata = std::make_unique<IndexMetadata>(
            key_type, index_name, table_name, key_columns);

        bool success = false;

        // 根据数据类型创建对应的B+树实例
        // 使用模板特化为不同类型创建专用的B+树
        switch (key_type) {
            case IndexKeyType::INT32: {
                auto tree = std::make_unique<BPlusTree<int32_t, RID>>(
                    index_name, buffer_pool_manager_);
                // 使用自定义删除器保存B+树实例
                metadata->index_instance =
                    std::unique_ptr<void, std::function<void(void*)>>(
                        tree.release(), [](void* ptr) {
                            delete static_cast<BPlusTree<int32_t, RID>*>(ptr);
                        });
                success = true;
                break;
            }
            case IndexKeyType::INT64: {
                auto tree = std::make_unique<BPlusTree<int64_t, RID>>(
                    index_name, buffer_pool_manager_);
                metadata->index_instance =
                    std::unique_ptr<void, std::function<void(void*)>>(
                        tree.release(), [](void* ptr) {
                            delete static_cast<BPlusTree<int64_t, RID>*>(ptr);
                        });
                success = true;
                break;
            }
            case IndexKeyType::FLOAT: {
                auto tree = std::make_unique<BPlusTree<float, RID>>(
                    index_name, buffer_pool_manager_);
                metadata->index_instance =
                    std::unique_ptr<void, std::function<void(void*)>>(
                        tree.release(), [](void* ptr) {
                            delete static_cast<BPlusTree<float, RID>*>(ptr);
                        });
                success = true;
                break;
            }
            case IndexKeyType::DOUBLE: {
                auto tree = std::make_unique<BPlusTree<double, RID>>(
                    index_name, buffer_pool_manager_);
                metadata->index_instance =
                    std::unique_ptr<void, std::function<void(void*)>>(
                        tree.release(), [](void* ptr) {
                            delete static_cast<BPlusTree<double, RID>*>(ptr);
                        });
                success = true;
                break;
            }
            case IndexKeyType::STRING: {
                auto tree = std::make_unique<BPlusTree<std::string, RID>>(
                    index_name, buffer_pool_manager_);
                metadata->index_instance =
                    std::unique_ptr<void, std::function<void(void*)>>(
                        tree.release(), [](void* ptr) {
                            delete static_cast<BPlusTree<std::string, RID>*>(
                                ptr);
                        });
                success = true;
                break;
            }
            default:
                LOG_ERROR(
                    "IndexManager: Unsupported key type for index creation");
                success = false;
                break;
        }

        if (success) {
            // 将创建的索引添加到管理器中
            indexes_[index_name] = std::move(metadata);
            LOG_INFO("IndexManager: Successfully created index "
                     << index_name << " (now have " << indexes_.size()
                     << " indexes)");

            // Debug: 列出当前所有索引
            LOG_DEBUG("IndexManager: Current indexes after creation:");
            for (const auto& [name, meta] : indexes_) {
                LOG_DEBUG("  - " << name << " on table " << meta->table_name);
            }
            return true;
        } else {
            LOG_ERROR("IndexManager: Failed to create index " << index_name);
            return false;
        }
    }

    /**
     * 删除指定的索引
     * @param index_name 要删除的索引名称
     * @return 成功返回true，失败返回false
     */
    bool DropIndex(const std::string& index_name) {
        std::lock_guard<std::mutex> lock(latch_);
        LOG_DEBUG("IndexManager: Dropping index "
                  << index_name << " (currently have " << indexes_.size()
                  << " indexes)");

        auto it = indexes_.find(index_name);
        if (it == indexes_.end()) {
            LOG_WARN("IndexManager: Index " << index_name << " not found");
            return false;
        }

        // 直接从map中删除，会自动调用析构函数清理B+树
        indexes_.erase(it);
        LOG_INFO("IndexManager: Successfully dropped index "
                 << index_name << " (now have " << indexes_.size()
                 << " indexes)");

        // Debug: 列出剩余的索引
        LOG_DEBUG("IndexManager: Remaining indexes after drop:");
        for (const auto& [name, meta] : indexes_) {
            LOG_DEBUG("  - " << name << " on table " << meta->table_name);
        }
        return true;
    }

    /**
     * 获取指定类型的B+树索引实例
     * 使用模板提供类型安全的访问
     * @param index_name 索引名称
     * @return 对应类型的B+树指针，不存在返回nullptr
     */
    template <typename KeyType>
    BPlusTree<KeyType, RID>* GetIndex(const std::string& index_name) {
        std::lock_guard<std::mutex> lock(latch_);

        auto it = indexes_.find(index_name);
        if (it == indexes_.end()) {
            LOG_WARN("IndexManager: Index " << index_name << " not found");
            return nullptr;
        }

        // 强制类型转换到具体的B+树类型
        return static_cast<BPlusTree<KeyType, RID>*>(
            it->second->index_instance.get());
    }

    /**
     * 获取索引的元数据信息
     * @param index_name 索引名称
     * @return 索引元数据指针，不存在返回nullptr
     */
    IndexMetadata* GetIndexMetadata(const std::string& index_name) {
        std::lock_guard<std::mutex> lock(latch_);

        auto it = indexes_.find(index_name);
        if (it == indexes_.end()) {
            return nullptr;
        }
        return it->second.get();
    }

    /**
     * 向索引中插入键值对
     * 根据索引的数据类型自动选择合适的B+树进行插入
     * @param index_name 索引名称
     * @param key 要插入的键值
     * @param rid 对应的记录ID
     * @return 成功返回true，失败返回false
     */
    bool InsertEntry(const std::string& index_name, const Value& key,
                     const RID& rid) {
        auto metadata = GetIndexMetadata(index_name);
        if (!metadata) {
            LOG_ERROR("IndexManager: Index " << index_name
                                             << " not found for insertion");
            return false;
        }

        // 根据索引类型调用对应的B+树插入方法
        switch (metadata->key_type) {
            case IndexKeyType::INT32: {
                auto* tree = GetIndex<int32_t>(index_name);
                if (tree && std::holds_alternative<int32_t>(key)) {
                    return tree->Insert(std::get<int32_t>(key), rid);
                }
                break;
            }
            case IndexKeyType::INT64: {
                auto* tree = GetIndex<int64_t>(index_name);
                if (tree && std::holds_alternative<int64_t>(key)) {
                    return tree->Insert(std::get<int64_t>(key), rid);
                }
                break;
            }
            case IndexKeyType::FLOAT: {
                auto* tree = GetIndex<float>(index_name);
                if (tree && std::holds_alternative<float>(key)) {
                    return tree->Insert(std::get<float>(key), rid);
                }
                break;
            }
            case IndexKeyType::DOUBLE: {
                auto* tree = GetIndex<double>(index_name);
                if (tree && std::holds_alternative<double>(key)) {
                    return tree->Insert(std::get<double>(key), rid);
                }
                break;
            }
            case IndexKeyType::STRING: {
                auto* tree = GetIndex<std::string>(index_name);
                if (tree && std::holds_alternative<std::string>(key)) {
                    return tree->Insert(std::get<std::string>(key), rid);
                }
                break;
            }
            default:
                LOG_ERROR("IndexManager: Unsupported key type for insertion");
                return false;
        }

        LOG_ERROR("IndexManager: Type mismatch or invalid tree for index "
                  << index_name);
        return false;
    }

    /**
     * 从索引中删除指定键值
     * @param index_name 索引名称
     * @param key 要删除的键值
     * @return 成功返回true，失败返回false
     */
    bool DeleteEntry(const std::string& index_name, const Value& key) {
        auto metadata = GetIndexMetadata(index_name);
        if (!metadata) {
            LOG_ERROR("IndexManager: Index " << index_name
                                             << " not found for deletion");
            return false;
        }

        // 根据索引类型调用对应的B+树删除方法
        switch (metadata->key_type) {
            case IndexKeyType::INT32: {
                auto* tree = GetIndex<int32_t>(index_name);
                if (tree && std::holds_alternative<int32_t>(key)) {
                    return tree->Remove(std::get<int32_t>(key));
                }
                break;
            }
            case IndexKeyType::INT64: {
                auto* tree = GetIndex<int64_t>(index_name);
                if (tree && std::holds_alternative<int64_t>(key)) {
                    return tree->Remove(std::get<int64_t>(key));
                }
                break;
            }
            case IndexKeyType::FLOAT: {
                auto* tree = GetIndex<float>(index_name);
                if (tree && std::holds_alternative<float>(key)) {
                    return tree->Remove(std::get<float>(key));
                }
                break;
            }
            case IndexKeyType::DOUBLE: {
                auto* tree = GetIndex<double>(index_name);
                if (tree && std::holds_alternative<double>(key)) {
                    return tree->Remove(std::get<double>(key));
                }
                break;
            }
            case IndexKeyType::STRING: {
                auto* tree = GetIndex<std::string>(index_name);
                if (tree && std::holds_alternative<std::string>(key)) {
                    return tree->Remove(std::get<std::string>(key));
                }
                break;
            }
            default:
                LOG_ERROR("IndexManager: Unsupported key type for deletion");
                return false;
        }

        LOG_ERROR("IndexManager: Type mismatch or invalid tree for index "
                  << index_name);
        return false;
    }

    /**
     * 在索引中查找指定键值对应的记录ID
     * 这是索引最核心的功能，用于加速查询
     * @param index_name 索引名称
     * @param key 要查找的键值
     * @param rid 输出参数，找到的记录ID
     * @return 找到返回true，未找到返回false
     */
    bool FindEntry(const std::string& index_name, const Value& key, RID* rid) {
        auto metadata = GetIndexMetadata(index_name);
        if (!metadata) {
            LOG_ERROR("IndexManager: Index " << index_name
                                             << " not found for search");
            return false;
        }

        LOG_DEBUG("IndexManager::FindEntry: Searching in index "
                  << index_name << " for key type "
                  << static_cast<int>(metadata->key_type));

        // 根据索引类型调用对应的B+树查找方法
        switch (metadata->key_type) {
            case IndexKeyType::INT32: {
                auto* tree = GetIndex<int32_t>(index_name);
                if (tree && std::holds_alternative<int32_t>(key)) {
                    bool found = tree->GetValue(std::get<int32_t>(key), rid);
                    LOG_DEBUG("IndexManager::FindEntry: INT32 search result = "
                              << found);
                    return found;
                }
                LOG_DEBUG(
                    "IndexManager::FindEntry: INT32 type mismatch or invalid "
                    "tree");
                break;
            }
            case IndexKeyType::INT64: {
                auto* tree = GetIndex<int64_t>(index_name);
                if (tree && std::holds_alternative<int64_t>(key)) {
                    bool found = tree->GetValue(std::get<int64_t>(key), rid);
                    LOG_DEBUG("IndexManager::FindEntry: INT64 search result = "
                              << found);
                    return found;
                }
                LOG_DEBUG(
                    "IndexManager::FindEntry: INT64 type mismatch or invalid "
                    "tree");
                break;
            }
            case IndexKeyType::FLOAT: {
                auto* tree = GetIndex<float>(index_name);
                if (tree && std::holds_alternative<float>(key)) {
                    bool found = tree->GetValue(std::get<float>(key), rid);
                    LOG_DEBUG("IndexManager::FindEntry: FLOAT search result = "
                              << found);
                    return found;
                }
                LOG_DEBUG(
                    "IndexManager::FindEntry: FLOAT type mismatch or invalid "
                    "tree");
                break;
            }
            case IndexKeyType::DOUBLE: {
                auto* tree = GetIndex<double>(index_name);
                if (tree && std::holds_alternative<double>(key)) {
                    bool found = tree->GetValue(std::get<double>(key), rid);
                    LOG_DEBUG("IndexManager::FindEntry: DOUBLE search result = "
                              << found);
                    return found;
                }
                LOG_DEBUG(
                    "IndexManager::FindEntry: DOUBLE type mismatch or invalid "
                    "tree");
                break;
            }
            case IndexKeyType::STRING: {
                auto* tree = GetIndex<std::string>(index_name);
                if (tree && std::holds_alternative<std::string>(key)) {
                    bool found =
                        tree->GetValue(std::get<std::string>(key), rid);
                    LOG_DEBUG("IndexManager::FindEntry: STRING search result = "
                              << found);
                    return found;
                }
                LOG_DEBUG(
                    "IndexManager::FindEntry: STRING type mismatch or invalid "
                    "tree");
                break;
            }
            default:
                LOG_ERROR("IndexManager: Unsupported key type for search");
                return false;
        }

        LOG_ERROR("IndexManager: Type mismatch or invalid tree for index "
                  << index_name);
        return false;
    }

    /**
     * 获取所有索引的名称列表
     * 用于管理和调试
     * @return 索引名称的vector
     */
    std::vector<std::string> GetAllIndexNames() const {
        std::lock_guard<std::mutex> lock(latch_);
        std::vector<std::string> names;
        names.reserve(indexes_.size());

        LOG_DEBUG("IndexManager::GetAllIndexNames: Scanning " << indexes_.size()
                                                              << " indexes");
        for (const auto& [name, metadata] : indexes_) {
            names.push_back(name);
            LOG_DEBUG("IndexManager::GetAllIndexNames: Found index "
                      << name << " on table " << metadata->table_name);
        }
        LOG_DEBUG("IndexManager::GetAllIndexNames: Total "
                  << names.size() << " indexes returned");
        return names;
    }

    /**
     * 获取指定表的所有索引
     * 用于表删除时的清理工作
     * @param table_name 表名
     * @return 该表的所有索引名称
     */
    std::vector<std::string> GetTableIndexes(
        const std::string& table_name) const {
        std::lock_guard<std::mutex> lock(latch_);
        std::vector<std::string> table_indexes;
        for (const auto& [index_name, metadata] : indexes_) {
            if (metadata->table_name == table_name) {
                table_indexes.push_back(index_name);
                LOG_DEBUG("IndexManager::GetTableIndexes: Found index "
                          << index_name << " for table " << table_name);
            }
        }
        LOG_DEBUG("IndexManager::GetTableIndexes: Table "
                  << table_name << " has " << table_indexes.size()
                  << " indexes");
        return table_indexes;
    }

   private:
    BufferPoolManager*
        buffer_pool_manager_;  // 缓冲池管理器，用于B+树的页面管理
    Catalog* catalog_;         // 目录管理器，用于验证表信息
    std::unordered_map<std::string, std::unique_ptr<IndexMetadata>>
        indexes_;               // 索引名到元数据的映射
    mutable std::mutex latch_;  // 保护indexes_的互斥锁，确保线程安全
};

// ==================== IndexManager 公共接口实现 ====================

IndexManager::IndexManager(BufferPoolManager* buffer_pool_manager,
                           Catalog* catalog)
    : impl_(std::make_unique<IndexManagerImpl>(buffer_pool_manager, catalog)) {}

IndexManager::~IndexManager() = default;

bool IndexManager::CreateIndex(const std::string& index_name,
                               const std::string& table_name,
                               const std::vector<std::string>& key_columns,
                               const Schema* table_schema) {
    return impl_->CreateIndex(index_name, table_name, key_columns,
                              table_schema);
}

bool IndexManager::DropIndex(const std::string& index_name) {
    return impl_->DropIndex(index_name);
}

bool IndexManager::InsertEntry(const std::string& index_name, const Value& key,
                               const RID& rid) {
    return impl_->InsertEntry(index_name, key, rid);
}

bool IndexManager::DeleteEntry(const std::string& index_name,
                               const Value& key) {
    return impl_->DeleteEntry(index_name, key);
}

bool IndexManager::FindEntry(const std::string& index_name, const Value& key,
                             RID* rid) {
    return impl_->FindEntry(index_name, key, rid);
}

std::vector<std::string> IndexManager::GetAllIndexNames() const {
    return impl_->GetAllIndexNames();
}

std::vector<std::string> IndexManager::GetTableIndexes(
    const std::string& table_name) const {
    return impl_->GetTableIndexes(table_name);
}

template <typename KeyType>
BPlusTree<KeyType, RID>* IndexManager::GetIndex(const std::string& index_name) {
    return impl_->GetIndex<KeyType>(index_name);
}

// ==================== 模板显式实例化 ====================
// 为了支持不同数据类型的索引，需要显式实例化模板
template BPlusTree<int32_t, RID>* IndexManager::GetIndex<int32_t>(
    const std::string& index_name);
template BPlusTree<int64_t, RID>* IndexManager::GetIndex<int64_t>(
    const std::string& index_name);
template BPlusTree<float, RID>* IndexManager::GetIndex<float>(
    const std::string& index_name);
template BPlusTree<double, RID>* IndexManager::GetIndex<double>(
    const std::string& index_name);
template BPlusTree<std::string, RID>* IndexManager::GetIndex<std::string>(
    const std::string& index_name);

}  // namespace SimpleRDBMS