/*
 * 文件: catalog.h
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述:
 * 数据库目录管理器，负责管理表和索引的元数据信息，包括创建、删除、查询等操作
 */

#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "common/debug.h"
#include "common/types.h"
#include "recovery/log_manager.h"

namespace SimpleRDBMS {

// 前向声明，避免头文件循环依赖
class BufferPoolManager;
class Schema;
class TableHeap;

/**
 * 表信息结构体
 * 包含表的所有必要信息：schema定义、表名、数据存储、OID和首页ID
 */
struct TableInfo {
    std::unique_ptr<Schema> schema;         // 表的schema定义，包含列信息和约束
    std::string table_name;                 // 表名
    std::unique_ptr<TableHeap> table_heap;  // 表的数据存储管理器
    oid_t table_oid;                        // 表的唯一标识符
    page_id_t first_page_id;                // 表数据的首页ID
};

/**
 * 索引信息结构体
 * 包含索引的基本信息：索引名、所属表名、索引列和OID
 */
struct IndexInfo {
    std::string index_name;                // 索引名称
    std::string table_name;                // 索引所属的表名
    std::vector<std::string> key_columns;  // 索引的键列名列表
    oid_t index_oid;                       // 索引的唯一标识符
};

/**
 * 数据库目录管理器
 *
 * 这是数据库系统的元数据管理中心，负责：
 * 1. 管理表的创建、删除和查询
 * 2. 管理索引的创建、删除和查询
 * 3. 维护表和索引的映射关系
 * 4. 提供元数据的持久化功能
 * 5. 分配和管理OID（对象标识符）
 *
 * 设计思路：
 * - 使用双向映射（name <-> oid）提供快速查找
 * - 通过unique_ptr管理内存，确保资源安全
 * - 支持并发访问，使用mutex保护关键操作
 * - 集成日志管理器，支持crash recovery
 */
class Catalog {
   public:
    /**
     * 构造函数
     * @param buffer_pool_manager 缓冲池管理器，用于页面I/O
     * @param log_manager 日志管理器，可选，用于恢复时重建catalog
     */
    explicit Catalog(BufferPoolManager* buffer_pool_manager,
                     LogManager* log_manager = nullptr);

    /**
     * 析构函数
     * 确保在销毁前保存catalog信息到磁盘
     */
    ~Catalog();

    // ======================== 表操作接口 ========================

    /**
     * 创建新表
     * @param table_name 表名
     * @param schema 表的schema定义
     * @return 创建成功返回true，失败返回false（如表已存在）
     *
     * 创建流程：
     * 1. 检查表名是否已存在
     * 2. 分配新的table_oid
     * 3. 创建TableHeap存储管理器
     * 4. 构建TableInfo并添加到映射表中
     */
    bool CreateTable(const std::string& table_name, const Schema& schema);

    /**
     * 删除表
     * @param table_name 要删除的表名
     * @return 删除成功返回true，失败返回false（如表不存在）
     *
     * 删除流程：
     * 1. 查找表是否存在
     * 2. 删除该表的所有索引
     * 3. 释放表的存储空间
     * 4. 从映射表中移除表信息
     */
    bool DropTable(const std::string& table_name);

    /**
     * 根据表名获取表信息
     * @param table_name 表名
     * @return 表信息指针，未找到返回nullptr
     */
    TableInfo* GetTable(const std::string& table_name);

    /**
     * 根据OID获取表信息
     * @param table_oid 表的OID
     * @return 表信息指针，未找到返回nullptr
     */
    TableInfo* GetTable(oid_t table_oid);

    // ======================== 索引操作接口 ========================

    /**
     * 创建索引
     * @param index_name 索引名
     * @param table_name 所属表名
     * @param key_columns 索引的键列名列表
     * @return 创建成功返回true，失败返回false
     *
     * 创建流程：
     * 1. 验证表是否存在
     * 2. 验证索引列是否存在于表中
     * 3. 检查索引名是否唯一
     * 4. 分配index_oid并创建IndexInfo
     */
    bool CreateIndex(const std::string& index_name,
                     const std::string& table_name,
                     const std::vector<std::string>& key_columns);

    /**
     * 删除索引
     * @param index_name 索引名
     * @return 删除成功返回true，失败返回false
     */
    bool DropIndex(const std::string& index_name);

    /**
     * 根据索引名获取索引信息
     * @param index_name 索引名
     * @return 索引信息指针，未找到返回nullptr
     */
    IndexInfo* GetIndex(const std::string& index_name);

    /**
     * 根据OID获取索引信息
     * @param index_oid 索引OID
     * @return 索引信息指针，未找到返回nullptr
     */
    IndexInfo* GetIndex(oid_t index_oid);

    /**
     * 获取指定表的所有索引
     * @param table_name 表名
     * @return 索引信息指针的vector
     */
    std::vector<IndexInfo*> GetTableIndexes(const std::string& table_name);

    // ======================== 工具方法 ========================

    /**
     * 设置日志管理器
     * 主要用于系统启动后的延迟设置
     */
    void SetLogManager(LogManager* log_manager) { log_manager_ = log_manager; }

    /**
     * 从磁盘加载catalog信息
     * 系统启动时调用，重建内存中的元数据结构
     */
    void LoadCatalogFromDisk();

    /**
     * 将catalog信息保存到磁盘
     * 系统关闭或checkpoint时调用
     */
    void SaveCatalogToDisk();

    /**
     * 调试用：打印所有表信息
     * 输出格式化的表列表，用于调试和测试
     */
    void DebugPrintTables() const;

    /**
     * 获取所有表名列表
     * @return 表名的vector，用于SHOW TABLES等操作
     */
    std::vector<std::string> GetAllTableNames() const;

    /**
     * 系统关闭时的清理操作
     * 确保所有元数据都被正确保存
     */
    void Shutdown() {
        try {
            SaveCatalogToDisk();
            buffer_pool_manager_ = nullptr;  // 避免关闭后访问buffer pool
        } catch (const std::exception& e) {
            LOG_WARN("Catalog::Shutdown: " << e.what());
        }
    }

   private:
    // ======================== 成员变量 ========================

    BufferPoolManager* buffer_pool_manager_;  // 缓冲池管理器，用于页面I/O操作
    mutable std::mutex save_mutex_;           // 保护磁盘I/O操作的互斥锁
    std::atomic<bool> save_in_progress_;      // 标记是否正在进行保存操作
    LogManager* log_manager_;                 // 日志管理器，用于恢复操作

    // 表相关的映射关系
    std::unordered_map<std::string, std::unique_ptr<TableInfo>>
        tables_;                                            // 表名 -> 表信息
    std::unordered_map<oid_t, std::string> table_oid_map_;  // 表OID -> 表名

    // 索引相关的映射关系
    std::unordered_map<std::string, std::unique_ptr<IndexInfo>>
        indexes_;  // 索引名 -> 索引信息
    std::unordered_map<oid_t, std::string> index_oid_map_;  // 索引OID -> 索引名

    // OID生成器，确保每个对象都有唯一标识符
    oid_t next_table_oid_;  // 下一个可用的表OID
    oid_t next_index_oid_;  // 下一个可用的索引OID

    // ======================== 序列化辅助方法 ========================

    /**
     * 将Schema序列化到缓冲区
     * @param schema 要序列化的schema
     * @param buffer 目标缓冲区
     * @param offset 当前偏移量，会被更新
     */
    void SerializeSchema(const Schema& schema, char* buffer, size_t& offset);

    /**
     * 从缓冲区反序列化Schema
     * @param buffer 源缓冲区
     * @param offset 当前偏移量，会被更新
     * @return 反序列化得到的Schema对象
     */
    std::unique_ptr<Schema> DeserializeSchema(const char* buffer,
                                              size_t& offset);
};

}  // namespace SimpleRDBMS