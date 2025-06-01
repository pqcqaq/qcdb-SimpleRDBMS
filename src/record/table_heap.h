/*
 * 文件: table_heap.h
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述: 表堆(TableHeap)和表页(TablePage)的头文件定义，
 *       实现了数据库表的存储管理，支持tuple的增删改查操作，
 *       以及基于迭代器的顺序扫描功能
 */

#pragma once

#include <memory>

#include "buffer/buffer_pool_manager.h"
#include "catalog/schema.h"
#include "record/tuple.h"
#include "recovery/log_manager.h"

namespace SimpleRDBMS {

/**
 * TablePage类 - 表页面的实现
 *
 * 继承自Page类，专门用于存储table数据的页面类型
 * 每个TablePage维护一个页面头信息和多个tuple的存储
 *
 * 页面布局设计：
 * - 页面头部：存储元数据（下一页ID、LSN、tuple数量、空闲空间偏移）
 * - tuple数据：从页面开头向后增长
 * - slot目录：从页面末尾向前增长，记录每个tuple的偏移和长度
 */
class TablePage : public Page {
   public:
    /**
     * 初始化页面
     *
     * @param page_id 当前页面ID
     * @param prev_page_id 前一个页面ID（用于页面链表）
     */
    void Init(page_id_t page_id, page_id_t prev_page_id);

    /**
     * 在页面中插入一个新的tuple
     *
     * @param tuple 要插入的tuple对象
     * @param rid 输出参数，返回插入位置的RID
     * @return 插入成功返回true，空间不足返回false
     */
    bool InsertTuple(const Tuple& tuple, RID* rid);

    /**
     * 从页面中删除指定的tuple
     *
     * @param rid 要删除的tuple的RID
     * @return 删除成功返回true，RID无效返回false
     */
    bool DeleteTuple(const RID& rid);

    /**
     * 更新页面中指定位置的tuple
     *
     * @param tuple 新的tuple内容
     * @param rid 要更新的tuple的RID
     * @return 更新成功返回true，失败返回false
     */
    bool UpdateTuple(const Tuple& tuple, const RID& rid);

    /**
     * 从页面中读取指定的tuple
     *
     * @param rid 要读取的tuple的RID
     * @param tuple 输出参数，存储读取的tuple内容
     * @param schema 表的schema信息，用于解析tuple结构
     * @return 读取成功返回true，RID无效返回false
     */
    bool GetTuple(const RID& rid, Tuple* tuple, const Schema* schema);

    /**
     * 获取当前RID之后的下一个有效tuple的RID
     * 用于实现迭代器的顺序遍历功能
     *
     * @param current_rid 当前RID位置
     * @param next_rid 输出参数，下一个有效tuple的RID
     * @return 找到下一个tuple返回true，已经是最后一个返回false
     */
    bool GetNextTupleRID(const RID& current_rid, RID* next_rid);

    /**
     * 获取下一个页面的ID（页面链表导航）
     *
     * @return 下一个页面的ID，如果是最后一页返回INVALID_PAGE_ID
     */
    page_id_t GetNextPageId() const;

    /**
     * 设置下一个页面的ID
     *
     * @param next_page_id 下一个页面的ID
     */
    void SetNextPageId(page_id_t next_page_id);

    /**
     * 表页面头结构
     * 存储页面的元数据信息，位于页面的开始位置
     */
    struct TablePageHeader {
        page_id_t next_page_id;      // 下一个页面ID，形成页面链表
        lsn_t lsn;                   // 日志序列号，用于WAL恢复
        uint16_t num_tuples;         // 当前页面中的tuple数量
        uint16_t free_space_offset;  // 空闲空间的起始偏移量
    };

    /**
     * 获取页面头指针（只读版本）
     *
     * @return 指向const TablePageHeader的指针
     */
    const TablePageHeader* GetHeader() const;

    /**
     * 获取页面头指针（可修改版本）
     *
     * @return 指向TablePageHeader的指针
     */
    TablePageHeader* GetHeader();

};

/**
 * TableHeap类 - 表堆的主要实现
 *
 * 管理一个表的所有页面，提供tuple级别的操作接口
 * 维护页面链表，支持跨页面的数据操作和遍历
 *
 * 主要功能：
 * - tuple的CRUD操作（增删改查）
 * - 事务日志记录支持
 * - 迭代器模式的顺序扫描
 * - 页面级别的并发控制
 */
class TableHeap {
   public:
    /**
     * 构造函数 - 创建新的表堆
     * 用于创建全新的表，会自动分配第一个页面
     *
     * @param buffer_pool_manager 缓冲池管理器
     * @param schema 表的schema定义
     */
    TableHeap(BufferPoolManager* buffer_pool_manager, const Schema* schema);

    /**
     * 构造函数 - 从已存在的页面恢复表堆
     * 用于数据库重启时从磁盘恢复已存在的表
     *
     * @param buffer_pool_manager 缓冲池管理器
     * @param schema 表的schema定义
     * @param first_page_id 第一个页面的ID
     */
    TableHeap(BufferPoolManager* buffer_pool_manager, const Schema* schema,
              page_id_t first_page_id);

    /**
     * 析构函数
     * 清理资源，确保所有页面正确释放
     */
    ~TableHeap();

    /**
     * 插入新的tuple到表中
     *
     * 实现思路：
     * - 从当前页面开始查找有足够空间的页面
     * - 如果所有页面都满了，分配新页面
     * - 记录INSERT操作到WAL日志
     *
     * @param tuple 要插入的tuple
     * @param rid 输出参数，返回插入位置的RID
     * @param txn_id 事务ID，用于日志记录
     * @return 插入成功返回true，失败返回false
     */
    bool InsertTuple(const Tuple& tuple, RID* rid, txn_id_t txn_id);

    /**
     * 删除指定RID的tuple
     *
     * @param rid 要删除的tuple的RID
     * @param txn_id 事务ID，用于日志记录
     * @return 删除成功返回true，失败返回false
     */
    bool DeleteTuple(const RID& rid, txn_id_t txn_id);

    /**
     * 更新指定RID的tuple内容
     *
     * @param tuple 新的tuple内容
     * @param rid 要更新的tuple的RID
     * @param txn_id 事务ID，用于日志记录
     * @return 更新成功返回true，失败返回false
     */
    bool UpdateTuple(const Tuple& tuple, const RID& rid, txn_id_t txn_id);

    /**
     * 读取指定RID的tuple内容
     *
     * @param rid 要读取的tuple的RID
     * @param tuple 输出参数，存储读取的tuple内容
     * @param txn_id 事务ID（当前版本未使用）
     * @return 读取成功返回true，失败返回false
     */
    bool GetTuple(const RID& rid, Tuple* tuple, txn_id_t txn_id);

    /**
     * 获取第一个页面的ID
     * 用于表的元数据管理和恢复
     *
     * @return 第一个页面的ID
     */
    page_id_t GetFirstPageId() const { return first_page_id_; }

    /**
     * 设置日志管理器
     * 用于支持WAL日志记录和崩溃恢复
     *
     * @param log_manager 日志管理器指针
     */
    void SetLogManager(LogManager* log_manager) { log_manager_ = log_manager; }

    /**
     * Iterator类 - 表的顺序扫描迭代器
     *
     * 支持跨页面的顺序遍历，实现了标准的迭代器接口
     * 内部维护当前RID位置，自动处理页面间的跳转
     */
    class Iterator {
       public:
        /**
         * 构造函数
         *
         * @param table_heap 所属的TableHeap指针
         * @param rid 初始RID位置
         */
        Iterator(TableHeap* table_heap, const RID& rid);

        /**
         * 默认构造函数
         * 创建一个end状态的迭代器
         */
        Iterator()
            : table_heap_(nullptr), current_rid_({INVALID_PAGE_ID, -1}) {}

        /**
         * 判断是否到达遍历结束位置
         *
         * @return 到达末尾返回true，否则返回false
         */
        bool IsEnd() const;

        /**
         * 迭代器前进操作（前置++）
         * 移动到下一个有效的tuple位置，支持跨页面导航
         */
        void operator++();

        /**
         * 解引用操作，获取当前位置的tuple
         *
         * @return 当前位置的Tuple对象
         */
        Tuple operator*();

       private:
        TableHeap* table_heap_;  // 所属的TableHeap指针
        RID current_rid_;        // 当前迭代器位置
    };

    /**
     * 获取指向表开始位置的迭代器
     *
     * @return 指向第一个有效tuple的迭代器
     */
    Iterator Begin();

    /**
     * 获取指向表结束位置的迭代器
     *
     * @return end迭代器，用于判断遍历结束
     */
    Iterator End();

   private:
    BufferPoolManager* buffer_pool_manager_;  // 缓冲池管理器，负责页面的读写
    const Schema* schema_;               // 表的schema定义，用于tuple的序列化
    page_id_t first_page_id_;            // 第一个页面的ID，页面链表的头部
    LogManager* log_manager_ = nullptr;  // 日志管理器，用于WAL记录和恢复
};

}  // namespace SimpleRDBMS