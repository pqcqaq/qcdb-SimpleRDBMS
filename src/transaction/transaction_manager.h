/*
 * 文件: transaction_manager.h
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述: 事务管理器的头文件，定义了 TransactionManager
 * 类，负责事务的创建、提交、回滚，
 *       并维护事务生命周期中的元信息。依赖于锁管理器（LockManager）和日志管理器（LogManager）。
 */

#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "buffer/buffer_pool_manager.h"
#include "lock_manager.h"
#include "recovery/log_manager.h"
#include "transaction/transaction.h"

namespace SimpleRDBMS {

/**
 * TransactionManager（事务管理器）：
 * - 管理当前系统中所有活跃事务；
 * - 负责生成全局唯一的事务 ID；
 * - 提供事务的 Begin / Commit / Abort 操作；
 * - 与 LockManager 协作实现并发控制；
 * - 与 LogManager 协作实现 WAL 日志记录。
 */
class TransactionManager {
   public:
    /**
     * 构造函数，传入锁管理器和日志管理器的指针
     */
    TransactionManager(LockManager* lock_manager, LogManager* log_manager);

    /**
     * 析构函数，确保所有活跃事务安全终止
     */
    ~TransactionManager();

    /**
     * 开启一个新事务，并根据设定的隔离级别进行初始化
     * 默认使用 Repeatable Read（可重复读）隔离级别
     */
    Transaction* Begin(
        IsolationLevel isolation_level = IsolationLevel::REPEATABLE_READ);

    /**
     * 提交指定事务：
     * - 写入 COMMIT 日志
     * - 释放所有锁
     * - 从事务表中移除
     */
    bool Commit(Transaction* txn);

    /**
     * 回滚指定事务：
     * - 写入 ABORT 日志
     * - 释放所有锁
     * - 从事务表中移除
     */
    bool Abort(Transaction* txn);

    /**
     * 获取当前关联的锁管理器实例
     */
    LockManager* GetLockManager() { return lock_manager_; }

    /**
     * 获取一个新的、唯一的事务 ID（自增原子操作）
     */
    txn_id_t GetNextTxnId() { return next_txn_id_.fetch_add(1); }

   private:
    /**
     * 全局事务 ID 计数器，保证每个事务分配到唯一 ID
     */
    std::atomic<txn_id_t> next_txn_id_{0};

    /**
     * 指向系统中的锁管理器（用于加锁、解锁）
     */
    LockManager* lock_manager_;

    /**
     * 指向日志管理器（用于 WAL 写入）
     */
    LogManager* log_manager_;

    /**
     * 活跃事务表：记录当前正在执行的所有事务
     */
    std::unordered_map<txn_id_t, std::unique_ptr<Transaction>> txn_map_;

    /**
     * 用于保护事务表的互斥锁（避免并发访问冲突）
     */
    std::mutex txn_map_latch_;
};

}  // namespace SimpleRDBMS
