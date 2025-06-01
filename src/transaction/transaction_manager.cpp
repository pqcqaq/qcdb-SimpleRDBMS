/*
 * 文件: transaction_manager.cpp
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述:
 * 事务管理模块的核心实现，提供事务的创建（Begin）、提交（Commit）和回滚（Abort）等功能。
 *       与 LockManager 以及 LogManager 配合，支持事务的隔离控制与WAL日志记录。
 */

#include "transaction/transaction_manager.h"

#include <chrono>

#include "common/debug.h"
#include "recovery/log_record.h"
#include "stat/stat.h"

namespace SimpleRDBMS {

/**
 * 构造函数，初始化事务管理器，绑定锁管理器和日志管理器
 */
TransactionManager::TransactionManager(LockManager* lock_manager,
                                       LogManager* log_manager)
    : lock_manager_(lock_manager), log_manager_(log_manager) {}

/**
 * 析构函数：
 * - 尝试终止所有仍处于活跃状态（Growing 或 Shrinking）的事务
 * - 释放它们持有的所有锁
 */
TransactionManager::~TransactionManager() {
    std::lock_guard<std::mutex> lock(txn_map_latch_);
    for (auto& [txn_id, txn] : txn_map_) {
        if (txn && (txn->GetState() == TransactionState::GROWING ||
                    txn->GetState() == TransactionState::SHRINKING)) {
            txn->SetState(TransactionState::ABORTED);
            if (lock_manager_) {
                try {
                    lock_manager_->UnlockAll(txn.get());
                } catch (...) {
                    // 析构期间不抛出异常，安全退出
                }
            }
        }
    }
}

/**
 * 创建一个新的事务对象，并将其加入事务映射表
 * 同时会记录 BEGIN 日志（若启用了日志模块）
 */
Transaction* TransactionManager::Begin(IsolationLevel isolation_level) {
    LOG_DEBUG("TransactionManager::Begin: Starting new transaction");

    auto transaction_start_time = std::chrono::high_resolution_clock::now();

    txn_id_t txn_id = GetNextTxnId();
    LOG_DEBUG("TransactionManager::Begin: Assigned transaction ID " << txn_id);

    // 分配事务对象并设置隔离级别
    auto txn = std::make_unique<Transaction>(txn_id, isolation_level);

    txn->SetStartTime(transaction_start_time);

    // 如果配置了日志管理器，写入 BEGIN 记录
    if (log_manager_ != nullptr) {
        try {
            LOG_DEBUG("TransactionManager::Begin: Writing begin log record");
            BeginLogRecord log_record(txn_id);
            lsn_t lsn = log_manager_->AppendLogRecord(&log_record);
            txn->SetPrevLSN(lsn);
            LOG_DEBUG(
                "TransactionManager::Begin: Begin log record written with LSN "
                << lsn);
        } catch (const std::exception& e) {
            LOG_WARN("TransactionManager::Begin: Failed to write log record: "
                     << e.what());
            // 写日志失败不影响事务创建
        }
    }

    // 添加事务到全局事务表中
    Transaction* txn_ptr = txn.get();
    {
        std::lock_guard<std::mutex> lock(txn_map_latch_);
        txn_map_[txn_id] = std::move(txn);
    }

    STATS.RecordTransactionBegin();

    LOG_DEBUG("TransactionManager::Begin: Transaction "
              << txn_id << " created successfully");
    return txn_ptr;
}

/**
 * 提交事务：
 * - 更改状态为 COMMITTED
 * - 写入 COMMIT 日志记录
 * - Flush 到磁盘
 * - 释放所有持有的锁
 * - 从全局事务表移除
 */
bool TransactionManager::Commit(Transaction* txn) {
    if (txn == nullptr) {
        return false;
    }

    auto commit_start_time = std::chrono::high_resolution_clock::now();

    txn->SetState(TransactionState::COMMITTED);

    if (log_manager_ != nullptr) {
        try {
            CommitLogRecord log_record(txn->GetTxnId(), txn->GetPrevLSN());
            lsn_t lsn = log_manager_->AppendLogRecord(&log_record);
            txn->SetPrevLSN(lsn);
            log_manager_->Flush(lsn);  // 将日志刷新到磁盘
        } catch (...) {
            // 即使日志失败，也不阻止事务提交
        }
    }

    if (lock_manager_) {
        lock_manager_->UnlockAll(txn);  // 释放该事务的所有锁
    }

    auto transaction_duration =
        std::chrono::duration_cast<std::chrono::microseconds>(
            commit_start_time - txn->GetStartTime());
    double duration_ms = transaction_duration.count() / 1000.0;

    {
        std::lock_guard<std::mutex> lock(txn_map_latch_);
        txn_map_.erase(txn->GetTxnId());
    }

    STATS.RecordTransactionCommit();
    STATS.RecordTransactionDuration(duration_ms);

    LOG_DEBUG("TransactionManager::Commit: Transaction "
              << txn->GetTxnId() << " committed successfully");
    return true;
}

/**
 * 回滚事务：
 * - 更改状态为 ABORTED
 * - 写入 ABORT 日志记录
 * - Flush 到磁盘
 * - 释放所有持有的锁
 * - 从全局事务表移除
 */
bool TransactionManager::Abort(Transaction* txn) {
    if (txn == nullptr) {
        return false;
    }

    auto abort_start_time = std::chrono::high_resolution_clock::now();

    LOG_DEBUG("TransactionManager::Abort: Aborting transaction "
              << txn->GetTxnId());

    txn->SetState(TransactionState::ABORTED);

    if (log_manager_ != nullptr) {
        try {
            AbortLogRecord log_record(txn->GetTxnId(), txn->GetPrevLSN());
            lsn_t lsn = log_manager_->AppendLogRecord(&log_record);
            txn->SetPrevLSN(lsn);
            log_manager_->Flush(lsn);  // 将日志刷新到磁盘
        } catch (...) {
            // 即使日志失败也继续回滚
        }
    }

    if (lock_manager_) {
        lock_manager_->UnlockAll(txn);  // 释放所有锁
    }

    auto transaction_duration =
        std::chrono::duration_cast<std::chrono::microseconds>(
            abort_start_time - txn->GetStartTime());
    double duration_ms = transaction_duration.count() / 1000.0;

    {
        std::lock_guard<std::mutex> lock(txn_map_latch_);
        txn_map_.erase(txn->GetTxnId());
    }

    STATS.RecordTransactionAbort();
    STATS.RecordTransactionDuration(duration_ms);

    LOG_DEBUG("TransactionManager::Abort: Transaction "
              << txn->GetTxnId() << " aborted successfully");
    return true;
}

}  // namespace SimpleRDBMS
