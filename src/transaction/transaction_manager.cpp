#include "transaction/transaction_manager.h"
#include "recovery/log_record.h"
#include "common/debug.h"

namespace SimpleRDBMS {

TransactionManager::TransactionManager(LockManager* lock_manager, LogManager* log_manager)
    : lock_manager_(lock_manager), log_manager_(log_manager) {
}

TransactionManager::~TransactionManager() {
    // 简化析构函数，减少异常可能性
    std::lock_guard<std::mutex> lock(txn_map_latch_);
    for (auto& [txn_id, txn] : txn_map_) {
        if (txn && (txn->GetState() == TransactionState::GROWING ||
                   txn->GetState() == TransactionState::SHRINKING)) {
            txn->SetState(TransactionState::ABORTED);
            if (lock_manager_) {
                try {
                    lock_manager_->UnlockAll(txn.get());
                } catch (...) {
                    // 忽略析构函数中的异常
                }
            }
        }
    }
}

Transaction* TransactionManager::Begin(IsolationLevel isolation_level) {
    LOG_DEBUG("TransactionManager::Begin: Starting new transaction");
    
    txn_id_t txn_id = GetNextTxnId();
    LOG_DEBUG("TransactionManager::Begin: Assigned transaction ID " << txn_id);
    
    // Create new transaction
    auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
    
    // 简化日志记录，避免可能的卡死
    if (log_manager_ != nullptr) {
        try {
            LOG_DEBUG("TransactionManager::Begin: Writing begin log record");
            BeginLogRecord log_record(txn_id);
            lsn_t lsn = log_manager_->AppendLogRecord(&log_record);
            txn->SetPrevLSN(lsn);
            LOG_DEBUG("TransactionManager::Begin: Begin log record written with LSN " << lsn);
        } catch (const std::exception& e) {
            LOG_WARN("TransactionManager::Begin: Failed to write log record: " << e.what());
            // 如果日志失败，继续执行，不影响事务创建
        }
    }

    // Add to transaction map
    Transaction* txn_ptr = txn.get();
    {
        std::lock_guard<std::mutex> lock(txn_map_latch_);
        txn_map_[txn_id] = std::move(txn);
    }
    
    LOG_DEBUG("TransactionManager::Begin: Transaction " << txn_id << " created successfully");
    return txn_ptr;
}

void TransactionManager::Commit(Transaction* txn) {
    if (txn == nullptr) {
        return;
    }
    
    // Change state to committed
    txn->SetState(TransactionState::COMMITTED);
    
    // 简化日志记录
    if (log_manager_ != nullptr) {
        try {
            CommitLogRecord log_record(txn->GetTxnId(), txn->GetPrevLSN());
            lsn_t lsn = log_manager_->AppendLogRecord(&log_record);
            txn->SetPrevLSN(lsn);
            
            // 简化flush，不等待
            log_manager_->Flush(lsn);
        } catch (...) {
            // 日志失败不影响事务提交
        }
    }
    
    // Release all locks
    if (lock_manager_) {
        lock_manager_->UnlockAll(txn);
    }
    
    // Remove from transaction map
    {
        std::lock_guard<std::mutex> lock(txn_map_latch_);
        txn_map_.erase(txn->GetTxnId());
    }
}

void TransactionManager::Abort(Transaction* txn) {
    if (txn == nullptr) {
        return;
    }
    
    // Change state to aborted
    txn->SetState(TransactionState::ABORTED);
    
    // 简化日志记录
    if (log_manager_ != nullptr) {
        try {
            AbortLogRecord log_record(txn->GetTxnId(), txn->GetPrevLSN());
            lsn_t lsn = log_manager_->AppendLogRecord(&log_record);
            txn->SetPrevLSN(lsn);
            
            // 简化flush，不等待
            log_manager_->Flush(lsn);
        } catch (...) {
            // 日志失败不影响事务abort
        }
    }
    
    // Release all locks
    if (lock_manager_) {
        lock_manager_->UnlockAll(txn);
    }
    
    // Remove from transaction map
    {
        std::lock_guard<std::mutex> lock(txn_map_latch_);
        txn_map_.erase(txn->GetTxnId());
    }
}

}  // namespace SimpleRDBMS