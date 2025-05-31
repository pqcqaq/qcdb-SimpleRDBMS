#pragma once

#include <atomic>
#include <memory>
#include <unordered_map>
#include "buffer/buffer_pool_manager.h"
#include "lock_manager.h"
#include "recovery/log_manager.h"
#include "transaction/transaction.h"

namespace SimpleRDBMS {

class TransactionManager {
public:
    TransactionManager(LockManager* lock_manager, LogManager* log_manager);
    ~TransactionManager();
    
    // Begin a new transaction
    Transaction* Begin(IsolationLevel isolation_level = IsolationLevel::REPEATABLE_READ);
    
    // Commit a transaction
    bool Commit(Transaction* txn);
    
    // Abort a transaction
    bool Abort(Transaction* txn);
    
    // Get lock manager
    LockManager* GetLockManager() { return lock_manager_; }
    
    // Get next transaction id
    txn_id_t GetNextTxnId() { return next_txn_id_.fetch_add(1); }

private:
    std::atomic<txn_id_t> next_txn_id_{0};
    LockManager* lock_manager_;
    LogManager* log_manager_;
    
    // Active transactions
    std::unordered_map<txn_id_t, std::unique_ptr<Transaction>> txn_map_;
    std::mutex txn_map_latch_;
};

}  // namespace SimpleRDBMS