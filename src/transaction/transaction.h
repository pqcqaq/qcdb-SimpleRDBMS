#pragma once

#include <atomic>
#include <memory>
#include <unordered_set>
#include <unordered_map>
#include "common/config.h"
#include "common/types.h"
#include "record/tuple.h"  // 添加这一行以包含 Tuple 定义

namespace SimpleRDBMS {

enum class TransactionState {
    INVALID = 0,
    GROWING,
    SHRINKING,
    COMMITTED,
    ABORTED
};

enum class IsolationLevel {
    READ_UNCOMMITTED = 0,
    READ_COMMITTED,
    REPEATABLE_READ,
    SERIALIZABLE
};

class Transaction {
public:
    explicit Transaction(txn_id_t txn_id, IsolationLevel isolation_level = IsolationLevel::REPEATABLE_READ);
    ~Transaction();
    
    // Get transaction id
    txn_id_t GetTxnId() const { return txn_id_; }
    
    // Get/Set transaction state
    TransactionState GetState() const { return state_; }
    void SetState(TransactionState state) { state_ = state; }
    
    // Get isolation level
    IsolationLevel GetIsolationLevel() const { return isolation_level_; }
    
    // Get/Set LSN
    lsn_t GetPrevLSN() const { return prev_lsn_; }
    void SetPrevLSN(lsn_t lsn) { prev_lsn_ = lsn; }
    
    // Lock management
    void AddSharedLock(const RID& rid) { shared_lock_set_.insert(rid); }
    void AddExclusiveLock(const RID& rid) { exclusive_lock_set_.insert(rid); }
    void RemoveSharedLock(const RID& rid) { shared_lock_set_.erase(rid); }
    void RemoveExclusiveLock(const RID& rid) { exclusive_lock_set_.erase(rid); }
    
    const std::unordered_set<RID>& GetSharedLockSet() const { return shared_lock_set_; }
    const std::unordered_set<RID>& GetExclusiveLockSet() const { return exclusive_lock_set_; }
    
    // Write set for rollback
    void AddToWriteSet(const RID& rid, const Tuple& tuple);
    const std::unordered_map<RID, Tuple>& GetWriteSet() const { return write_set_; }
    
    // Abort flag
    bool IsAborted() const { return state_ == TransactionState::ABORTED; }

private:
    txn_id_t txn_id_;
    TransactionState state_;
    IsolationLevel isolation_level_;
    lsn_t prev_lsn_;
    
    // Lock sets
    std::unordered_set<RID> shared_lock_set_;
    std::unordered_set<RID> exclusive_lock_set_;
    
    // Write set for rollback
    std::unordered_map<RID, Tuple> write_set_;
};

}  // namespace SimpleRDBMS