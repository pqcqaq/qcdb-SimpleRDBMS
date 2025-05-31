#pragma once

#include <condition_variable>
#include <list>
#include <memory>
#include <mutex>
#include <chrono>
#include <unordered_map>
#include "common/config.h"
#include "common/types.h"
#include "transaction/transaction.h"

namespace SimpleRDBMS {

enum class LockMode {
    SHARED = 0,
    EXCLUSIVE
};

class LockManager {
public:
    LockManager() = default;
    ~LockManager() = default;
    
    // Lock operations
    bool LockShared(Transaction* txn, const RID& rid);
    bool LockExclusive(Transaction* txn, const RID& rid);
    bool LockUpgrade(Transaction* txn, const RID& rid);
    
    // Unlock operations
    bool Unlock(Transaction* txn, const RID& rid);
    
    // Unlock all locks held by transaction
    void UnlockAll(Transaction* txn);

private:
    struct LockRequest {
        txn_id_t txn_id;
        LockMode lock_mode;
        bool granted;
    };
    
    struct LockRequestQueue {
        std::list<std::unique_ptr<LockRequest>> request_queue;
        std::condition_variable cv;
        bool upgrading = false;
    };
    
    std::mutex latch_;
    std::unordered_map<RID, std::unique_ptr<LockRequestQueue>> lock_table_;
    
    // Helper functions
    bool GrantLock(LockRequest* request, LockRequestQueue* queue);
    void GrantNewLocksInQueue(LockRequestQueue* queue);
    bool CheckAbort(Transaction* txn);
};

}  // namespace SimpleRDBMS