#include "transaction/lock_manager.h"

namespace SimpleRDBMS {

bool LockManager::LockShared(Transaction* txn, const RID& rid) {
    std::unique_lock<std::mutex> lock(latch_);
    
    if (txn->GetState() == TransactionState::SHRINKING) {
        txn->SetState(TransactionState::ABORTED);
        return false;
    }
    
    if (txn->GetExclusiveLockSet().count(rid) > 0) {
        return true;
    }
    
    if (txn->GetSharedLockSet().count(rid) > 0) {
        return true;
    }
    
    if (lock_table_.find(rid) == lock_table_.end()) {
        lock_table_[rid] = std::make_unique<LockRequestQueue>();
    }
    
    auto* queue = lock_table_[rid].get();
    
    auto request = std::make_unique<LockRequest>();
    request->txn_id = txn->GetTxnId();
    request->lock_mode = LockMode::SHARED;
    request->granted = false;
    
    bool can_grant = GrantLock(request.get(), queue);
    
    if (can_grant) {
        request->granted = true;
        txn->AddSharedLock(rid);
        queue->request_queue.push_back(std::move(request));
        return true;
    }
    
    // 如果无法立即获得锁，在测试环境中直接返回false
    // 在生产环境中可以添加超时等待机制
    queue->request_queue.push_back(std::move(request));
    
    // 尝试等待一小段时间，如果还是无法获得锁就返回false
    auto timeout = std::chrono::milliseconds(100);
    if (queue->cv.wait_for(lock, timeout, [&]() {
        return CheckAbort(txn) || GrantLock(queue->request_queue.back().get(), queue);
    })) {
        if (CheckAbort(txn)) {
            // 移除请求
            queue->request_queue.pop_back();
            return false;
        }
        
        // 成功获得锁
        queue->request_queue.back()->granted = true;
        txn->AddSharedLock(rid);
        return true;
    }
    
    // 超时，移除请求并返回false
    queue->request_queue.pop_back();
    return false;
}


bool LockManager::LockExclusive(Transaction* txn, const RID& rid) {
    std::unique_lock<std::mutex> lock(latch_);
    
    if (txn->GetState() == TransactionState::SHRINKING) {
        txn->SetState(TransactionState::ABORTED);
        return false;
    }
    
    if (txn->GetExclusiveLockSet().count(rid) > 0) {
        return true;
    }
    
    if (lock_table_.find(rid) == lock_table_.end()) {
        lock_table_[rid] = std::make_unique<LockRequestQueue>();
    }
    
    auto* queue = lock_table_[rid].get();
    
    auto request = std::make_unique<LockRequest>();
    request->txn_id = txn->GetTxnId();
    request->lock_mode = LockMode::EXCLUSIVE;
    request->granted = false;
    
    bool can_grant = GrantLock(request.get(), queue);
    
    if (can_grant) {
        request->granted = true;
        txn->AddExclusiveLock(rid);
        queue->request_queue.push_back(std::move(request));
        return true;
    }
    
    // 如果无法立即获得锁，在测试环境中直接返回false
    queue->request_queue.push_back(std::move(request));
    
    // 尝试等待一小段时间，如果还是无法获得锁就返回false
    auto timeout = std::chrono::milliseconds(100);
    if (queue->cv.wait_for(lock, timeout, [&]() {
        return CheckAbort(txn) || GrantLock(queue->request_queue.back().get(), queue);
    })) {
        if (CheckAbort(txn)) {
            // 移除请求
            queue->request_queue.pop_back();
            return false;
        }
        
        // 成功获得锁
        queue->request_queue.back()->granted = true;
        txn->AddExclusiveLock(rid);
        return true;
    }
    
    // 超时，移除请求并返回false
    queue->request_queue.pop_back();
    return false;
}

bool LockManager::LockUpgrade(Transaction* txn, const RID& rid) {
    std::unique_lock<std::mutex> lock(latch_);
    
    if (txn->GetState() == TransactionState::SHRINKING) {
        txn->SetState(TransactionState::ABORTED);
        return false;
    }
    
    if (txn->GetExclusiveLockSet().count(rid) > 0) {
        return true;
    }
    
    if (txn->GetSharedLockSet().count(rid) == 0) {
        return false;
    }
    
    auto* queue = lock_table_[rid].get();
    
    if (queue->upgrading) {
        txn->SetState(TransactionState::ABORTED);
        return false;
    }
    
    queue->upgrading = true;
    
    auto it = queue->request_queue.begin();
    while (it != queue->request_queue.end()) {
        if ((*it)->txn_id == txn->GetTxnId()) {
            (*it)->lock_mode = LockMode::EXCLUSIVE;
            (*it)->granted = false;
            break;
        }
        ++it;
    }
    
    if (it == queue->request_queue.end()) {
        queue->upgrading = false;
        return false;
    }
    
    txn->RemoveSharedLock(rid);
    
    bool can_grant = GrantLock(it->get(), queue);
    
    if (can_grant) {
        (*it)->granted = true;
        txn->AddExclusiveLock(rid);
        queue->upgrading = false;
        queue->cv.notify_all();
        return true;
    }
    
    // 尝试等待一小段时间
    auto timeout = std::chrono::milliseconds(100);
    if (queue->cv.wait_for(lock, timeout, [&]() {
        return CheckAbort(txn) || GrantLock(it->get(), queue);
    })) {
        if (CheckAbort(txn)) {
            queue->upgrading = false;
            queue->cv.notify_all();
            queue->request_queue.erase(it);
            return false;
        }
        
        (*it)->granted = true;
        txn->AddExclusiveLock(rid);
        queue->upgrading = false;
        queue->cv.notify_all();
        return true;
    }
    
    // 超时，恢复原状态
    txn->AddSharedLock(rid);
    (*it)->lock_mode = LockMode::SHARED;
    (*it)->granted = true;
    queue->upgrading = false;
    queue->cv.notify_all();
    return false;
}

bool LockManager::Unlock(Transaction* txn, const RID& rid) {
    std::unique_lock<std::mutex> lock(latch_);
    
    if (txn->GetState() == TransactionState::GROWING) {
        txn->SetState(TransactionState::SHRINKING);
    }
    
    bool found = false;
    
    if (txn->GetSharedLockSet().count(rid) > 0) {
        txn->RemoveSharedLock(rid);
        found = true;
    } else if (txn->GetExclusiveLockSet().count(rid) > 0) {
        txn->RemoveExclusiveLock(rid);
        found = true;
    }
    
    if (!found) {
        return false;
    }
    
    auto* queue = lock_table_[rid].get();
    
    auto it = queue->request_queue.begin();
    while (it != queue->request_queue.end()) {
        if ((*it)->txn_id == txn->GetTxnId()) {
            queue->request_queue.erase(it);
            break;
        }
        ++it;
    }
    
    GrantNewLocksInQueue(queue);
    queue->cv.notify_all();
    
    return true;
}

void LockManager::UnlockAll(Transaction* txn) {
    std::unique_lock<std::mutex> lock(latch_);
    
    std::unordered_set<RID> lock_set;
    
    for (const auto& rid : txn->GetSharedLockSet()) {
        lock_set.insert(rid);
    }
    
    for (const auto& rid : txn->GetExclusiveLockSet()) {
        lock_set.insert(rid);
    }
    
    for (const auto& rid : lock_set) {
        txn->RemoveSharedLock(rid);
        txn->RemoveExclusiveLock(rid);
        
        if (lock_table_.find(rid) != lock_table_.end()) {
            auto* queue = lock_table_[rid].get();
            
            auto it = queue->request_queue.begin();
            while (it != queue->request_queue.end()) {
                if ((*it)->txn_id == txn->GetTxnId()) {
                    queue->request_queue.erase(it);
                    break;
                }
                ++it;
            }
            
            GrantNewLocksInQueue(queue);
            queue->cv.notify_all();
        }
    }
}

bool LockManager::GrantLock(LockRequest* request, LockRequestQueue* queue) {
    for (const auto& req : queue->request_queue) {
        if (req->granted) {
            if (req->txn_id == request->txn_id) {
                continue;
            }
            
            if (request->lock_mode == LockMode::EXCLUSIVE) {
                return false;
            }
            
            if (req->lock_mode == LockMode::EXCLUSIVE) {
                return false;
            }
        }
    }
    
    if (queue->upgrading) {
        if (request->lock_mode != LockMode::EXCLUSIVE) {
            return false;
        }
        
        for (const auto& req : queue->request_queue) {
            if (req->granted && req->txn_id != request->txn_id) {
                return false;
            }
        }
    }
    
    return true;
}

void LockManager::GrantNewLocksInQueue(LockRequestQueue* queue) {
    for (auto& request : queue->request_queue) {
        if (!request->granted && GrantLock(request.get(), queue)) {
            request->granted = true;
        }
    }
}

bool LockManager::CheckAbort(Transaction* txn) {
    return txn->IsAborted();
}

}  // namespace SimpleRDBMS