/*
 * 文件: lock_manager.cpp
 * 描述: 事务锁管理器实现，支持 S/X 锁申请、升级、释放，以及基本的锁调度逻辑。
 * 作者: QCQCQC
 * 日期: 2025-6-1
 */

#include "transaction/lock_manager.h"

namespace SimpleRDBMS {

/**
 * 申请共享锁（S锁）
 * - 如果当前事务处于收缩阶段（shrinking），直接 abort。
 * - 如果已有 S 或 X 锁，直接返回 true。
 * - 否则尝试申请，如果冲突则等待一定时间。
 */
bool LockManager::LockShared(Transaction* txn, const RID& rid) {
    std::unique_lock<std::mutex> lock(latch_);

    if (txn->GetState() == TransactionState::SHRINKING) {
        txn->SetState(TransactionState::ABORTED);
        return false;
    }

    if (txn->GetExclusiveLockSet().count(rid) > 0 ||
        txn->GetSharedLockSet().count(rid) > 0) {
        return true;
    }

    // 创建请求队列（如无）
    if (lock_table_.find(rid) == lock_table_.end()) {
        lock_table_[rid] = std::make_unique<LockRequestQueue>();
    }
    auto* queue = lock_table_[rid].get();

    auto request = std::make_unique<LockRequest>();
    request->txn_id = txn->GetTxnId();
    request->lock_mode = LockMode::SHARED;
    request->granted = false;

    // 判断是否可立即获得锁
    bool can_grant = GrantLock(request.get(), queue);
    if (can_grant) {
        request->granted = true;
        txn->AddSharedLock(rid);
        queue->request_queue.push_back(std::move(request));
        return true;
    }

    // 加入队列并等待
    queue->request_queue.push_back(std::move(request));
    auto timeout = std::chrono::milliseconds(100);
    if (queue->cv.wait_for(lock, timeout, [&]() {
            return CheckAbort(txn) ||
                   GrantLock(queue->request_queue.back().get(), queue);
        })) {
        if (CheckAbort(txn)) {
            queue->request_queue.pop_back();
            return false;
        }
        queue->request_queue.back()->granted = true;
        txn->AddSharedLock(rid);
        return true;
    }

    // 超时失败
    queue->request_queue.pop_back();
    return false;
}

/**
 * 申请排他锁（X锁）
 */
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

    // 加入队列等待
    queue->request_queue.push_back(std::move(request));
    auto timeout = std::chrono::milliseconds(100);
    if (queue->cv.wait_for(lock, timeout, [&]() {
            return CheckAbort(txn) ||
                   GrantLock(queue->request_queue.back().get(), queue);
        })) {
        if (CheckAbort(txn)) {
            queue->request_queue.pop_back();
            return false;
        }
        queue->request_queue.back()->granted = true;
        txn->AddExclusiveLock(rid);
        return true;
    }

    queue->request_queue.pop_back();
    return false;
}

/**
 * 将已有 S 锁升级为 X 锁
 */
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

    // 等待升级锁
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

    // 升级失败还原
    txn->AddSharedLock(rid);
    (*it)->lock_mode = LockMode::SHARED;
    (*it)->granted = true;
    queue->upgrading = false;
    queue->cv.notify_all();
    return false;
}

/**
 * 释放某个资源上的锁
 */
bool LockManager::Unlock(Transaction* txn, const RID& rid) {
    std::unique_lock<std::mutex> lock(latch_);

    if (txn->GetState() == TransactionState::GROWING) {
        txn->SetState(TransactionState::SHRINKING);
    }

    bool found = false;
    if (txn->GetSharedLockSet().count(rid)) {
        txn->RemoveSharedLock(rid);
        found = true;
    } else if (txn->GetExclusiveLockSet().count(rid)) {
        txn->RemoveExclusiveLock(rid);
        found = true;
    }

    if (!found) return false;

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

/**
 * 释放该事务上的所有锁
 */
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

/**
 * 判断当前请求是否可以被授权
 */
bool LockManager::GrantLock(LockRequest* request, LockRequestQueue* queue) {
    for (const auto& req : queue->request_queue) {
        if (!req->granted) continue;
        if (req->txn_id == request->txn_id) continue;

        if (request->lock_mode == LockMode::EXCLUSIVE ||
            req->lock_mode == LockMode::EXCLUSIVE) {
            return false;
        }
    }

    if (queue->upgrading && request->lock_mode != LockMode::EXCLUSIVE) {
        return false;
    }

    return true;
}

/**
 * 每次释放锁后，尝试授权队列中尚未 granted 的锁请求
 */
void LockManager::GrantNewLocksInQueue(LockRequestQueue* queue) {
    for (auto& request : queue->request_queue) {
        if (!request->granted && GrantLock(request.get(), queue)) {
            request->granted = true;
        }
    }
}

/**
 * 判断事务是否已被中止
 */
bool LockManager::CheckAbort(Transaction* txn) { return txn->IsAborted(); }

}  // namespace SimpleRDBMS
