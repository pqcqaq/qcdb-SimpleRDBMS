/*
 * 文件: transaction.h
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述:
 * 事务类的定义，包含事务的基本状态、锁集合、隔离级别、写集合等。用于配合事务管理器进行并发控制和恢复操作。
 */

#pragma once

#include <atomic>
#include <memory>
#include <unordered_map>
#include <unordered_set>

#include "common/config.h"
#include "common/types.h"
#include "record/tuple.h"

namespace SimpleRDBMS {

/*
 * TransactionState - 表示事务当前的状态：
 * - INVALID: 非法或未初始化
 * - GROWING: 正在获得锁的阶段
 * - SHRINKING: 已经开始释放锁，不允许再获得新锁
 * - COMMITTED: 事务已提交
 * - ABORTED: 事务已回滚
 */
enum class TransactionState {
    INVALID = 0,
    GROWING,
    SHRINKING,
    COMMITTED,
    ABORTED
};

/*
 * IsolationLevel - 事务的隔离级别：
 * - READ_UNCOMMITTED: 最低级别，可能脏读
 * - READ_COMMITTED: 只读已提交数据
 * - REPEATABLE_READ: 同一事务多次读取结果一致
 * - SERIALIZABLE: 完全隔离，防止幻读
 */
enum class IsolationLevel {
    READ_UNCOMMITTED = 0,
    READ_COMMITTED,
    REPEATABLE_READ,
    SERIALIZABLE
};

/*
 * Transaction 类
 * 每个正在执行的事务在系统中对应一个 Transaction 对象
 * 包含事务的 ID、隔离级别、状态、锁信息、写集合等内容
 */
class Transaction {
   public:
    // 构造函数：指定事务 ID 和隔离级别（默认为可重复读）
    explicit Transaction(txn_id_t txn_id, IsolationLevel isolation_level =
                                              IsolationLevel::REPEATABLE_READ);

    // 析构函数：清理资源（当前无额外操作）
    ~Transaction();

    // 获取事务ID
    txn_id_t GetTxnId() const { return txn_id_; }

    // 获取/设置事务状态
    TransactionState GetState() const { return state_; }
    void SetState(TransactionState state) { state_ = state; }

    // 获取事务的隔离级别
    IsolationLevel GetIsolationLevel() const { return isolation_level_; }

    // 获取/设置该事务最近的日志序列号（用于WAL恢复）
    lsn_t GetPrevLSN() const { return prev_lsn_; }
    void SetPrevLSN(lsn_t lsn) { prev_lsn_ = lsn; }

    // 锁管理相关函数 —— 添加和移除共享锁/排他锁
    void AddSharedLock(const RID& rid) { shared_lock_set_.insert(rid); }
    void AddExclusiveLock(const RID& rid) { exclusive_lock_set_.insert(rid); }
    void RemoveSharedLock(const RID& rid) { shared_lock_set_.erase(rid); }
    void RemoveExclusiveLock(const RID& rid) { exclusive_lock_set_.erase(rid); }

    // 获取锁集合（只读）
    const std::unordered_set<RID>& GetSharedLockSet() const {
        return shared_lock_set_;
    }
    const std::unordered_set<RID>& GetExclusiveLockSet() const {
        return exclusive_lock_set_;
    }

    // 写集合管理 —— 添加旧值用于回滚
    void AddToWriteSet(const RID& rid, const Tuple& tuple);

    // 获取写集合（只读）
    const std::unordered_map<RID, Tuple>& GetWriteSet() const {
        return write_set_;
    }

    // 判断事务是否已中止
    bool IsAborted() const { return state_ == TransactionState::ABORTED; }

   private:
    txn_id_t txn_id_;                 // 当前事务的唯一标识符
    TransactionState state_;          // 当前事务状态
    IsolationLevel isolation_level_;  // 当前隔离级别
    lsn_t prev_lsn_;                  // 上一个WAL日志的LSN（用于恢复）

    // 锁集合，用于记录事务已经持有的锁
    std::unordered_set<RID> shared_lock_set_;     // 已获得的共享锁
    std::unordered_set<RID> exclusive_lock_set_;  // 已获得的排他锁

    // 写集合，记录了事务修改过的数据（用于回滚时还原）
    std::unordered_map<RID, Tuple> write_set_;
};

}  // namespace SimpleRDBMS
