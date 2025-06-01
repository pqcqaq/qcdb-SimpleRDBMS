/*
 * 文件: lock_manager.h
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述: 锁管理器的声明。提供了对记录的共享锁和排他锁的加锁与解锁能力，
 *       用于支持事务并发控制（主要是两段锁协议）。
 *       支持锁升级、解锁、事务中止检测等功能。
 *
 * 版权所有 (c) 2025 QCQCQC. 保留所有权利。
 */

#pragma once

#include <chrono>
#include <condition_variable>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "common/config.h"
#include "common/types.h"
#include "transaction/transaction.h"

namespace SimpleRDBMS {

/**
 * LockMode 枚举表示锁的类型：
 * - SHARED: 共享锁，允许多个事务并发读取。
 * - EXCLUSIVE: 排他锁，只允许一个事务写入。
 */
enum class LockMode { SHARED = 0, EXCLUSIVE };

/**
 * LockManager 类负责管理所有记录上的锁，维护一个 lock_table_。
 * 提供加锁、解锁、锁升级、批量解锁等接口，用于事务在并发环境下的同步控制。
 */
class LockManager {
   public:
    LockManager() = default;
    ~LockManager() = default;

    /**
     * 尝试对某个记录加共享锁，适用于只读事务。
     * 返回 true 表示成功加锁；false 表示被阻塞或中止。
     */
    bool LockShared(Transaction* txn, const RID& rid);

    /**
     * 尝试对某个记录加排他锁，适用于写操作。
     */
    bool LockExclusive(Transaction* txn, const RID& rid);

    /**
     * 尝试将已有的共享锁升级为排他锁。
     * 注意：同一时间只能有一个事务在进行锁升级。
     */
    bool LockUpgrade(Transaction* txn, const RID& rid);

    /**
     * 解锁某个事务对某个记录持有的锁。
     * 会唤醒等待队列中符合条件的后续请求。
     */
    bool Unlock(Transaction* txn, const RID& rid);

    /**
     * 解锁事务持有的所有锁，一般在事务提交或中止时调用。
     */
    void UnlockAll(Transaction* txn);

   private:
    /**
     * LockRequest 结构体表示某个事务对某个记录的加锁请求。
     * 包含事务 ID、请求的锁类型、是否已授予等信息。
     */
    struct LockRequest {
        txn_id_t txn_id;
        LockMode lock_mode;
        bool granted;
    };

    /**
     * LockRequestQueue 是记录对应的锁等待队列。
     * 内含请求队列、条件变量（用于等待/唤醒）、以及升级标志。
     */
    struct LockRequestQueue {
        std::list<std::unique_ptr<LockRequest>> request_queue;
        std::condition_variable cv;  // 等待当前锁释放时唤醒阻塞事务
        bool upgrading = false;      // 是否有事务正在进行锁升级
    };

    std::mutex latch_;  // 全局锁管理器的互斥锁，保护 lock_table_ 并发访问

    /**
     * lock_table_ 是整个数据库的锁表。
     * 键是记录的 RID，值是对应的锁请求队列。
     */
    std::unordered_map<RID, std::unique_ptr<LockRequestQueue>> lock_table_;

    /**
     * 内部函数：尝试授予某个锁请求。
     * 根据当前队列状态和锁兼容性来判断是否可以立即授予。
     */
    bool GrantLock(LockRequest* request, LockRequestQueue* queue);

    /**
     * 内部函数：在某个请求被释放之后，尝试授予等待队列中的新请求。
     * 主要逻辑是：看前面有哪些请求现在可以执行了。
     */
    void GrantNewLocksInQueue(LockRequestQueue* queue);

    /**
     * 内部函数：根据当前事务的状态判断是否需要中止。
     * 一般用于检测死锁后是否被标记为 ABORTED。
     */
    bool CheckAbort(Transaction* txn);
};

}  // namespace SimpleRDBMS
