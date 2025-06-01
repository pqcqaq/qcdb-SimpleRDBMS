/*
 * 文件: transaction.cpp
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述: 事务模块的实现部分，主要定义了 Transaction
 * 类的构造、析构以及写集合的操作。
 */

#include "transaction/transaction.h"

namespace SimpleRDBMS {

/*
 * Transaction 构造函数
 * 参数:
 *   - txn_id: 当前事务的唯一 ID
 *   - isolation_level: 当前事务使用的隔离级别
 *
 * 初始化事务的基本状态，包括默认的事务状态（GROWING）、隔离级别，以及上一条日志的LSN设置为无效值
 */
Transaction::Transaction(txn_id_t txn_id, IsolationLevel isolation_level)
    : txn_id_(txn_id),
      state_(TransactionState::GROWING),
      isolation_level_(isolation_level),
      prev_lsn_(INVALID_LSN) {
    // 初始化事务对象的时候，事务开始处于 GROWING
    // 状态（可获得锁、写操作还未提交）
}

/*
 * Transaction 析构函数
 * 用于清理事务对象，当前未定义特殊行为
 */
Transaction::~Transaction() = default;

/*
 * AddToWriteSet - 将一条写操作添加到事务的写集合中（write set）
 * 参数:
 *   - rid: 被修改的记录ID
 *   - tuple: 修改前的原始记录（旧值）
 *
 * 思路说明:
 *   - 写集合用于实现回滚（Undo），保存的是旧数据的副本
 *   - 如果该记录之前没有被加入写集合，就保存下来
 *   - 如果已经存在，就说明我们已经记录过旧值，避免覆盖
 */
void Transaction::AddToWriteSet(const RID& rid, const Tuple& tuple) {
    // 如果这条记录之前没有加入写集合，则保存一份旧值备份
    if (write_set_.find(rid) == write_set_.end()) {
        write_set_[rid] = tuple;
    }
}

}  // namespace SimpleRDBMS
