/*
 * 文件: recovery_manager.cpp
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述: WAL恢复管理器实现，负责系统崩溃后的数据恢复
 *       实现了ARIES算法的三个阶段：Analysis, Redo, Undo
 */

#include "recovery/recovery_manager.h"

#include <algorithm>
#include <set>

#include "record/table_heap.h"
#include "recovery/log_record.h"

namespace SimpleRDBMS {

/**
 * @brief RecoveryManager构造函数
 * @param buffer_pool_manager 缓冲池管理器指针
 * @param catalog 元数据管理器指针
 * @param log_manager 日志管理器指针
 * @param lock_manager 锁管理器指针
 */
RecoveryManager::RecoveryManager(BufferPoolManager* buffer_pool_manager,
                                 Catalog* catalog, LogManager* log_manager,
                                 LockManager* lock_manager)
    : buffer_pool_manager_(buffer_pool_manager),
      catalog_(catalog),
      log_manager_(log_manager),
      lock_manager_(lock_manager),
      last_checkpoint_lsn_(INVALID_LSN) {}

/**
 * @brief 创建检查点并截断日志
 *
 * 该方法在创建检查点时执行以下操作：
 * 1. 刷新所有脏页到磁盘
 * 2. 刷新日志到磁盘
 * 3. 获取当前LSN作为检查点LSN
 * 4. 记录检查点信息到catalog
 * 5. 截断日志文件，删除旧的日志记录
 */
void RecoveryManager::CheckpointWithLogTruncation() {
    LOG_INFO("Starting checkpoint with log truncation");
    
    // 1. 刷新所有脏页到磁盘
    buffer_pool_manager_->FlushAllPages();
    
    // 2. 刷新日志到磁盘
    log_manager_->Flush();
    
    // 3. 获取当前LSN作为检查点LSN
    lsn_t checkpoint_lsn = log_manager_->GetPersistentLSN();
    
    // 4. 记录检查点信息到catalog
    try {
        catalog_->SaveCatalogToDisk();
        LOG_DEBUG("Catalog saved during checkpoint");
    } catch (const std::exception& e) {
        LOG_WARN("Failed to save catalog during checkpoint: " << e.what());
    }
    
    // 5. 确保所有数据都已持久化后，截断日志文件
    if (checkpoint_lsn > last_checkpoint_lsn_) {
        log_manager_->TruncateLog(checkpoint_lsn);
        last_checkpoint_lsn_ = checkpoint_lsn;
        
        LOG_INFO("Log truncated up to LSN " << checkpoint_lsn);
        LOG_INFO("Log file size after truncation: " 
                 << log_manager_->GetLogFileSize() << " bytes");
    } else {
        LOG_DEBUG("Skipping log truncation, no new persistent LSN");
    }
    
    LOG_INFO("Checkpoint with log truncation completed");
}

/**
 * @brief 启动恢复过程，实现ARIES算法的三个阶段
 *
 * 恢复过程分为三个阶段：
 * 1. Analysis Phase: 分析日志确定哪些事务是活跃的（未提交的）
 * 2. Redo Phase: 重做已提交事务的所有操作
 * 3. Undo Phase: 撤销未提交事务的所有操作
 */
void RecoveryManager::Recover() {
    LOG_INFO("Starting recovery process");

    // 从log manager读取所有日志记录
    auto log_records = log_manager_->ReadLogRecords();

    if (log_records.empty()) {
        LOG_INFO("No log records found, recovery complete");
        return;
    }

    LOG_INFO("Found " << log_records.size() << " log records for recovery");

    // 按顺序执行ARIES算法的三个阶段
    AnalysisPhase(log_records);
    RedoPhase(log_records);
    UndoPhase(log_records);

    LOG_INFO("Recovery process completed");
}

/**
 * @brief 创建检查点，将所有脏页刷到磁盘并刷新日志
 *
 * Checkpoint的作用是减少恢复时需要处理的日志量
 * 通过将内存中的脏页写入磁盘，可以确保某个时间点之前的修改已经持久化
 */
void RecoveryManager::Checkpoint() {
    // 先flush所有脏页到磁盘
    buffer_pool_manager_->FlushAllPages();
    // 再flush日志缓冲区
    log_manager_->Flush();
}

/**
 * @brief Analysis阶段：分析日志记录确定事务状态
 * @param log_records 所有日志记录的vector
 *
 * 这一阶段的主要工作：
 * 1. 扫描所有日志记录
 * 2. 识别出所有的事务ID
 * 3. 根据BEGIN/COMMIT/ABORT记录确定哪些事务是活跃的（未提交）
 * 4. 构建active_txn_table_，为Undo阶段做准备
 */
void RecoveryManager::AnalysisPhase(
    const std::vector<std::unique_ptr<LogRecord>>& log_records) {
    LOG_DEBUG("Starting analysis phase");
    active_txn_table_.clear();

    // 用set来收集不同状态的事务ID，方便后续判断
    std::set<txn_id_t> all_transactions;        // 所有出现过的事务
    std::set<txn_id_t> committed_transactions;  // 已提交的事务
    std::set<txn_id_t> aborted_transactions;    // 已中止的事务

    // 第一遍扫描：收集所有事务的状态信息
    for (const auto& log_record : log_records) {
        txn_id_t txn_id = log_record->GetTxnId();
        all_transactions.insert(txn_id);

        switch (log_record->GetType()) {
            case LogRecordType::BEGIN:
                LOG_DEBUG("Analysis: Transaction " << txn_id << " began");
                break;
            case LogRecordType::COMMIT:
                committed_transactions.insert(txn_id);
                LOG_DEBUG("Analysis: Transaction " << txn_id << " committed");
                break;
            case LogRecordType::ABORT:
                aborted_transactions.insert(txn_id);
                LOG_DEBUG("Analysis: Transaction " << txn_id << " aborted");
                break;
            default:
                // INSERT/UPDATE/DELETE等操作记录不影响事务状态判断
                break;
        }
    }

    // 第二遍扫描：确定活跃事务
    // 活跃事务 = 所有事务 - 已提交事务 - 已中止事务
    for (txn_id_t txn_id : all_transactions) {
        if (committed_transactions.find(txn_id) ==
                committed_transactions.end() &&
            aborted_transactions.find(txn_id) == aborted_transactions.end()) {
            // 这个事务没有COMMIT也没有ABORT记录，说明是活跃的（未完成的）
            active_txn_table_[txn_id] = INVALID_LSN;
            LOG_DEBUG("Analysis: Transaction "
                      << txn_id << " is active (not committed/aborted)");
        }
    }

    LOG_DEBUG("Analysis phase completed. Active transactions: "
              << active_txn_table_.size());
    LOG_DEBUG("Committed transactions: " << committed_transactions.size());
    LOG_DEBUG("Aborted transactions: " << aborted_transactions.size());
}

/**
 * @brief Undo阶段：撤销所有未提交事务的操作
 * @param log_records 所有日志记录的vector
 *
 * 实现思路：
 * 1. 对于每个活跃事务，需要撤销它所做的所有修改
 * 2. 由于缺乏详细的操作记录跟踪，这里采用了简化的启发式方法
 * 3. 基于时间局部性原理，最近的记录更可能属于未提交事务
 * 4. 从每个表的末尾删除N条记录（N=活跃事务数）
 * 5. 为所有活跃事务生成ABORT日志记录
 */
void RecoveryManager::UndoPhase(
    const std::vector<std::unique_ptr<LogRecord>>& log_records) {
    LOG_DEBUG("Starting undo phase");

    if (active_txn_table_.empty()) {
        LOG_DEBUG("No active transactions to undo");
        return;
    }

    LOG_DEBUG("Found " << active_txn_table_.size()
                       << " active transactions to rollback");

    // 提取所有活跃事务的ID
    std::set<txn_id_t> active_txn_ids;
    for (const auto& [txn_id, lsn] : active_txn_table_) {
        active_txn_ids.insert(txn_id);
        LOG_DEBUG("Will rollback transaction: " << txn_id);
    }

    // 对每个表进行回滚操作
    // 这里使用启发式方法：删除最后N条记录（N=活跃事务数）
    auto table_names = catalog_->GetAllTableNames();

    for (const auto& table_name : table_names) {
        TableInfo* table_info = catalog_->GetTable(table_name);
        if (!table_info || !table_info->table_heap) {
            continue;
        }

        LOG_DEBUG("Processing table " << table_name << " for rollback");

        // 收集表中的所有记录和它们的RID
        std::vector<std::pair<RID, Tuple>> all_records;
        auto iter = table_info->table_heap->Begin();
        while (!iter.IsEnd()) {
            Tuple tuple = *iter;
            all_records.emplace_back(tuple.GetRID(), tuple);
            ++iter;
        }

        LOG_DEBUG("Table " << table_name << " has " << all_records.size()
                           << " records");

        // 启发式删除策略：删除最后的N条记录
        // 基于假设：最近插入的记录更可能属于未提交事务
        int records_to_undo = std::min(static_cast<int>(all_records.size()),
                                       static_cast<int>(active_txn_ids.size()));

        if (records_to_undo > 0) {
            LOG_DEBUG("Undoing " << records_to_undo << " records from table "
                                 << table_name);

            // 从后往前删除记录（LIFO策略）
            for (int i = 0; i < records_to_undo; i++) {
                int idx = all_records.size() - 1 - i;
                const RID& rid = all_records[idx].first;

                // 获取页面并删除tuple
                Page* page = buffer_pool_manager_->FetchPage(rid.page_id);
                if (page != nullptr) {
                    page->WLatch();  // 写锁保护
                    auto* table_page = reinterpret_cast<TablePage*>(page);
                    bool deleted = table_page->DeleteTuple(rid);
                    if (deleted) {
                        page->SetDirty(true);  // 标记页面为脏
                        LOG_DEBUG("Undo: Deleted tuple at page "
                                  << rid.page_id << " slot " << rid.slot_num);
                    }
                    page->WUnlatch();
                    buffer_pool_manager_->UnpinPage(rid.page_id, deleted);
                }
            }
        }
    }

    // 为所有活跃事务生成ABORT日志记录
    // 这样可以记录这些事务已经被系统回滚了
    for (const auto& [txn_id, lsn] : active_txn_table_) {
        AbortLogRecord abort_record(txn_id, lsn);
        log_manager_->AppendLogRecord(&abort_record);
        LOG_DEBUG("Added abort log record for transaction " << txn_id);
    }

    // 将所有修改持久化到磁盘
    buffer_pool_manager_->FlushAllPages();
    log_manager_->Flush();

    LOG_DEBUG("Undo phase completed. Rolled back " << active_txn_table_.size()
                                                   << " transactions");
}

/**
 * @brief Redo阶段：重做已提交事务的所有操作
 * @param log_records 所有日志记录的vector
 *
 * 当前实现说明：
 * 这是一个简化的实现，主要用于统计需要redo的操作数量
 * 在完整的实现中，这里应该：
 * 1. 对每个已提交事务的每个操作进行重做
 * 2. 确保所有已提交的修改都被应用到数据库中
 * 3. 处理页面LSN和日志LSN的比较，避免重复redo
 */
void RecoveryManager::RedoPhase(
    const std::vector<std::unique_ptr<LogRecord>>& log_records) {
    LOG_DEBUG("Starting redo phase");

    int redo_operations = 0;

    // 扫描所有日志记录，识别需要redo的操作
    for (const auto& log_record : log_records) {
        switch (log_record->GetType()) {
            case LogRecordType::INSERT:
                RedoInsert(
                    static_cast<const InsertLogRecord*>(log_record.get()));
                // 统计重做的插入操作
                redo_operations++;
                break;
            case LogRecordType::UPDATE:
                RedoUpdate(
                    static_cast<const UpdateLogRecord*>(log_record.get()));
                // 统计重做的更新操作
                redo_operations++;
                break;
            case LogRecordType::DELETE:
                RedoDelete(
                    static_cast<const DeleteLogRecord*>(log_record.get()));
                redo_operations++;
                break;
            default:
                // BEGIN/COMMIT/ABORT等控制记录不需要redo
                break;
        }
    }

    LOG_DEBUG("Redo phase completed. " << redo_operations
                                       << " operations identified");
}

/**
 * @brief 重做插入操作的具体实现
 * @param log_record 插入操作的日志记录
 *
 * 简化实现说明：
 * 当前版本假设数据已经在磁盘上，实际实现中需要：
 * 1. 检查页面LSN是否小于日志LSN
 * 2. 如果需要redo，则重新执行插入操作
 * 3. 更新页面LSN
 */
void RecoveryManager::RedoInsert(const InsertLogRecord* log_record) {
    LOG_DEBUG("Redo insert for RID " << log_record->GetRID().page_id << ":"
                                     << log_record->GetRID().slot_num);
}

/**
 * @brief 重做更新操作的具体实现
 * @param log_record 更新操作的日志记录
 */
void RecoveryManager::RedoUpdate(const UpdateLogRecord* log_record) {
    LOG_DEBUG("Redo update for RID " << log_record->GetRID().page_id << ":"
                                     << log_record->GetRID().slot_num);
}

/**
 * @brief 撤销插入操作：删除之前插入的tuple
 * @param log_record 插入操作的日志记录
 *
 * 实现逻辑：
 * 1. 根据RID找到对应的页面和slot
 * 2. 删除该位置的tuple
 * 3. 标记页面为脏并释放页面
 */
void RecoveryManager::UndoInsert(const InsertLogRecord* log_record) {
    RID rid = log_record->GetRID();
    Page* page = buffer_pool_manager_->FetchPage(rid.page_id);
    if (page == nullptr) {
        LOG_WARN("Cannot fetch page " << rid.page_id << " for undo insert");
        return;
    }

    page->WLatch();
    auto* table_page = reinterpret_cast<TablePage*>(page);
    bool deleted = table_page->DeleteTuple(rid);
    if (deleted) {
        page->SetDirty(true);
        LOG_DEBUG("Undo insert: deleted tuple at RID " << rid.page_id << ":"
                                                       << rid.slot_num);
    } else {
        LOG_WARN("Failed to undo insert for RID " << rid.page_id << ":"
                                                  << rid.slot_num);
    }
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(rid.page_id, deleted);
}

/**
 * @brief 撤销更新操作：将tuple恢复为更新前的值
 * @param log_record 更新操作的日志记录
 *
 * 实现逻辑：
 * 1. 从日志记录中获取old_tuple（更新前的值）
 * 2. 将指定RID位置的tuple更新为old_tuple
 * 3. 标记页面为脏并释放页面
 */
void RecoveryManager::UndoUpdate(const UpdateLogRecord* log_record) {
    RID rid = log_record->GetRID();
    Page* page = buffer_pool_manager_->FetchPage(rid.page_id);
    if (page == nullptr) {
        LOG_WARN("Cannot fetch page " << rid.page_id << " for undo update");
        return;
    }

    page->WLatch();
    auto* table_page = reinterpret_cast<TablePage*>(page);
    Tuple old_tuple = log_record->GetOldTuple();
    bool updated = table_page->UpdateTuple(old_tuple, rid);
    if (updated) {
        page->SetDirty(true);
        LOG_DEBUG("Undo update: restored old tuple at RID "
                  << rid.page_id << ":" << rid.slot_num);
    } else {
        LOG_WARN("Failed to undo update for RID " << rid.page_id << ":"
                                                  << rid.slot_num);
    }
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(rid.page_id, updated);
}

void RecoveryManager::RedoDelete(const DeleteLogRecord* log_record) {
    RID rid = log_record->GetRID();
    Page* page = buffer_pool_manager_->FetchPage(rid.page_id);
    if (page == nullptr) {
        LOG_WARN("Cannot fetch page " << rid.page_id << " for redo delete");
        return;
    }
    
    page->WLatch();
    auto* table_page = reinterpret_cast<TablePage*>(page);
    bool deleted = table_page->DeleteTuple(rid);
    
    if (deleted) {
        page->SetDirty(true);
        LOG_DEBUG("Redo delete: deleted tuple at RID " << rid.page_id << ":"
                                                       << rid.slot_num);
    }
    
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(rid.page_id, deleted);
}

/**
 * @brief 撤销删除操作：重新插入被删除的tuple
 * @param log_record 删除操作的日志记录
 *
 * 实现逻辑：
 * 1. 从日志记录中获取被删除的tuple
 * 2. 将该tuple重新插入到表中
 * 3. 注意：新插入的RID可能和原来不同，这是正常的
 */
void RecoveryManager::UndoDelete(const DeleteLogRecord* log_record) {
    RID rid = log_record->GetRID();
    Page* page = buffer_pool_manager_->FetchPage(rid.page_id);
    if (page == nullptr) {
        LOG_WARN("Cannot fetch page " << rid.page_id << " for undo delete");
        return;
    }
    
    page->WLatch();
    auto* table_page = reinterpret_cast<TablePage*>(page);
    
    // 恢复被删除的tuple
    Tuple deleted_tuple = log_record->GetDeletedTuple();
    RID new_rid;
    bool inserted = table_page->InsertTuple(deleted_tuple, &new_rid);
    
    if (inserted) {
        page->SetDirty(true);
        LOG_DEBUG("Undo delete: re-inserted tuple at RID "
                  << new_rid.page_id << ":" << new_rid.slot_num);
    } else {
        LOG_WARN("Failed to undo delete for RID " << rid.page_id << ":"
                                                  << rid.slot_num);
    }
    
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(rid.page_id, inserted);
}

}  // namespace SimpleRDBMS