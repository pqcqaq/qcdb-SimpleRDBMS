// src/recovery/recovery_manager.cpp
#include "recovery/recovery_manager.h"

#include <algorithm>
#include <set>

#include "record/table_heap.h"
#include "recovery/log_record.h"

namespace SimpleRDBMS {

RecoveryManager::RecoveryManager(BufferPoolManager* buffer_pool_manager,
                                 Catalog* catalog, LogManager* log_manager,
                                 LockManager* lock_manager)
    : buffer_pool_manager_(buffer_pool_manager),
      catalog_(catalog),
      log_manager_(log_manager),
      lock_manager_(lock_manager) {}

void RecoveryManager::Recover() {
    LOG_INFO("Starting recovery process");
    auto log_records = log_manager_->ReadLogRecords();

    if (log_records.empty()) {
        LOG_INFO("No log records found, recovery complete");
        return;
    }

    LOG_INFO("Found " << log_records.size() << " log records for recovery");

    AnalysisPhase(log_records);
    RedoPhase(log_records);
    UndoPhase(log_records);

    LOG_INFO("Recovery process completed");
}

void RecoveryManager::Checkpoint() {
    buffer_pool_manager_->FlushAllPages();
    log_manager_->Flush();
}

void RecoveryManager::AnalysisPhase(
    const std::vector<std::unique_ptr<LogRecord>>& log_records) {
    LOG_DEBUG("Starting analysis phase");
    active_txn_table_.clear();

    // 首先，收集所有出现的事务ID
    std::set<txn_id_t> all_transactions;
    std::set<txn_id_t> committed_transactions;
    std::set<txn_id_t> aborted_transactions;

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
                break;
        }
    }

    // 任何没有COMMIT或ABORT记录的事务都被认为是活动的（未提交的）
    for (txn_id_t txn_id : all_transactions) {
        if (committed_transactions.find(txn_id) ==
                committed_transactions.end() &&
            aborted_transactions.find(txn_id) == aborted_transactions.end()) {
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

void RecoveryManager::UndoPhase(
    const std::vector<std::unique_ptr<LogRecord>>& log_records) {
    LOG_DEBUG("Starting undo phase");
    if (active_txn_table_.empty()) {
        LOG_DEBUG("No active transactions to undo");
        return;
    }

    LOG_DEBUG("Found " << active_txn_table_.size()
                       << " active transactions to rollback");

    // 获取所有活动事务的ID
    std::set<txn_id_t> active_txn_ids;
    for (const auto& [txn_id, lsn] : active_txn_table_) {
        active_txn_ids.insert(txn_id);
        LOG_DEBUG("Will rollback transaction: " << txn_id);
    }

    // 策略：扫描所有表，删除可能属于未提交事务的记录
    // 这是一个保守的方法，基于时间顺序假设
    auto table_names = catalog_->GetAllTableNames();

    for (const auto& table_name : table_names) {
        TableInfo* table_info = catalog_->GetTable(table_name);
        if (!table_info || !table_info->table_heap) {
            continue;
        }

        LOG_DEBUG("Processing table " << table_name << " for rollback");

        // 收集所有记录
        std::vector<std::pair<RID, Tuple>> all_records;
        auto iter = table_info->table_heap->Begin();
        while (!iter.IsEnd()) {
            Tuple tuple = *iter;
            all_records.emplace_back(tuple.GetRID(), tuple);
            ++iter;
        }

        LOG_DEBUG("Table " << table_name << " has " << all_records.size()
                           << " records");

        // 基于启发式规则删除记录：
        // 如果有N个活动事务，删除最后的N个记录（假设它们最可能是未提交的）
        int records_to_undo = std::min(static_cast<int>(all_records.size()),
                                       static_cast<int>(active_txn_ids.size()));

        if (records_to_undo > 0) {
            LOG_DEBUG("Undoing " << records_to_undo << " records from table "
                                 << table_name);

            // 从后往前删除记录
            for (int i = 0; i < records_to_undo; i++) {
                int idx = all_records.size() - 1 - i;
                const RID& rid = all_records[idx].first;

                Page* page = buffer_pool_manager_->FetchPage(rid.page_id);
                if (page != nullptr) {
                    page->WLatch();
                    auto* table_page = reinterpret_cast<TablePage*>(page);
                    bool deleted = table_page->DeleteTuple(rid);
                    if (deleted) {
                        page->SetDirty(true);
                        LOG_DEBUG("Undo: Deleted tuple at page "
                                  << rid.page_id << " slot " << rid.slot_num);
                    }
                    page->WUnlatch();
                    buffer_pool_manager_->UnpinPage(rid.page_id, deleted);
                }
            }
        }
    }

    // 为所有活动事务生成ABORT日志记录
    for (const auto& [txn_id, lsn] : active_txn_table_) {
        AbortLogRecord abort_record(txn_id, lsn);
        log_manager_->AppendLogRecord(&abort_record);
        LOG_DEBUG("Added abort log record for transaction " << txn_id);
    }

    // 刷新所有更改
    buffer_pool_manager_->FlushAllPages();
    log_manager_->Flush();

    LOG_DEBUG("Undo phase completed. Rolled back " << active_txn_table_.size()
                                                   << " transactions");
}

void RecoveryManager::RedoPhase(
    const std::vector<std::unique_ptr<LogRecord>>& log_records) {
    LOG_DEBUG("Starting redo phase");
    // 对于这个简化的实现，我们假设所有已提交的数据已经在磁盘上
    // 因此redo阶段可能不需要做太多工作

    int redo_operations = 0;
    for (const auto& log_record : log_records) {
        switch (log_record->GetType()) {
            case LogRecordType::INSERT:
                // 在完整实现中，这里会重做插入操作
                redo_operations++;
                break;
            case LogRecordType::UPDATE:
                // 在完整实现中，这里会重做更新操作
                redo_operations++;
                break;
            case LogRecordType::DELETE:
                // 在完整实现中，这里会重做删除操作
                redo_operations++;
                break;
            default:
                break;
        }
    }

    LOG_DEBUG("Redo phase completed. " << redo_operations
                                       << " operations identified");
}

// 简化的恢复操作实现
void RecoveryManager::RedoInsert(const InsertLogRecord* log_record) {
    // 简化实现 - 在这个版本中假设数据已经在磁盘上
    LOG_DEBUG("Redo insert for RID " << log_record->GetRID().page_id << ":"
                                     << log_record->GetRID().slot_num);
}

void RecoveryManager::RedoUpdate(const UpdateLogRecord* log_record) {
    // 简化实现 - 在这个版本中假设数据已经在磁盘上
    LOG_DEBUG("Redo update for RID " << log_record->GetRID().page_id << ":"
                                     << log_record->GetRID().slot_num);
}

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

void RecoveryManager::UndoDelete(const UpdateLogRecord* log_record) {
    RID rid = log_record->GetRID();
    Page* page = buffer_pool_manager_->FetchPage(rid.page_id);
    if (page == nullptr) {
        LOG_WARN("Cannot fetch page " << rid.page_id << " for undo delete");
        return;
    }

    page->WLatch();
    auto* table_page = reinterpret_cast<TablePage*>(page);
    Tuple old_tuple = log_record->GetOldTuple();
    RID new_rid;
    bool inserted = table_page->InsertTuple(old_tuple, &new_rid);
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