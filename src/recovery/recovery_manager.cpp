// src/recovery/recovery_manager.cpp
#include "recovery/recovery_manager.h"
#include "recovery/log_record.h"
#include "record/table_heap.h"
#include <algorithm>

namespace SimpleRDBMS {

RecoveryManager::RecoveryManager(BufferPoolManager* buffer_pool_manager,
                                 Catalog* catalog,
                                 LogManager* log_manager,
                                 LockManager* lock_manager)
    : buffer_pool_manager_(buffer_pool_manager),
      catalog_(catalog),
      log_manager_(log_manager),
      lock_manager_(lock_manager) {
}

void RecoveryManager::Recover() {
    // Read all log records from disk
    auto log_records = log_manager_->ReadLogRecords();
    
    if (log_records.empty()) {
        return;
    }
    
    // Phase 1: Analysis
    AnalysisPhase(log_records);
    
    // Phase 2: Redo
    RedoPhase(log_records);
    
    // Phase 3: Undo
    UndoPhase();
}

void RecoveryManager::Checkpoint() {
    // 1. Write BEGIN_CHECKPOINT log record
    // CheckpointLogRecord begin_ckpt(INVALID_TXN_ID);
    // lsn_t begin_lsn = log_manager_->AppendLogRecord(&begin_ckpt);
    
    // 2. Flush all dirty pages
    buffer_pool_manager_->FlushAllPages();
    
    // 3. Write END_CHECKPOINT log record with ATT and DPT
    // EndCheckpointLogRecord end_ckpt(active_txn_table_, dirty_page_table_);
    // lsn_t end_lsn = log_manager_->AppendLogRecord(&end_ckpt);
    
    // 4. Update master record with checkpoint LSN
    // For simplicity, we'll just flush the log
    log_manager_->Flush();
}

void RecoveryManager::AnalysisPhase(const std::vector<std::unique_ptr<LogRecord>>& log_records) {
    for (const auto& log_record : log_records) {
        txn_id_t txn_id = log_record->GetTxnId();
        
        switch (log_record->GetType()) {
            case LogRecordType::BEGIN:
                active_txn_table_[txn_id] = log_record->GetPrevLSN();
                break;
                
            case LogRecordType::COMMIT:
            case LogRecordType::ABORT:
                active_txn_table_.erase(txn_id);
                break;
                
            case LogRecordType::INSERT:
            case LogRecordType::UPDATE:
            case LogRecordType::DELETE:
                // Update active transaction table
                active_txn_table_[txn_id] = log_record->GetPrevLSN();
                
                // Update dirty page table
                // TODO: Extract page_id from log record and update DPT
                break;
                
            default:
                break;
        }
    }
}

void RecoveryManager::RedoPhase(const std::vector<std::unique_ptr<LogRecord>>& log_records) {
    for (const auto& log_record : log_records) {
        switch (log_record->GetType()) {
            case LogRecordType::INSERT:
                RedoInsert(static_cast<InsertLogRecord*>(log_record.get()));
                break;
                
            case LogRecordType::UPDATE:
                RedoUpdate(static_cast<UpdateLogRecord*>(log_record.get()));
                break;
                
            case LogRecordType::DELETE:
                // TODO: Implement delete redo
                break;
                
            default:
                // Transaction control records don't need redo
                break;
        }
    }
}

void RecoveryManager::UndoPhase() {
    // Find all transactions to undo
    std::vector<txn_id_t> txns_to_undo;
    for (const auto& [txn_id, lsn] : active_txn_table_) {
        txns_to_undo.push_back(txn_id);
    }
    
    if (txns_to_undo.empty()) {
        return;
    }
    
    // Process transactions in reverse LSN order
    while (!txns_to_undo.empty()) {
        // Find transaction with highest LSN
        txn_id_t max_txn_id = INVALID_TXN_ID;
        lsn_t max_lsn = INVALID_LSN;
        
        for (txn_id_t txn_id : txns_to_undo) {
            if (active_txn_table_[txn_id] > max_lsn) {
                max_lsn = active_txn_table_[txn_id];
                max_txn_id = txn_id;
            }
        }
        
        if (max_txn_id == INVALID_TXN_ID) {
            break;
        }
        
        // Undo operations for this transaction
        // This is simplified - in reality, we'd follow the log chain
        // For now, we'll just remove it from active transactions
        active_txn_table_.erase(max_txn_id);
        txns_to_undo.erase(
            std::remove(txns_to_undo.begin(), txns_to_undo.end(), max_txn_id),
            txns_to_undo.end()
        );
        
        // Write ABORT record
        AbortLogRecord abort_record(max_txn_id, max_lsn);
        log_manager_->AppendLogRecord(&abort_record);
    }
}

void RecoveryManager::RedoInsert(const InsertLogRecord* log_record) {
    RID rid = log_record->GetRID();
    Page* page = buffer_pool_manager_->FetchPage(rid.page_id);
    
    if (page == nullptr) {
        return;
    }
    
    // Check if redo is needed by comparing page LSN
    if (page->GetLSN() >= log_record->GetPrevLSN()) {
        buffer_pool_manager_->UnpinPage(rid.page_id, false);
        return;
    }
    
    // Apply the insert
    auto* table_page = reinterpret_cast<TablePage*>(page);
    Tuple tuple = log_record->GetTuple();
    RID new_rid;
    
    if (table_page->InsertTuple(tuple, &new_rid)) {
        page->SetLSN(log_record->GetPrevLSN());
    }
    
    buffer_pool_manager_->UnpinPage(rid.page_id, true);
}

void RecoveryManager::RedoUpdate(const UpdateLogRecord* log_record) {
    RID rid = log_record->GetRID();
    Page* page = buffer_pool_manager_->FetchPage(rid.page_id);
    
    if (page == nullptr) {
        return;
    }
    
    // Check if redo is needed
    if (page->GetLSN() >= log_record->GetPrevLSN()) {
        buffer_pool_manager_->UnpinPage(rid.page_id, false);
        return;
    }
    
    // Apply the update
    auto* table_page = reinterpret_cast<TablePage*>(page);
    Tuple new_tuple = log_record->GetNewTuple();
    
    if (table_page->UpdateTuple(new_tuple, rid)) {
        page->SetLSN(log_record->GetPrevLSN());
    }
    
    buffer_pool_manager_->UnpinPage(rid.page_id, true);
}

void RecoveryManager::UndoInsert(const InsertLogRecord* log_record) {
    RID rid = log_record->GetRID();
    Page* page = buffer_pool_manager_->FetchPage(rid.page_id);
    
    if (page == nullptr) {
        return;
    }
    
    // Delete the inserted tuple
    auto* table_page = reinterpret_cast<TablePage*>(page);
    table_page->DeleteTuple(rid);
    
    buffer_pool_manager_->UnpinPage(rid.page_id, true);
}

void RecoveryManager::UndoUpdate(const UpdateLogRecord* log_record) {
    RID rid = log_record->GetRID();
    Page* page = buffer_pool_manager_->FetchPage(rid.page_id);
    
    if (page == nullptr) {
        return;
    }
    
    // Restore old tuple
    auto* table_page = reinterpret_cast<TablePage*>(page);
    Tuple old_tuple = log_record->GetOldTuple();
    
    table_page->UpdateTuple(old_tuple, rid);
    
    buffer_pool_manager_->UnpinPage(rid.page_id, true);
}

}  // namespace SimpleRDBMS