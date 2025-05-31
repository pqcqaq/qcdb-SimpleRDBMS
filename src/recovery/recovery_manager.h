#pragma once

#include <unordered_map>
#include <unordered_set>
#include "buffer/buffer_pool_manager.h"
#include "catalog/catalog.h"
#include "recovery/log_manager.h"
#include "transaction/lock_manager.h"

namespace SimpleRDBMS {

class RecoveryManager {
public:
    RecoveryManager(BufferPoolManager* buffer_pool_manager,
                    Catalog* catalog,
                    LogManager* log_manager,
                    LockManager* lock_manager);
    
    // Perform recovery after crash
    void Recover();
    
    // Create a checkpoint
    void Checkpoint();

private:
    BufferPoolManager* buffer_pool_manager_;
    Catalog* catalog_;
    LogManager* log_manager_;
    LockManager* lock_manager_;
    
    // Active transaction table (ATT)
    std::unordered_map<txn_id_t, lsn_t> active_txn_table_;
    
    // Dirty page table (DPT)
    std::unordered_map<page_id_t, lsn_t> dirty_page_table_;
    
    // Helper functions for ARIES
    void AnalysisPhase(const std::vector<std::unique_ptr<LogRecord>>& log_records);
    void RedoPhase(const std::vector<std::unique_ptr<LogRecord>>& log_records);
    void UndoPhase();
    
    // Apply log record
    void RedoInsert(const InsertLogRecord* log_record);
    void RedoUpdate(const UpdateLogRecord* log_record);
    // void RedoDelete(const DeleteLogRecord* log_record);
    
    void UndoInsert(const InsertLogRecord* log_record);
    void UndoUpdate(const UpdateLogRecord* log_record);
    // void UndoDelete(const DeleteLogRecord* log_record);
};

}  // namespace SimpleRDBMS
