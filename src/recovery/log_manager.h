#pragma once

#include <atomic>
#include <condition_variable>
#include <future>
#include <mutex>
#include <queue>
#include "common/config.h"
#include "recovery/log_record.h"
#include "storage/disk_manager.h"

namespace SimpleRDBMS {

class LogManager {
public:
    explicit LogManager(DiskManager* disk_manager);
    ~LogManager();
    
    // Append a log record
    lsn_t AppendLogRecord(LogRecord* log_record);
    
    // Flush all logs to disk
    void Flush(lsn_t lsn = -1);
    
    // Get the next LSN
    lsn_t GetNextLSN() { return next_lsn_.fetch_add(1); }
    
    // Get persistent LSN (flushed to disk)
    lsn_t GetPersistentLSN() const { return persistent_lsn_.load(); }
    
    // Read log records from disk
    std::vector<std::unique_ptr<LogRecord>> ReadLogRecords();
    
    // Enable/Disable logging
    void SetEnable(bool enable) { enable_logging_ = enable; }

private:
    DiskManager* disk_manager_;
    
    // Log buffer
    char* log_buffer_;
    size_t log_buffer_size_;
    size_t log_buffer_offset_;
    
    // LSN tracking
    std::atomic<lsn_t> next_lsn_{0};
    std::atomic<lsn_t> persistent_lsn_{INVALID_LSN};
    
    // Thread safety
    std::mutex latch_;
    std::condition_variable flush_cv_;
    
    // Background flush thread (simplified - not used)
    std::atomic<bool> flush_thread_running_{false};
    
    // Logging flag
    bool enable_logging_{true};

    // 当前日志页面ID
    page_id_t current_log_page_id_{0};
    
    // Helper functions
    void FlushLogBuffer();
    void BackgroundFlush();
};

}  // namespace SimpleRDBMS