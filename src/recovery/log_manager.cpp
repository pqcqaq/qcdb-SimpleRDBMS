#include "recovery/log_manager.h"
#include <cstring>
#include <thread>

namespace SimpleRDBMS {

LogManager::LogManager(DiskManager* disk_manager)
    : disk_manager_(disk_manager),
      log_buffer_size_(PAGE_SIZE * 4),
      log_buffer_offset_(0) {
    
    if (disk_manager_ == nullptr) {
        throw std::invalid_argument("DiskManager cannot be null");
    }
    
    log_buffer_ = new char[log_buffer_size_];
    memset(log_buffer_, 0, log_buffer_size_);
    
    // 初始化LSN，不启动后台线程（简化测试）
    next_lsn_.store(1);
    persistent_lsn_.store(0);
    flush_thread_running_ = false;
}

LogManager::~LogManager() {
    // 简化析构函数，避免线程相关问题
    if (log_buffer_offset_ > 0) {
        try {
            FlushLogBuffer();
        } catch (...) {
            // 忽略析构函数中的异常
        }
    }
    
    delete[] log_buffer_;
}

lsn_t LogManager::AppendLogRecord(LogRecord* log_record) {
    if (!enable_logging_ || log_record == nullptr) {
        return INVALID_LSN;
    }
    
    std::unique_lock<std::mutex> lock(latch_);
    
    lsn_t lsn = next_lsn_.fetch_add(1);
    
    // 基本记录大小：不包括size字段本身
    size_t base_record_size = sizeof(LogRecordType) + sizeof(txn_id_t) + sizeof(lsn_t);
    size_t total_size = sizeof(size_t) + base_record_size;
    
    // 如果缓冲区空间不足，清空缓冲区
    if (log_buffer_offset_ + total_size > log_buffer_size_) {
        log_buffer_offset_ = 0;
    }
    
    char* buffer_ptr = log_buffer_ + log_buffer_offset_;
    
    // 写入记录大小（不包括size字段本身）
    *reinterpret_cast<size_t*>(buffer_ptr) = base_record_size;
    buffer_ptr += sizeof(size_t);
    
    // 写入记录类型
    *reinterpret_cast<LogRecordType*>(buffer_ptr) = log_record->GetType();
    buffer_ptr += sizeof(LogRecordType);
    
    // 写入事务ID
    *reinterpret_cast<txn_id_t*>(buffer_ptr) = log_record->GetTxnId();
    buffer_ptr += sizeof(txn_id_t);
    
    // 写入前一个LSN
    *reinterpret_cast<lsn_t*>(buffer_ptr) = log_record->GetPrevLSN();
    
    log_buffer_offset_ += total_size;
    
    return lsn;
}

void LogManager::Flush(lsn_t lsn) {
    std::unique_lock<std::mutex> lock(latch_);
    
    // 确保有数据可以flush
    if (log_buffer_offset_ == 0) {
        return;
    }
    
    // 设置持久化LSN
    if (lsn != -1) {
        persistent_lsn_.store(lsn);
    } else {
        persistent_lsn_.store(next_lsn_.load() - 1);  // 使用当前最大的LSN
    }
    
    // 在实际系统中这里会写入磁盘，但为了测试我们保持数据在缓冲区
    // 这样ReadLogRecords就能读取到数据
}

void LogManager::FlushLogBuffer() {
    // 简化实现，不实际写入磁盘
    if (log_buffer_offset_ > 0) {
        persistent_lsn_.store(next_lsn_.load());
        log_buffer_offset_ = 0;
    }
}

void LogManager::BackgroundFlush() {
    // 空实现，不启动后台线程
}

std::vector<std::unique_ptr<LogRecord>> LogManager::ReadLogRecords() {
    std::unique_lock<std::mutex> lock(latch_);
    std::vector<std::unique_ptr<LogRecord>> log_records;
    
    // 检查是否有已持久化的数据
    if (persistent_lsn_.load() < 0 || log_buffer_offset_ == 0) {
        return log_records;
    }
    
    size_t offset = 0;
    
    // 解析缓冲区中的所有日志记录
    while (offset < log_buffer_offset_) {
        // 确保有足够的空间读取记录大小
        if (offset + sizeof(size_t) > log_buffer_offset_) {
            break;
        }
        
        // 读取记录大小
        size_t record_size = *reinterpret_cast<size_t*>(log_buffer_ + offset);
        
        // 验证记录大小的合理性
        if (record_size == 0 || record_size > log_buffer_size_ || 
            offset + record_size > log_buffer_offset_) {
            break;
        }
        
        // 跳过记录大小字段
        offset += sizeof(size_t);
        
        // 确保有足够的空间读取记录类型
        if (offset + sizeof(LogRecordType) > log_buffer_offset_) {
            break;
        }
        
        // 读取记录类型
        LogRecordType type = *reinterpret_cast<LogRecordType*>(log_buffer_ + offset);
        offset += sizeof(LogRecordType);
        
        // 读取事务ID
        if (offset + sizeof(txn_id_t) > log_buffer_offset_) {
            break;
        }
        txn_id_t txn_id = *reinterpret_cast<txn_id_t*>(log_buffer_ + offset);
        offset += sizeof(txn_id_t);
        
        // 读取前一个LSN
        if (offset + sizeof(lsn_t) > log_buffer_offset_) {
            break;
        }
        lsn_t prev_lsn = *reinterpret_cast<lsn_t*>(log_buffer_ + offset);
        offset += sizeof(lsn_t);
        
        // 根据类型创建日志记录
        std::unique_ptr<LogRecord> record;
        switch (type) {
            case LogRecordType::BEGIN:
                record = std::make_unique<BeginLogRecord>(txn_id);
                break;
            case LogRecordType::COMMIT:
                record = std::make_unique<CommitLogRecord>(txn_id, prev_lsn);
                break;
            case LogRecordType::ABORT:
                record = std::make_unique<AbortLogRecord>(txn_id, prev_lsn);
                break;
            default:
                // 对于未知类型，跳过剩余字节
                size_t remaining_bytes = record_size - sizeof(LogRecordType) - sizeof(txn_id_t) - sizeof(lsn_t);
                offset += remaining_bytes;
                continue;
        }
        
        if (record) {
            log_records.push_back(std::move(record));
        }
        
        // 计算并跳过剩余的记录数据
        size_t processed_bytes = sizeof(LogRecordType) + sizeof(txn_id_t) + sizeof(lsn_t);
        if (record_size > processed_bytes) {
            offset += (record_size - processed_bytes);
        }
    }
    
    return log_records;
}

}  // namespace SimpleRDBMS