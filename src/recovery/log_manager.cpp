#include "recovery/log_manager.h"
#include "recovery/log_record.h"
#include "storage/disk_manager.h"
#include "common/config.h"
#include "common/debug.h"
#include <cstring>

namespace SimpleRDBMS {

LogManager::LogManager(DiskManager* disk_manager)
    : disk_manager_(disk_manager),
      log_buffer_size_(PAGE_SIZE),
      log_buffer_offset_(0),
      current_log_page_id_(0) {
    if (disk_manager_ == nullptr) {
        throw std::invalid_argument("DiskManager cannot be null");
    }
    log_buffer_ = new char[log_buffer_size_];
    memset(log_buffer_, 0, log_buffer_size_);
    next_lsn_.store(1);
    persistent_lsn_.store(0);
    flush_thread_running_ = false;
    
    LOG_DEBUG("LogManager initialized with buffer size: " << log_buffer_size_);
}

LogManager::~LogManager() {
    try {
        if (log_buffer_offset_ > 0) {
            FlushLogBuffer();
        }
    } catch (const std::exception& e) {
        LOG_ERROR("Exception in LogManager destructor: " << e.what());
    } catch (...) {
        LOG_ERROR("Unknown exception in LogManager destructor");
    }
    delete[] log_buffer_;
    LOG_DEBUG("LogManager destroyed");
}

lsn_t LogManager::AppendLogRecord(LogRecord* log_record) {
    if (!enable_logging_ || log_record == nullptr) {
        return INVALID_LSN;
    }
    
    std::unique_lock<std::mutex> lock(latch_);
    lsn_t lsn = next_lsn_.fetch_add(1);
    
    size_t header_size = sizeof(LogRecordType) + sizeof(txn_id_t) + sizeof(lsn_t);
    size_t record_data_size = log_record->GetLogRecordSize();
    size_t total_record_size = header_size + record_data_size;
    size_t total_size_with_length = sizeof(uint32_t) + total_record_size;
    
    LOG_DEBUG("AppendLogRecord: LSN=" << lsn 
              << ", type=" << static_cast<int>(log_record->GetType())
              << ", txn=" << log_record->GetTxnId()
              << ", size=" << total_size_with_length);
    
    if (log_buffer_offset_ + total_size_with_length > log_buffer_size_) {
        LOG_DEBUG("Log buffer full, flushing before append");
        FlushLogBuffer();
    }
    
    char* buffer_ptr = log_buffer_ + log_buffer_offset_;
    *reinterpret_cast<uint32_t*>(buffer_ptr) = static_cast<uint32_t>(total_record_size);
    buffer_ptr += sizeof(uint32_t);
    
    *reinterpret_cast<LogRecordType*>(buffer_ptr) = log_record->GetType();
    buffer_ptr += sizeof(LogRecordType);
    
    *reinterpret_cast<txn_id_t*>(buffer_ptr) = log_record->GetTxnId();
    buffer_ptr += sizeof(txn_id_t);
    
    *reinterpret_cast<lsn_t*>(buffer_ptr) = log_record->GetPrevLSN();
    buffer_ptr += sizeof(lsn_t);
    
    if (record_data_size > 0) {
        log_record->SerializeTo(buffer_ptr);
    }
    
    log_buffer_offset_ += total_size_with_length;
    
    return lsn;
}

void LogManager::Flush(lsn_t lsn) {
    std::unique_lock<std::mutex> lock(latch_);
    
    if (log_buffer_offset_ == 0) {
        LOG_DEBUG("Log buffer empty, nothing to flush");
        return;
    }
    
    LOG_DEBUG("Flushing log buffer to disk, buffer size: " << log_buffer_offset_);
    
    try {
        FlushLogBuffer();
        
        if (lsn != -1) {
            persistent_lsn_.store(lsn);
        } else {
            persistent_lsn_.store(next_lsn_.load() - 1);
        }
        
        LOG_DEBUG("Log flush completed, persistent LSN: " << persistent_lsn_.load());
    } catch (const std::exception& e) {
        LOG_ERROR("Exception during log flush: " << e.what());
        throw;
    }
}

void LogManager::FlushLogBuffer() {
    if (log_buffer_offset_ == 0) {
        return;
    }
    
    try {
        // 分配新的日志页面
        page_id_t log_page_id = disk_manager_->AllocatePage();
        
        // 创建一个完整的页面缓冲区
        char page_buffer[PAGE_SIZE];
        memset(page_buffer, 0, PAGE_SIZE);
        
        // 复制日志数据到页面缓冲区
        memcpy(page_buffer, log_buffer_, log_buffer_offset_);
        
        // 写入磁盘
        disk_manager_->WritePage(log_page_id, page_buffer);
        
        LOG_DEBUG("Log buffer flushed to page " << log_page_id 
                  << ", bytes written: " << log_buffer_offset_);
        
        // 重置缓冲区
        log_buffer_offset_ = 0;
        memset(log_buffer_, 0, log_buffer_size_);
        
        current_log_page_id_ = log_page_id;
        
    } catch (const std::exception& e) {
        LOG_ERROR("Failed to flush log buffer: " << e.what());
        throw;
    }
}

void LogManager::BackgroundFlush() {
    // 实现可选，目前保持空
}

std::vector<std::unique_ptr<LogRecord>> LogManager::ReadLogRecords() {
    std::unique_lock<std::mutex> lock(latch_);
    std::vector<std::unique_ptr<LogRecord>> log_records;
    
    // 获取磁盘上的总页面数
    int total_pages = disk_manager_->GetNumPages();
    LOG_DEBUG("ReadLogRecords: Scanning " << total_pages << " pages for log records");
    
    if (total_pages <= 0) {
        LOG_DEBUG("No pages on disk, no log records to read");
        return log_records;
    }
    
    // 扫描所有页面寻找日志记录
    for (page_id_t page_id = 0; page_id < total_pages; page_id++) {
        try {
            char page_buffer[PAGE_SIZE];
            disk_manager_->ReadPage(page_id, page_buffer);
            
            // 检查页面是否包含日志记录（简单的启发式检查）
            // 如果页面开始处不是一个合理的记录大小，跳过此页面
            if (page_id > 0) {  // 跳过第一个页面（通常是元数据页面）
                uint32_t first_record_size = *reinterpret_cast<uint32_t*>(page_buffer);
                if (first_record_size == 0 || first_record_size > PAGE_SIZE) {
                    continue;  // 这个页面可能不包含日志记录
                }
            }
            
            // 解析页面中的日志记录
            size_t offset = 0;
            int records_in_page = 0;
            
            while (offset + sizeof(uint32_t) <= PAGE_SIZE) {
                uint32_t record_size = *reinterpret_cast<uint32_t*>(page_buffer + offset);
                
                // 如果记录大小为0或过大，说明已经到了页面末尾
                if (record_size == 0 || record_size > PAGE_SIZE || 
                    offset + sizeof(uint32_t) + record_size > PAGE_SIZE) {
                    break;
                }
                
                offset += sizeof(uint32_t);
                
                // 确保有足够空间读取记录头部
                if (offset + sizeof(LogRecordType) + sizeof(txn_id_t) + sizeof(lsn_t) > PAGE_SIZE) {
                    break;
                }
                
                LogRecordType type = *reinterpret_cast<LogRecordType*>(page_buffer + offset);
                offset += sizeof(LogRecordType);
                
                txn_id_t txn_id = *reinterpret_cast<txn_id_t*>(page_buffer + offset);
                offset += sizeof(txn_id_t);
                
                lsn_t prev_lsn = *reinterpret_cast<lsn_t*>(page_buffer + offset);
                offset += sizeof(lsn_t);
                
                // 创建对应的日志记录对象
                std::unique_ptr<LogRecord> record;
                switch (type) {
                    case LogRecordType::BEGIN:
                        record = std::make_unique<BeginLogRecord>(txn_id);
                        LOG_DEBUG("Found BEGIN record for txn " << txn_id);
                        break;
                    case LogRecordType::COMMIT:
                        record = std::make_unique<CommitLogRecord>(txn_id, prev_lsn);
                        LOG_DEBUG("Found COMMIT record for txn " << txn_id);
                        break;
                    case LogRecordType::ABORT:
                        record = std::make_unique<AbortLogRecord>(txn_id, prev_lsn);
                        LOG_DEBUG("Found ABORT record for txn " << txn_id);
                        break;
                    case LogRecordType::INSERT:
                    case LogRecordType::UPDATE:
                    case LogRecordType::DELETE:
                        // 对于数据操作记录，我们创建一个简化的记录
                        // 这里我们不需要完整的数据，只需要知道事务ID
                        LOG_DEBUG("Found " << static_cast<int>(type) << " record for txn " << txn_id);
                        // 跳过数据部分，但记录事务有数据操作
                        {
                            size_t remaining_size = record_size - sizeof(LogRecordType) - sizeof(txn_id_t) - sizeof(lsn_t);
                            offset += remaining_size;
                            
                            // 创建一个虚拟的日志记录来表示数据操作
                            // 我们使用BEGIN记录类型来标记这个事务有活动
                            record = std::make_unique<BeginLogRecord>(txn_id);
                        }
                        break;
                    default:
                        LOG_DEBUG("Unknown log record type: " << static_cast<int>(type));
                        // 跳过未知类型的记录
                        {
                            size_t remaining_size = record_size - sizeof(LogRecordType) - sizeof(txn_id_t) - sizeof(lsn_t);
                            offset += remaining_size;
                        }
                        continue;
                }
                
                if (record) {
                    log_records.push_back(std::move(record));
                    records_in_page++;
                }
            }
            
            if (records_in_page > 0) {
                LOG_DEBUG("Found " << records_in_page << " records in page " << page_id);
            }
            
        } catch (const std::exception& e) {
            LOG_DEBUG("Exception reading page " << page_id << ": " << e.what());
            continue;
        }
    }
    
    LOG_DEBUG("ReadLogRecords: Found " << log_records.size() << " total log records");
    return log_records;
}

}