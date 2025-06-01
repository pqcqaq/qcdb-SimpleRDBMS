/*
 * 文件: log_manager.cpp
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述: WAL日志管理器实现，负责日志记录的写入、缓冲、刷盘和读取操作
 */

#include "recovery/log_manager.h"

#include <cstring>

#include "common/config.h"
#include "common/debug.h"
#include "recovery/log_record.h"
#include "storage/disk_manager.h"

namespace SimpleRDBMS {

/**
 * LogManager构造函数
 * @param disk_manager 磁盘管理器指针，用于实际的页面读写操作
 */
LogManager::LogManager(DiskManager* disk_manager)
    : disk_manager_(disk_manager),
      log_buffer_size_(PAGE_SIZE),
      log_buffer_offset_(0),
      current_log_page_id_(0) {
    // 参数合法性检查
    if (disk_manager_ == nullptr) {
        throw std::invalid_argument("DiskManager cannot be null");
    }

    // 初始化日志缓冲区，大小为一个页面
    log_buffer_ = new char[log_buffer_size_];
    memset(log_buffer_, 0, log_buffer_size_);

    // 初始化LSN相关变量
    // next_lsn_: 下一个要分配的LSN，从1开始
    // persistent_lsn_: 已经持久化到磁盘的最大LSN
    next_lsn_.store(1);
    persistent_lsn_.store(0);
    flush_thread_running_ = false;

    LOG_DEBUG("LogManager initialized with buffer size: " << log_buffer_size_);
}

/**
 * LogManager析构函数
 * 确保所有缓冲区内容都已刷盘
 */
LogManager::~LogManager() {
    try {
        // 如果缓冲区还有数据，先刷盘再销毁
        if (log_buffer_offset_ > 0) {
            FlushLogBuffer();
        }
    } catch (const std::exception& e) {
        LOG_ERROR("Exception in LogManager destructor: " << e.what());
    } catch (...) {
        LOG_ERROR("Unknown exception in LogManager destructor");
    }

    // 释放缓冲区内存
    delete[] log_buffer_;
    LOG_DEBUG("LogManager destroyed");
}

/**
 * 追加日志记录到缓冲区
 * @param log_record 要写入的日志记录
 * @return 分配给该记录的LSN，如果失败返回INVALID_LSN
 */
lsn_t LogManager::AppendLogRecord(LogRecord* log_record) {
    // 如果日志功能未启用或记录为空，直接返回
    if (!enable_logging_ || log_record == nullptr) {
        return INVALID_LSN;
    }

    // 获取互斥锁，保证线程安全
    std::unique_lock<std::mutex> lock(latch_);

    // 分配新的LSN，使用原子操作确保唯一性
    lsn_t lsn = next_lsn_.fetch_add(1);

    // 计算记录的各个部分大小
    // header包含：记录类型 + 事务ID + 前一个LSN
    size_t header_size =
        sizeof(LogRecordType) + sizeof(txn_id_t) + sizeof(lsn_t);
    size_t record_data_size = log_record->GetLogRecordSize();
    size_t total_record_size = header_size + record_data_size;
    // 每条记录前还要存储记录长度(4字节)
    size_t total_size_with_length = sizeof(uint32_t) + total_record_size;

    LOG_DEBUG("AppendLogRecord: LSN=" << lsn << ", type="
                                      << static_cast<int>(log_record->GetType())
                                      << ", txn=" << log_record->GetTxnId()
                                      << ", size=" << total_size_with_length);

    // 检查缓冲区是否有足够空间，如果不够就先刷盘
    if (log_buffer_offset_ + total_size_with_length > log_buffer_size_) {
        LOG_DEBUG("Log buffer full, flushing before append");
        FlushLogBuffer();
    }

    // 开始序列化日志记录到缓冲区
    char* buffer_ptr = log_buffer_ + log_buffer_offset_;

    // 1. 写入记录总长度
    *reinterpret_cast<uint32_t*>(buffer_ptr) =
        static_cast<uint32_t>(total_record_size);
    buffer_ptr += sizeof(uint32_t);

    // 2. 写入记录类型
    *reinterpret_cast<LogRecordType*>(buffer_ptr) = log_record->GetType();
    buffer_ptr += sizeof(LogRecordType);

    // 3. 写入事务ID
    *reinterpret_cast<txn_id_t*>(buffer_ptr) = log_record->GetTxnId();
    buffer_ptr += sizeof(txn_id_t);

    // 4. 写入前一个LSN（用于链接同一事务的日志记录）
    *reinterpret_cast<lsn_t*>(buffer_ptr) = log_record->GetPrevLSN();
    buffer_ptr += sizeof(lsn_t);

    // 5. 如果有额外数据，调用记录自己的序列化方法
    if (record_data_size > 0) {
        log_record->SerializeTo(buffer_ptr);
    }

    // 更新缓冲区偏移量
    log_buffer_offset_ += total_size_with_length;

    return lsn;
}

/**
 * 强制刷盘操作，确保指定LSN之前的所有日志都持久化
 * @param lsn 需要持久化的LSN，-1表示刷新所有当前缓冲区内容
 */
void LogManager::Flush(lsn_t lsn) {
    std::unique_lock<std::mutex> lock(latch_);

    // 如果缓冲区为空，没有内容需要刷盘
    if (log_buffer_offset_ == 0) {
        LOG_DEBUG("Log buffer empty, nothing to flush");
        return;
    }

    LOG_DEBUG(
        "Flushing log buffer to disk, buffer size: " << log_buffer_offset_);

    try {
        // 执行实际的缓冲区刷盘操作
        FlushLogBuffer();

        // 更新持久化LSN标记
        if (lsn != -1) {
            persistent_lsn_.store(lsn);
        } else {
            // 如果没有指定LSN，就用当前最大的LSN减1
            persistent_lsn_.store(next_lsn_.load() - 1);
        }

        LOG_DEBUG(
            "Log flush completed, persistent LSN: " << persistent_lsn_.load());
    } catch (const std::exception& e) {
        LOG_ERROR("Exception during log flush: " << e.what());
        throw;
    }
}

/**
 * 内部方法：将日志缓冲区内容写入磁盘页面
 * 这里实现WAL的核心机制 - 先写日志再写数据
 */
void LogManager::FlushLogBuffer() {
    // 如果缓冲区为空，直接返回
    if (log_buffer_offset_ == 0) {
        return;
    }

    try {
        // 向disk_manager申请一个新的页面来存储日志
        page_id_t log_page_id = disk_manager_->AllocatePage();

        // 创建一个完整页面大小的缓冲区，确保写入完整页面
        char page_buffer[PAGE_SIZE];
        memset(page_buffer, 0, PAGE_SIZE);

        // 将日志缓冲区的内容复制到页面缓冲区
        memcpy(page_buffer, log_buffer_, log_buffer_offset_);

        // 通过disk_manager写入磁盘
        disk_manager_->WritePage(log_page_id, page_buffer);

        LOG_DEBUG("Log buffer flushed to page "
                  << log_page_id << ", bytes written: " << log_buffer_offset_);

        // 重置缓冲区状态，准备接收新的日志记录
        log_buffer_offset_ = 0;
        memset(log_buffer_, 0, log_buffer_size_);

        // 记录当前使用的日志页面ID
        current_log_page_id_ = log_page_id;

    } catch (const std::exception& e) {
        LOG_ERROR("Failed to flush log buffer: " << e.what());
        throw;
    }
}

/**
 * 后台异步刷盘方法（预留接口）
 * 可以实现定期自动刷盘，避免缓冲区积累过多数据
 */
void LogManager::BackgroundFlush() {
    // 目前暂时不实现，可以后续添加定时刷盘逻辑
}

/**
 * 从磁盘读取所有日志记录，主要用于崩溃恢复
 * 扫描所有磁盘页面，解析其中的日志记录
 * @return 包含所有日志记录的vector
 */
std::vector<std::unique_ptr<LogRecord>> LogManager::ReadLogRecords() {
    std::unique_lock<std::mutex> lock(latch_);
    std::vector<std::unique_ptr<LogRecord>> log_records;

    // 获取磁盘上的总页面数，需要扫描所有页面找日志
    int total_pages = disk_manager_->GetNumPages();
    LOG_DEBUG("ReadLogRecords: Scanning " << total_pages
                                          << " pages for log records");

    if (total_pages <= 0) {
        LOG_DEBUG("No pages on disk, no log records to read");
        return log_records;
    }

    // 逐页扫描寻找日志记录
    for (page_id_t page_id = 0; page_id < total_pages; page_id++) {
        try {
            char page_buffer[PAGE_SIZE];
            disk_manager_->ReadPage(page_id, page_buffer);

            // 简单的启发式判断：检查页面是否包含有效的日志记录
            // 通过检查第一个记录长度是否合理来判断
            if (page_id > 0) {  // 跳过第一个页面（通常是系统元数据页面）
                uint32_t first_record_size =
                    *reinterpret_cast<uint32_t*>(page_buffer);
                if (first_record_size == 0 || first_record_size > PAGE_SIZE) {
                    continue;  // 这个页面可能不是日志页面
                }
            }

            // 解析当前页面中的所有日志记录
            size_t offset = 0;
            int records_in_page = 0;

            // 按照我们的日志格式逐条解析记录
            while (offset + sizeof(uint32_t) <= PAGE_SIZE) {
                // 读取记录长度
                uint32_t record_size =
                    *reinterpret_cast<uint32_t*>(page_buffer + offset);

                // 检查记录长度的合理性
                if (record_size == 0 || record_size > PAGE_SIZE ||
                    offset + sizeof(uint32_t) + record_size > PAGE_SIZE) {
                    break;  // 到达页面末尾或遇到无效记录
                }

                offset += sizeof(uint32_t);

                // 确保有足够空间读取记录头部信息
                if (offset + sizeof(LogRecordType) + sizeof(txn_id_t) +
                        sizeof(lsn_t) >
                    PAGE_SIZE) {
                    break;
                }

                // 解析记录头部：类型、事务ID、前一个LSN
                LogRecordType type =
                    *reinterpret_cast<LogRecordType*>(page_buffer + offset);
                offset += sizeof(LogRecordType);

                txn_id_t txn_id =
                    *reinterpret_cast<txn_id_t*>(page_buffer + offset);
                offset += sizeof(txn_id_t);

                lsn_t prev_lsn =
                    *reinterpret_cast<lsn_t*>(page_buffer + offset);
                offset += sizeof(lsn_t);

                // 根据记录类型创建对应的日志记录对象
                std::unique_ptr<LogRecord> record;
                switch (type) {
                    case LogRecordType::BEGIN:
                        record = std::make_unique<BeginLogRecord>(txn_id);
                        LOG_DEBUG("Found BEGIN record for txn " << txn_id);
                        break;
                    case LogRecordType::COMMIT:
                        record =
                            std::make_unique<CommitLogRecord>(txn_id, prev_lsn);
                        LOG_DEBUG("Found COMMIT record for txn " << txn_id);
                        break;
                    case LogRecordType::ABORT:
                        record =
                            std::make_unique<AbortLogRecord>(txn_id, prev_lsn);
                        LOG_DEBUG("Found ABORT record for txn " << txn_id);
                        break;
                    case LogRecordType::INSERT:
                        LOG_DEBUG("Found INSERT record for txn " << txn_id);
                        {
                            size_t remaining_size =
                                record_size - sizeof(LogRecordType) -
                                sizeof(txn_id_t) - sizeof(lsn_t);
                            offset += remaining_size;
                            record = std::make_unique<BeginLogRecord>(
                                txn_id);  // 简化处理
                        }
                        break;
                    case LogRecordType::UPDATE:
                        LOG_DEBUG("Found UPDATE record for txn " << txn_id);
                        {
                            size_t remaining_size =
                                record_size - sizeof(LogRecordType) -
                                sizeof(txn_id_t) - sizeof(lsn_t);
                            offset += remaining_size;
                            record = std::make_unique<BeginLogRecord>(
                                txn_id);  // 简化处理
                        }
                        break;
                    case LogRecordType::DELETE:
                        LOG_DEBUG("Found DELETE record for txn " << txn_id);
                        {
                            size_t remaining_size =
                                record_size - sizeof(LogRecordType) -
                                sizeof(txn_id_t) - sizeof(lsn_t);
                            offset += remaining_size;
                            record = std::make_unique<BeginLogRecord>(
                                txn_id);  // 简化处理
                        }
                        break;
                    default:
                        LOG_DEBUG("Unknown log record type: "
                                  << static_cast<int>(type));
                        // 跳过未知类型的记录，继续处理后续记录
                        {
                            size_t remaining_size =
                                record_size - sizeof(LogRecordType) -
                                sizeof(txn_id_t) - sizeof(lsn_t);
                            offset += remaining_size;
                        }
                        continue;
                }

                // 将解析出的记录添加到结果集合
                if (record) {
                    log_records.push_back(std::move(record));
                    records_in_page++;
                }
            }

            if (records_in_page > 0) {
                LOG_DEBUG("Found " << records_in_page << " records in page "
                                   << page_id);
            }

        } catch (const std::exception& e) {
            LOG_DEBUG("Exception reading page " << page_id << ": " << e.what());
            continue;  // 跳过有问题的页面，继续处理其他页面
        }
    }

    LOG_DEBUG("ReadLogRecords: Found " << log_records.size()
                                       << " total log records");
    return log_records;
}

}  // namespace SimpleRDBMS