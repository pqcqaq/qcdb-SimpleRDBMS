/*
 * 文件: log_manager.h
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述: WAL日志管理器头文件，定义了日志记录的缓冲、写入和恢复相关接口
 */

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

/**
 * LogManager - WAL日志管理器
 *
 * 核心职责：
 * 1. 管理日志记录的缓冲和写入，实现Write-Ahead Logging机制
 * 2. 提供LSN(Log Sequence Number)的分配和跟踪
 * 3. 支持强制刷盘操作，确保日志持久化
 * 4. 提供日志读取功能，用于崩溃恢复
 * 5. 线程安全的并发访问控制
 *
 * WAL核心原则：在数据页面写入磁盘之前，相关的日志记录必须先写入磁盘
 */
class LogManager {
   public:
    /**
     * 构造函数
     * @param disk_manager 磁盘管理器指针，用于实际的页面I/O操作
     */
    explicit LogManager(DiskManager* disk_manager);

    /**
     * 析构函数
     * 确保所有缓冲区内容都已刷盘，避免日志丢失
     */
    ~LogManager();

    /**
     * 追加日志记录到缓冲区
     * @param log_record 要写入的日志记录指针
     * @return 分配给该记录的LSN，失败时返回INVALID_LSN
     */
    lsn_t AppendLogRecord(LogRecord* log_record);

    /**
     * 强制将日志缓冲区刷盘
     * @param lsn 需要确保持久化的LSN，默认-1表示刷新所有当前缓冲区内容
     */
    void Flush(lsn_t lsn = -1);

    /**
     * 获取下一个可用的LSN
     * @return 新分配的LSN值
     * 注意：这个方法会原子性地递增LSN计数器
     */
    lsn_t GetNextLSN() { return next_lsn_.fetch_add(1); }

    /**
     * 获取已持久化到磁盘的最大LSN
     * @return 持久化LSN值
     */
    lsn_t GetPersistentLSN() const { return persistent_lsn_.load(); }

    /**
     * 从磁盘读取所有日志记录
     * @return 包含所有日志记录的vector，主要用于崩溃恢复
     */
    std::vector<std::unique_ptr<LogRecord>> ReadLogRecords();

    /**
     * 启用或禁用日志记录功能
     * @param enable true启用，false禁用
     */
    void SetEnable(bool enable) { enable_logging_ = enable; }

   private:
    // ========== 核心组件 ==========

    /** 磁盘管理器指针，负责实际的页面读写 */
    DiskManager* disk_manager_;

    // ========== 日志缓冲区管理 ==========

    /** 日志缓冲区指针，用于暂存日志记录 */
    char* log_buffer_;

    /** 缓冲区总大小，通常为一个页面大小 */
    size_t log_buffer_size_;

    /** 当前缓冲区使用的偏移量，指示下一条记录的写入位置 */
    size_t log_buffer_offset_;

    // ========== LSN跟踪管理 ==========

    /** 下一个要分配的LSN，使用原子变量保证线程安全 */
    std::atomic<lsn_t> next_lsn_{0};

    /** 已持久化到磁盘的最大LSN，用于恢复时确定重做起点 */
    std::atomic<lsn_t> persistent_lsn_{INVALID_LSN};

    // ========== 线程安全控制 ==========

    /** 互斥锁，保护缓冲区和其他共享资源的并发访问 */
    std::mutex latch_;

    /** 条件变量，用于协调刷盘操作（预留接口） */
    std::condition_variable flush_cv_;

    // ========== 后台处理控制 ==========

    /** 后台刷盘线程运行标志（预留接口，当前未使用） */
    std::atomic<bool> flush_thread_running_{false};

    // ========== 功能控制标志 ==========

    /** 日志功能启用标志，可用于性能测试时临时关闭日志 */
    bool enable_logging_{true};

    /** 当前正在使用的日志页面ID，用于跟踪日志文件增长 */
    page_id_t current_log_page_id_{0};

    // ========== 内部辅助方法 ==========

    /**
     * 将缓冲区内容写入磁盘页面
     * 这是WAL机制的核心实现，确保日志先于数据写入磁盘
     */
    void FlushLogBuffer();

    /**
     * 后台异步刷盘方法（预留接口）
     * 可用于实现定期自动刷盘，减少同步刷盘的性能影响
     */
    void BackgroundFlush();
};

}  // namespace SimpleRDBMS