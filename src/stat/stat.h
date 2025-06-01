#pragma once

#include <atomic>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <unordered_map>

#include "common/config.h"
#include "common/debug.h"

namespace SimpleRDBMS {

// B+树统计信息结构
struct BTreeStats {
    std::atomic<uint64_t> splits{0};
    std::atomic<uint64_t> merges{0};
    std::atomic<uint64_t> insertions{0};
    std::atomic<uint64_t> deletions{0};
    std::atomic<uint64_t> searches{0};
    std::atomic<int> node_count{0};
    std::atomic<int> height{0};
    std::atomic<double> fill_factor{0.0};
    std::atomic<uint64_t> last_updated{0};

    // 自定义拷贝构造函数
    BTreeStats(const BTreeStats& other)
        : splits(other.splits.load()),
          merges(other.merges.load()),
          insertions(other.insertions.load()),
          deletions(other.deletions.load()),
          searches(other.searches.load()),
          node_count(other.node_count.load()),
          height(other.height.load()),
          fill_factor(other.fill_factor.load()),
          last_updated(other.last_updated.load()) {}

    // 自定义拷贝赋值运算符
    BTreeStats& operator=(const BTreeStats& other) {
        if (this != &other) {
            splits.store(other.splits.load());
            merges.store(other.merges.load());
            insertions.store(other.insertions.load());
            deletions.store(other.deletions.load());
            searches.store(other.searches.load());
            node_count.store(other.node_count.load());
            height.store(other.height.load());
            fill_factor.store(other.fill_factor.load());
            last_updated.store(other.last_updated.load());
        }
        return *this;
    }

    // 默认构造函数仍然有效
    BTreeStats() = default;
};

// 查询统计信息结构
struct QueryStats {
    uint64_t count = 0;
    double total_time_ms = 0.0;
    double min_time_ms = std::numeric_limits<double>::max();
    double max_time_ms = 0.0;
    uint64_t total_tuples_processed = 0;
};

class Statistics {
   public:
    // 单例模式
    static Statistics& GetInstance() {
        static Statistics instance;
        return instance;
    }

    // 删除拷贝构造和赋值操作
    Statistics(const Statistics&) = delete;
    Statistics& operator=(const Statistics&) = delete;

    // ==================== B+树统计 ====================

    void RecordBTreeSplit(const std::string& index_name);
    void RecordBTreeMerge(const std::string& index_name);
    void RecordBTreeInsertion(const std::string& index_name);
    void RecordBTreeDeletion(const std::string& index_name);
    void RecordBTreeSearch(const std::string& index_name);
    void UpdateBTreeNodeCount(const std::string& index_name, int node_count);
    void UpdateBTreeHeight(const std::string& index_name, int height);
    void UpdateBTreeFillFactor(const std::string& index_name,
                               double fill_factor);

    // ==================== 缓存统计 ====================

    void RecordBufferPoolHit();
    void RecordBufferPoolMiss();
    void UpdateBufferPoolSize(int size);
    void RecordPageEviction();
    void RecordPagePin();
    void RecordPageUnpin();

    // ==================== 磁盘统计 ====================

    void RecordDiskRead(size_t bytes = PAGE_SIZE);
    void RecordDiskWrite(size_t bytes = PAGE_SIZE);
    void RecordPageAllocation();
    void RecordPageDeallocation();

    // ==================== 事务统计 ====================

    void RecordTransactionBegin();
    void RecordTransactionCommit();
    void RecordTransactionAbort();
    void RecordTransactionDuration(double duration_ms);

    // ==================== 查询统计 ====================

    void RecordQueryExecution(const std::string& query_type,
                              double execution_time_ms,
                              uint64_t tuples_processed = 0);

    // ==================== 锁统计 ====================

    void RecordLockAcquisition(const std::string& lock_type);
    void RecordLockWait(double wait_time_ms);
    void RecordLockConflict();
    void RecordDeadlock();

    // ==================== 日志统计 ====================

    void RecordLogWrite(size_t bytes);
    void RecordLogFlush();
    void RecordLogTruncation();

    // ==================== 索引统计 ====================

    void RecordIndexCreation(const std::string& index_name);
    void RecordIndexDrop(const std::string& index_name);
    void RecordIndexRebuild(const std::string& index_name);

    // ==================== 获取统计信息 ====================

    // 获取缓存命中率
    double GetBufferPoolHitRatio() const;

    // 获取B+树统计
    BTreeStats GetBTreeStats(const std::string& index_name) const;

    // 获取查询统计
    QueryStats GetQueryStats(const std::string& query_type) const;

    // 获取总的磁盘IO
    uint64_t GetTotalDiskReads() const { return disk_reads_.load(); }
    uint64_t GetTotalDiskWrites() const { return disk_writes_.load(); }
    uint64_t GetTotalDiskReadBytes() const { return disk_read_bytes_.load(); }
    uint64_t GetTotalDiskWriteBytes() const { return disk_write_bytes_.load(); }

    // 获取事务统计
    uint64_t GetTotalTransactions() const { return transaction_begins_.load(); }
    uint64_t GetCommittedTransactions() const {
        return transaction_commits_.load();
    }
    uint64_t GetAbortedTransactions() const {
        return transaction_aborts_.load();
    }
    double GetTransactionSuccessRate() const;

    // ==================== 输出和重置 ====================

    void PrintStatistics() const;
    void PrintBTreeStatistics() const;
    void PrintBufferPoolStatistics() const;
    void PrintDiskStatistics() const;
    void PrintTransactionStatistics() const;
    void PrintQueryStatistics() const;
    void PrintLockStatistics() const;
    void PrintLogStatistics() const;

    void Reset();

    // ==================== 性能监控 ====================

    // 开始性能计时
    class PerformanceTimer {
       public:
        PerformanceTimer()
            : start_time_(std::chrono::high_resolution_clock::now()) {}

        double GetElapsedMs() const {
            auto end_time = std::chrono::high_resolution_clock::now();
            auto duration =
                std::chrono::duration_cast<std::chrono::microseconds>(
                    end_time - start_time_);
            return duration.count() / 1000.0;  // 转换为毫秒
        }

       private:
        std::chrono::high_resolution_clock::time_point start_time_;
    };

   private:
    Statistics() = default;
    ~Statistics() = default;

    mutable std::mutex mutex_;

    // ==================== B+树统计 ====================
    std::unordered_map<std::string, std::unique_ptr<BTreeStats>> btree_stats_;

    // ==================== 缓存统计 ====================
    std::atomic<uint64_t> buffer_pool_hits_{0};
    std::atomic<uint64_t> buffer_pool_misses_{0};
    std::atomic<int> buffer_pool_size_{0};
    std::atomic<uint64_t> page_evictions_{0};
    std::atomic<uint64_t> page_pins_{0};
    std::atomic<uint64_t> page_unpins_{0};

    // ==================== 磁盘统计 ====================
    std::atomic<uint64_t> disk_reads_{0};
    std::atomic<uint64_t> disk_writes_{0};
    std::atomic<uint64_t> disk_read_bytes_{0};
    std::atomic<uint64_t> disk_write_bytes_{0};
    std::atomic<uint64_t> page_allocations_{0};
    std::atomic<uint64_t> page_deallocations_{0};

    // ==================== 事务统计 ====================
    std::atomic<uint64_t> transaction_begins_{0};
    std::atomic<uint64_t> transaction_commits_{0};
    std::atomic<uint64_t> transaction_aborts_{0};
    std::atomic<double> total_transaction_time_ms_{0.0};

    // ==================== 查询统计 ====================
    std::unordered_map<std::string, QueryStats> query_stats_;

    // ==================== 锁统计 ====================
    std::unordered_map<std::string, uint64_t> lock_acquisitions_;
    std::atomic<uint64_t> lock_waits_{0};
    std::atomic<double> total_lock_wait_time_ms_{0.0};
    std::atomic<uint64_t> lock_conflicts_{0};
    std::atomic<uint64_t> deadlocks_{0};

    // ==================== 日志统计 ====================
    std::atomic<uint64_t> log_writes_{0};
    std::atomic<uint64_t> log_write_bytes_{0};
    std::atomic<uint64_t> log_flushes_{0};
    std::atomic<uint64_t> log_truncations_{0};

    // ==================== 索引统计 ====================
    std::atomic<uint64_t> index_creations_{0};
    std::atomic<uint64_t> index_drops_{0};
    std::atomic<uint64_t> index_rebuilds_{0};

    // 辅助函数
    std::string FormatBytes(uint64_t bytes) const;
    std::string FormatNumber(uint64_t number) const;
    std::string FormatPercentage(double percentage) const;
    std::string FormatTime(double time_ms) const;
};

// 宏定义，方便在代码中使用
#define STATS Statistics::GetInstance()

// RAII性能计时器，用于自动记录函数执行时间
#define RECORD_QUERY_TIME(query_type)                                   \
    Statistics::PerformanceTimer timer;                                 \
    auto record_time = [&timer, query_type]() {                         \
        STATS.RecordQueryExecution(query_type, timer.GetElapsedMs());   \
    };                                                                  \
    std::unique_ptr<void, decltype(record_time)> time_recorder(nullptr, \
                                                               record_time)

}  // namespace SimpleRDBMS