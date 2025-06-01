#include "stat/stat.h"

#include <algorithm>
#include <cmath>

namespace SimpleRDBMS {

// ==================== B+树统计 ====================

void Statistics::RecordBTreeSplit(const std::string& index_name) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (btree_stats_.find(index_name) == btree_stats_.end()) {
        btree_stats_[index_name] = std::make_unique<BTreeStats>();
    }
    btree_stats_[index_name]->splits.fetch_add(1);
    LOG_DEBUG("Statistics: Recorded B+Tree split for index " << index_name);
}

void Statistics::RecordBTreeMerge(const std::string& index_name) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (btree_stats_.find(index_name) == btree_stats_.end()) {
        btree_stats_[index_name] = std::make_unique<BTreeStats>();
    }
    btree_stats_[index_name]->merges.fetch_add(1);
    LOG_DEBUG("Statistics: Recorded B+Tree merge for index " << index_name);
}

void Statistics::RecordBTreeInsertion(const std::string& index_name) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (btree_stats_.find(index_name) == btree_stats_.end()) {
        btree_stats_[index_name] = std::make_unique<BTreeStats>();
    }
    btree_stats_[index_name]->insertions.fetch_add(1);
}

void Statistics::RecordBTreeDeletion(const std::string& index_name) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (btree_stats_.find(index_name) == btree_stats_.end()) {
        btree_stats_[index_name] = std::make_unique<BTreeStats>();
    }
    btree_stats_[index_name]->deletions.fetch_add(1);
}

void Statistics::RecordBTreeSearch(const std::string& index_name) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (btree_stats_.find(index_name) == btree_stats_.end()) {
        btree_stats_[index_name] = std::make_unique<BTreeStats>();
    }
    btree_stats_[index_name]->searches.fetch_add(1);
}

void Statistics::UpdateBTreeNodeCount(const std::string& index_name,
                                      int node_count) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (btree_stats_.find(index_name) == btree_stats_.end()) {
        btree_stats_[index_name] = std::make_unique<BTreeStats>();
    }
    btree_stats_[index_name]->node_count.store(node_count);
    auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
                   .count();
    btree_stats_[index_name]->last_updated.store(now);
}

void Statistics::UpdateBTreeHeight(const std::string& index_name, int height) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (btree_stats_.find(index_name) == btree_stats_.end()) {
        btree_stats_[index_name] = std::make_unique<BTreeStats>();
    }
    btree_stats_[index_name]->height.store(height);
}

void Statistics::UpdateBTreeFillFactor(const std::string& index_name,
                                       double fill_factor) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (btree_stats_.find(index_name) == btree_stats_.end()) {
        btree_stats_[index_name] = std::make_unique<BTreeStats>();
    }
    btree_stats_[index_name]->fill_factor.store(fill_factor);
}

// ==================== 缓存统计 ====================

void Statistics::RecordBufferPoolHit() { buffer_pool_hits_.fetch_add(1); }

void Statistics::RecordBufferPoolMiss() { buffer_pool_misses_.fetch_add(1); }

void Statistics::UpdateBufferPoolSize(int size) {
    buffer_pool_size_.store(size);
}

void Statistics::RecordPageEviction() { page_evictions_.fetch_add(1); }

void Statistics::RecordPagePin() { page_pins_.fetch_add(1); }

void Statistics::RecordPageUnpin() { page_unpins_.fetch_add(1); }

// ==================== 磁盘统计 ====================

void Statistics::RecordDiskRead(size_t bytes) {
    disk_reads_.fetch_add(1);
    disk_read_bytes_.fetch_add(bytes);
}

void Statistics::RecordDiskWrite(size_t bytes) {
    disk_writes_.fetch_add(1);
    disk_write_bytes_.fetch_add(bytes);
}

void Statistics::RecordPageAllocation() { page_allocations_.fetch_add(1); }

void Statistics::RecordPageDeallocation() { page_deallocations_.fetch_add(1); }

// ==================== 事务统计 ====================

void Statistics::RecordTransactionBegin() { transaction_begins_.fetch_add(1); }

void Statistics::RecordTransactionCommit() {
    transaction_commits_.fetch_add(1);
}

void Statistics::RecordTransactionAbort() { transaction_aborts_.fetch_add(1); }

void Statistics::RecordTransactionDuration(double duration_ms) {
    double current_total = total_transaction_time_ms_.load();
    while (!total_transaction_time_ms_.compare_exchange_weak(
        current_total, current_total + duration_ms)) {
        // 自旋直到成功更新
    }
}

// ==================== 查询统计 ====================

void Statistics::RecordQueryExecution(const std::string& query_type,
                                      double execution_time_ms,
                                      uint64_t tuples_processed) {
    std::lock_guard<std::mutex> lock(mutex_);

    QueryStats& stats = query_stats_[query_type];
    stats.count++;
    stats.total_time_ms += execution_time_ms;
    stats.min_time_ms = std::min(stats.min_time_ms, execution_time_ms);
    stats.max_time_ms = std::max(stats.max_time_ms, execution_time_ms);
    stats.total_tuples_processed += tuples_processed;

    LOG_DEBUG("Statistics: Recorded "
              << query_type << " query execution: " << execution_time_ms
              << "ms, " << tuples_processed << " tuples");
}

// ==================== 锁统计 ====================

void Statistics::RecordLockAcquisition(const std::string& lock_type) {
    std::lock_guard<std::mutex> lock(mutex_);
    lock_acquisitions_[lock_type]++;
}

void Statistics::RecordLockWait(double wait_time_ms) {
    lock_waits_.fetch_add(1);
    double current_total = total_lock_wait_time_ms_.load();
    while (!total_lock_wait_time_ms_.compare_exchange_weak(
        current_total, current_total + wait_time_ms)) {
        // 自旋直到成功更新
    }
}

void Statistics::RecordLockConflict() { lock_conflicts_.fetch_add(1); }

void Statistics::RecordDeadlock() { deadlocks_.fetch_add(1); }

// ==================== 日志统计 ====================

void Statistics::RecordLogWrite(size_t bytes) {
    log_writes_.fetch_add(1);
    log_write_bytes_.fetch_add(bytes);
}

void Statistics::RecordLogFlush() { log_flushes_.fetch_add(1); }

void Statistics::RecordLogTruncation() { log_truncations_.fetch_add(1); }

// ==================== 索引统计 ====================

void Statistics::RecordIndexCreation(const std::string& index_name) {
    index_creations_.fetch_add(1);
    LOG_INFO("Statistics: Recorded index creation: " << index_name);
}

void Statistics::RecordIndexDrop(const std::string& index_name) {
    index_drops_.fetch_add(1);
    LOG_INFO("Statistics: Recorded index drop: " << index_name);
}

void Statistics::RecordIndexRebuild(const std::string& index_name) {
    index_rebuilds_.fetch_add(1);
    LOG_INFO("Statistics: Recorded index rebuild: " << index_name);
}

// ==================== 获取统计信息 ====================

double Statistics::GetBufferPoolHitRatio() const {
    uint64_t hits = buffer_pool_hits_.load();
    uint64_t misses = buffer_pool_misses_.load();
    uint64_t total = hits + misses;
    return total > 0 ? (static_cast<double>(hits) / total) * 100.0 : 0.0;
}

BTreeStats Statistics::GetBTreeStats(const std::string& index_name) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = btree_stats_.find(index_name);
    if (it != btree_stats_.end()) {
        return *(it->second);
    }
    return BTreeStats();  // 返回默认构造的统计信息
}

QueryStats Statistics::GetQueryStats(const std::string& query_type) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = query_stats_.find(query_type);
    if (it != query_stats_.end()) {
        return it->second;
    }
    return QueryStats();  // 返回默认构造的统计信息
}

double Statistics::GetTransactionSuccessRate() const {
    uint64_t commits = transaction_commits_.load();
    uint64_t aborts = transaction_aborts_.load();
    uint64_t total = commits + aborts;
    return total > 0 ? (static_cast<double>(commits) / total) * 100.0 : 0.0;
}
// ==================== 输出功能 ====================

void Statistics::PrintStatistics() const {
    PrintBufferPoolStatistics();
    PrintDiskStatistics();
    PrintTransactionStatistics();
    PrintQueryStatistics();
    PrintBTreeStatistics();
    PrintLockStatistics();
    PrintLogStatistics();
}

void Statistics::PrintBTreeStatistics() const {
    std::cout << "\n" << std::string(65, '-') << std::endl;
    std::cout << " B+树索引统计信息" << std::endl;
    std::cout << std::string(65, '-') << std::endl;

    std::lock_guard<std::mutex> lock(mutex_);
    if (btree_stats_.empty()) {
        std::cout << "  暂无B+树统计数据" << std::endl;
        return;
    }

    for (const auto& [index_name, stats] : btree_stats_) {
        std::cout << std::endl;
        std::cout << "  索引名称: " << index_name << std::endl;
        std::cout << "    |-- 树高度: " << stats->height.load() << " 层"
                  << std::endl;
        std::cout << "    |-- 节点数: "
                  << FormatNumber(stats->node_count.load()) << " 个"
                  << std::endl;
        std::cout << "    |-- 填充率: "
                  << FormatPercentage(stats->fill_factor.load()) << std::endl;
        std::cout << "    |-- 操作统计:" << std::endl;
        std::cout << "        |-- 插入操作: "
                  << FormatNumber(stats->insertions.load()) << " 次"
                  << std::endl;
        std::cout << "        |-- 删除操作: "
                  << FormatNumber(stats->deletions.load()) << " 次"
                  << std::endl;
        std::cout << "        |-- 查找操作: "
                  << FormatNumber(stats->searches.load()) << " 次" << std::endl;
        std::cout << "        |-- 节点分裂: "
                  << FormatNumber(stats->splits.load()) << " 次" << std::endl;
        std::cout << "        |-- 节点合并: "
                  << FormatNumber(stats->merges.load()) << " 次" << std::endl;
    }
}

void Statistics::PrintBufferPoolStatistics() const {
    std::cout << "\n" << std::string(65, '-') << std::endl;
    std::cout << " 缓冲池统计信息" << std::endl;
    std::cout << std::string(65, '-') << std::endl;

    uint64_t hits = buffer_pool_hits_.load();
    uint64_t misses = buffer_pool_misses_.load();
    uint64_t total_accesses = hits + misses;

    std::cout << std::endl;
    std::cout << "  基本信息:" << std::endl;
    std::cout << "    |-- 缓冲池大小: " << buffer_pool_size_.load() << " 页"
              << std::endl;
    std::cout << "    |-- 总访问次数: " << FormatNumber(total_accesses) << " 次"
              << std::endl;
    std::cout << std::endl;
    std::cout << "  命中统计:" << std::endl;
    std::cout << "    |-- 缓存命中: " << FormatNumber(hits) << " 次"
              << std::endl;
    std::cout << "    |-- 缓存未命中: " << FormatNumber(misses) << " 次"
              << std::endl;
    std::cout << "    |-- 命中率: " << FormatPercentage(GetBufferPoolHitRatio())
              << std::endl;
    std::cout << std::endl;
    std::cout << "  页面操作:" << std::endl;
    std::cout << "    |-- 页面淘汰: " << FormatNumber(page_evictions_.load())
              << " 次" << std::endl;
    std::cout << "    |-- 页面锁定: " << FormatNumber(page_pins_.load())
              << " 次" << std::endl;
    std::cout << "    |-- 页面解锁: " << FormatNumber(page_unpins_.load())
              << " 次" << std::endl;
}

void Statistics::PrintDiskStatistics() const {
    std::cout << "\n" << std::string(65, '-') << std::endl;
    std::cout << " 磁盘I/O统计信息" << std::endl;
    std::cout << std::string(65, '-') << std::endl;

    uint64_t reads = disk_reads_.load();
    uint64_t writes = disk_writes_.load();
    uint64_t read_bytes = disk_read_bytes_.load();
    uint64_t write_bytes = disk_write_bytes_.load();

    std::cout << std::endl;
    std::cout << "  I/O操作统计:" << std::endl;
    std::cout << "    |-- 磁盘读取: " << FormatNumber(reads) << " 次 ("
              << FormatBytes(read_bytes) << ")" << std::endl;
    std::cout << "    |-- 磁盘写入: " << FormatNumber(writes) << " 次 ("
              << FormatBytes(write_bytes) << ")" << std::endl;
    std::cout << "    |-- 总I/O操作: " << FormatNumber(reads + writes) << " 次"
              << std::endl;
    std::cout << std::endl;
    std::cout << "  页面管理:" << std::endl;
    std::cout << "    |-- 页面分配: " << FormatNumber(page_allocations_.load())
              << " 次" << std::endl;
    std::cout << "    |-- 页面释放: "
              << FormatNumber(page_deallocations_.load()) << " 次" << std::endl;

    if (reads + writes > 0) {
        double read_ratio =
            (static_cast<double>(reads) / (reads + writes)) * 100.0;
        std::cout << std::endl;
        std::cout << "  读写比例:" << std::endl;
        std::cout << "    |-- 读取/写入: " << FormatPercentage(read_ratio)
                  << " / " << FormatPercentage(100.0 - read_ratio) << std::endl;
    }
}

void Statistics::PrintTransactionStatistics() const {
    std::cout << "\n" << std::string(65, '-') << std::endl;
    std::cout << " 事务统计信息" << std::endl;
    std::cout << std::string(65, '-') << std::endl;

    uint64_t begins = transaction_begins_.load();
    uint64_t commits = transaction_commits_.load();
    uint64_t aborts = transaction_aborts_.load();
    double total_time = total_transaction_time_ms_.load();

    std::cout << std::endl;
    std::cout << "  事务概览:" << std::endl;
    std::cout << "    |-- 事务总数: " << FormatNumber(begins) << " 个"
              << std::endl;
    std::cout << "    |-- 成功提交: " << FormatNumber(commits) << " 个"
              << std::endl;
    std::cout << "    |-- 回滚事务: " << FormatNumber(aborts) << " 个"
              << std::endl;
    std::cout << "    |-- 成功率: "
              << FormatPercentage(GetTransactionSuccessRate()) << std::endl;

    if (begins > 0) {
        std::cout << std::endl;
        std::cout << "  性能指标:" << std::endl;
        std::cout << "    |-- 平均执行时间: " << FormatTime(total_time / begins)
                  << std::endl;
    }
}

void Statistics::PrintQueryStatistics() const {
    std::cout << "\n" << std::string(65, '-') << std::endl;
    std::cout << " 查询统计信息" << std::endl;
    std::cout << std::string(65, '-') << std::endl;

    std::lock_guard<std::mutex> lock(mutex_);
    if (query_stats_.empty()) {
        std::cout << "  暂无查询统计数据" << std::endl;
        return;
    }

    for (const auto& [query_type, stats] : query_stats_) {
        std::cout << std::endl;
        std::cout << "  查询类型: " << query_type << std::endl;
        std::cout << "    |-- 执行次数: " << FormatNumber(stats.count) << " 次"
                  << std::endl;
        std::cout << "    |-- 总执行时间: " << FormatTime(stats.total_time_ms)
                  << std::endl;

        if (stats.count > 0) {
            double avg_time = stats.total_time_ms / stats.count;
            std::cout << "    |-- 平均时间: " << FormatTime(avg_time)
                      << std::endl;
            std::cout << "    |-- 最短时间: " << FormatTime(stats.min_time_ms)
                      << std::endl;
            std::cout << "    |-- 最长时间: " << FormatTime(stats.max_time_ms)
                      << std::endl;
        }

        if (stats.total_tuples_processed > 0) {
            std::cout << "    |-- 处理元组数: "
                      << FormatNumber(stats.total_tuples_processed) << " 个"
                      << std::endl;
            if (stats.count > 0) {
                double avg_tuples =
                    static_cast<double>(stats.total_tuples_processed) /
                    stats.count;
                std::cout << "    |-- 平均元组/查询: " << std::fixed
                          << std::setprecision(1) << avg_tuples << " 个"
                          << std::endl;
            }
        }
    }
}

void Statistics::PrintLockStatistics() const {
    std::cout << "\n" << std::string(65, '-') << std::endl;
    std::cout << " 锁管理统计信息" << std::endl;
    std::cout << std::string(65, '-') << std::endl;

    uint64_t waits = lock_waits_.load();
    double total_wait_time = total_lock_wait_time_ms_.load();
    uint64_t conflicts = lock_conflicts_.load();
    uint64_t deadlocks = deadlocks_.load();

    std::cout << std::endl;
    std::cout << "  锁等待统计:" << std::endl;
    std::cout << "    |-- 锁等待次数: " << FormatNumber(waits) << " 次"
              << std::endl;
    if (waits > 0) {
        std::cout << "    |-- 平均等待时间: "
                  << FormatTime(total_wait_time / waits) << std::endl;
    }
    std::cout << std::endl;
    std::cout << "  冲突统计:" << std::endl;
    std::cout << "    |-- 锁冲突次数: " << FormatNumber(conflicts) << " 次"
              << std::endl;
    std::cout << "    |-- 死锁次数: " << FormatNumber(deadlocks) << " 次"
              << std::endl;

    std::lock_guard<std::mutex> lock(mutex_);
    if (!lock_acquisitions_.empty()) {
        std::cout << std::endl;
        std::cout << "  锁获取统计(按类型):" << std::endl;
        for (const auto& [lock_type, count] : lock_acquisitions_) {
            std::cout << "    |-- " << lock_type << ": " << FormatNumber(count)
                      << " 次" << std::endl;
        }
    }
}

void Statistics::PrintLogStatistics() const {
    std::cout << "\n" << std::string(65, '-') << std::endl;
    std::cout << " 日志统计信息" << std::endl;
    std::cout << std::string(65, '-') << std::endl;

    uint64_t writes = log_writes_.load();
    uint64_t write_bytes = log_write_bytes_.load();
    uint64_t flushes = log_flushes_.load();
    uint64_t truncations = log_truncations_.load();

    std::cout << std::endl;
    std::cout << "  日志操作统计:" << std::endl;
    std::cout << "    |-- 日志写入: " << FormatNumber(writes) << " 次 ("
              << FormatBytes(write_bytes) << ")" << std::endl;
    std::cout << "    |-- 日志刷盘: " << FormatNumber(flushes) << " 次"
              << std::endl;
    std::cout << "    |-- 日志截断: " << FormatNumber(truncations) << " 次"
              << std::endl;

    if (writes > 0 && write_bytes > 0) {
        double avg_record_size = static_cast<double>(write_bytes) / writes;
        std::cout << std::endl;
        std::cout << "  记录大小:" << std::endl;
        std::cout << "    |-- 平均记录大小: " << std::fixed
                  << std::setprecision(1) << avg_record_size << " 字节"
                  << std::endl;
    }
}

void Statistics::Reset() {
    std::lock_guard<std::mutex> lock(mutex_);

    // 重置所有原子变量
    buffer_pool_hits_.store(0);
    buffer_pool_misses_.store(0);
    buffer_pool_size_.store(0);
    page_evictions_.store(0);
    page_pins_.store(0);
    page_unpins_.store(0);

    disk_reads_.store(0);
    disk_writes_.store(0);
    disk_read_bytes_.store(0);
    disk_write_bytes_.store(0);
    page_allocations_.store(0);
    page_deallocations_.store(0);

    transaction_begins_.store(0);
    transaction_commits_.store(0);
    transaction_aborts_.store(0);
    total_transaction_time_ms_.store(0.0);

    lock_waits_.store(0);
    total_lock_wait_time_ms_.store(0.0);
    lock_conflicts_.store(0);
    deadlocks_.store(0);

    log_writes_.store(0);
    log_write_bytes_.store(0);
    log_flushes_.store(0);
    log_truncations_.store(0);

    index_creations_.store(0);
    index_drops_.store(0);
    index_rebuilds_.store(0);

    // 清空容器
    btree_stats_.clear();
    query_stats_.clear();
    lock_acquisitions_.clear();

    LOG_INFO("Statistics: All statistics have been reset");
}

// ==================== 辅助函数 ====================

std::string Statistics::FormatBytes(uint64_t bytes) const {
    const char* units[] = {"B", "KB", "MB", "GB", "TB"};
    int unit_index = 0;
    double size = static_cast<double>(bytes);

    while (size >= 1024.0 && unit_index < 4) {
        size /= 1024.0;
        unit_index++;
    }

    std::ostringstream oss;
    oss << std::fixed << std::setprecision(unit_index == 0 ? 0 : 1) << size
        << " " << units[unit_index];
    return oss.str();
}

std::string Statistics::FormatNumber(uint64_t number) const {
    std::ostringstream oss;
    oss.imbue(std::locale(""));
    oss << number;
    return oss.str();
}

std::string Statistics::FormatPercentage(double percentage) const {
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2) << percentage << "%";
    return oss.str();
}

std::string Statistics::FormatTime(double time_ms) const {
    if (time_ms < 1.0) {
        return std::to_string(static_cast<int>(time_ms * 1000)) + " μs";
    } else if (time_ms < 1000.0) {
        std::ostringstream oss;
        oss << std::fixed << std::setprecision(2) << time_ms << " ms";
        return oss.str();
    } else {
        std::ostringstream oss;
        oss << std::fixed << std::setprecision(2) << (time_ms / 1000.0) << " s";
        return oss.str();
    }
}

}  // namespace SimpleRDBMS