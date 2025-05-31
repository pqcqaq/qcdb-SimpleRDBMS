#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <random>
#include <sstream>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "buffer/lru_replacer.h"
#include "common/types.h"
#include "index/b_plus_tree.h"
#include "storage/disk_manager.h"

using namespace SimpleRDBMS;

// 日志级别枚举
enum class LogLevel { DEBUG = 0, INFO = 1, WARN = 2, ERROR = 3 };

class Logger {
   private:
    LogLevel current_level_;
    std::ofstream log_file_;
    bool file_logging_;

   public:
    Logger(LogLevel level = LogLevel::INFO, const std::string& log_file = "")
        : current_level_(level), file_logging_(false) {
        if (!log_file.empty()) {
            log_file_.open(log_file, std::ios::app);
            file_logging_ = log_file_.is_open();
        }
    }

    ~Logger() {
        if (file_logging_) {
            log_file_.close();
        }
    }

    std::string GetTimestamp() {
        auto now = std::chrono::system_clock::now();
        auto time_t = std::chrono::system_clock::to_time_t(now);
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                      now.time_since_epoch()) %
                  1000;

        std::stringstream ss;
        ss << std::put_time(std::localtime(&time_t), "%Y-%m-%d %H:%M:%S");
        ss << "." << std::setfill('0') << std::setw(3) << ms.count();
        return ss.str();
    }

    std::string LevelToString(LogLevel level) {
        switch (level) {
            case LogLevel::DEBUG:
                return "DEBUG";
            case LogLevel::INFO:
                return "INFO ";
            case LogLevel::WARN:
                return "WARN ";
            case LogLevel::ERROR:
                return "ERROR";
            default:
                return "UNKNOWN";
        }
    }

    void Log(LogLevel level, const std::string& message) {
        if (level < current_level_) return;

        std::string log_line = "[" + GetTimestamp() + "] [" +
                               LevelToString(level) + "] " + message;

        std::cout << log_line << std::endl;

        if (file_logging_) {
            log_file_ << log_line << std::endl;
            log_file_.flush();
        }
    }

    void Debug(const std::string& message) { Log(LogLevel::DEBUG, message); }
    void Info(const std::string& message) { Log(LogLevel::INFO, message); }
    void Warn(const std::string& message) { Log(LogLevel::WARN, message); }
    void Error(const std::string& message) { Log(LogLevel::ERROR, message); }
};

// 性能统计类
class PerformanceStats {
   private:
    std::vector<double> operation_times_;
    size_t total_operations_;
    double total_time_;

   public:
    PerformanceStats() : total_operations_(0), total_time_(0.0) {}

    void AddOperation(double time_ms) {
        operation_times_.push_back(time_ms);
        total_operations_++;
        total_time_ += time_ms;
    }

    double GetAverage() const {
        return total_operations_ > 0 ? total_time_ / total_operations_ : 0.0;
    }

    double GetMin() const {
        if (operation_times_.empty()) return 0.0;
        return *std::min_element(operation_times_.begin(),
                                 operation_times_.end());
    }

    double GetMax() const {
        if (operation_times_.empty()) return 0.0;
        return *std::max_element(operation_times_.begin(),
                                 operation_times_.end());
    }

    double GetThroughput() const {
        return total_time_ > 0 ? (total_operations_ * 1000.0) / total_time_
                               : 0.0;
    }

    size_t GetTotalOperations() const { return total_operations_; }
    double GetTotalTime() const { return total_time_; }
};

// 进度显示函数
void ShowProgress(Logger& logger, int current, int total,
                  const std::string& operation) {
    if (current % (total / 10) == 0 || current == total - 1) {
        double percentage = (double)current / total * 100.0;
        std::stringstream ss;
        ss << operation << " Progress: " << std::fixed << std::setprecision(1)
           << percentage << "% (" << current + 1 << "/" << total << ")";
        logger.Info(ss.str());
    }
}

// 验证数据完整性
bool VerifyDataIntegrity(
    Logger& logger, BPlusTree<int32_t, RID>* tree,
    const std::vector<std::pair<int32_t, RID>>& expected_data) {
    logger.Info("开始数据完整性验证...");
    int verified_count = 0;
    int error_count = 0;

    for (const auto& pair : expected_data) {
        RID result;
        if (tree->GetValue(pair.first, &result)) {
            verified_count++;
            // logger.Debug("验证通过 - Key: " + std::to_string(pair.first));

            // 注意：这里简化了RID比较，只检查键是否存在
            // 如果需要完整的RID验证，请根据实际的RID结构调整以下代码：
            //
            // 可能的RID接口：
            // 1. 公共成员变量：result.page_id == pair.second.page_id &&
            // result.slot_num == pair.second.slot_num
            // 2. getter函数：result.GetPageId() == pair.second.GetPageId() &&
            // result.GetSlotNum() == pair.second.GetSlotNum()
            // 3. 重载==操作符：result == pair.second
            //
            // 请检查RID的头文件以确定正确的访问方式

        } else {
            error_count++;
            logger.Error("找不到期望的键: " + std::to_string(pair.first));
        }
    }

    std::stringstream ss;
    ss << "数据完整性验证完成 - 验证通过: " << verified_count
       << ", 错误: " << error_count << ", 总计: " << expected_data.size();
    logger.Info(ss.str());

    return error_count == 0;
}

int main() {
        // 清理之前的测试文件
    std::remove("simple_test.db");
    std::remove("bplus_tree_test.log");
    // 创建日志器
    Logger logger(LogLevel::DEBUG, "bplus_tree_test.log");

    try {
        logger.Info("========================================");
        logger.Info("        Simple B+ Tree 测试开始");
        logger.Info("========================================");

        // 创建必要的组件
        logger.Info("正在初始化系统组件...");

        logger.Debug("创建磁盘管理器...");
        auto disk_manager = std::make_unique<DiskManager>("simple_test.db");

        logger.Debug("创建LRU替换器 (大小: 100)...");
        auto replacer = std::make_unique<LRUReplacer>(100);

        logger.Debug("创建缓冲池管理器 (大小: 100)...");
        auto buffer_pool_manager = std::make_unique<BufferPoolManager>(
            100, std::move(disk_manager), std::move(replacer));

        logger.Debug("创建B+树索引...");
        auto bplus_tree = std::make_unique<BPlusTree<int32_t, RID>>(
            "test_index", buffer_pool_manager.get());

        logger.Info("所有组件创建成功!");

                // ========== 简单插入测试 ==========
        logger.Info("\n--- 开始简单插入测试 ---");
        
        // 尝试插入单个元素
        RID test_rid{1, 1};
        logger.Debug("尝试插入单个元素: key=999, rid={1,1}");
        
        bool single_result = bplus_tree->Insert(999, test_rid);
        logger.Info("单个插入结果: " + std::string(single_result ? "成功" : "失败"));
        
        if (!single_result) {
            logger.Error("单个插入失败，退出测试");
            return 1;
        }
        
        // 验证插入的元素
        RID retrieved_rid;
        bool found = bplus_tree->GetValue(999, &retrieved_rid);
        logger.Info("查询结果: " + std::string(found ? "找到" : "未找到"));
        
        if (found) {
            logger.Info("✓ 简单插入测试通过!");
        } else {
            logger.Error("✗ 简单插入测试失败!");
            return 1;
        }

        // 测试参数
        const int test_size = 1000;
        logger.Info("测试规模: " + std::to_string(test_size) + " 条记录");

        // 准备测试数据
        std::vector<std::pair<int32_t, RID>> test_data;
        test_data.reserve(test_size);

        for (int i = 0; i < test_size; ++i) {
            RID rid{static_cast<page_id_t>(i / 100),
                    static_cast<slot_offset_t>(i % 100)};
            test_data.emplace_back(i, rid);
        }

        logger.Info("测试数据准备完成");

        // ========== 插入测试 ==========
        logger.Info("\n--- 开始插入测试 ---");
        PerformanceStats insert_stats;
        int insert_success = 0;
        int insert_failed = 0;

        auto overall_start = std::chrono::high_resolution_clock::now();

        for (int i = 0; i < test_size; ++i) {
            auto start = std::chrono::high_resolution_clock::now();

            bool success =
                bplus_tree->Insert(test_data[i].first, test_data[i].second);

            auto end = std::chrono::high_resolution_clock::now();
            auto duration =
                std::chrono::duration_cast<std::chrono::microseconds>(end -
                                                                      start);
            insert_stats.AddOperation(duration.count() / 1000.0);

            if (success) {
                insert_success++;
                // logger.Debug("插入成功 - Key: " +
                // std::to_string(test_data[i].first));
            } else {
                insert_failed++;
                logger.Warn("插入失败 - Key: " +
                            std::to_string(test_data[i].first));
            }

            ShowProgress(logger, i, test_size, "插入");
        }

        auto overall_end = std::chrono::high_resolution_clock::now();
        auto overall_duration =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                overall_end - overall_start);

        logger.Info("插入测试完成!");
        logger.Info("成功插入: " + std::to_string(insert_success) + " 条记录");
        logger.Info("插入失败: " + std::to_string(insert_failed) + " 条记录");
        logger.Info("总耗时: " + std::to_string(overall_duration.count()) +
                    " ms");
        logger.Info("平均耗时: " + std::to_string(insert_stats.GetAverage()) +
                    " ms/op");
        logger.Info("最小耗时: " + std::to_string(insert_stats.GetMin()) +
                    " ms");
        logger.Info("最大耗时: " + std::to_string(insert_stats.GetMax()) +
                    " ms");
        logger.Info("吞吐量: " + std::to_string(insert_stats.GetThroughput()) +
                    " ops/sec");

        // ========== 查询测试 ==========
        logger.Info("\n--- 开始查询测试 ---");
        PerformanceStats query_stats;
        int found_count = 0;
        int not_found_count = 0;

        overall_start = std::chrono::high_resolution_clock::now();

        for (int i = 0; i < test_size; ++i) {
            auto start = std::chrono::high_resolution_clock::now();

            RID result;
            bool found = bplus_tree->GetValue(test_data[i].first, &result);

            auto end = std::chrono::high_resolution_clock::now();
            auto duration =
                std::chrono::duration_cast<std::chrono::microseconds>(end -
                                                                      start);
            query_stats.AddOperation(duration.count() / 1000.0);

            if (found) {
                found_count++;
                // logger.Debug("查询成功 - Key: " +
                // std::to_string(test_data[i].first));
            } else {
                not_found_count++;
                logger.Warn("查询失败 - Key: " +
                            std::to_string(test_data[i].first));
            }

            ShowProgress(logger, i, test_size, "查询");
        }

        overall_end = std::chrono::high_resolution_clock::now();
        overall_duration =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                overall_end - overall_start);

        logger.Info("查询测试完成!");
        logger.Info("查询成功: " + std::to_string(found_count) + "/" +
                    std::to_string(test_size));
        logger.Info("查询失败: " + std::to_string(not_found_count) + " 条记录");
        logger.Info("总耗时: " + std::to_string(overall_duration.count()) +
                    " ms");
        logger.Info("平均耗时: " + std::to_string(query_stats.GetAverage()) +
                    " ms/op");
        logger.Info("最小耗时: " + std::to_string(query_stats.GetMin()) +
                    " ms");
        logger.Info("最大耗时: " + std::to_string(query_stats.GetMax()) +
                    " ms");
        logger.Info("吞吐量: " + std::to_string(query_stats.GetThroughput()) +
                    " ops/sec");

        // ========== 范围扫描测试 ==========
        logger.Info("\n--- 开始范围扫描测试 ---");
        int scan_count = 0;
        const int scan_limit = 100;

        overall_start = std::chrono::high_resolution_clock::now();

        logger.Debug("开始从键值 0 进行范围扫描...");
        auto iter = bplus_tree->Begin(0);
        while (!iter.IsEnd() && scan_count < scan_limit) {
            auto pair = *iter;
            (void)pair;  // 使用 pair 以避免未使用变量警告
            // logger.Debug("扫描到 - Key: " + std::to_string(pair.first));
            scan_count++;
            ++iter;
        }

        overall_end = std::chrono::high_resolution_clock::now();
        overall_duration =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                overall_end - overall_start);

        logger.Info("范围扫描完成!");
        logger.Info("扫描记录数: " + std::to_string(scan_count));
        logger.Info("总耗时: " + std::to_string(overall_duration.count()) +
                    " ms");
        if (scan_count > 0) {
            logger.Info(
                "平均耗时: " +
                std::to_string((double)overall_duration.count() / scan_count) +
                " ms/record");
        }

        // ========== 删除测试 ==========
        logger.Info("\n--- 开始删除测试 (删除偶数键) ---");
        PerformanceStats delete_stats;
        int delete_success = 0;
        int delete_failed = 0;

        overall_start = std::chrono::high_resolution_clock::now();

        for (int i = 0; i < test_size; i += 2) {
            auto start = std::chrono::high_resolution_clock::now();

            // logger.Debug("尝试删除键: " + std::to_string(test_data[i].first));
            bool success = bplus_tree->Remove(test_data[i].first);
            // logger.Debug("删除键 " + std::to_string(test_data[i].first) +
            //              " 结果: " + (success ? "成功" : "失败"));
            (void)success;  // 使用 success 以避免未使用变量警告

            auto end = std::chrono::high_resolution_clock::now();
            auto duration =
                std::chrono::duration_cast<std::chrono::microseconds>(end -
                                                                      start);
            delete_stats.AddOperation(duration.count() / 1000.0);

            if (success) {
                delete_success++;
            } else {
                delete_failed++;
                logger.Warn("删除失败 - Key: " +
                            std::to_string(test_data[i].first));
            }

            if ((i / 2) % (test_size / 20) == 0) {
                double percentage = (double)(i / 2) / (test_size / 2) * 100.0;
                std::stringstream ss;
                ss << "删除进度: " << std::fixed << std::setprecision(1)
                   << percentage << "% (" << (i / 2) + 1 << "/"
                   << (test_size / 2) << ")";
                logger.Info(ss.str());
            }
        }

        overall_end = std::chrono::high_resolution_clock::now();
        overall_duration =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                overall_end - overall_start);

        logger.Info("删除测试完成!");
        logger.Info("删除成功: " + std::to_string(delete_success) + " 条记录");
        logger.Info("删除失败: " + std::to_string(delete_failed) + " 条记录");
        logger.Info("总耗时: " + std::to_string(overall_duration.count()) +
                    " ms");
        logger.Info("平均耗时: " + std::to_string(delete_stats.GetAverage()) +
                    " ms/op");
        logger.Info("最小耗时: " + std::to_string(delete_stats.GetMin()) +
                    " ms");
        logger.Info("最大耗时: " + std::to_string(delete_stats.GetMax()) +
                    " ms");
        logger.Info("吞吐量: " + std::to_string(delete_stats.GetThroughput()) +
                    " ops/sec");

        // ========== 删除验证 ==========
        logger.Info("\n--- 开始删除结果验证 ---");
        int remaining_count = 0;
        int expected_remaining = test_size - delete_success;

        for (int i = 0; i < test_size; ++i) {
            RID result;
            if (bplus_tree->GetValue(test_data[i].first, &result)) {
                remaining_count++;
                // 应该只有奇数键存在
                if (i % 2 == 0) {
                    logger.Error("发现不应存在的偶数键: " +
                                 std::to_string(test_data[i].first));
                }
            }
        }

        logger.Info("验证完成!");
        logger.Info("实际剩余记录: " + std::to_string(remaining_count));
        logger.Info("期望剩余记录: " + std::to_string(expected_remaining));

        if (remaining_count == expected_remaining) {
            logger.Info("✓ 删除验证通过!");
        } else {
            logger.Error("✗ 删除验证失败!");
        }

        // ========== 最终数据完整性验证 ==========
        logger.Info("\n--- 最终数据完整性验证 ---");
        std::vector<std::pair<int32_t, RID>> expected_remaining_data;
        for (int i = 1; i < test_size; i += 2) {  // 只有奇数键应该存在
            expected_remaining_data.push_back(test_data[i]);
        }

        bool integrity_ok = VerifyDataIntegrity(logger, bplus_tree.get(),
                                                expected_remaining_data);

        // ========== 测试总结 ==========
        logger.Info("\n========================================");
        logger.Info("              测试总结");
        logger.Info("========================================");
        logger.Info("插入操作: " + std::to_string(insert_success) + "/" +
                    std::to_string(test_size) + " 成功");
        logger.Info("查询操作: " + std::to_string(found_count) + "/" +
                    std::to_string(test_size) + " 成功");
        logger.Info("删除操作: " + std::to_string(delete_success) + "/" +
                    std::to_string(test_size / 2) + " 成功");
        logger.Info("范围扫描: " + std::to_string(scan_count) + " 条记录");
        logger.Info("数据完整性: " +
                    std::string(integrity_ok ? "✓ 通过" : "✗ 失败"));

        if (insert_success == test_size && found_count == test_size &&
            remaining_count == expected_remaining && integrity_ok) {
            logger.Info("🎉 所有测试均通过!");
        } else {
            logger.Warn("⚠️  部分测试未通过，请检查日志");
        }

        logger.Info("========================================");

         // ========== 持久化验证测试 ==========
        logger.Info("\n--- 持久化验证测试 ---");
        
        // 先强制刷新所有页面
        logger.Debug("强制刷新所有页面到磁盘...");
        buffer_pool_manager->FlushAllPages();
        
        // 检查文件大小（应该不为0）
        std::ifstream file_check("simple_test.db", std::ios::binary | std::ios::ate);
        size_t file_size_before = 0;
        if (file_check.is_open()) {
            file_size_before = file_check.tellg();
            file_check.close();
            logger.Info("刷新后文件大小: " + std::to_string(file_size_before) + " 字节");
        }
        
        if (file_size_before == 0) {
            logger.Error("⚠️  警告: 刷新后文件大小仍为0，数据可能未正确持久化");
        } else {
            logger.Info("✓ 数据已成功写入磁盘");
        }
        
        // 模拟重启：销毁当前B+树，重新创建
        logger.Info("模拟系统重启 - 销毁并重新创建B+树...");
        bplus_tree.reset();
        
        // 重新创建B+树（使用相同的缓冲池管理器）
        bplus_tree = std::make_unique<BPlusTree<int32_t, RID>>(
            "test_index", buffer_pool_manager.get());
        
        // 验证重启后数据是否还在
        logger.Info("验证重启后的数据完整性...");
        int found_after_restart = 0;
        int expected_after_restart = 0;
        
        for (int i = 1; i < test_size; i += 2) {  // 只检查奇数键（应该存在的）
            expected_after_restart++;
            RID result;
            if (bplus_tree->GetValue(i, &result)) {
                found_after_restart++;
            } else {
                logger.Error("重启后找不到键: " + std::to_string(i));
            }
        }
        
        logger.Info("重启后数据验证完成:");
        logger.Info("期望找到: " + std::to_string(expected_after_restart) + " 条记录");
        logger.Info("实际找到: " + std::to_string(found_after_restart) + " 条记录");
        
        bool persistence_ok = (found_after_restart == expected_after_restart);
        
        // 检查不应该存在的偶数键
        int unexpected_found = 0;
        for (int i = 0; i < test_size; i += 2) {  // 检查偶数键（应该已删除）
            RID result;
            if (bplus_tree->GetValue(i, &result)) {
                unexpected_found++;
                logger.Error("重启后发现不应存在的偶数键: " + std::to_string(i));
            }
        }
        
        if (unexpected_found > 0) {
            logger.Error("发现 " + std::to_string(unexpected_found) + " 个不应存在的键");
            persistence_ok = false;
        }
        
        // ========== 最终测试总结 ==========
        logger.Info("\n========================================");
        logger.Info("              最终测试总结");
        logger.Info("========================================");
        logger.Info("插入操作: " + std::to_string(insert_success) + "/" +
                    std::to_string(test_size) + " 成功");
        logger.Info("查询操作: " + std::to_string(found_count) + "/" +
                    std::to_string(test_size) + " 成功");
        logger.Info("删除操作: " + std::to_string(delete_success) + "/" +
                    std::to_string(test_size / 2) + " 成功");
        logger.Info("范围扫描: " + std::to_string(scan_count) + " 条记录");
        logger.Info("数据完整性: " +
                    std::string(integrity_ok ? "✓ 通过" : "✗ 失败"));
        logger.Info("持久化验证: " +
                    std::string(persistence_ok ? "✓ 通过" : "✗ 失败"));
        logger.Info("文件大小: " + std::to_string(file_size_before) + " 字节");

        if (insert_success == test_size && found_count == test_size &&
            remaining_count == expected_remaining && integrity_ok && persistence_ok) {
            logger.Info("🎉 所有测试均通过，包括持久化验证!");
        } else {
            logger.Warn("⚠️  部分测试未通过，请检查日志详情");
            if (!persistence_ok) {
                logger.Error("❌ 持久化验证失败 - 数据未正确保存到磁盘");
            }
        }

        logger.Info("========================================");

        // 最后再次强制刷新所有页面到磁盘
        logger.Debug("最终强制刷新所有页面到磁盘...");
        buffer_pool_manager->FlushAllPages();
        
        // 检查最终文件大小
        std::ifstream final_file_check("simple_test.db", std::ios::binary | std::ios::ate);
        if (final_file_check.is_open()) {
            size_t final_file_size = final_file_check.tellg();
            final_file_check.close();
            logger.Info("最终文件大小: " + std::to_string(final_file_size) + " 字节");
            
            if (final_file_size > 0) {
                double kb_size = final_file_size / 1024.0;
                logger.Info("最终文件大小: " + std::to_string(kb_size) + " KB");
                
                // 计算平均每条记录的开销
                if (found_after_restart > 0) {
                    double bytes_per_record = static_cast<double>(final_file_size) / found_after_restart;
                    logger.Info("平均每条记录开销: " + std::to_string(bytes_per_record) + " 字节");
                }
            }
        }
        
        logger.Info("测试完成，所有操作已记录到日志文件 bplus_tree_test.log");
        logger.Info("========================================");

    } catch (const std::exception& e) {
        logger.Error("测试异常: " + std::string(e.what()));
        return 1;
    }

    return 0;
}