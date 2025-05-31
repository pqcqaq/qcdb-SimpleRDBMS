#include <execinfo.h>

#include <algorithm>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <memory>
#include <random>
#include <vector>
#include <unistd.h>

#include "buffer/buffer_pool_manager.h"
#include "buffer/lru_replacer.h"
#include "common/types.h"
#include "index/b_plus_tree.h"
#include "storage/disk_manager.h"

using namespace SimpleRDBMS;

class BPlusTreePerformanceTest {
   private:
    static constexpr int TEST_DATA_SIZE = 100000;
    static constexpr int BUFFER_POOL_SIZE = 1000;
    static constexpr int QUERY_TEST_SIZE = 10000;

    std::unique_ptr<DiskManager> disk_manager_;
    std::unique_ptr<LRUReplacer> replacer_;
    std::unique_ptr<BufferPoolManager> buffer_pool_manager_;
    std::unique_ptr<BPlusTree<int32_t, RID>> bplus_tree_;

    std::vector<int32_t> test_keys_;
    std::vector<RID> test_values_;

   public:
    BPlusTreePerformanceTest() {
        // Initialize components
        disk_manager_ = std::make_unique<DiskManager>("bplus_tree_test.db");
        replacer_ = std::make_unique<LRUReplacer>(BUFFER_POOL_SIZE);
        buffer_pool_manager_ = std::make_unique<BufferPoolManager>(
            BUFFER_POOL_SIZE, std::move(disk_manager_), std::move(replacer_));

        bplus_tree_ = std::make_unique<BPlusTree<int32_t, RID>>(
            "test_index", buffer_pool_manager_.get());

        GenerateTestData();
    }

    ~BPlusTreePerformanceTest() {
        // Cleanup
        std::remove("bplus_tree_test.db");
    }

   private:
    void GenerateTestData() {
        std::cout << "Generating test data..." << std::endl;

        // Generate sequential keys and random RID values
        test_keys_.reserve(TEST_DATA_SIZE);
        test_values_.reserve(TEST_DATA_SIZE);

        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<page_id_t> page_dist(1, 10000);
        std::uniform_int_distribution<slot_offset_t> slot_dist(0, 100);

        for (int i = 0; i < TEST_DATA_SIZE; ++i) {
            test_keys_.push_back(i);
            test_values_.push_back({page_dist(gen), slot_dist(gen)});
        }

        // Shuffle keys for random insertion order
        std::shuffle(test_keys_.begin(), test_keys_.end(), gen);
    }

    template <typename Func>
    double MeasureTime(const std::string& operation, Func&& func) {
        auto start = std::chrono::high_resolution_clock::now();
        func();
        auto end = std::chrono::high_resolution_clock::now();

        auto duration =
            std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        double time_ms = duration.count() / 1000.0;

        std::cout << operation << " took: " << std::fixed
                  << std::setprecision(2) << time_ms << " ms" << std::endl;

        return time_ms;
    }

   public:
    void RunAllTests() {
        std::cout << "=== B+ Tree Performance Test ===" << std::endl;
        std::cout << "Test data size: " << TEST_DATA_SIZE << std::endl;
        std::cout << "Buffer pool size: " << BUFFER_POOL_SIZE << " pages"
                  << std::endl;
        std::cout << std::endl;

        TestInsertPerformance();
        TestSequentialQueryPerformance();
        TestRandomQueryPerformance();
        TestRangeQueryPerformance();
        
        // 在删除前检查内存使用
        std::cout << "\n=== Memory Usage Before Delete ===" << std::endl;
        TestMemoryUsage();
        
        TestDeletePerformance();
        
        // 最终内存使用统计
        std::cout << "\n=== Final Memory Usage Statistics ===" << std::endl;
        TestMemoryUsage();

        std::cout << "\n=== Test Completed ===" << std::endl;
    }

    void TestInsertPerformance() {
        std::cout << "1. Testing Insert Performance" << std::endl;
        std::cout << "------------------------------" << std::endl;

        int insert_count = 0;
        int duplicate_count = 0;

        double insert_time = MeasureTime(
            "Inserting " + std::to_string(TEST_DATA_SIZE) + " records", [&]() {
                for (int i = 0; i < TEST_DATA_SIZE; ++i) {
                    bool success =
                        bplus_tree_->Insert(test_keys_[i], test_values_[i]);
                    if (success) {
                        insert_count++;
                    } else {
                        duplicate_count++;
                    }
                    // 输出插入了多少
                    if (i % 10000 == 0) {
                        std::cout << "Inserted " << i << " records..." << std::endl;
                    }
                }
            });

        double throughput =
            (TEST_DATA_SIZE / insert_time) * 1000;  // records per second

        std::cout << "Successful inserts: " << insert_count << std::endl;
        std::cout << "Duplicate keys: " << duplicate_count << std::endl;
        std::cout << "Insert throughput: " << std::fixed << std::setprecision(0)
                  << throughput << " records/second" << std::endl;
        std::cout << "Average time per insert: " << std::fixed
                  << std::setprecision(3)
                  << (insert_time * 1000) / TEST_DATA_SIZE << " μs"
                  << std::endl;
        std::cout << std::endl;
        
        // 强制刷新一些页面到磁盘以观察文件大小变化
        std::cout << "Flushing pages to disk..." << std::endl;
        buffer_pool_manager_->FlushAllPages();
    }

    void TestSequentialQueryPerformance() {
        std::cout << "2. Testing Sequential Query Performance" << std::endl;
        std::cout << "---------------------------------------" << std::endl;

        int found_count = 0;
        int not_found_count = 0;

        // Test sequential queries (first QUERY_TEST_SIZE keys)
        double query_time =
            MeasureTime("Sequential queries (" +
                            std::to_string(QUERY_TEST_SIZE) + " records)",
                        [&]() {
                            for (int i = 0; i < QUERY_TEST_SIZE; ++i) {
                                RID result;
                                bool found = bplus_tree_->GetValue(i, &result);
                                if (found) {
                                    found_count++;
                                } else {
                                    not_found_count++;
                                }
                            }
                        });

        double throughput = (QUERY_TEST_SIZE / query_time) * 1000;

        std::cout << "Records found: " << found_count << std::endl;
        std::cout << "Records not found: " << not_found_count << std::endl;
        std::cout << "Query throughput: " << std::fixed << std::setprecision(0)
                  << throughput << " queries/second" << std::endl;
        std::cout << "Average time per query: " << std::fixed
                  << std::setprecision(3)
                  << (query_time * 1000) / QUERY_TEST_SIZE << " μs"
                  << std::endl;
        std::cout << std::endl;
    }

    void TestRandomQueryPerformance() {
        std::cout << "3. Testing Random Query Performance" << std::endl;
        std::cout << "------------------------------------" << std::endl;

        // Generate random keys for querying
        std::vector<int32_t> random_keys;
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<int32_t> dist(
            0, TEST_DATA_SIZE * 2);  // Some keys won't exist

        for (int i = 0; i < QUERY_TEST_SIZE; ++i) {
            random_keys.push_back(dist(gen));
        }

        int found_count = 0;
        int not_found_count = 0;

        double query_time = MeasureTime(
            "Random queries (" + std::to_string(QUERY_TEST_SIZE) + " records)",
            [&]() {
                for (const auto& key : random_keys) {
                    RID result;
                    bool found = bplus_tree_->GetValue(key, &result);
                    if (found) {
                        found_count++;
                    } else {
                        not_found_count++;
                    }
                }
            });

        double throughput = (QUERY_TEST_SIZE / query_time) * 1000;

        std::cout << "Records found: " << found_count << std::endl;
        std::cout << "Records not found: " << not_found_count << std::endl;
        std::cout << "Query throughput: " << std::fixed << std::setprecision(0)
                  << throughput << " queries/second" << std::endl;
        std::cout << "Average time per query: " << std::fixed
                  << std::setprecision(3)
                  << (query_time * 1000) / QUERY_TEST_SIZE << " μs"
                  << std::endl;
        std::cout << std::endl;
    }

    void TestRangeQueryPerformance() {
        std::cout << "4. Testing Range Query Performance (Iterator)"
                  << std::endl;
        std::cout << "----------------------------------------------"
                  << std::endl;

        int scan_count = 0;
        const int range_size = 1000;  // Scan 1000 consecutive records

        double scan_time = MeasureTime("Range scan (1000 records)", [&]() {
            auto iter = bplus_tree_->Begin(0);  // Start from key 0
            auto end_iter = bplus_tree_->End();

            (void)end_iter;

            while (!iter.IsEnd() && scan_count < range_size) {
                auto pair = *iter;
                (void)pair;  // Use pair if needed, here we just count
                scan_count++;
                ++iter;
            }
        });

        double throughput = (scan_count / scan_time) * 1000;

        std::cout << "Records scanned: " << scan_count << std::endl;
        std::cout << "Scan throughput: " << std::fixed << std::setprecision(0)
                  << throughput << " records/second" << std::endl;
        std::cout << "Average time per record: " << std::fixed
                  << std::setprecision(3) << (scan_time * 1000) / scan_count
                  << " μs" << std::endl;
        std::cout << std::endl;
    }

    void TestDeletePerformance() {
        std::cout << "5. Testing Delete Performance" << std::endl;
        std::cout << "------------------------------" << std::endl;

        // Delete every 20th record instead of 10th to preserve more data
        const int delete_interval = 20;
        int delete_count = 0;
        int not_found_count = 0;

        double delete_time = MeasureTime(
            "Deleting every " + std::to_string(delete_interval) + "th record",
            [&]() {
                for (int i = 0; i < TEST_DATA_SIZE; i += delete_interval) {
                    bool success = bplus_tree_->Remove(i);
                    if (success) {
                        delete_count++;
                    } else {
                        not_found_count++;
                    }
                }
            });

        double throughput = (delete_count / delete_time) * 1000;

        std::cout << "Records deleted: " << delete_count << std::endl;
        std::cout << "Records not found: " << not_found_count << std::endl;
        std::cout << "Delete throughput: " << std::fixed << std::setprecision(0)
                  << throughput << " deletes/second" << std::endl;
        std::cout << "Average time per delete: " << std::fixed
                  << std::setprecision(3) << (delete_time * 1000) / delete_count
                  << " μs" << std::endl;
        std::cout << std::endl;

        // Verify deletions worked
        int verification_found = 0;
        for (int i = 0; i < TEST_DATA_SIZE; i += delete_interval) {
            RID result;
            if (bplus_tree_->GetValue(i, &result)) {
                verification_found++;
            }
        }
        std::cout << "Verification - Deleted records still found: "
                  << verification_found << std::endl;
        std::cout << std::endl;
    }

    void TestMemoryUsage() {
        std::cout << "Memory Usage Statistics" << std::endl;
        std::cout << "-----------------------" << std::endl;

        // Force flush all pages to get accurate disk usage
        buffer_pool_manager_->FlushAllPages();

        // Get file size
        std::ifstream file("bplus_tree_test.db",
                           std::ios::binary | std::ios::ate);
        if (file.is_open()) {
            size_t file_size = file.tellg();
            file.close();

            std::cout << "Database file size: " << file_size << " bytes"
                      << std::endl;
            std::cout << "Database file size: " << std::fixed
                      << std::setprecision(2) << file_size / 1024.0 << " KB"
                      << std::endl;
            std::cout << "Database file size: " << std::fixed
                      << std::setprecision(2) << file_size / (1024.0 * 1024.0)
                      << " MB" << std::endl;

            if (file_size > 0) {
                // 计算估算的记录数（假设还有80%的数据）
                int estimated_records = static_cast<int>(TEST_DATA_SIZE * 0.8);
                if (estimated_records > 0) {
                    double bytes_per_record = static_cast<double>(file_size) / estimated_records;
                    std::cout << "Average bytes per record: " << std::fixed
                              << std::setprecision(2) << bytes_per_record << " bytes"
                              << std::endl;
                }
                
                // 计算页面使用情况
                int pages_used = static_cast<int>((file_size + PAGE_SIZE - 1) / PAGE_SIZE);
                std::cout << "Pages used: " << pages_used << std::endl;
                std::cout << "Page utilization: " << std::fixed 
                          << std::setprecision(1) 
                          << (100.0 * file_size) / (pages_used * PAGE_SIZE) << "%"
                          << std::endl;
            } else {
                std::cout << "⚠️  Warning: Database file is empty!" << std::endl;
                std::cout << "   This might indicate that all data is in memory" << std::endl;
                std::cout << "   or the disk manager is not writing properly." << std::endl;
            }
        } else {
            std::cout << "❌ Could not open database file for size check" << std::endl;
        }

        std::cout << "Buffer pool pages: " << BUFFER_POOL_SIZE << std::endl;
        std::cout << "Page size: " << PAGE_SIZE << " bytes" << std::endl;
        std::cout << "Total buffer memory: "
                  << (BUFFER_POOL_SIZE * PAGE_SIZE) / 1024 << " KB"
                  << std::endl;
        std::cout << std::endl;
    }
};

int main() {
    try {
        BPlusTreePerformanceTest test;
        test.RunAllTests();
    } catch (const std::exception& e) {
        std::cerr << "Test failed with exception: " << e.what() << std::endl;
        void* array[10];
        size_t size = backtrace(array, 10);
        std::cerr << "Backtrace (size: " << size << "):" << std::endl;
        backtrace_symbols_fd(array, size, STDERR_FILENO);
        return 1;
    }

    return 0;
}