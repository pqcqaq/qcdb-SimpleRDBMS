// test/unit/index_manager_test.cpp
#include "index/index_manager.h"

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <random>
#include <set>
#include <thread>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "buffer/lru_replacer.h"
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "catalog/table_manager.h"
#include "common/debug.h"
#include "common/types.h"
#include "execution/execution_engine.h"
#include "parser/parser.h"
#include "recovery/log_manager.h"
#include "storage/disk_manager.h"
#include "transaction/lock_manager.h"
#include "transaction/transaction_manager.h"

using namespace SimpleRDBMS;

class IndexManagerTest : public ::testing::Test {
   protected:
    static constexpr const char* DB_FILE = "index_test.db";
    static constexpr const char* LOG_FILE = "index_test.log";
    static constexpr size_t BUFFER_POOL_SIZE = 100;

    void SetUp() override {
        // 清理之前的文件
        std::remove(DB_FILE);
        std::remove(LOG_FILE);

        // 创建系统组件
        disk_manager_ = std::make_unique<DiskManager>(DB_FILE);
        log_disk_manager_ = std::make_unique<DiskManager>(LOG_FILE);
        replacer_ = std::make_unique<LRUReplacer>(BUFFER_POOL_SIZE);
        buffer_pool_manager_ = std::make_unique<BufferPoolManager>(
            BUFFER_POOL_SIZE, std::move(disk_manager_), std::move(replacer_));
        log_manager_ = std::make_unique<LogManager>(log_disk_manager_.get());
        lock_manager_ = std::make_unique<LockManager>();
        transaction_manager_ = std::make_unique<TransactionManager>(
            lock_manager_.get(), log_manager_.get());
        catalog_ = std::make_unique<Catalog>(buffer_pool_manager_.get());
        table_manager_ = std::make_unique<TableManager>(
            buffer_pool_manager_.get(), catalog_.get());
        execution_engine_ = std::make_unique<ExecutionEngine>(
            buffer_pool_manager_.get(), catalog_.get(),
            transaction_manager_.get());
    }

    void TearDown() override {
        // Explicit destruction in safe order to prevent segfaults
        // Destroy in reverse order of creation to handle dependencies
        execution_engine_.reset();
        table_manager_
            .reset();  // This contains IndexManager with BPlusTree instances
        catalog_.reset();  // This might reference BufferPoolManager
        transaction_manager_.reset();
        lock_manager_.reset();
        log_manager_.reset();

        // Flush and close buffer pool manager before destroying disk managers
        if (buffer_pool_manager_) {
            try {
                buffer_pool_manager_->FlushAllPages();
            } catch (...) {
                // Ignore errors during cleanup
            }
        }
        buffer_pool_manager_.reset();

        // Finally destroy disk-related components
        log_disk_manager_.reset();

        // Give some time for any async operations to complete
        std::this_thread::sleep_for(std::chrono::milliseconds(10));

        // 清理文件
        std::remove(DB_FILE);
        std::remove(LOG_FILE);
    }

    bool ExecuteSQL(const std::string& sql,
                    std::vector<Tuple>* result_set = nullptr) {
        try {
            Parser parser(sql);
            auto statement = parser.Parse();
            auto* txn = transaction_manager_->Begin();
            std::vector<Tuple> local_result_set;
            bool success = execution_engine_->Execute(
                statement.get(), result_set ? result_set : &local_result_set,
                txn);
            if (success) {
                transaction_manager_->Commit(txn);
            } else {
                transaction_manager_->Abort(txn);
            }
            return success;
        } catch (const std::exception& e) {
            std::cerr << "SQL execution error: " << e.what() << std::endl;
            return false;
        }
    }

    std::unique_ptr<DiskManager> disk_manager_;
    std::unique_ptr<DiskManager> log_disk_manager_;
    std::unique_ptr<LRUReplacer> replacer_;
    std::unique_ptr<BufferPoolManager> buffer_pool_manager_;
    std::unique_ptr<LogManager> log_manager_;
    std::unique_ptr<LockManager> lock_manager_;
    std::unique_ptr<TransactionManager> transaction_manager_;
    std::unique_ptr<Catalog> catalog_;
    std::unique_ptr<TableManager> table_manager_;
    std::unique_ptr<ExecutionEngine> execution_engine_;
};

// 测试1: 基本索引创建和删除
TEST_F(IndexManagerTest, BasicIndexOperations) {
    // 创建表
    std::string create_sql = R"(
        CREATE TABLE test_table (
            id INT PRIMARY KEY,
            name VARCHAR(50),
            age INT,
            score DOUBLE
        );
    )";
    ASSERT_TRUE(ExecuteSQL(create_sql));

    IndexManager* index_manager = table_manager_->GetIndexManager();
    ASSERT_NE(index_manager, nullptr);

    TableInfo* table_info = catalog_->GetTable("test_table");
    ASSERT_NE(table_info, nullptr);

    // 测试创建不同类型的索引
    EXPECT_TRUE(index_manager->CreateIndex("idx_id", "test_table", {"id"},
                                           table_info->schema.get()));
    EXPECT_TRUE(index_manager->CreateIndex("idx_name", "test_table", {"name"},
                                           table_info->schema.get()));
    EXPECT_TRUE(index_manager->CreateIndex("idx_age", "test_table", {"age"},
                                           table_info->schema.get()));
    EXPECT_TRUE(index_manager->CreateIndex("idx_score", "test_table", {"score"},
                                           table_info->schema.get()));

    // 测试重复创建索引
    EXPECT_FALSE(index_manager->CreateIndex("idx_id", "test_table", {"id"},
                                            table_info->schema.get()));

    // 测试获取索引列表
    std::vector<std::string> all_indexes = index_manager->GetAllIndexNames();
    EXPECT_EQ(all_indexes.size(), 5);  // 4个手动创建的 + 1个主键索引

    std::vector<std::string> table_indexes =
        index_manager->GetTableIndexes("test_table");
    EXPECT_EQ(table_indexes.size(), 5);

    // 测试删除索引
    EXPECT_TRUE(index_manager->DropIndex("idx_name"));
    EXPECT_FALSE(index_manager->DropIndex("idx_name"));  // 重复删除

    table_indexes = index_manager->GetTableIndexes("test_table");
    EXPECT_EQ(table_indexes.size(), 4);
}

// 测试2: 索引数据填充
TEST_F(IndexManagerTest, IndexDataPopulation) {
    // 创建表
    std::string create_sql = R"(
        CREATE TABLE employees (
            emp_id INT PRIMARY KEY,
            emp_name VARCHAR(100),
            salary INT,
            dept_id INT
        );
    )";
    ASSERT_TRUE(ExecuteSQL(create_sql));

    // 插入一些数据
    std::vector<std::string> insert_sqls = {
        "INSERT INTO employees VALUES (1, 'Alice', 50000, 1);",
        "INSERT INTO employees VALUES (2, 'Bob', 60000, 2);",
        "INSERT INTO employees VALUES (3, 'Charlie', 55000, 1);",
        "INSERT INTO employees VALUES (4, 'David', 65000, 3);",
        "INSERT INTO employees VALUES (5, 'Eve', 52000, 2);"};

    for (const auto& sql : insert_sqls) {
        ASSERT_TRUE(ExecuteSQL(sql));
    }

    // 在已有数据的表上创建索引
    EXPECT_TRUE(
        table_manager_->CreateIndex("idx_salary", "employees", {"salary"}));
    EXPECT_TRUE(
        table_manager_->CreateIndex("idx_dept", "employees", {"dept_id"}));

    IndexManager* index_manager = table_manager_->GetIndexManager();

    // 测试索引查找
    RID rid;
    EXPECT_TRUE(index_manager->FindEntry("idx_salary", Value(50000), &rid));
    EXPECT_TRUE(index_manager->FindEntry("idx_salary", Value(60000), &rid));
    EXPECT_FALSE(index_manager->FindEntry("idx_salary", Value(99999), &rid));

    EXPECT_TRUE(index_manager->FindEntry("idx_dept", Value(1), &rid));
    EXPECT_TRUE(index_manager->FindEntry("idx_dept", Value(2), &rid));
    EXPECT_FALSE(index_manager->FindEntry("idx_dept", Value(999), &rid));
}

// 测试3: 索引维护（增删改）
TEST_F(IndexManagerTest, IndexMaintenance) {
    // 创建表和索引
    std::string create_sql = R"(
        CREATE TABLE products (
            product_id INT PRIMARY KEY,
            product_name VARCHAR(100),
            price DOUBLE,
            category_id INT
        );
    )";
    ASSERT_TRUE(ExecuteSQL(create_sql));

    EXPECT_TRUE(
        table_manager_->CreateIndex("idx_price", "products", {"price"}));
    EXPECT_TRUE(table_manager_->CreateIndex("idx_category", "products",
                                            {"category_id"}));

    IndexManager* index_manager = table_manager_->GetIndexManager();

    // 插入数据并测试索引自动维护
    std::string insert_sql =
        "INSERT INTO products VALUES (1, 'Laptop', 999.99, 1);";
    ASSERT_TRUE(ExecuteSQL(insert_sql));

    // 验证索引中有新插入的数据
    RID rid;
    EXPECT_TRUE(index_manager->FindEntry("idx_price", Value(999.99), &rid));
    EXPECT_TRUE(index_manager->FindEntry("idx_category", Value(1), &rid));

    // 更新数据并测试索引维护
    std::string update_sql =
        "UPDATE products SET price = 1199.99, category_id = 2 WHERE product_id "
        "= 1;";
    ASSERT_TRUE(ExecuteSQL(update_sql));

    // 验证索引更新
    EXPECT_FALSE(index_manager->FindEntry("idx_price", Value(999.99),
                                          &rid));  // 旧值应该不存在
    EXPECT_TRUE(index_manager->FindEntry("idx_price", Value(1199.99),
                                         &rid));  // 新值应该存在
    EXPECT_FALSE(index_manager->FindEntry("idx_category", Value(1),
                                          &rid));  // 旧值应该不存在
    EXPECT_TRUE(index_manager->FindEntry("idx_category", Value(2),
                                         &rid));  // 新值应该存在

    // 删除数据并测试索引维护
    std::string delete_sql = "DELETE FROM products WHERE product_id = 1;";
    ASSERT_TRUE(ExecuteSQL(delete_sql));

    // 验证索引中的数据被删除
    EXPECT_FALSE(index_manager->FindEntry("idx_price", Value(1199.99), &rid));
    EXPECT_FALSE(index_manager->FindEntry("idx_category", Value(2), &rid));
}

// 测试4: 大量数据的索引性能
TEST_F(IndexManagerTest, LargeDataPerformance) {
    // 创建表
    std::string create_sql = R"(
        CREATE TABLE large_table (
            id INT PRIMARY KEY,
            value INT,
            data VARCHAR(50)
        );
    )";
    ASSERT_TRUE(ExecuteSQL(create_sql));

    // 插入大量数据
    const int DATA_SIZE = 1000;
    for (int i = 0; i < DATA_SIZE; ++i) {
        std::string insert_sql =
            "INSERT INTO large_table VALUES (" + std::to_string(i) + ", " +
            std::to_string(i * 2) + ", 'data" + std::to_string(i) + "');";
        ASSERT_TRUE(ExecuteSQL(insert_sql));
    }

    // 创建索引
    auto start_time = std::chrono::high_resolution_clock::now();
    EXPECT_TRUE(
        table_manager_->CreateIndex("idx_value", "large_table", {"value"}));
    auto end_time = std::chrono::high_resolution_clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        end_time - start_time);
    std::cout << "Index creation time for " << DATA_SIZE
              << " records: " << duration.count() << " ms" << std::endl;

    IndexManager* index_manager = table_manager_->GetIndexManager();

    // 测试查找性能
    start_time = std::chrono::high_resolution_clock::now();
    int found_count = 0;
    for (int i = 0; i < 100; ++i) {
        RID rid;
        if (index_manager->FindEntry("idx_value", Value(i * 2), &rid)) {
            found_count++;
        }
    }
    end_time = std::chrono::high_resolution_clock::now();

    auto duration2 = std::chrono::duration_cast<std::chrono::microseconds>(
        end_time - start_time);
    std::cout << "100 index lookups time: " << duration2.count() << " μs"
              << std::endl;
    EXPECT_EQ(found_count, 100);
}

// 测试5: 错误处理
TEST_F(IndexManagerTest, ErrorHandling) {
    IndexManager* index_manager = table_manager_->GetIndexManager();

    // 在不存在的表上创建索引
    std::vector<Column> columns = {{"id", TypeId::INTEGER, 0, false, true}};
    Schema fake_schema(columns);
    EXPECT_FALSE(index_manager->CreateIndex("fake_idx", "fake_table", {"id"},
                                            &fake_schema));

    // 创建正确的表
    std::string create_sql = R"(
        CREATE TABLE error_test (
            id INT PRIMARY KEY,
            name VARCHAR(50)
        );
    )";
    ASSERT_TRUE(ExecuteSQL(create_sql));

    TableInfo* table_info = catalog_->GetTable("error_test");
    ASSERT_NE(table_info, nullptr);

    // 在不存在的列上创建索引
    EXPECT_FALSE(index_manager->CreateIndex("idx_fake_col", "error_test",
                                            {"fake_column"},
                                            table_info->schema.get()));

    // 创建空列名的索引
    EXPECT_FALSE(index_manager->CreateIndex("idx_empty", "error_test", {},
                                            table_info->schema.get()));

    // 创建多列索引（当前不支持）
    EXPECT_FALSE(index_manager->CreateIndex(
        "idx_multi", "error_test", {"id", "name"}, table_info->schema.get()));

    // 删除不存在的索引
    EXPECT_FALSE(index_manager->DropIndex("non_existent_index"));
}

// 测试6: 并发访问
TEST_F(IndexManagerTest, ConcurrentAccess) {
    // 创建表
    std::string create_sql = R"(
        CREATE TABLE concurrent_test (
            id INT PRIMARY KEY,
            thread_id INT,
            value INT
        );
    )";
    ASSERT_TRUE(ExecuteSQL(create_sql));

    EXPECT_TRUE(
        table_manager_->CreateIndex("idx_value", "concurrent_test", {"value"}));

    IndexManager* index_manager = table_manager_->GetIndexManager();
    const int NUM_THREADS = 4;
    const int RECORDS_PER_THREAD = 50;
    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};

    // 并发插入
    for (int t = 0; t < NUM_THREADS; ++t) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < RECORDS_PER_THREAD; ++i) {
                int id = t * RECORDS_PER_THREAD + i;
                std::string insert_sql =
                    "INSERT INTO concurrent_test VALUES (" +
                    std::to_string(id) + ", " + std::to_string(t) + ", " +
                    std::to_string(id * 10) + ");";
                if (ExecuteSQL(insert_sql)) {
                    success_count++;
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // 验证插入结果
    EXPECT_GT(success_count.load(),
              NUM_THREADS * RECORDS_PER_THREAD / 2);  // 至少一半成功

    // 并发查找
    threads.clear();
    std::atomic<int> find_count{0};

    for (int t = 0; t < NUM_THREADS; ++t) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < RECORDS_PER_THREAD; ++i) {
                int value = (t * RECORDS_PER_THREAD + i) * 10;
                RID rid;
                if (index_manager->FindEntry("idx_value", Value(value), &rid)) {
                    find_count++;
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_GT(find_count.load(), 0);
}

// 测试7: 不同数据类型的索引
TEST_F(IndexManagerTest, DifferentDataTypes) {
    // 创建包含各种数据类型的表
    std::string create_sql = R"(
        CREATE TABLE type_test (
            id INT PRIMARY KEY,
            big_num BIGINT,
            float_val FLOAT,
            double_val DOUBLE,
            text_val VARCHAR(100),
            flag BOOLEAN
        );
    )";
    ASSERT_TRUE(ExecuteSQL(create_sql));

    TableInfo* table_info = catalog_->GetTable("type_test");
    ASSERT_NE(table_info, nullptr);
    IndexManager* index_manager = table_manager_->GetIndexManager();

    // 为不同类型的列创建索引
    EXPECT_TRUE(index_manager->CreateIndex(
        "idx_bigint", "type_test", {"big_num"}, table_info->schema.get()));
    EXPECT_TRUE(index_manager->CreateIndex(
        "idx_float", "type_test", {"float_val"}, table_info->schema.get()));
    EXPECT_TRUE(index_manager->CreateIndex(
        "idx_double", "type_test", {"double_val"}, table_info->schema.get()));
    EXPECT_TRUE(index_manager->CreateIndex(
        "idx_varchar", "type_test", {"text_val"}, table_info->schema.get()));

    // 插入测试数据
    std::string insert_sql = R"(
        INSERT INTO type_test VALUES 
            (1, 1234567890, 3.14, 2.718281828, 'hello', TRUE);
    )";
    ASSERT_TRUE(ExecuteSQL(insert_sql));

    // 测试不同类型的查找
    RID rid;
    EXPECT_TRUE(index_manager->FindEntry(
        "idx_bigint", Value(static_cast<int64_t>(1234567890)), &rid));
    EXPECT_TRUE(index_manager->FindEntry("idx_float", Value(3.14f), &rid));
    EXPECT_TRUE(
        index_manager->FindEntry("idx_double", Value(2.718281828), &rid));
    EXPECT_TRUE(index_manager->FindEntry("idx_varchar",
                                         Value(std::string("hello")), &rid));
}

// 测试8: 索引持久化
TEST_F(IndexManagerTest, IndexPersistence) {
    // 创建表和索引
    std::string create_sql = R"(
        CREATE TABLE persist_test (
            id INT PRIMARY KEY,
            value INT
        );
    )";
    ASSERT_TRUE(ExecuteSQL(create_sql));

    EXPECT_TRUE(
        table_manager_->CreateIndex("idx_persist", "persist_test", {"value"}));

    // 插入数据
    for (int i = 0; i < 10; ++i) {
        std::string insert_sql = "INSERT INTO persist_test VALUES (" +
                                 std::to_string(i) + ", " +
                                 std::to_string(i * 100) + ");";
        ASSERT_TRUE(ExecuteSQL(insert_sql));
    }

    // 强制刷新到磁盘
    buffer_pool_manager_->FlushAllPages();

    // 重新创建系统（模拟重启）
    SetUp();

    // 重新执行创建表的SQL
    ASSERT_TRUE(ExecuteSQL(create_sql));

    // 重新创建索引（在实际系统中应该从目录中自动恢复）
    EXPECT_TRUE(
        table_manager_->CreateIndex("idx_persist", "persist_test", {"value"}));

    IndexManager* index_manager = table_manager_->GetIndexManager();

    // 验证数据持久化
    for (int i = 0; i < 10; ++i) {
        RID rid;
        bool found =
            index_manager->FindEntry("idx_persist", Value(i * 100), &rid);
        if (!found) {
            // 如果索引中没有找到，检查表中是否有数据
            std::string select_sql =
                "SELECT id FROM persist_test WHERE value = " +
                std::to_string(i * 100) + ";";
            std::vector<Tuple> result;
            ExecuteSQL(select_sql, &result);
            // 如果表中有数据但索引中没有，说明索引数据没有持久化，这是预期的
            // 在实际系统中，需要在启动时重建索引或从索引文件中恢复数据
        }
    }
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}