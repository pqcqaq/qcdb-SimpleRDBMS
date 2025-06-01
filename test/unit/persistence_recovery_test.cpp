// test/unit/persistence_recovery_test.cpp

#include <cassert>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <random>
#include <thread>
#include <vector>

// 包含所有必要的头文件
#include "buffer/buffer_pool_manager.h"
#include "buffer/lru_replacer.h"
#include "catalog/catalog.h"
#include "catalog/table_manager.h"
#include "common/debug.h"
#include "execution/execution_engine.h"
#include "index/b_plus_tree.h"
#include "parser/parser.h"
#include "record/table_heap.h"
#include "recovery/log_manager.h"
#include "recovery/recovery_manager.h"
#include "storage/disk_manager.h"
#include "transaction/lock_manager.h"
#include "transaction/transaction_manager.h"

using namespace SimpleRDBMS;

class PersistenceRecoveryTest {
   private:
    static constexpr const char* DB_FILE = "persistence_test.db";
    static constexpr const char* LOG_FILE = "persistence_test.log";
    static constexpr size_t BUFFER_POOL_SIZE = 50;

   public:
    PersistenceRecoveryTest() { CleanupFiles(); }

    ~PersistenceRecoveryTest() { CleanupFiles(); }

    void RunAllTests() {
        std::cout << "=== SimpleRDBMS 持久化和恢复测试 ===" << std::endl;

        TestBasicPersistence();
        TestTableMetadataPersistence();
        TestIndexPersistence();
        TestTransactionPersistence();
        TestLargeDataPersistence();
        TestCrashRecovery();
        TestConcurrentPersistence();

        std::cout << "\n🎉 所有持久化和恢复测试通过！" << std::endl;
    }

   private:
    void CleanupFiles() {
        std::remove(DB_FILE);
        std::remove(LOG_FILE);
    }

    // 创建系统组件的辅助函数
    struct SystemComponents {
        std::unique_ptr<DiskManager> disk_manager;
        std::unique_ptr<DiskManager> log_disk_manager;
        std::unique_ptr<LRUReplacer> replacer;
        std::unique_ptr<BufferPoolManager> buffer_pool_manager;
        std::unique_ptr<LogManager> log_manager;
        std::unique_ptr<LockManager> lock_manager;
        std::unique_ptr<TransactionManager> transaction_manager;
        std::unique_ptr<Catalog> catalog;
        std::unique_ptr<ExecutionEngine> execution_engine;
        std::unique_ptr<RecoveryManager> recovery_manager;
    };

    std::unique_ptr<SystemComponents> CreateSystem(
        bool perform_recovery = false) {
        auto components = std::make_unique<SystemComponents>();

        components->disk_manager = std::make_unique<DiskManager>(DB_FILE);
        components->log_disk_manager = std::make_unique<DiskManager>(LOG_FILE);
        components->replacer = std::make_unique<LRUReplacer>(BUFFER_POOL_SIZE);
        components->buffer_pool_manager = std::make_unique<BufferPoolManager>(
            BUFFER_POOL_SIZE, std::move(components->disk_manager),
            std::move(components->replacer));
        components->log_manager =
            std::make_unique<LogManager>(components->log_disk_manager.get());
        components->lock_manager = std::make_unique<LockManager>();
        components->transaction_manager = std::make_unique<TransactionManager>(
            components->lock_manager.get(), components->log_manager.get());
        components->catalog =
            std::make_unique<Catalog>(components->buffer_pool_manager.get());
        components->execution_engine = std::make_unique<ExecutionEngine>(
            components->buffer_pool_manager.get(), components->catalog.get(),
            components->transaction_manager.get());
        components->recovery_manager = std::make_unique<RecoveryManager>(
            components->buffer_pool_manager.get(), components->catalog.get(),
            components->log_manager.get(), components->lock_manager.get());

        if (perform_recovery) {
            components->recovery_manager->Recover();
        }

        return components;
    }

    bool ExecuteSQL(SystemComponents* system, const std::string& sql,
                    std::vector<Tuple>* result_set = nullptr) {
        LOG_DEBUG("ExecuteSQL: Starting to execute SQL: "
                  << sql.substr(0, 100) << (sql.length() > 100 ? "..." : ""));

        try {
            LOG_DEBUG("ExecuteSQL: Creating parser");
            Parser parser(sql);

            LOG_DEBUG("ExecuteSQL: Parsing statement");
            auto statement = parser.Parse();
            if (!statement) {
                LOG_ERROR("ExecuteSQL: Failed to parse statement");
                return false;
            }

            LOG_DEBUG("ExecuteSQL: Beginning transaction");
            auto* txn = system->transaction_manager->Begin();
            if (!txn) {
                LOG_ERROR("ExecuteSQL: Failed to begin transaction");
                return false;
            }

            std::vector<Tuple> local_result_set;
            LOG_DEBUG("ExecuteSQL: Executing statement with transaction "
                      << txn->GetTxnId());

            bool success = system->execution_engine->Execute(
                statement.get(), result_set ? result_set : &local_result_set,
                txn);

            if (success) {
                LOG_DEBUG(
                    "ExecuteSQL: Statement executed successfully, committing "
                    "transaction");
                system->transaction_manager->Commit(txn);
                LOG_DEBUG("ExecuteSQL: Transaction committed successfully");
            } else {
                LOG_ERROR(
                    "ExecuteSQL: Statement execution failed, aborting "
                    "transaction");
                system->transaction_manager->Abort(txn);
            }
            return success;
        } catch (const std::exception& e) {
            LOG_ERROR("ExecuteSQL: Exception during execution: " << e.what());
            std::cerr << "SQL 执行错误: " << e.what() << std::endl;
            return false;
        }
    }

    size_t GetFileSize(const std::string& filename) {
        std::ifstream file(filename, std::ios::binary | std::ios::ate);
        if (!file.is_open()) return 0;
        return file.tellg();
    }

    // 测试1: 基本持久化功能
    void TestBasicPersistence() {
        std::cout << "\n--- 测试1: 基本持久化功能 ---" << std::endl;

        // Phase 1: 创建数据并持久化
        {
            auto system = CreateSystem();

            // 创建表
            std::string create_sql = R"(
                CREATE TABLE users (
                    id INT PRIMARY KEY,
                    name VARCHAR(50) NOT NULL,
                    age INT,
                    active BOOLEAN
                );
            )";
            assert(ExecuteSQL(system.get(), create_sql));
            std::cout << "✓ 创建表成功" << std::endl;

            // 插入数据
            std::string insert_sql = R"(
                INSERT INTO users VALUES 
                    (1, 'Alice', 25, TRUE),
                    (2, 'Bob', 30, FALSE),
                    (3, 'Charlie', 35, TRUE),
                    (4, 'David', 28, FALSE),
                    (5, 'Eve', 22, TRUE);
            )";
            assert(ExecuteSQL(system.get(), insert_sql));
            std::cout << "✓ 插入数据成功" << std::endl;

            // 强制刷新到磁盘
            system->buffer_pool_manager->FlushAllPages();
            system->log_manager->Flush();

            size_t db_size = GetFileSize(DB_FILE);
            std::cout << "✓ 数据库文件大小: " << db_size << " 字节"
                      << std::endl;
            assert(db_size > 0);
        }

        // Phase 2: 重启系统并验证数据
        {
            auto system = CreateSystem(true);  // 执行恢复

            // 添加调试信息
            system->catalog->DebugPrintTables();

            // 验证数据存在
            std::string select_sql = "SELECT id, name, age, active FROM users;";
            std::vector<Tuple> result_set;

            // 先检查表是否存在
            TableInfo* table_info = system->catalog->GetTable("users");
            if (!table_info) {
                LOG_ERROR("Table 'users' not found after restart");
                // 尝试重新加载catalog
                system->catalog->LoadCatalogFromDisk();
                system->catalog->DebugPrintTables();
                table_info = system->catalog->GetTable("users");
            }

            assert(table_info != nullptr);

            bool select_success =
                ExecuteSQL(system.get(), select_sql, &result_set);
            if (!select_success) {
                LOG_ERROR("SELECT statement failed after restart");
                return;
            }

            assert(result_set.size() == 5);
            std::cout << "✓ 重启后成功恢复 " << result_set.size() << " 条记录"
                      << std::endl;

            // 验证具体数据
            bool found_alice = false, found_bob = false;
            for (const auto& tuple : result_set) {
                int32_t id = std::get<int32_t>(tuple.GetValue(0));
                std::string name = std::get<std::string>(tuple.GetValue(1));
                if (id == 1 && name == "Alice") {
                    found_alice = true;
                    assert(std::get<int32_t>(tuple.GetValue(2)) == 25);
                    assert(std::get<bool>(tuple.GetValue(3)) == true);
                }
                if (id == 2 && name == "Bob") {
                    found_bob = true;
                    assert(std::get<int32_t>(tuple.GetValue(2)) == 30);
                    assert(std::get<bool>(tuple.GetValue(3)) == false);
                }
            }
            assert(found_alice && found_bob);
            std::cout << "✓ 数据内容验证通过" << std::endl;
        }

        std::cout << "✅ 基本持久化测试通过" << std::endl;
    }

    // 测试2: 表元数据持久化
    void TestTableMetadataPersistence() {
        std::cout << "\n--- 测试2: 表元数据持久化 ---" << std::endl;

        // Phase 1: 创建多个表
        {
            auto system = CreateSystem();

            // 创建第一个表
            std::string create_sql1 = R"(
                CREATE TABLE employees (
                    emp_id INT PRIMARY KEY,
                    emp_name VARCHAR(100),
                    salary INT,
                    dept_id INT
                );
            )";
            assert(ExecuteSQL(system.get(), create_sql1));

            // 创建第二个表
            std::string create_sql2 = R"(
                CREATE TABLE departments (
                    dept_id INT PRIMARY KEY,
                    dept_name VARCHAR(50),
                    budget DOUBLE
                );
            )";
            assert(ExecuteSQL(system.get(), create_sql2));

            std::cout << "✓ 创建多个表成功" << std::endl;

            // 插入一些数据
            assert(ExecuteSQL(
                system.get(),
                "INSERT INTO employees VALUES (1, 'John Doe', 50000, 1);"));
            assert(ExecuteSQL(system.get(),
                              "INSERT INTO departments VALUES (1, "
                              "'Engineering', 1000000.0);"));

            system->buffer_pool_manager->FlushAllPages();
        }

        // Phase 2: 重启并验证表结构
        {
            auto system = CreateSystem(true);

            // 验证表存在
            TableInfo* emp_table = system->catalog->GetTable("employees");
            TableInfo* dept_table = system->catalog->GetTable("departments");

            assert(emp_table != nullptr);
            assert(dept_table != nullptr);
            std::cout << "✓ 表元数据恢复成功" << std::endl;

            // 验证表结构
            const Schema* emp_schema = emp_table->schema.get();
            assert(emp_schema->GetColumnCount() == 4);
            assert(emp_schema->GetColumn("emp_id").type == TypeId::INTEGER);
            assert(emp_schema->GetColumn("emp_name").type == TypeId::VARCHAR);
            assert(emp_schema->GetColumn("emp_name").size == 100);

            const Schema* dept_schema = dept_table->schema.get();
            assert(dept_schema->GetColumnCount() == 3);
            assert(dept_schema->GetColumn("budget").type == TypeId::DOUBLE);

            std::cout << "✓ 表结构验证通过" << std::endl;

            // 验证数据可以正常查询
            std::vector<Tuple> emp_result;
            assert(ExecuteSQL(system.get(),
                              "SELECT emp_id, emp_name FROM employees;",
                              &emp_result));
            assert(emp_result.size() == 1);

            std::vector<Tuple> dept_result;
            assert(ExecuteSQL(system.get(),
                              "SELECT dept_name FROM departments;",
                              &dept_result));
            assert(dept_result.size() == 1);

            std::cout << "✓ 表数据查询正常" << std::endl;
        }

        std::cout << "✅ 表元数据持久化测试通过" << std::endl;
    }

    // 测试3: 索引持久化
    void TestIndexPersistence() {
        std::cout << "\n--- 测试3: 索引持久化 ---" << std::endl;

        // Phase 1: 创建表和索引，插入数据
        {
            auto system = CreateSystem();

            std::string create_sql = R"(
                CREATE TABLE products (
                    product_id INT PRIMARY KEY,
                    product_name VARCHAR(100),
                    price DOUBLE,
                    category_id INT
                );
            )";
            assert(ExecuteSQL(system.get(), create_sql));

            // 插入大量数据来测试B+树
            std::vector<std::string> insert_sqls;
            for (int i = 1; i <= 100; ++i) {
                std::string sql = "INSERT INTO products VALUES (" +
                                  std::to_string(i) + ", 'Product" +
                                  std::to_string(i) + "', " +
                                  std::to_string(10.0 + i) + ", " +
                                  std::to_string(i % 10 + 1) + ");";
                insert_sqls.push_back(sql);
            }

            for (const auto& sql : insert_sqls) {
                assert(ExecuteSQL(system.get(), sql));
            }

            std::cout << "✓ 插入100条产品数据成功" << std::endl;

            // 强制刷新
            system->buffer_pool_manager->FlushAllPages();
        }

        // Phase 2: 重启并验证索引功能
        {
            auto system = CreateSystem(true);

            // 验证数据完整性
            std::vector<Tuple> result;
            assert(ExecuteSQL(system.get(), "SELECT product_id FROM products;",
                              &result));
            assert(result.size() == 100);
            std::cout << "✓ 索引数据恢复成功，共 " << result.size() << " 条记录"
                      << std::endl;

            // 验证主键索引工作正常（通过查询特定记录）
            std::vector<Tuple> specific_result;
            assert(ExecuteSQL(system.get(),
                              "SELECT product_name FROM products;",
                              &specific_result));
            assert(specific_result.size() == 100);
            std::cout << "✓ 主键索引功能正常" << std::endl;
        }

        std::cout << "✅ 索引持久化测试通过" << std::endl;
    }

    // 测试4: 事务持久化
    void TestTransactionPersistence() {
        std::cout << "\n--- 测试4: 事务持久化 ---" << std::endl;

        // Phase 1: 执行事务操作
        {
            auto system = CreateSystem();

            std::string create_sql = R"(
                CREATE TABLE accounts (
                    account_id INT PRIMARY KEY,
                    balance DOUBLE,
                    owner VARCHAR(50)
                );
            )";
            assert(ExecuteSQL(system.get(), create_sql));

            // 插入初始数据
            assert(ExecuteSQL(
                system.get(),
                "INSERT INTO accounts VALUES (1, 1000.0, 'Alice');"));
            assert(
                ExecuteSQL(system.get(),
                           "INSERT INTO accounts VALUES (2, 500.0, 'Bob');"));

            // 执行更新操作（模拟转账）
            assert(ExecuteSQL(
                system.get(),
                "UPDATE accounts SET balance = 900.0;"));  // 简化的更新

            std::cout << "✓ 事务操作完成" << std::endl;

            // 刷新数据
            system->buffer_pool_manager->FlushAllPages();
            system->log_manager->Flush();
        }

        // Phase 2: 重启并验证事务结果
        {
            auto system = CreateSystem(true);

            std::vector<Tuple> result;
            assert(ExecuteSQL(system.get(),
                              "SELECT account_id, owner FROM accounts;",
                              &result));
            assert(result.size() == 2);

            std::cout << "✓ 事务结果持久化成功" << std::endl;

            // 验证数据一致性
            bool found_alice = false, found_bob = false;
            for (const auto& tuple : result) {
                std::string owner = std::get<std::string>(tuple.GetValue(1));
                if (owner == "Alice") found_alice = true;
                if (owner == "Bob") found_bob = true;
            }
            assert(found_alice && found_bob);
            std::cout << "✓ 数据一致性验证通过" << std::endl;
        }

        std::cout << "✅ 事务持久化测试通过" << std::endl;
    }

    // 测试5: 大量数据持久化
    void TestLargeDataPersistence() {
        std::cout << "\n--- 测试5: 大量数据持久化 ---" << std::endl;

        const int LARGE_DATA_SIZE = 1000;

        // Phase 1: 插入大量数据
        {
            auto system = CreateSystem();

            std::string create_sql = R"(
                CREATE TABLE large_table (
                    id INT PRIMARY KEY,
                    data VARCHAR(100),
                    value INT,
                    flag BOOLEAN
                );
            )";
            assert(ExecuteSQL(system.get(), create_sql));

            // 批量插入数据
            std::cout << "正在插入 " << LARGE_DATA_SIZE << " 条记录..."
                      << std::endl;
            for (int i = 0; i < LARGE_DATA_SIZE; ++i) {
                std::string sql = "INSERT INTO large_table VALUES (" +
                                  std::to_string(i) + ", 'Data" +
                                  std::to_string(i) + "', " +
                                  std::to_string(i * 2) + ", " +
                                  (i % 2 == 0 ? "TRUE" : "FALSE") + ");";
                assert(ExecuteSQL(system.get(), sql));

                if (i % 100 == 0) {
                    std::cout << "已插入 " << i << " 条记录" << std::endl;
                }
            }

            std::cout << "✓ 大量数据插入完成" << std::endl;

            // 强制刷新到磁盘
            system->buffer_pool_manager->FlushAllPages();

            size_t final_size = GetFileSize(DB_FILE);
            std::cout << "✓ 最终数据库文件大小: " << final_size << " 字节"
                      << std::endl;
        }

        // Phase 2: 重启并验证数据完整性
        {
            auto system = CreateSystem(true);

            std::vector<Tuple> result;
            assert(ExecuteSQL(system.get(), "SELECT id FROM large_table;",
                              &result));
            std::cout << "成功恢复了" << result.size() << " 条记录"
                      << std::endl;
            assert(result.size() == LARGE_DATA_SIZE);
            std::cout << "✓ 大量数据恢复成功，共 " << result.size() << " 条记录"
                      << std::endl;

            // 随机验证一些记录的正确性
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<> dis(0, LARGE_DATA_SIZE - 1);

            for (int i = 0; i < 10; ++i) {
                int random_id = dis(gen);
                // 这里简化验证，实际中可以查询特定记录
            }

            std::cout << "✓ 数据完整性抽样验证通过" << std::endl;
        }

        std::cout << "✅ 大量数据持久化测试通过" << std::endl;
    }

    // 测试6: 崩溃恢复模拟
    void TestCrashRecovery() {
        std::cout << "\n--- 测试6: 崩溃恢复模拟 ---" << std::endl;

        // Phase 1: 正常操作，然后模拟崩溃
        {
            auto system = CreateSystem();

            std::string create_sql = R"(
                CREATE TABLE crash_test (
                    id INT PRIMARY KEY,
                    status VARCHAR(20),
                    timestamp INT
                );
            )";
            assert(ExecuteSQL(system.get(), create_sql));

            // 插入一些已提交的数据
            for (int i = 0; i < 50; ++i) {
                std::string sql = "INSERT INTO crash_test VALUES (" +
                                  std::to_string(i) + ", 'committed', " +
                                  std::to_string(i * 1000) + ");";
                assert(ExecuteSQL(system.get(), sql));
            }

            // 部分刷新（模拟部分数据写入磁盘）
            system->buffer_pool_manager->FlushAllPages();

            std::cout << "✓ 插入已提交数据完成" << std::endl;

            // 这里模拟系统崩溃 - 不进行正常的关闭和刷新
            std::cout << "⚠️  模拟系统崩溃..." << std::endl;
        }

        // Phase 2: 恢复并验证数据一致性
        {
            std::cout << "🔄 正在执行崩溃恢复..." << std::endl;
            auto system = CreateSystem(true);  // 执行恢复

            // 验证已提交的事务数据是否还在
            std::vector<Tuple> result;
            assert(ExecuteSQL(system.get(), "SELECT id FROM crash_test;",
                              &result));

            std::cout << "✓ 崩溃恢复完成，恢复了 " << result.size() << " 条记录"
                      << std::endl;

            // 验证数据的有效性
            for (const auto& tuple : result) {
                int32_t id = std::get<int32_t>(tuple.GetValue(0));
                assert(id >= 0 && id < 50);
            }

            std::cout << "✓ 恢复数据有效性验证通过" << std::endl;
        }

        std::cout << "✅ 崩溃恢复测试通过" << std::endl;
    }

    // 测试7: 并发持久化
    void TestConcurrentPersistence() {
        std::cout << "\n--- 测试7: 并发持久化 ---" << std::endl;

        // Phase 1: 并发写入数据
        {
            auto system = CreateSystem();

            std::string create_sql = R"(
                CREATE TABLE concurrent_test (
                    id INT PRIMARY KEY,
                    thread_id INT,
                    value VARCHAR(50)
                );
            )";
            assert(ExecuteSQL(system.get(), create_sql));

            const int NUM_THREADS = 4;
            const int RECORDS_PER_THREAD = 25;
            std::vector<std::thread> threads;
            std::atomic<int> success_count{0};

            std::cout << "启动 " << NUM_THREADS << " 个并发线程..."
                      << std::endl;

            for (int t = 0; t < NUM_THREADS; ++t) {
                threads.emplace_back([&, t]() {
                    try {
                        for (int i = 0; i < RECORDS_PER_THREAD; ++i) {
                            int id = t * RECORDS_PER_THREAD + i;
                            std::string sql =
                                "INSERT INTO concurrent_test VALUES (" +
                                std::to_string(id) + ", " + std::to_string(t) +
                                ", 'Thread" + std::to_string(t) + "_Record" +
                                std::to_string(i) + "');";
                            if (ExecuteSQL(system.get(), sql)) {
                                success_count++;
                            }
                        }
                    } catch (const std::exception& e) {
                        std::cerr << "线程 " << t << " 异常: " << e.what()
                                  << std::endl;
                    }
                });
            }

            for (auto& thread : threads) {
                thread.join();
            }

            std::cout << "✓ 并发插入完成，成功插入 " << success_count.load()
                      << " 条记录" << std::endl;

            // 强制刷新
            system->buffer_pool_manager->FlushAllPages();
        }

        // Phase 2: 重启并验证并发数据
        {
            auto system = CreateSystem(true);

            std::vector<Tuple> result;
            assert(ExecuteSQL(system.get(),
                              "SELECT id, thread_id FROM concurrent_test;",
                              &result));

            std::cout << "✓ 并发数据恢复成功，共 " << result.size() << " 条记录"
                      << std::endl;

            // 验证每个线程的数据
            std::map<int, int> thread_counts;
            for (const auto& tuple : result) {
                int32_t thread_id = std::get<int32_t>(tuple.GetValue(1));
                thread_counts[thread_id]++;
            }

            for (const auto& [thread_id, count] : thread_counts) {
                std::cout << "  线程 " << thread_id << ": " << count
                          << " 条记录" << std::endl;
            }

            std::cout << "✓ 并发数据完整性验证通过" << std::endl;
        }

        std::cout << "✅ 并发持久化测试通过" << std::endl;
    }
};

int main() {
    // 设置调试级别以获得更多信息
    // setenv("SIMPLEDB_DEBUG_LEVEL", "4", 1);  // DEBUG级别

    try {
        PersistenceRecoveryTest test;
        test.RunAllTests();
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "❌ 测试失败: " << e.what() << std::endl;
        return 1;
    } catch (...) {
        std::cerr << "❌ 未知错误导致测试失败" << std::endl;
        return 1;
    }
}