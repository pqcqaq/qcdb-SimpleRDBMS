// test/unit/simple_index_manager_test.cpp
#include <cassert>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <memory>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "buffer/lru_replacer.h"
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "catalog/table_manager.h"
#include "common/debug.h"
#include "common/types.h"
#include "execution/execution_engine.h"
#include "index/index_manager.h"
#include "parser/parser.h"
#include "recovery/log_manager.h"
#include "storage/disk_manager.h"
#include "transaction/lock_manager.h"
#include "transaction/transaction_manager.h"

using namespace SimpleRDBMS;

class SimpleIndexTest {
   private:
    static constexpr const char* DB_FILE = "simple_index_test.db";
    static constexpr const char* LOG_FILE = "simple_index_test.log";
    static constexpr size_t BUFFER_POOL_SIZE = 100;

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

    int test_count_;
    int passed_count_;

   public:
    SimpleIndexTest() : test_count_(0), passed_count_(0) {}

    void SetUp() {
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

        std::cout << "✓ 测试环境初始化完成" << std::endl;
    }

    void TearDown() {
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
            std::cerr << "SQL 执行错误: " << e.what() << std::endl;
            return false;
        }
    }

    void AssertTrue(bool condition, const std::string& message) {
        test_count_++;
        if (condition) {
            passed_count_++;
            std::cout << "✓ " << message << std::endl;
        } else {
            std::cout << "✗ " << message << std::endl;
        }
    }

    void AssertFalse(bool condition, const std::string& message) {
        AssertTrue(!condition, message);
    }

    void AssertEqual(int expected, int actual, const std::string& message) {
        test_count_++;
        if (expected == actual) {
            passed_count_++;
            std::cout << "✓ " << message << " (期望: " << expected
                      << ", 实际: " << actual << ")" << std::endl;
        } else {
            std::cout << "✗ " << message << " (期望: " << expected
                      << ", 实际: " << actual << ")" << std::endl;
        }
    }

    // 测试1: 基本索引创建和删除
    void TestBasicIndexOperations() {
        std::cout << "\n--- 测试1: 基本索引操作 ---" << std::endl;

        std::string create_sql = R"(
        CREATE TABLE test_table (
            id INT PRIMARY KEY,
            name VARCHAR(50),
            age INT,
            score DOUBLE
        );
    )";
        AssertTrue(ExecuteSQL(create_sql), "创建测试表");

        IndexManager* index_manager = table_manager_->GetIndexManager();
        AssertTrue(index_manager != nullptr, "获取索引管理器");

        TableInfo* table_info = catalog_->GetTable("test_table");
        AssertTrue(table_info != nullptr, "获取表信息");

        // 使用 TableManager 的方法而不是直接调用 IndexManager
        AssertTrue(table_manager_->CreateIndex("idx_id", "test_table", {"id"}),
                   "创建整数类型索引");
        AssertTrue(
            table_manager_->CreateIndex("idx_name", "test_table", {"name"}),
            "创建字符串类型索引");
        AssertTrue(
            table_manager_->CreateIndex("idx_age", "test_table", {"age"}),
            "创建年龄索引");
        AssertTrue(
            table_manager_->CreateIndex("idx_score", "test_table", {"score"}),
            "创建分数索引");

        // 测试重复创建索引
        AssertFalse(table_manager_->CreateIndex("idx_id", "test_table", {"id"}),
                    "重复创建索引应该失败");

        std::vector<std::string> all_indexes =
            index_manager->GetAllIndexNames();
        AssertEqual(5, static_cast<int>(all_indexes.size()), "总索引数量检查");

        std::vector<std::string> table_indexes =
            index_manager->GetTableIndexes("test_table");
        AssertEqual(5, static_cast<int>(table_indexes.size()),
                    "表索引数量检查");

        AssertTrue(table_manager_->DropIndex("idx_name"), "删除字符串索引");
        AssertFalse(table_manager_->DropIndex("idx_name"),
                    "重复删除索引应该失败");

        table_indexes = index_manager->GetTableIndexes("test_table");
        AssertEqual(4, static_cast<int>(table_indexes.size()),
                    "删除后的索引数量检查");
    }

    // 测试2: 索引数据填充
    void TestIndexDataPopulation() {
        std::cout << "\n--- 测试2: 索引数据填充 ---" << std::endl;

        std::string create_sql = R"(
        CREATE TABLE employees (
            emp_id INT PRIMARY KEY,
            emp_name VARCHAR(100),
            salary INT,
            dept_id INT
        );
    )";
        AssertTrue(ExecuteSQL(create_sql), "创建员工表");

        std::vector<std::string> insert_sqls = {
            "INSERT INTO employees VALUES (1, 'Alice', 50000, 1);",
            "INSERT INTO employees VALUES (2, 'Bob', 60000, 2);",
            "INSERT INTO employees VALUES (3, 'Charlie', 55000, 1);",
            "INSERT INTO employees VALUES (4, 'David', 65000, 3);",
            "INSERT INTO employees VALUES (5, 'Eve', 52000, 2);"};

        for (size_t i = 0; i < insert_sqls.size(); ++i) {
            AssertTrue(ExecuteSQL(insert_sqls[i]),
                       "插入员工数据 " + std::to_string(i + 1));
        }

        AssertTrue(
            table_manager_->CreateIndex("idx_salary", "employees", {"salary"}),
            "创建薪资索引");
        AssertTrue(
            table_manager_->CreateIndex("idx_dept", "employees", {"dept_id"}),
            "创建部门索引");

        IndexManager* index_manager = table_manager_->GetIndexManager();
        RID rid;
        AssertTrue(index_manager->FindEntry("idx_salary", Value(50000), &rid),
                   "查找薪资 50000");
        AssertTrue(index_manager->FindEntry("idx_salary", Value(60000), &rid),
                   "查找薪资 60000");
        AssertFalse(index_manager->FindEntry("idx_salary", Value(99999), &rid),
                    "查找不存在的薪资");

        AssertTrue(index_manager->FindEntry("idx_dept", Value(1), &rid),
                   "查找部门 1");
        AssertTrue(index_manager->FindEntry("idx_dept", Value(2), &rid),
                   "查找部门 2");
        AssertFalse(index_manager->FindEntry("idx_dept", Value(999), &rid),
                    "查找不存在的部门");
    }

    // 测试3: 索引维护（增删改）
    void TestIndexMaintenance() {
        std::cout << "\n--- 测试3: 索引维护 ---" << std::endl;

        std::string create_sql = R"(
        CREATE TABLE products (
            product_id INT PRIMARY KEY,
            product_name VARCHAR(100),
            price DOUBLE,
            category_id INT
        );
    )";
        AssertTrue(ExecuteSQL(create_sql), "创建产品表");

        AssertTrue(
            table_manager_->CreateIndex("idx_price", "products", {"price"}),
            "创建价格索引");
        AssertTrue(table_manager_->CreateIndex("idx_category", "products",
                                               {"category_id"}),
                   "创建分类索引");

        IndexManager* index_manager = table_manager_->GetIndexManager();

        std::string insert_sql =
            "INSERT INTO products VALUES (1, 'Laptop', 999.99, 1);";
        AssertTrue(ExecuteSQL(insert_sql), "插入产品数据");

        RID rid;
        AssertTrue(index_manager->FindEntry("idx_price", Value(999.99), &rid),
                   "在价格索引中找到新插入的数据");
        AssertTrue(index_manager->FindEntry("idx_category", Value(1), &rid),
                   "在分类索引中找到新插入的数据");

        std::string update_sql =
            "UPDATE products SET price = 1199.99, category_id = 2 WHERE "
            "product_id = 1;";
        AssertTrue(ExecuteSQL(update_sql), "更新产品数据");

        AssertFalse(index_manager->FindEntry("idx_price", Value(999.99), &rid),
                    "价格索引中旧值应该不存在");
        AssertTrue(index_manager->FindEntry("idx_price", Value(1199.99), &rid),
                   "价格索引中新值应该存在");
        AssertFalse(index_manager->FindEntry("idx_category", Value(1), &rid),
                    "分类索引中旧值应该不存在");
        AssertTrue(index_manager->FindEntry("idx_category", Value(2), &rid),
                   "分类索引中新值应该存在");

        std::string delete_sql = "DELETE FROM products WHERE product_id = 1;";
        AssertTrue(ExecuteSQL(delete_sql), "删除产品数据");

        AssertFalse(index_manager->FindEntry("idx_price", Value(1199.99), &rid),
                    "删除后价格索引中数据应该不存在");
        AssertFalse(index_manager->FindEntry("idx_category", Value(2), &rid),
                    "删除后分类索引中数据应该不存在");
    }

    // 测试4: 性能测试
    void TestPerformance() {
        std::cout << "\n--- 测试4: 性能测试 ---" << std::endl;

        // 创建表
        std::string create_sql = R"(
            CREATE TABLE perf_test (
                id INT PRIMARY KEY,
                value INT,
                data VARCHAR(50)
            );
        )";
        AssertTrue(ExecuteSQL(create_sql), "创建性能测试表");

        // 插入数据
        const int DATA_SIZE = 500;  // 减少数据量以提高测试速度
        std::cout << "插入 " << DATA_SIZE << " 条记录..." << std::endl;

        auto start_time = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < DATA_SIZE; ++i) {
            std::string insert_sql =
                "INSERT INTO perf_test VALUES (" + std::to_string(i) + ", " +
                std::to_string(i * 2) + ", 'data" + std::to_string(i) + "');";
            if (!ExecuteSQL(insert_sql)) {
                std::cout << "插入第 " << i << " 条记录失败" << std::endl;
                break;
            }
        }
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            end_time - start_time);
        std::cout << "数据插入完成，耗时: " << duration.count() << " ms"
                  << std::endl;

        // 创建索引
        start_time = std::chrono::high_resolution_clock::now();
        AssertTrue(
            table_manager_->CreateIndex("idx_value", "perf_test", {"value"}),
            "创建性能测试索引");
        end_time = std::chrono::high_resolution_clock::now();

        duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            end_time - start_time);
        std::cout << "索引创建耗时: " << duration.count() << " ms" << std::endl;

        IndexManager* index_manager = table_manager_->GetIndexManager();

        // 测试查找性能
        start_time = std::chrono::high_resolution_clock::now();
        int found_count = 0;
        const int LOOKUP_COUNT = 100;
        for (int i = 0; i < LOOKUP_COUNT; ++i) {
            RID rid;
            if (index_manager->FindEntry("idx_value", Value(i * 2), &rid)) {
                found_count++;
            }
        }
        end_time = std::chrono::high_resolution_clock::now();

        auto lookup_duration =
            std::chrono::duration_cast<std::chrono::microseconds>(end_time -
                                                                  start_time);
        std::cout << LOOKUP_COUNT
                  << " 次索引查找耗时: " << lookup_duration.count() << " μs"
                  << std::endl;
        std::cout << "平均每次查找: "
                  << (double)lookup_duration.count() / LOOKUP_COUNT << " μs"
                  << std::endl;

        AssertEqual(LOOKUP_COUNT, found_count, "查找成功次数");
    }

    // 测试5: 错误处理
    void TestErrorHandling() {
        std::cout << "\n--- 测试5: 错误处理 ---" << std::endl;

        std::string create_sql = R"(
        CREATE TABLE error_test (
            id INT PRIMARY KEY,
            name VARCHAR(50)
        );
    )";
        AssertTrue(ExecuteSQL(create_sql), "创建错误测试表");

        TableInfo* table_info = catalog_->GetTable("error_test");
        AssertTrue(table_info != nullptr, "获取错误测试表信息");

        AssertFalse(table_manager_->CreateIndex("idx_fake_col", "error_test",
                                                {"fake_column"}),
                    "在不存在的列上创建索引应该失败");
        AssertFalse(table_manager_->CreateIndex("idx_empty", "error_test", {}),
                    "创建空列名的索引应该失败");
        AssertFalse(table_manager_->CreateIndex("idx_multi", "error_test",
                                                {"id", "name"}),
                    "创建多列索引应该失败");

        AssertFalse(table_manager_->DropIndex("non_existent_index"),
                    "删除不存在的索引应该失败");
    }

    // 测试6: 不同数据类型
    void TestDifferentDataTypes() {
        std::cout << "\n--- 测试6: 不同数据类型 ---" << std::endl;

        std::string create_sql = R"(
        CREATE TABLE type_test (
            id INT PRIMARY KEY,
            big_num BIGINT,
            float_val FLOAT,
            double_val DOUBLE,
            text_val VARCHAR(100)
        );
    )";
        AssertTrue(ExecuteSQL(create_sql), "创建类型测试表");

        TableInfo* table_info = catalog_->GetTable("type_test");
        AssertTrue(table_info != nullptr, "获取类型测试表信息");

        AssertTrue(
            table_manager_->CreateIndex("idx_bigint", "type_test", {"big_num"}),
            "创建 BIGINT 类型索引");
        AssertTrue(table_manager_->CreateIndex("idx_float", "type_test",
                                               {"float_val"}),
                   "创建 FLOAT 类型索引");
        AssertTrue(table_manager_->CreateIndex("idx_double", "type_test",
                                               {"double_val"}),
                   "创建 DOUBLE 类型索引");
        AssertTrue(table_manager_->CreateIndex("idx_varchar", "type_test",
                                               {"text_val"}),
                   "创建 VARCHAR 类型索引");

        std::string insert_sql = R"(
        INSERT INTO type_test VALUES 
            (1, 1234567890, 3.14, 2.718281828, 'hello');
    )";
        AssertTrue(ExecuteSQL(insert_sql), "插入类型测试数据");

        IndexManager* index_manager = table_manager_->GetIndexManager();
        RID rid;
        AssertTrue(
            index_manager->FindEntry(
                "idx_bigint", Value(static_cast<int64_t>(1234567890)), &rid),
            "查找 BIGINT 类型数据");
        AssertTrue(index_manager->FindEntry("idx_float", Value(3.14f), &rid),
                   "查找 FLOAT 类型数据");
        AssertTrue(
            index_manager->FindEntry("idx_double", Value(2.718281828), &rid),
            "查找 DOUBLE 类型数据");
        AssertTrue(index_manager->FindEntry("idx_varchar",
                                            Value(std::string("hello")), &rid),
                   "查找 VARCHAR 类型数据");
    }

    void RunAllTests() {
        std::cout << "===========================================" << std::endl;
        std::cout << "       索引管理器功能测试开始" << std::endl;
        std::cout << "===========================================" << std::endl;

        SetUp();

        TestBasicIndexOperations();
        TestIndexDataPopulation();
        TestIndexMaintenance();
        TestPerformance();
        TestErrorHandling();
        TestDifferentDataTypes();

        std::cout << "\n==========================================="
                  << std::endl;
        std::cout << "               测试总结" << std::endl;
        std::cout << "===========================================" << std::endl;
        std::cout << "总测试数: " << test_count_ << std::endl;
        std::cout << "通过数: " << passed_count_ << std::endl;
        std::cout << "失败数: " << (test_count_ - passed_count_) << std::endl;
        std::cout << "成功率: " << std::fixed << std::setprecision(1)
                  << (100.0 * passed_count_ / test_count_) << "%" << std::endl;

        if (passed_count_ == test_count_) {
            std::cout << "🎉 所有测试通过！" << std::endl;
        } else {
            std::cout << "⚠️  部分测试失败，请检查实现" << std::endl;
        }

        TearDown();
    }
};

int main() {
    try {
        SimpleIndexTest test;
        test.RunAllTests();
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "测试异常: " << e.what() << std::endl;
        return 1;
    }
}