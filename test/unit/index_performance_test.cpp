#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <iomanip>
#include <memory>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "buffer/lru_replacer.h"
#include "catalog/catalog.h"
#include "catalog/table_manager.h"
#include "execution/execution_engine.h"
#include "parser/parser.h"
#include "recovery/log_manager.h"
#include "recovery/recovery_manager.h"
#include "storage/disk_manager.h"
#include "transaction/lock_manager.h"
#include "transaction/transaction_manager.h"

using namespace SimpleRDBMS;

class PerformanceTestFixture : public ::testing::Test {
   protected:
    void SetUp() override {
        // 清理之前的测试文件
        std::remove("test_perf.db");
        std::remove("test_perf.db.log");

        // 初始化数据库组件
        disk_manager_ = std::make_unique<DiskManager>("test_perf.db");
        log_disk_manager_ = std::make_unique<DiskManager>("test_perf.db.log");
        replacer_ = std::make_unique<LRUReplacer>(1000);  // 增大缓冲池
        buffer_pool_manager_ = std::make_unique<BufferPoolManager>(
            1000, std::move(disk_manager_), std::move(replacer_));

        log_manager_ = std::make_unique<LogManager>(log_disk_manager_.get());
        lock_manager_ = std::make_unique<LockManager>();
        transaction_manager_ = std::make_unique<TransactionManager>(
            lock_manager_.get(), log_manager_.get());

        catalog_ = std::make_unique<Catalog>(buffer_pool_manager_.get(),
                                             log_manager_.get());
        table_manager_ = std::make_unique<TableManager>(
            buffer_pool_manager_.get(), catalog_.get());
        recovery_manager_ = std::make_unique<RecoveryManager>(
            buffer_pool_manager_.get(), catalog_.get(), log_manager_.get(),
            lock_manager_.get());
        execution_engine_ = std::make_unique<ExecutionEngine>(
            buffer_pool_manager_.get(), catalog_.get(),
            transaction_manager_.get(), log_manager_.get());

        recovery_manager_->Recover();

        // 初始化随机数生成器
        rng_.seed(42);  // 固定种子以保证结果可重现
    }

    void TearDown() override {
        recovery_manager_->Checkpoint();

        // 清理测试文件
        std::remove("test_perf.db");
        std::remove("test_perf.db.log");
    }

    // 执行SQL并返回执行时间（毫秒）
    long long ExecuteSQL(const std::string& sql) {
        auto start = std::chrono::high_resolution_clock::now();

        try {
            Parser parser(sql);
            auto statement = parser.Parse();
            auto* txn = transaction_manager_->Begin();

            std::vector<Tuple> result_set;
            bool success =
                execution_engine_->Execute(statement.get(), &result_set, txn);

            if (success) {
                transaction_manager_->Commit(txn);
            } else {
                transaction_manager_->Abort(txn);
            }

            auto end = std::chrono::high_resolution_clock::now();
            return std::chrono::duration_cast<std::chrono::milliseconds>(end -
                                                                         start)
                .count();
        } catch (const std::exception& e) {
            std::cerr << "SQL: " << sql << " 执行失败: " << e.what()
                      << std::endl;
            auto end = std::chrono::high_resolution_clock::now();
            return std::chrono::duration_cast<std::chrono::milliseconds>(end -
                                                                         start)
                .count();
        }
    }

    // 生成随机字符串
    std::string GenerateRandomString(int length) {
        const std::string chars =
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        std::string result;
        result.reserve(length);

        std::uniform_int_distribution<> dis(0, chars.size() - 1);
        for (int i = 0; i < length; ++i) {
            result += chars[dis(rng_)];
        }
        return result;
    }

    // 生成随机日期字符串
    std::string GenerateRandomDate() {
        std::uniform_int_distribution<> year_dis(2020, 2024);
        std::uniform_int_distribution<> month_dis(1, 12);
        std::uniform_int_distribution<> day_dis(1, 28);

        int year = year_dis(rng_);
        int month = month_dis(rng_);
        int day = day_dis(rng_);

        std::stringstream ss;
        ss << year << "-" << std::setfill('0') << std::setw(2) << month << "-"
           << std::setfill('0') << std::setw(2) << day;
        return ss.str();
    }

    // 生成随机城市
    std::string GenerateRandomCity() {
        std::vector<std::string> cities = {
            "Beijing", "Shanghai", "Guangzhou", "Shenzhen", "Hangzhou",
            "Nanjing", "Wuhan",    "Chengdu",   "Xian",     "Tianjin",
            "Qingdao", "Dalian",   "Suzhou",    "Ningbo",   "Xiamen"};
        std::uniform_int_distribution<> dis(0, cities.size() - 1);
        return cities[dis(rng_)];
    }

    // 生成随机状态
    std::string GenerateRandomStatus() {
        std::vector<std::string> statuses = {"pending", "completed",
                                             "cancelled", "shipped"};
        std::uniform_int_distribution<> dis(0, statuses.size() - 1);
        return statuses[dis(rng_)];
    }

    // 生成随机支付方式
    std::string GenerateRandomPaymentMethod() {
        std::vector<std::string> methods = {"credit_card", "alipay", "wechat",
                                            "cash"};
        std::uniform_int_distribution<> dis(0, methods.size() - 1);
        return methods[dis(rng_)];
    }

   protected:
    std::unique_ptr<DiskManager> disk_manager_;
    std::unique_ptr<DiskManager> log_disk_manager_;
    std::unique_ptr<Replacer> replacer_;
    std::unique_ptr<BufferPoolManager> buffer_pool_manager_;
    std::unique_ptr<LogManager> log_manager_;
    std::unique_ptr<LockManager> lock_manager_;
    std::unique_ptr<TransactionManager> transaction_manager_;
    std::unique_ptr<RecoveryManager> recovery_manager_;
    std::unique_ptr<Catalog> catalog_;
    std::unique_ptr<TableManager> table_manager_;
    std::unique_ptr<ExecutionEngine> execution_engine_;

    std::mt19937 rng_;
};

// 测试1：创建表结构
TEST_F(PerformanceTestFixture, CreateTablesTest) {
    std::cout << "=== 创建表结构测试 ===" << std::endl;

    // 创建用户表
    std::string create_users_sql = R"(
        CREATE TABLE users (
            user_id INT,
            username VARCHAR(50),
            email VARCHAR(100),
            age INT,
            city VARCHAR(30),
            registration_date VARCHAR(20),
            account_balance FLOAT,
            is_premium INT
        );
    )";

    auto time_users = ExecuteSQL(create_users_sql);
    std::cout << "创建用户表耗时: " << time_users << " ms" << std::endl;
    EXPECT_GT(time_users, 0);

    // 创建商品表
    std::string create_products_sql = R"(
        CREATE TABLE products (
            product_id INT,
            product_name VARCHAR(100),
            category VARCHAR(30),
            price FLOAT,
            stock_quantity INT,
            supplier_id INT,
            created_date VARCHAR(20)
        );
    )";

    auto time_products = ExecuteSQL(create_products_sql);
    std::cout << "创建商品表耗时: " << time_products << " ms" << std::endl;
    EXPECT_GT(time_products, 0);

    // 创建订单表（主要测试表）
    std::string create_orders_sql = R"(
        CREATE TABLE orders (
            order_id INT,
            user_id INT,
            product_id INT,
            quantity INT,
            total_amount FLOAT,
            order_status VARCHAR(20),
            order_date VARCHAR(20),
            payment_method VARCHAR(20),
            shipping_city VARCHAR(30),
            discount_rate FLOAT
        );
    )";

    auto time_orders = ExecuteSQL(create_orders_sql);
    std::cout << "创建订单表耗时: " << time_orders << " ms" << std::endl;
    EXPECT_GT(time_orders, 0);
}

// 测试2：插入基础数据
TEST_F(PerformanceTestFixture, InsertBaseDataTest) {
    std::cout << "\n=== 插入基础数据测试 ===" << std::endl;

    // 先创建表
    ExecuteSQL(
        R"(CREATE TABLE users (user_id INT, username VARCHAR(50), email VARCHAR(100), age INT, city VARCHAR(30), registration_date VARCHAR(20), account_balance FLOAT, is_premium INT);)");
    ExecuteSQL(
        R"(CREATE TABLE products (product_id INT, product_name VARCHAR(100), category VARCHAR(30), price FLOAT, stock_quantity INT, supplier_id INT, created_date VARCHAR(20));)");

    // 插入1万用户数据
    auto start = std::chrono::high_resolution_clock::now();

    const int USER_COUNT = 100;
    std::uniform_int_distribution<> age_dis(18, 65);
    std::uniform_real_distribution<> balance_dis(100.0, 10000.0);
    std::uniform_int_distribution<> premium_dis(0, 1);

    for (int i = 1; i <= USER_COUNT; ++i) {
        std::stringstream ss;
        ss << "INSERT INTO users VALUES (" << i << ", "
           << "'user" << std::setfill('0') << std::setw(6) << i << "', "
           << "'user" << i << "@email.com', " << age_dis(rng_) << ", "
           << "'" << GenerateRandomCity() << "', "
           << "'" << GenerateRandomDate() << "', " << std::fixed
           << std::setprecision(2) << balance_dis(rng_) << ", "
           << premium_dis(rng_) << ");";

        ExecuteSQL(ss.str());

        if (i % 1000 == 0) {
            std::cout << "已插入用户: " << i << std::endl;
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto user_insert_time =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
            .count();
    std::cout << "插入" << USER_COUNT << "用户耗时: " << user_insert_time
              << " ms" << std::endl;

    // 插入1000商品数据
    start = std::chrono::high_resolution_clock::now();

    const int PRODUCT_COUNT = 1000;
    std::vector<std::string> categories = {"Electronics", "Fashion", "Home",
                                           "Sports", "Books"};
    std::uniform_int_distribution<> cat_dis(0, categories.size() - 1);
    std::uniform_real_distribution<> price_dis(10.0, 5000.0);
    std::uniform_int_distribution<> stock_dis(0, 1000);
    std::uniform_int_distribution<> supplier_dis(1, 100);

    for (int i = 1; i <= PRODUCT_COUNT; ++i) {
        std::stringstream ss;
        ss << "INSERT INTO products VALUES (" << i << ", "
           << "'Product" << std::setfill('0') << std::setw(4) << i << "', "
           << "'" << categories[cat_dis(rng_)] << "', " << std::fixed
           << std::setprecision(2) << price_dis(rng_) << ", " << stock_dis(rng_)
           << ", " << supplier_dis(rng_) << ", "
           << "'" << GenerateRandomDate() << "');";

        ExecuteSQL(ss.str());

        if (i % 100 == 0) {
            std::cout << "已插入商品: " << i << std::endl;
        }
    }

    end = std::chrono::high_resolution_clock::now();
    auto product_insert_time =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
            .count();
    std::cout << "插入" << PRODUCT_COUNT << "商品耗时: " << product_insert_time
              << " ms" << std::endl;
}

// 测试3：插入100万订单数据
TEST_F(PerformanceTestFixture, InsertMillionOrdersTest) {
    std::cout << "\n=== 插入10万订单数据测试 ===" << std::endl;

    // 先创建表和基础数据
    ExecuteSQL(
        R"(CREATE TABLE users (user_id INT, username VARCHAR(50), email VARCHAR(100), age INT, city VARCHAR(30), registration_date VARCHAR(20), account_balance FLOAT, is_premium INT);)");
    ExecuteSQL(
        R"(CREATE TABLE products (product_id INT, product_name VARCHAR(100), category VARCHAR(30), price FLOAT, stock_quantity INT, supplier_id INT, created_date VARCHAR(20));)");
    ExecuteSQL(
        R"(CREATE TABLE orders (order_id INT, user_id INT, product_id INT, quantity INT, total_amount FLOAT, order_status VARCHAR(20), order_date VARCHAR(20), payment_method VARCHAR(20), shipping_city VARCHAR(30), discount_rate FLOAT);)");

    // 插入100万订单数据
    auto start = std::chrono::high_resolution_clock::now();

    const int ORDER_COUNT = 100000;
    std::uniform_int_distribution<> user_dis(1, 10000);
    std::uniform_int_distribution<> product_dis(1, 1000);
    std::uniform_int_distribution<> quantity_dis(1, 10);
    std::uniform_real_distribution<> amount_dis(50.0, 5000.0);
    std::uniform_real_distribution<> discount_dis(0.0, 0.3);

    for (int i = 1; i <= ORDER_COUNT; ++i) {
        std::stringstream ss;
        ss << "INSERT INTO orders VALUES (" << i << ", " << user_dis(rng_)
           << ", " << product_dis(rng_) << ", " << quantity_dis(rng_) << ", "
           << std::fixed << std::setprecision(2) << amount_dis(rng_) << ", "
           << "'" << GenerateRandomStatus() << "', "
           << "'" << GenerateRandomDate() << "', "
           << "'" << GenerateRandomPaymentMethod() << "', "
           << "'" << GenerateRandomCity() << "', " << std::fixed
           << std::setprecision(3) << discount_dis(rng_) << ");";

        ExecuteSQL(ss.str());

        if (i % 10000 == 0) {
            std::cout << "已插入订单: " << i << " (" << std::fixed
                      << std::setprecision(1) << (100.0 * i / ORDER_COUNT)
                      << "%)" << std::endl;
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto insert_time =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
            .count();
    std::cout << "插入" << ORDER_COUNT << "订单耗时: " << insert_time << " ms"
              << std::endl;
    std::cout << "平均每条记录插入时间: " << (double)insert_time / ORDER_COUNT
              << " ms" << std::endl;
}

// 测试4：查询性能测试（无索引）
TEST_F(PerformanceTestFixture, QueryPerformanceWithoutIndexTest) {
    std::cout << "\n=== 查询性能测试（无索引） ===" << std::endl;

    // 准备数据（简化版本，插入1万条数据用于快速测试）
    ExecuteSQL(
        R"(CREATE TABLE orders (order_id INT, user_id INT, product_id INT, quantity INT, total_amount FLOAT, order_status VARCHAR(20), order_date VARCHAR(20), payment_method VARCHAR(20), shipping_city VARCHAR(30), discount_rate FLOAT);)");

    const int TEST_ORDER_COUNT = 100000;
    std::uniform_int_distribution<> user_dis(1, 10000);
    std::uniform_int_distribution<> product_dis(1, 1000);
    std::uniform_int_distribution<> quantity_dis(1, 10);
    std::uniform_real_distribution<> amount_dis(50.0, 5000.0);
    std::uniform_real_distribution<> discount_dis(0.0, 0.3);

    std::cout << "准备测试数据..." << std::endl;
    for (int i = 1; i <= TEST_ORDER_COUNT; ++i) {
        std::stringstream ss;
        ss << "INSERT INTO orders VALUES (" << i << ", " << user_dis(rng_)
           << ", " << product_dis(rng_) << ", " << quantity_dis(rng_) << ", "
           << std::fixed << std::setprecision(2) << amount_dis(rng_) << ", "
           << "'" << GenerateRandomStatus() << "', '" << GenerateRandomDate()
           << "', "
           << "'" << GenerateRandomPaymentMethod() << "', '"
           << GenerateRandomCity() << "', " << std::fixed
           << std::setprecision(3) << discount_dis(rng_) << ");";
        ExecuteSQL(ss.str());

        if (i % 10000 == 0) {
            std::cout << "已插入: " << i << std::endl;
        }
    }

    // 执行各种查询测试
    std::vector<std::pair<std::string, std::string>> queries = {
        {"按用户ID查询", "SELECT * FROM orders WHERE user_id = 1234;"},
        {"按状态查询",
         "SELECT * FROM orders WHERE order_status = 'completed';"},
        {"按城市查询", "SELECT * FROM orders WHERE shipping_city = 'Beijing';"},
        {"按支付方式查询",
         "SELECT * FROM orders WHERE payment_method = 'alipay';"},
        {"按商品ID查询", "SELECT * FROM orders WHERE product_id = 567;"}};

    for (const auto& query_pair : queries) {
        auto query_time = ExecuteSQL(query_pair.second);
        std::cout << query_pair.first << " 耗时: " << query_time << " ms"
                  << std::endl;
    }
}

// 测试5：创建索引
TEST_F(PerformanceTestFixture, CreateIndexTest) {
    std::cout << "\n=== 创建索引测试 ===" << std::endl;

    // 先准备数据
    ExecuteSQL(
        R"(CREATE TABLE orders (order_id INT, user_id INT, product_id INT, quantity INT, total_amount FLOAT, order_status VARCHAR(20), order_date VARCHAR(20), payment_method VARCHAR(20), shipping_city VARCHAR(30), discount_rate FLOAT);)");

    // 插入一些测试数据
    const int TEST_COUNT = 100000;
    std::uniform_int_distribution<> user_dis(1, 10000);
    for (int i = 1; i <= TEST_COUNT; ++i) {
        std::stringstream ss;
        ss << "INSERT INTO orders VALUES (" << i << ", " << user_dis(rng_)
           << ", 1, 1, 100.0, 'completed', '2023-01-01', 'alipay', 'Beijing', "
              "0.0);";
        ExecuteSQL(ss.str());

        if (i % 10000 == 0) {
            std::cout << "已插入: " << i << " (" << std::fixed
                      << std::setprecision(1) << (100.0 * i / TEST_COUNT)
                      << "%)" << std::endl;
        }
    }

    // 创建索引
    std::vector<std::pair<std::string, std::string>> indexes = {
        {"用户ID索引", "CREATE INDEX idx_user_id ON orders (user_id);"},
        {"状态索引", "CREATE INDEX idx_status ON orders (order_status);"},
        {"城市索引", "CREATE INDEX idx_city ON orders (shipping_city);"},
        {"支付方式索引",
         "CREATE INDEX idx_payment ON orders (payment_method);"}};

    for (const auto& index_pair : indexes) {
        auto index_time = ExecuteSQL(index_pair.second);
        std::cout << "创建" << index_pair.first << " 耗时: " << index_time
                  << " ms" << std::endl;
    }
}

// 测试6：查询性能测试（有索引）
TEST_F(PerformanceTestFixture, QueryPerformanceWithIndexTest) {
    std::cout << "\n=== 查询性能测试（有索引） ===" << std::endl;

    // 准备数据和索引
    ExecuteSQL(
        R"(CREATE TABLE orders (order_id INT, user_id INT, product_id INT, quantity INT, total_amount FLOAT, order_status VARCHAR(20), order_date VARCHAR(20), payment_method VARCHAR(20), shipping_city VARCHAR(30), discount_rate FLOAT);)");

    // 插入测试数据
    const int TEST_COUNT = 100000;
    std::uniform_int_distribution<> user_dis(1, 10000);
    for (int i = 1; i <= TEST_COUNT; ++i) {
        std::stringstream ss;
        ss << "INSERT INTO orders VALUES (" << i << ", " << user_dis(rng_)
           << ", 1, 1, 100.0, 'completed', '2023-01-01', 'alipay', 'Beijing', "
              "0.0);";
        ExecuteSQL(ss.str());

        if (i % 10000 == 0) {
            std::cout << "已插入: " << i << " (" << std::fixed
                      << std::setprecision(1) << (100.0 * i / TEST_COUNT)
                      << "%)" << std::endl;
        }
    }

    // 创建索引
    ExecuteSQL("CREATE INDEX idx_user_id ON orders (user_id);");
    ExecuteSQL("CREATE INDEX idx_status ON orders (order_status);");
    ExecuteSQL("CREATE INDEX idx_city ON orders (shipping_city);");
    ExecuteSQL("CREATE INDEX idx_payment ON orders (payment_method);");

    // 执行查询测试
    std::vector<std::pair<std::string, std::string>> queries = {
        {"按用户ID查询（有索引）",
         "SELECT * FROM orders WHERE user_id = 1234;"},
        {"按状态查询（有索引）",
         "SELECT * FROM orders WHERE order_status = 'completed';"},
        {"按城市查询（有索引）",
         "SELECT * FROM orders WHERE shipping_city = 'Beijing';"},
        {"按支付方式查询（有索引）",
         "SELECT * FROM orders WHERE payment_method = 'alipay';"}};

    for (const auto& query_pair : queries) {
        auto query_time = ExecuteSQL(query_pair.second);
        std::cout << query_pair.first << " 耗时: " << query_time << " ms"
                  << std::endl;
    }
}

// 测试7：综合性能对比
TEST_F(PerformanceTestFixture, ComprehensivePerformanceTest) {
    std::cout << "\n=== 综合性能对比测试 ===" << std::endl;

    // 这个测试会比较有索引和无索引的查询性能差异
    // 由于完整的100万数据测试时间较长，这里使用较小的数据集进行演示

    ExecuteSQL(
        R"(CREATE TABLE test_orders (order_id INT, user_id INT, status VARCHAR(20));)");

    const int TEST_SIZE = 10000;
    std::uniform_int_distribution<> user_dis(1, 1000);
    std::vector<std::string> statuses = {"pending", "completed", "cancelled"};
    std::uniform_int_distribution<> status_dis(0, statuses.size() - 1);

    // 插入测试数据
    for (int i = 1; i <= TEST_SIZE; ++i) {
        std::stringstream ss;
        ss << "INSERT INTO test_orders VALUES (" << i << ", " << user_dis(rng_)
           << ", '" << statuses[status_dis(rng_)] << "');";
        ExecuteSQL(ss.str());

        if (i % 1000 == 0) {
            std::cout << "已插入: " << i << " (" << std::fixed
                      << std::setprecision(1) << (100.0 * i / TEST_SIZE)
                      << "%)" << std::endl;
        }
    }

    // 无索引查询
    auto no_index_time =
        ExecuteSQL("SELECT * FROM test_orders WHERE user_id = 500;");
    std::cout << "无索引查询耗时: " << no_index_time << " ms" << std::endl;

    // 创建索引
    auto index_create_time =
        ExecuteSQL("CREATE INDEX idx_test_user ON test_orders (user_id);");
    std::cout << "创建索引耗时: " << index_create_time << " ms" << std::endl;

    // 有索引查询
    auto with_index_time =
        ExecuteSQL("SELECT * FROM test_orders WHERE user_id = 500;");
    std::cout << "有索引查询耗时: " << with_index_time << " ms" << std::endl;

    if (no_index_time > 0 && with_index_time > 0) {
        double improvement =
            (double)(no_index_time - with_index_time) / no_index_time * 100;
        std::cout << "性能提升: " << std::fixed << std::setprecision(1)
                  << improvement << "%" << std::endl;
    }
}

// 主函数
int main(int argc, char** argv) {
    setenv("SIMPLEDB_DEBUG_LEVEL", "0", 1);  // DEBUG级别
    ::testing::InitGoogleTest(&argc, argv);

    std::cout << "SimpleRDBMS 性能测试开始..." << std::endl;
    std::cout << "==========================================\n" << std::endl;

    return RUN_ALL_TESTS();
}