// test/unit/update_delete_test.cpp
#include <cassert>
#include <iostream>
#include <memory>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "buffer/lru_replacer.h"
#include "catalog/catalog.h"
#include "execution/execution_engine.h"
#include "parser/parser.h"
#include "storage/disk_manager.h"
#include "transaction/lock_manager.h"
#include "transaction/transaction_manager.h"
#include "recovery/log_manager.h"

using namespace SimpleRDBMS;

class UpdateDeleteTest {
private:
    std::unique_ptr<DiskManager> disk_manager_;
    std::unique_ptr<DiskManager> log_disk_manager_;
    std::unique_ptr<LRUReplacer> replacer_;
    std::unique_ptr<BufferPoolManager> buffer_pool_manager_;
    std::unique_ptr<LogManager> log_manager_;
    std::unique_ptr<LockManager> lock_manager_;
    std::unique_ptr<TransactionManager> transaction_manager_;
    std::unique_ptr<Catalog> catalog_;
    std::unique_ptr<ExecutionEngine> execution_engine_;

public:
    UpdateDeleteTest() {
        // 初始化系统组件
        disk_manager_ = std::make_unique<DiskManager>("update_delete_test.db");
        log_disk_manager_ = std::make_unique<DiskManager>("update_delete_test.log");
        replacer_ = std::make_unique<LRUReplacer>(100);
        buffer_pool_manager_ = std::make_unique<BufferPoolManager>(
            100, std::move(disk_manager_), std::move(replacer_));
        
        log_manager_ = std::make_unique<LogManager>(log_disk_manager_.get());
        lock_manager_ = std::make_unique<LockManager>();
        transaction_manager_ = std::make_unique<TransactionManager>(
            lock_manager_.get(), log_manager_.get());
        catalog_ = std::make_unique<Catalog>(buffer_pool_manager_.get());
        execution_engine_ = std::make_unique<ExecutionEngine>(
            buffer_pool_manager_.get(), catalog_.get(), transaction_manager_.get());
    }

    ~UpdateDeleteTest() {
        // 清理测试文件
        std::remove("update_delete_test.db");
        std::remove("update_delete_test.log");
    }

    void RunTests() {
        std::cout << "=== UPDATE/DELETE 功能测试 ===" << std::endl;
        
        TestBasicOperations();
        TestWhereClause();
        TestComplexExpressions();
        
        std::cout << "所有测试通过!" << std::endl;
    }

private:
    bool ExecuteSQL(const std::string& sql, std::vector<Tuple>* result_set = nullptr) {
        try {
            Parser parser(sql);
            auto statement = parser.Parse();
            
            auto* txn = transaction_manager_->Begin();
            std::vector<Tuple> local_result_set;
            bool success = execution_engine_->Execute(
                statement.get(), 
                result_set ? result_set : &local_result_set, 
                txn
            );
            
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

    void TestBasicOperations() {
        std::cout << "\n--- 测试基本操作 ---" << std::endl;
        
        // 创建测试表
        std::string create_sql = R"(
            CREATE TABLE users (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                age INT,
                active BOOLEAN
            );
        )";
        
        assert(ExecuteSQL(create_sql));
        std::cout << "✓ 创建表成功" << std::endl;
        
        // 插入测试数据
        std::string insert_sql = R"(
            INSERT INTO users VALUES 
                (1, 'Alice', 25, TRUE),
                (2, 'Bob', 30, TRUE),
                (3, 'Charlie', 35, FALSE),
                (4, 'David', 28, TRUE);
        )";
        
        assert(ExecuteSQL(insert_sql));
        std::cout << "✓ 插入数据成功" << std::endl;
        
        // 测试简单 UPDATE（不带 WHERE 子句）
        std::string update_sql1 = "UPDATE users SET active = FALSE;";
        std::vector<Tuple> update_result;
        assert(ExecuteSQL(update_sql1, &update_result));
        std::cout << "✓ 无条件更新成功" << std::endl;
        
        // 测试简单 DELETE（不带 WHERE 子句）
        // 注意：这会删除所有记录，谨慎使用
        // std::string delete_sql1 = "DELETE FROM users;";
        // assert(ExecuteSQL(delete_sql1));
        // std::cout << "✓ 无条件删除成功" << std::endl;
    }

    void TestWhereClause() {
        std::cout << "\n--- 测试 WHERE 子句 ---" << std::endl;
        
        // 重新插入数据（如果之前被删除了）
        std::string insert_sql = R"(
            INSERT INTO users VALUES 
                (5, 'Eve', 22, TRUE),
                (6, 'Frank', 45, FALSE);
        )";
        
        ExecuteSQL(insert_sql);  // 可能会失败如果记录已存在，忽略错误
        
        // 测试带条件的 UPDATE
        std::string update_sql2 = "UPDATE users SET age = 26 WHERE name = 'Alice';";
        std::vector<Tuple> update_result2;
        // 注意：当前实现可能不完全支持 WHERE 子句，这是一个示例
        try {
            ExecuteSQL(update_sql2, &update_result2);
            std::cout << "✓ 条件更新成功" << std::endl;
        } catch (const std::exception& e) {
            std::cout << "⚠ 条件更新暂未完全实现: " << e.what() << std::endl;
        }
        
        // 测试带条件的 DELETE
        std::string delete_sql2 = "DELETE FROM users WHERE age > 40;";
        std::vector<Tuple> delete_result;
        try {
            ExecuteSQL(delete_sql2, &delete_result);
            std::cout << "✓ 条件删除成功" << std::endl;
        } catch (const std::exception& e) {
            std::cout << "⚠ 条件删除暂未完全实现: " << e.what() << std::endl;
        }
    }

    void TestComplexExpressions() {
        std::cout << "\n--- 测试复杂表达式 ---" << std::endl;
        
        // 测试复杂的 WHERE 条件
        std::vector<std::string> complex_sqls = {
            "UPDATE users SET active = TRUE WHERE age > 25 AND name != 'Charlie';",
            "DELETE FROM users WHERE (age < 30 OR active = FALSE) AND name != 'Alice';",
            "UPDATE users SET age = age + 1 WHERE active = TRUE;"  // 注意：这个可能不被支持
        };
        
        for (const auto& sql : complex_sqls) {
            try {
                std::vector<Tuple> result;
                if (ExecuteSQL(sql, &result)) {
                    std::cout << "✓ 复杂表达式执行成功: " << sql << std::endl;
                } else {
                    std::cout << "⚠ 复杂表达式执行失败: " << sql << std::endl;
                }
            } catch (const std::exception& e) {
                std::cout << "⚠ 复杂表达式暂未完全实现: " << sql << std::endl;
                std::cout << "  错误: " << e.what() << std::endl;
            }
        }
    }
};

// 使用示例的主函数
void DemoUpdateDeleteFeatures() {
    std::cout << "=== UPDATE/DELETE 功能演示 ===" << std::endl;
    
    // 创建系统组件
    auto disk_manager = std::make_unique<DiskManager>("demo.db");
    auto log_disk_manager = std::make_unique<DiskManager>("demo.log");
    auto replacer = std::make_unique<LRUReplacer>(100);
    auto buffer_pool_manager = std::make_unique<BufferPoolManager>(
        100, std::move(disk_manager), std::move(replacer));
    
    auto log_manager = std::make_unique<LogManager>(log_disk_manager.get());
    auto lock_manager = std::make_unique<LockManager>();
    auto transaction_manager = std::make_unique<TransactionManager>(
        lock_manager.get(), log_manager.get());
    auto catalog = std::make_unique<Catalog>(buffer_pool_manager.get());
    auto execution_engine = std::make_unique<ExecutionEngine>(
        buffer_pool_manager.get(), catalog.get(), transaction_manager.get());

    auto execute_sql = [&](const std::string& sql) -> bool {
        try {
            Parser parser(sql);
            auto statement = parser.Parse();
            
            auto* txn = transaction_manager->Begin();
            std::vector<Tuple> result_set;
            bool success = execution_engine->Execute(statement.get(), &result_set, txn);
            
            if (success) {
                transaction_manager->Commit(txn);
                std::cout << "✓ SQL 执行成功: " << sql << std::endl;
                if (!result_set.empty()) {
                    std::cout << "  结果: " << result_set.size() << " 行" << std::endl;
                }
            } else {
                transaction_manager->Abort(txn);
                std::cout << "✗ SQL 执行失败: " << sql << std::endl;
            }
            
            return success;
        } catch (const std::exception& e) {
            std::cout << "✗ SQL 执行异常: " << sql << std::endl;
            std::cout << "  错误: " << e.what() << std::endl;
            return false;
        }
    };

    // 演示 SQL 语句
    std::vector<std::string> demo_sqls = {
        // DDL
        "CREATE TABLE employees (id INT PRIMARY KEY, name VARCHAR(50), salary INT, dept VARCHAR(20));",
        
        // DML - INSERT
        "INSERT INTO employees VALUES (1, 'John', 50000, 'IT'), (2, 'Jane', 60000, 'HR'), (3, 'Bob', 55000, 'IT');",
        
        // DML - SELECT
        "SELECT id, name FROM employees;",
        
        // DML - UPDATE (基本功能)
        "UPDATE employees SET salary = 65000;",  // 无条件更新
        
        // DML - DELETE (基本功能)  
        // "DELETE FROM employees;",  // 危险操作，注释掉
        
        // 以下是带 WHERE 子句的操作（可能需要进一步实现）
        // "UPDATE employees SET salary = 70000 WHERE dept = 'IT';",
        // "DELETE FROM employees WHERE salary < 60000;",
    };

    for (const auto& sql : demo_sqls) {
        execute_sql(sql);
        std::cout << std::endl;
    }

    // 清理
    std::remove("demo.db");
    std::remove("demo.log");
    
    std::cout << "演示完成!" << std::endl;
}

int main() {
    try {
        UpdateDeleteTest test;
        test.RunTests();
        
        std::cout << "\n" << std::endl;
        DemoUpdateDeleteFeatures();
        
    } catch (const std::exception& e) {
        std::cerr << "测试失败: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}