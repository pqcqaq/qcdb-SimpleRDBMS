#include <gtest/gtest.h>
#include <iostream>
#include <fstream>
#include <filesystem>
#include <vector>
#include <memory>
#include <thread>
#include <chrono>

// 包含所有必要的头文件
#include "storage/disk_manager.h"
#include "buffer/buffer_pool_manager.h"
#include "buffer/lru_replacer.h"
#include "recovery/log_manager.h"
#include "recovery/recovery_manager.h"
#include "recovery/log_record.h"
#include "transaction/transaction_manager.h"
#include "transaction/lock_manager.h"
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "record/tuple.h"
#include "record/table_heap.h"
#include "common/config.h"
#include "common/debug.h"

using namespace SimpleRDBMS;

class LogRecoveryTest : public ::testing::Test {
protected:
    void SetUp() override {
        // 清理测试文件
        CleanupTestFiles();
        
        // 创建基础组件
        db_file_ = "test_log_recovery.db";
        log_file_ = "test_log_recovery.db.log";
        
        disk_manager_ = std::make_unique<DiskManager>(db_file_);
        log_disk_manager_ = std::make_unique<DiskManager>(log_file_);
        replacer_ = std::make_unique<LRUReplacer>(BUFFER_POOL_SIZE);
        buffer_pool_manager_ = std::make_unique<BufferPoolManager>(
            BUFFER_POOL_SIZE, std::move(disk_manager_), std::move(replacer_));
        
        log_manager_ = std::make_unique<LogManager>(log_disk_manager_.get());
        lock_manager_ = std::make_unique<LockManager>();
        transaction_manager_ = std::make_unique<TransactionManager>(
            lock_manager_.get(), log_manager_.get());
        
        catalog_ = std::make_unique<Catalog>(buffer_pool_manager_.get());
        recovery_manager_ = std::make_unique<RecoveryManager>(
            buffer_pool_manager_.get(), catalog_.get(), 
            log_manager_.get(), lock_manager_.get());
        
        // 创建测试表
        CreateTestTable();
    }
    
    void TearDown() override {
        // 清理资源
        recovery_manager_.reset();
        catalog_.reset();
        transaction_manager_.reset();
        lock_manager_.reset();
        log_manager_.reset();
        buffer_pool_manager_.reset();
        replacer_.reset();
        log_disk_manager_.reset();
        
        CleanupTestFiles();
    }
    
    void CleanupTestFiles() {
        std::vector<std::string> files = {
            "test_log_recovery.db",
            "test_log_recovery.db.log"
        };
        
        for (const auto& file : files) {
            if (std::filesystem::exists(file)) {
                std::filesystem::remove(file);
            }
        }
    }
    
    void CreateTestTable() {
        std::vector<Column> columns = {
            {"id", TypeId::INTEGER, 0, false, true},
            {"name", TypeId::VARCHAR, 50, false, false},
            {"age", TypeId::INTEGER, 0, true, false}
        };
        Schema schema(columns);
        
        bool success = catalog_->CreateTable("test_table", schema);
        ASSERT_TRUE(success) << "Failed to create test table";
    }
    
    TableInfo* GetTestTable() {
        return catalog_->GetTable("test_table");
    }
    
    Tuple CreateTestTuple(int id, const std::string& name, int age) {
        TableInfo* table_info = GetTestTable();
        EXPECT_NE(table_info, nullptr);
        
        std::vector<Value> values = {
            Value(id),
            Value(name),
            Value(age)
        };
        
        return Tuple(values, table_info->schema.get());
    }
    
    void PrintLogFileSize() {
        if (std::filesystem::exists(log_file_)) {
            auto size = std::filesystem::file_size(log_file_);
            std::cout << "Log file size: " << size << " bytes" << std::endl;
        } else {
            std::cout << "Log file does not exist" << std::endl;
        }
    }
    
    int CountTuplesInTable() {
        TableInfo* table_info = GetTestTable();
        if (!table_info) return 0;
        
        int count = 0;
        auto iter = table_info->table_heap->Begin();
        while (!iter.IsEnd()) {
            count++;
            ++iter;
        }
        return count;
    }
    
    void SimulateCrash() {
        // 模拟突然崩溃 - 不进行正常的清理
        log_manager_.reset();
        buffer_pool_manager_.reset();
        
        // 重新初始化，模拟重启
        disk_manager_ = std::make_unique<DiskManager>(db_file_);
        log_disk_manager_ = std::make_unique<DiskManager>(log_file_);
        replacer_ = std::make_unique<LRUReplacer>(BUFFER_POOL_SIZE);
        buffer_pool_manager_ = std::make_unique<BufferPoolManager>(
            BUFFER_POOL_SIZE, std::move(disk_manager_), std::move(replacer_));
        
        log_manager_ = std::make_unique<LogManager>(log_disk_manager_.get());
        catalog_ = std::make_unique<Catalog>(buffer_pool_manager_.get());
        recovery_manager_ = std::make_unique<RecoveryManager>(
            buffer_pool_manager_.get(), catalog_.get(), 
            log_manager_.get(), lock_manager_.get());
    }

protected:
    std::string db_file_;
    std::string log_file_;
    
    std::unique_ptr<DiskManager> disk_manager_;
    std::unique_ptr<DiskManager> log_disk_manager_;
    std::unique_ptr<LRUReplacer> replacer_;
    std::unique_ptr<BufferPoolManager> buffer_pool_manager_;
    std::unique_ptr<LogManager> log_manager_;
    std::unique_ptr<LockManager> lock_manager_;
    std::unique_ptr<TransactionManager> transaction_manager_;
    std::unique_ptr<Catalog> catalog_;
    std::unique_ptr<RecoveryManager> recovery_manager_;
};

// 基础日志功能测试
TEST_F(LogRecoveryTest, BasicLogFunctionality) {
    std::cout << "\n=== Testing Basic Log Functionality ===" << std::endl;
    
    // 测试事务日志记录
    auto* txn = transaction_manager_->Begin();
    ASSERT_NE(txn, nullptr);
    
    // 记录一些日志
    BeginLogRecord begin_record(txn->GetTxnId());
    lsn_t begin_lsn = log_manager_->AppendLogRecord(&begin_record);
    EXPECT_NE(begin_lsn, INVALID_LSN);
    std::cout << "Begin log record LSN: " << begin_lsn << std::endl;
    
    CommitLogRecord commit_record(txn->GetTxnId(), begin_lsn);
    lsn_t commit_lsn = log_manager_->AppendLogRecord(&commit_record);
    EXPECT_NE(commit_lsn, INVALID_LSN);
    std::cout << "Commit log record LSN: " << commit_lsn << std::endl;
    
    // 刷新日志
    log_manager_->Flush();
    PrintLogFileSize();
    
    // 读取日志记录
    auto log_records = log_manager_->ReadLogRecords();
    std::cout << "Read " << log_records.size() << " log records" << std::endl;
    
    EXPECT_GE(log_records.size(), 2);
    
    // 验证日志记录类型
    bool found_begin = false, found_commit = false;
    for (const auto& record : log_records) {
        if (record->GetType() == LogRecordType::BEGIN) {
            found_begin = true;
            EXPECT_EQ(record->GetTxnId(), txn->GetTxnId());
        } else if (record->GetType() == LogRecordType::COMMIT) {
            found_commit = true;
            EXPECT_EQ(record->GetTxnId(), txn->GetTxnId());
        }
    }
    
    EXPECT_TRUE(found_begin) << "BEGIN log record not found";
    EXPECT_TRUE(found_commit) << "COMMIT log record not found";
    
    transaction_manager_->Commit(txn);
}

// 事务提交测试
TEST_F(LogRecoveryTest, TransactionCommitRecovery) {
    std::cout << "\n=== Testing Transaction Commit Recovery ===" << std::endl;
    
    TableInfo* table_info = GetTestTable();
    ASSERT_NE(table_info, nullptr);
    
    // 插入一些数据并提交
    auto* txn1 = transaction_manager_->Begin();
    
    Tuple tuple1 = CreateTestTuple(1, "Alice", 25);
    RID rid1;
    bool success = table_info->table_heap->InsertTuple(tuple1, &rid1, txn1->GetTxnId());
    ASSERT_TRUE(success);
    
    Tuple tuple2 = CreateTestTuple(2, "Bob", 30);
    RID rid2;
    success = table_info->table_heap->InsertTuple(tuple2, &rid2, txn1->GetTxnId());
    ASSERT_TRUE(success);
    
    // 提交事务
    transaction_manager_->Commit(txn1);
    log_manager_->Flush();
    
    std::cout << "Before crash - tuples in table: " << CountTuplesInTable() << std::endl;
    PrintLogFileSize();
    
    // 模拟崩溃和恢复
    SimulateCrash();
    
    std::cout << "After crash, before recovery - tuples in table: " 
              << CountTuplesInTable() << std::endl;
    
    // 执行恢复
    recovery_manager_->Recover();
    
    std::cout << "After recovery - tuples in table: " << CountTuplesInTable() << std::endl;
    
    // 验证数据恢复正确
    EXPECT_EQ(CountTuplesInTable(), 2);
}

// 事务回滚测试
TEST_F(LogRecoveryTest, TransactionAbortRecovery) {
    std::cout << "\n=== Testing Transaction Abort Recovery ===" << std::endl;
    
    TableInfo* table_info = GetTestTable();
    ASSERT_NE(table_info, nullptr);
    
    // 先插入一些已提交的数据
    auto* txn1 = transaction_manager_->Begin();
    Tuple tuple1 = CreateTestTuple(1, "Alice", 25);
    RID rid1;
    table_info->table_heap->InsertTuple(tuple1, &rid1, txn1->GetTxnId());
    transaction_manager_->Commit(txn1);
    
    std::cout << "After first transaction - tuples: " << CountTuplesInTable() << std::endl;
    
    // 开始一个新事务并插入数据，但不提交
    auto* txn2 = transaction_manager_->Begin();
    Tuple tuple2 = CreateTestTuple(2, "Bob", 30);
    RID rid2;
    table_info->table_heap->InsertTuple(tuple2, &rid2, txn2->GetTxnId());
    
    Tuple tuple3 = CreateTestTuple(3, "Charlie", 35);
    RID rid3;
    table_info->table_heap->InsertTuple(tuple3, &rid3, txn2->GetTxnId());
    
    std::cout << "After second transaction (before abort) - tuples: " 
              << CountTuplesInTable() << std::endl;
    
    // 刷新日志但不提交事务
    log_manager_->Flush();
    PrintLogFileSize();
    
    // 模拟崩溃（未提交的事务应该被回滚）
    SimulateCrash();
    
    std::cout << "After crash, before recovery - tuples: " << CountTuplesInTable() << std::endl;
    
    // 执行恢复
    recovery_manager_->Recover();
    
    std::cout << "After recovery - tuples: " << CountTuplesInTable() << std::endl;
    
    // 验证只有已提交的数据存在
    EXPECT_EQ(CountTuplesInTable(), 1);
}

// 混合事务恢复测试
TEST_F(LogRecoveryTest, MixedTransactionRecovery) {
    std::cout << "\n=== Testing Mixed Transaction Recovery ===" << std::endl;
    
    TableInfo* table_info = GetTestTable();
    ASSERT_NE(table_info, nullptr);
    
    // 事务1：提交
    auto* txn1 = transaction_manager_->Begin();
    Tuple tuple1 = CreateTestTuple(1, "Alice", 25);
    RID rid1;
    table_info->table_heap->InsertTuple(tuple1, &rid1, txn1->GetTxnId());
    transaction_manager_->Commit(txn1);
    
    // 事务2：未提交（应该回滚）
    auto* txn2 = transaction_manager_->Begin();
    Tuple tuple2 = CreateTestTuple(2, "Bob", 30);
    RID rid2;
    table_info->table_heap->InsertTuple(tuple2, &rid2, txn2->GetTxnId());
    
    // 事务3：提交
    auto* txn3 = transaction_manager_->Begin();
    Tuple tuple3 = CreateTestTuple(3, "Charlie", 35);
    RID rid3;
    table_info->table_heap->InsertTuple(tuple3, &rid3, txn3->GetTxnId());
    transaction_manager_->Commit(txn3);
    
    // 事务4：未提交（应该回滚）
    auto* txn4 = transaction_manager_->Begin();
    Tuple tuple4 = CreateTestTuple(4, "David", 40);
    RID rid4;
    table_info->table_heap->InsertTuple(tuple4, &rid4, txn4->GetTxnId());
    
    std::cout << "Before crash - tuples: " << CountTuplesInTable() << std::endl;
    log_manager_->Flush();
    PrintLogFileSize();
    
    // 模拟崩溃
    SimulateCrash();
    
    std::cout << "After crash, before recovery - tuples: " << CountTuplesInTable() << std::endl;
    
    // 执行恢复
    recovery_manager_->Recover();
    
    std::cout << "After recovery - tuples: " << CountTuplesInTable() << std::endl;
    
    // 验证只有已提交的事务数据存在（txn1和txn3）
    EXPECT_EQ(CountTuplesInTable(), 2);
}

// 更新操作恢复测试
TEST_F(LogRecoveryTest, UpdateOperationRecovery) {
    std::cout << "\n=== Testing Update Operation Recovery ===" << std::endl;
    
    TableInfo* table_info = GetTestTable();
    ASSERT_NE(table_info, nullptr);
    
    // 先插入一条记录
    auto* txn1 = transaction_manager_->Begin();
    Tuple original_tuple = CreateTestTuple(1, "Alice", 25);
    RID rid;
    table_info->table_heap->InsertTuple(original_tuple, &rid, txn1->GetTxnId());
    transaction_manager_->Commit(txn1);
    
    // 验证原始数据
    Tuple retrieved_tuple;
    bool success = table_info->table_heap->GetTuple(rid, &retrieved_tuple, INVALID_TXN_ID);
    ASSERT_TRUE(success);
    EXPECT_EQ(std::get<std::string>(retrieved_tuple.GetValue(1)), "Alice");
    EXPECT_EQ(std::get<int32_t>(retrieved_tuple.GetValue(2)), 25);
    
    // 更新记录
    auto* txn2 = transaction_manager_->Begin();
    Tuple updated_tuple = CreateTestTuple(1, "Alice Smith", 26);
    success = table_info->table_heap->UpdateTuple(updated_tuple, rid, txn2->GetTxnId());
    ASSERT_TRUE(success);
    
    // 验证更新后的数据
    success = table_info->table_heap->GetTuple(rid, &retrieved_tuple, txn2->GetTxnId());
    ASSERT_TRUE(success);
    EXPECT_EQ(std::get<std::string>(retrieved_tuple.GetValue(1)), "Alice Smith");
    EXPECT_EQ(std::get<int32_t>(retrieved_tuple.GetValue(2)), 26);
    
    transaction_manager_->Commit(txn2);
    log_manager_->Flush();
    
    std::cout << "Before crash - updated tuple exists" << std::endl;
    PrintLogFileSize();
    
    // 模拟崩溃
    SimulateCrash();
    
    // 执行恢复
    recovery_manager_->Recover();
    
    // 验证更新的数据在恢复后仍然存在
    table_info = GetTestTable(); // 重新获取table_info
    success = table_info->table_heap->GetTuple(rid, &retrieved_tuple, INVALID_TXN_ID);
    ASSERT_TRUE(success);
    
    std::cout << "After recovery - verifying updated data" << std::endl;
    EXPECT_EQ(std::get<std::string>(retrieved_tuple.GetValue(1)), "Alice Smith");
    EXPECT_EQ(std::get<int32_t>(retrieved_tuple.GetValue(2)), 26);
}

// 删除操作恢复测试
TEST_F(LogRecoveryTest, DeleteOperationRecovery) {
    std::cout << "\n=== Testing Delete Operation Recovery ===" << std::endl;
    
    TableInfo* table_info = GetTestTable();
    ASSERT_NE(table_info, nullptr);
    
    // 插入多条记录
    auto* txn1 = transaction_manager_->Begin();
    
    Tuple tuple1 = CreateTestTuple(1, "Alice", 25);
    RID rid1;
    table_info->table_heap->InsertTuple(tuple1, &rid1, txn1->GetTxnId());
    
    Tuple tuple2 = CreateTestTuple(2, "Bob", 30);
    RID rid2;
    table_info->table_heap->InsertTuple(tuple2, &rid2, txn1->GetTxnId());
    
    Tuple tuple3 = CreateTestTuple(3, "Charlie", 35);
    RID rid3;
    table_info->table_heap->InsertTuple(tuple3, &rid3, txn1->GetTxnId());
    
    transaction_manager_->Commit(txn1);
    
    std::cout << "After insert - tuples: " << CountTuplesInTable() << std::endl;
    
    // 删除一条记录
    auto* txn2 = transaction_manager_->Begin();
    bool success = table_info->table_heap->DeleteTuple(rid2, txn2->GetTxnId());
    ASSERT_TRUE(success);
    
    transaction_manager_->Commit(txn2);
    log_manager_->Flush();
    
    std::cout << "After delete - tuples: " << CountTuplesInTable() << std::endl;
    PrintLogFileSize();
    
    // 模拟崩溃
    SimulateCrash();
    
    std::cout << "After crash, before recovery - tuples: " << CountTuplesInTable() << std::endl;
    
    // 执行恢复
    recovery_manager_->Recover();
    
    std::cout << "After recovery - tuples: " << CountTuplesInTable() << std::endl;
    
    // 验证删除操作被正确恢复（应该有2条记录）
    EXPECT_EQ(CountTuplesInTable(), 2);
    
    // 验证被删除的记录确实不存在
    table_info = GetTestTable();
    Tuple retrieved_tuple;
    success = table_info->table_heap->GetTuple(rid2, &retrieved_tuple, INVALID_TXN_ID);
    EXPECT_FALSE(success) << "Deleted tuple should not be retrievable";
}

// 检查点测试
TEST_F(LogRecoveryTest, CheckpointTest) {
    std::cout << "\n=== Testing Checkpoint Functionality ===" << std::endl;
    
    TableInfo* table_info = GetTestTable();
    ASSERT_NE(table_info, nullptr);
    
    // 插入一些数据
    for (int i = 1; i <= 5; i++) {
        auto* txn = transaction_manager_->Begin();
        Tuple tuple = CreateTestTuple(i, "User" + std::to_string(i), 20 + i);
        RID rid;
        table_info->table_heap->InsertTuple(tuple, &rid, txn->GetTxnId());
        transaction_manager_->Commit(txn);
    }
    
    std::cout << "Before checkpoint - tuples: " << CountTuplesInTable() << std::endl;
    
    // 执行检查点
    recovery_manager_->Checkpoint();
    PrintLogFileSize();
    
    // 继续插入更多数据
    for (int i = 6; i <= 8; i++) {
        auto* txn = transaction_manager_->Begin();
        Tuple tuple = CreateTestTuple(i, "User" + std::to_string(i), 20 + i);
        RID rid;
        table_info->table_heap->InsertTuple(tuple, &rid, txn->GetTxnId());
        transaction_manager_->Commit(txn);
    }
    
    std::cout << "After more inserts - tuples: " << CountTuplesInTable() << std::endl;
    log_manager_->Flush();
    
    // 模拟崩溃
    SimulateCrash();
    
    std::cout << "After crash, before recovery - tuples: " << CountTuplesInTable() << std::endl;
    
    // 执行恢复
    recovery_manager_->Recover();
    
    std::cout << "After recovery - tuples: " << CountTuplesInTable() << std::endl;
    
    // 验证所有数据都正确恢复
    EXPECT_EQ(CountTuplesInTable(), 8);
}

// 并发事务模拟测试
TEST_F(LogRecoveryTest, ConcurrentTransactionRecovery) {
    std::cout << "\n=== Testing Concurrent Transaction Recovery ===" << std::endl;
    
    TableInfo* table_info = GetTestTable();
    ASSERT_NE(table_info, nullptr);
    
    // 模拟多个并发事务
    std::vector<Transaction*> transactions;
    std::vector<RID> rids;
    
    // 创建多个事务
    for (int i = 0; i < 5; i++) {
        auto* txn = transaction_manager_->Begin();
        transactions.push_back(txn);
        
        Tuple tuple = CreateTestTuple(i + 1, "User" + std::to_string(i + 1), 20 + i);
        RID rid;
        table_info->table_heap->InsertTuple(tuple, &rid, txn->GetTxnId());
        rids.push_back(rid);
    }
    
    // 只提交一部分事务（偶数索引的事务）
    for (size_t i = 0; i < transactions.size(); i++) {
        if (i % 2 == 0) {
            transaction_manager_->Commit(transactions[i]);
            std::cout << "Committed transaction " << i << std::endl;
        } else {
            std::cout << "Left transaction " << i << " uncommitted" << std::endl;
        }
    }
    
    std::cout << "Before crash - tuples: " << CountTuplesInTable() << std::endl;
    log_manager_->Flush();
    PrintLogFileSize();
    
    // 模拟崩溃
    SimulateCrash();
    
    std::cout << "After crash, before recovery - tuples: " << CountTuplesInTable() << std::endl;
    
    // 执行恢复
    recovery_manager_->Recover();
    
    std::cout << "After recovery - tuples: " << CountTuplesInTable() << std::endl;
    
    // 验证只有已提交的事务数据存在（应该是3个，索引0,2,4）
    EXPECT_EQ(CountTuplesInTable(), 3);
}

// 运行所有测试的主函数
int main(int argc, char** argv) {
    // 设置调试级别
    setenv("SIMPLEDB_DEBUG_LEVEL", "3", 1); // INFO level
    
    ::testing::InitGoogleTest(&argc, argv);
    
    std::cout << "Starting Log Recovery Tests..." << std::endl;
    
    int result = RUN_ALL_TESTS();
    
    std::cout << "Log Recovery Tests completed." << std::endl;
    
    return result;
}