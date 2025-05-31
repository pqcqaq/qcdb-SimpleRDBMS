#include <iostream>
#include <memory>
#include <string>
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

class SimpleRDBMSServer {
public:
    SimpleRDBMSServer(const std::string& db_file) {
        // Initialize components
        disk_manager_ = std::make_unique<DiskManager>(db_file);
        log_disk_manager_ = std::make_unique<DiskManager>(db_file + ".log");
        
        // Create replacer and buffer pool
        replacer_ = std::make_unique<LRUReplacer>(BUFFER_POOL_SIZE);
        buffer_pool_manager_ = std::make_unique<BufferPoolManager>(
            BUFFER_POOL_SIZE, 
            std::move(disk_manager_), 
            std::move(replacer_)
        );
        
        // Create log manager
        log_manager_ = std::make_unique<LogManager>(log_disk_manager_.get());
        
        // Create lock manager
        lock_manager_ = std::make_unique<LockManager>();
        
        // Create transaction manager
        transaction_manager_ = std::make_unique<TransactionManager>(
            lock_manager_.get(), 
            log_manager_.get()
        );
        
        // Create catalog
        catalog_ = std::make_unique<Catalog>(buffer_pool_manager_.get());
        
        // Create table manager
        table_manager_ = std::make_unique<TableManager>(
            buffer_pool_manager_.get(), 
            catalog_.get()
        );
        
        // Create recovery manager
        recovery_manager_ = std::make_unique<RecoveryManager>(
            buffer_pool_manager_.get(),
            catalog_.get(),
            log_manager_.get(),
            lock_manager_.get()
        );
        
        // Create execution engine
        execution_engine_ = std::make_unique<ExecutionEngine>(
            buffer_pool_manager_.get(),
            catalog_.get(),
            transaction_manager_.get()
        );
        
        // Perform recovery if needed
        recovery_manager_->Recover();
    }
    
    void Run() {
        std::cout << "SimpleRDBMS Server Started!" << std::endl;
        std::cout << "Enter SQL commands (type 'exit' to quit):" << std::endl;
        
        std::string line;
        while (true) {
            std::cout << "SimpleRDBMS> ";
            if (!std::getline(std::cin, line)) {
                break;
            }
            
            if (line == "exit" || line == "quit") {
                break;
            }
            
            if (line.empty()) {
                continue;
            }
            
            ExecuteSQL(line);
        }
        
        std::cout << "Shutting down..." << std::endl;
        Shutdown();
    }

private:
    void ExecuteSQL(const std::string& sql) {
        try {
            // Parse SQL
            Parser parser(sql);
            auto statement = parser.Parse();
            
            // Begin transaction
            auto* txn = transaction_manager_->Begin();
            
            // Execute statement
            std::vector<Tuple> result_set;
            bool success = execution_engine_->Execute(
                statement.get(), 
                &result_set, 
                txn
            );
            
            if (success) {
                transaction_manager_->Commit(txn);
                std::cout << "Query executed successfully." << std::endl;
                
                // Print results if any
                if (!result_set.empty()) {
                    std::cout << "Results: " << result_set.size() << " rows" << std::endl;
                    // TODO: Pretty print results
                }
            } else {
                transaction_manager_->Abort(txn);
                std::cout << "Query execution failed." << std::endl;
            }
            
        } catch (const std::exception& e) {
            std::cout << "Error: " << e.what() << std::endl;
        }
    }
    
    void Shutdown() {
        // Create checkpoint
        recovery_manager_->Checkpoint();
        
        // Flush all pages
        buffer_pool_manager_->FlushAllPages();
        
        // Flush logs
        log_manager_->Flush();
    }
    
    // Storage components
    std::unique_ptr<DiskManager> disk_manager_;
    std::unique_ptr<DiskManager> log_disk_manager_;
    std::unique_ptr<Replacer> replacer_;
    std::unique_ptr<BufferPoolManager> buffer_pool_manager_;
    
    // Transaction components
    std::unique_ptr<LogManager> log_manager_;
    std::unique_ptr<LockManager> lock_manager_;
    std::unique_ptr<TransactionManager> transaction_manager_;
    std::unique_ptr<RecoveryManager> recovery_manager_;
    
    // Catalog and execution
    std::unique_ptr<Catalog> catalog_;
    std::unique_ptr<TableManager> table_manager_;
    std::unique_ptr<ExecutionEngine> execution_engine_;
};

int main(int argc, char* argv[]) {
    std::string db_file = "simple_rdbms.db";
    
    if (argc > 1) {
        db_file = argv[1];
    }
    
    try {
        SimpleRDBMSServer server(db_file);
        server.Run();
    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}