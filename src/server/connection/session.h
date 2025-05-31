#pragma once

#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "common/config.h"
#include "common/types.h"
#include "record/tuple.h"
#include "transaction/transaction.h"

namespace SimpleRDBMS {

// Forward declarations
class Transaction;
class TransactionManager;
class ExecutionEngine;
class QueryProcessor;

enum class SessionState {
    INVALID = 0,
    IDLE,            // 空闲状态
    EXECUTING,       // 执行查询中
    IN_TRANSACTION,  // 事务中
    WAITING,         // 等待状态
    CLOSED           // 已关闭
};

struct SessionInfo {
    std::string session_id;
    std::string client_address;
    std::chrono::system_clock::time_point connect_time;
    std::chrono::system_clock::time_point last_activity;
    SessionState state;
    size_t queries_executed;
    size_t bytes_sent;
    size_t bytes_received;
};

class Session {
   public:
    explicit Session(const std::string& session_id,
                     const std::string& client_address);
    ~Session();

    // Session lifecycle
    bool Initialize();
    void Close();
    bool IsValid() const {
        return state_ != SessionState::INVALID &&
               state_ != SessionState::CLOSED;
    }

    // State management
    SessionState GetState() const { return state_; }
    void SetState(SessionState state);

    // Session info
    const SessionInfo& GetSessionInfo() const { return session_info_; }
    std::string GetSessionId() const { return session_info_.session_id; }

    // Transaction management
    bool BeginTransaction();
    bool CommitTransaction();
    bool RollbackTransaction();
    bool IsInTransaction() const { return current_transaction_ != nullptr; }
    Transaction* GetCurrentTransaction() const { return current_transaction_; }

    // Query execution
    bool ExecuteQuery(const std::string& query, std::vector<Tuple>* result_set);

    // Session variables
    void SetVariable(const std::string& name, const std::string& value);
    std::string GetVariable(const std::string& name) const;
    bool HasVariable(const std::string& name) const;

    // Activity tracking
    void UpdateLastActivity();
    bool IsIdle(std::chrono::seconds timeout) const;

    // Statistics
    void IncrementQueriesExecuted() { session_info_.queries_executed++; }
    void AddBytesSent(size_t bytes) { session_info_.bytes_sent += bytes; }
    void AddBytesReceived(size_t bytes) {
        session_info_.bytes_received += bytes;
    }

    // Dependencies injection
    void SetTransactionManager(TransactionManager* txn_manager) {
        transaction_manager_ = txn_manager;
    }
    void SetQueryProcessor(QueryProcessor* query_processor) {
        query_processor_ = query_processor;
    }

    QueryProcessor* GetQueryProcessor() const { return query_processor_; }
    TransactionManager* GetTransactionManager() const {
        return transaction_manager_;
    }

   private:
    SessionInfo session_info_;
    SessionState state_;

    // Transaction state
    Transaction* current_transaction_;
    TransactionManager* transaction_manager_;

    // Query processing
    QueryProcessor* query_processor_;

    // Session variables
    std::unordered_map<std::string, std::string> session_variables_;

    // Thread safety
    mutable std::mutex session_mutex_;

    // Helper methods
    void InitializeSessionVariables();
    bool ValidateQuery(const std::string& query) const;
};

}  // namespace SimpleRDBMS