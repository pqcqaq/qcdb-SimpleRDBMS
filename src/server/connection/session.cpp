#include "server/connection/session.h"
#include "transaction/transaction.h"
#include "transaction/transaction_manager.h"
#include "server/query/query_processor.h"
#include <iostream>

namespace SimpleRDBMS {

Session::Session(const std::string& session_id, const std::string& client_address)
    : state_(SessionState::INVALID),
      current_transaction_(nullptr),
      transaction_manager_(nullptr),
      query_processor_(nullptr) {
    
    session_info_.session_id = session_id;
    session_info_.client_address = client_address;
    session_info_.connect_time = std::chrono::system_clock::now();
    session_info_.last_activity = session_info_.connect_time;
    session_info_.state = state_;
    session_info_.queries_executed = 0;
    session_info_.bytes_sent = 0;
    session_info_.bytes_received = 0;
}

Session::~Session() {
    Close();
}

bool Session::Initialize() {
    std::lock_guard<std::mutex> lock(session_mutex_);
    
    if (state_ != SessionState::INVALID) {
        return false;
    }
    
    // Initialize session variables
    InitializeSessionVariables();
    
    state_ = SessionState::IDLE;
    session_info_.state = state_;
    
    return true;
}

void Session::Close() {
    std::lock_guard<std::mutex> lock(session_mutex_);
    
    if (state_ == SessionState::CLOSED) {
        return;
    }
    
    // Rollback any active transaction
    if (current_transaction_ && transaction_manager_) {
        transaction_manager_->Abort(current_transaction_);
        current_transaction_ = nullptr;
    }
    
    // Clear session variables
    session_variables_.clear();
    
    state_ = SessionState::CLOSED;
    session_info_.state = state_;
}

void Session::SetState(SessionState state) {
    std::lock_guard<std::mutex> lock(session_mutex_);
    state_ = state;
    session_info_.state = state;
}

bool Session::BeginTransaction() {
    std::cout << "[DEBUG] Session::BeginTransaction: Attempting to acquire session lock" << std::endl;
    std::lock_guard<std::mutex> lock(session_mutex_);
    std::cout << "[DEBUG] Session::BeginTransaction: Session lock acquired" << std::endl;
    
    if (!transaction_manager_) {
        std::cerr << "[ERROR] Session::BeginTransaction: Transaction manager not set" << std::endl;
        return false;
    }
    
    if (current_transaction_) {
        std::cerr << "[ERROR] Session::BeginTransaction: Transaction already in progress: " 
                  << current_transaction_->GetTxnId() << std::endl;
        return false;
    }
    
    std::cout << "[DEBUG] Session::BeginTransaction: Calling transaction_manager_->Begin()" << std::endl;
    current_transaction_ = transaction_manager_->Begin();
    std::cout << "[DEBUG] Session::BeginTransaction: transaction_manager_->Begin() returned: " 
              << (current_transaction_ ? "success" : "null") << std::endl;
    
    if (!current_transaction_) {
        std::cerr << "[ERROR] Session::BeginTransaction: Failed to begin transaction" << std::endl;
        return false;
    }
    
    std::cout << "[DEBUG] Session::BeginTransaction: New transaction created: " 
              << current_transaction_->GetTxnId() << std::endl;
    
    state_ = SessionState::IN_TRANSACTION;
    session_info_.state = state_;
    return true;
}

bool Session::CommitTransaction() {
    std::lock_guard<std::mutex> lock(session_mutex_);
    
    if (!transaction_manager_ || !current_transaction_) {
        std::cerr << "No active transaction to commit" << std::endl;
        return false;
    }
    
    bool success = transaction_manager_->Commit(current_transaction_);
    current_transaction_ = nullptr;
    
    state_ = SessionState::IDLE;
    session_info_.state = state_;
    
    return success;
}

bool Session::RollbackTransaction() {
    std::lock_guard<std::mutex> lock(session_mutex_);
    
    if (!transaction_manager_ || !current_transaction_) {
        std::cerr << "No active transaction to rollback" << std::endl;
        return false;
    }
    
    bool success = transaction_manager_->Abort(current_transaction_);
    current_transaction_ = nullptr;
    
    state_ = SessionState::IDLE;
    session_info_.state = state_;
    
    return success;
}

bool Session::ExecuteQuery(const std::string& query, std::vector<Tuple>* result_set) {
    // 执行就不加锁了，之前加过了
    // std::lock_guard<std::mutex> lock(session_mutex_);
    if (!query_processor_) {
        std::cerr << "Query processor not set" << std::endl;
        return false;
    }
    
    if (state_ == SessionState::CLOSED) {
        std::cerr << "Session is closed" << std::endl;
        return false;
    }
    
    // Validate query
    if (!ValidateQuery(query)) {
        std::cerr << "Invalid query" << std::endl;
        return false;
    }
    
    std::cout << "[DEBUG] Session::ExecuteQuery: Processing query: " << query << std::endl;
    
    // Update state
    SessionState old_state = state_;
    state_ = SessionState::EXECUTING;
    session_info_.state = state_;
    
    // Execute query
    UpdateLastActivity();
    
    try {
        QueryResult result = query_processor_->ProcessQuery(this, query);
        
        std::cout << "[DEBUG] Session::ExecuteQuery: Query processor returned: " << result.success << std::endl;
        
        // Update statistics
        IncrementQueriesExecuted();
        
        // Copy result set if requested
        if (result_set && result.success) {
            *result_set = result.result_set;
        }
        
        // Restore state
        state_ = (current_transaction_ ? SessionState::IN_TRANSACTION : SessionState::IDLE);
        session_info_.state = state_;
        
        if (!result.success) {
            std::cout << "[ERROR] Session::ExecuteQuery: Query failed: " << result.error_message << std::endl;
        }
        
        return result.success;
    } catch (const std::exception& e) {
        std::cout << "[ERROR] Session::ExecuteQuery: Exception: " << e.what() << std::endl;
        
        // Restore state on exception
        state_ = old_state;
        session_info_.state = state_;
        
        return false;
    }
}

void Session::SetVariable(const std::string& name, const std::string& value) {
    std::lock_guard<std::mutex> lock(session_mutex_);
    session_variables_[name] = value;
}

std::string Session::GetVariable(const std::string& name) const {
    std::lock_guard<std::mutex> lock(session_mutex_);
    
    auto it = session_variables_.find(name);
    if (it != session_variables_.end()) {
        return it->second;
    }
    
    return "";
}

bool Session::HasVariable(const std::string& name) const {
    std::lock_guard<std::mutex> lock(session_mutex_);
    return session_variables_.find(name) != session_variables_.end();
}

void Session::UpdateLastActivity() {
    session_info_.last_activity = std::chrono::system_clock::now();
}

bool Session::IsIdle(std::chrono::seconds timeout) const {
    auto now = std::chrono::system_clock::now();
    auto idle_time = std::chrono::duration_cast<std::chrono::seconds>(
        now - session_info_.last_activity);
    return idle_time >= timeout;
}

void Session::InitializeSessionVariables() {
    // Set default session variables
    session_variables_["autocommit"] = "true";
    session_variables_["transaction_isolation"] = "read_committed";
    session_variables_["query_timeout"] = "60";
    session_variables_["max_result_rows"] = "1000";
    session_variables_["client_encoding"] = "utf8";
}

bool Session::ValidateQuery(const std::string& query) const {
    // Basic validation
    if (query.empty()) {
        return false;
    }
    
    // Check query length
    const size_t MAX_QUERY_LENGTH = 1024 * 1024;  // 1MB
    if (query.length() > MAX_QUERY_LENGTH) {
        return false;
    }
    
    // More validation can be added here
    
    return true;
}

} // namespace SimpleRDBMS