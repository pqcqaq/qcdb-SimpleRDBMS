#pragma once

#include "common/types.h"
#include "record/tuple.h"
#include <string>
#include <vector>
#include <memory>
#include <chrono>
#include <atomic>
#include <unordered_map>

namespace SimpleRDBMS {

// Forward declarations
class Session;
class Transaction;
class Statement;

enum class QueryState {
    CREATED = 0,
    PARSING,
    PLANNING,
    EXECUTING,
    COMPLETED,
    FAILED,
    CANCELLED
};

struct QueryMetrics {
    std::chrono::system_clock::time_point start_time;
    std::chrono::system_clock::time_point end_time;
    std::chrono::milliseconds parse_time{0};
    std::chrono::milliseconds plan_time{0};
    std::chrono::milliseconds execution_time{0};
    std::chrono::milliseconds total_time{0};
    
    size_t rows_examined;
    size_t rows_returned;
    size_t bytes_sent;
    size_t temp_tables_created;
    size_t index_seeks;
    size_t index_scans;
    size_t table_scans;
    
    QueryMetrics() : rows_examined(0), rows_returned(0), bytes_sent(0),
                    temp_tables_created(0), index_seeks(0), index_scans(0), table_scans(0) {}
};

class QueryContext {
public:
    explicit QueryContext(Session* session, const std::string& query_string);
    ~QueryContext();
    
    // Basic information
    const std::string& GetQueryString() const { return query_string_; }
    const std::string& GetQueryId() const { return query_id_; }
    Session* GetSession() const { return session_; }
    
    // State management
    QueryState GetState() const { return state_; }
    void SetState(QueryState state);
    bool IsCompleted() const { return state_ == QueryState::COMPLETED; }
    bool IsFailed() const { return state_ == QueryState::FAILED; }
    bool IsCancelled() const { return state_ == QueryState::CANCELLED; }
    
    // Transaction context
    Transaction* GetTransaction() const { return transaction_; }
    void SetTransaction(Transaction* transaction) { transaction_ = transaction; }
    
    // Statement and execution
    Statement* GetStatement() const { return statement_.get(); }
    void SetStatement(std::unique_ptr<Statement> statement) { statement_ = std::move(statement); }
    
    // Results
    const std::vector<Tuple>& GetResultSet() const { return result_set_; }
    void SetResultSet(const std::vector<Tuple>& result_set) { result_set_ = result_set; }
    void AddResultTuple(const Tuple& tuple) { result_set_.push_back(tuple); }
    void ClearResultSet() { result_set_.clear(); }
    
    size_t GetAffectedRows() const { return affected_rows_; }
    void SetAffectedRows(size_t rows) { affected_rows_ = rows; }
    
    // Error handling
    bool HasError() const { return !error_message_.empty(); }
    const std::string& GetErrorMessage() const { return error_message_; }
    void SetError(const std::string& error_message);
    void ClearError() { error_message_.clear(); }
    
    // Metrics and timing
    const QueryMetrics& GetMetrics() const { return metrics_; }
    QueryMetrics& GetMetrics() { return metrics_; }
    void StartTimer() { metrics_.start_time = std::chrono::system_clock::now(); }
    void EndTimer() { 
        metrics_.end_time = std::chrono::system_clock::now();
        metrics_.total_time = std::chrono::duration_cast<std::chrono::milliseconds>(
            metrics_.end_time - metrics_.start_time);
    }
    
    // Query parameters and variables
    void SetParameter(const std::string& name, const Value& value);
    Value GetParameter(const std::string& name) const;
    bool HasParameter(const std::string& name) const;
    const std::unordered_map<std::string, Value>& GetParameters() const { return parameters_; }
    
    // Query hints and options
    void SetHint(const std::string& hint, const std::string& value);
    std::string GetHint(const std::string& hint) const;
    bool HasHint(const std::string& hint) const;
    
    // Cancellation support
    void Cancel();
    bool IsCancellationRequested() const { return cancellation_requested_; }
    
    // Resource tracking
    void IncrementRowsExamined(size_t count = 1) { metrics_.rows_examined += count; }
    void IncrementRowsReturned(size_t count = 1) { metrics_.rows_returned += count; }
    void IncrementBytesSent(size_t bytes) { metrics_.bytes_sent += bytes; }
    void IncrementTempTables(size_t count = 1) { metrics_.temp_tables_created += count; }
    void IncrementIndexSeeks(size_t count = 1) { metrics_.index_seeks += count; }
    void IncrementIndexScans(size_t count = 1) { metrics_.index_scans += count; }
    void IncrementTableScans(size_t count = 1) { metrics_.table_scans += count; }
    
    // Debug and logging
    std::string ToString() const;
    void LogMetrics() const;

private:
    // Basic query information
    std::string query_id_;
    std::string query_string_;
    Session* session_;
    QueryState state_;
    
    // Execution context
    Transaction* transaction_;
    std::unique_ptr<Statement> statement_;
    
    // Results
    std::vector<Tuple> result_set_;
    size_t affected_rows_;
    
    // Error state
    std::string error_message_;
    
    // Metrics and timing
    QueryMetrics metrics_;
    
    // Query parameters
    std::unordered_map<std::string, Value> parameters_;
    
    // Query hints and options
    std::unordered_map<std::string, std::string> hints_;
    
    // Cancellation
    std::atomic<bool> cancellation_requested_;
    
    // Helper methods
    std::string GenerateQueryId() const;
    void ValidateState(QueryState new_state) const;
};

} // namespace SimpleRDBMS