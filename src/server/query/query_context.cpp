#include "server/query/query_context.h"
#include "server/connection/session.h"
#include "transaction/transaction.h"
#include "parser/parser.h" 
#include <sstream>
#include <random>
#include <iomanip>
#include <iostream>

namespace SimpleRDBMS {

QueryContext::QueryContext(Session* session, const std::string& query_string)
    : query_string_(query_string),
      session_(session),
      state_(QueryState::CREATED),
      transaction_(nullptr),
      affected_rows_(0),
      cancellation_requested_(false) {
    
    query_id_ = GenerateQueryId();
    StartTimer();
}

QueryContext::~QueryContext() {
    // Cleanup is automatic with smart pointers
}

void QueryContext::SetState(QueryState state) {
    ValidateState(state);
    state_ = state;
}

void QueryContext::SetError(const std::string& error_message) {
    error_message_ = error_message;
    state_ = QueryState::FAILED;
}

void QueryContext::SetParameter(const std::string& name, const Value& value) {
    parameters_[name] = value;
}

Value QueryContext::GetParameter(const std::string& name) const {
    auto it = parameters_.find(name);
    if (it != parameters_.end()) {
        return it->second;
    }
    return Value{};  // Return empty variant
}

bool QueryContext::HasParameter(const std::string& name) const {
    return parameters_.find(name) != parameters_.end();
}

void QueryContext::SetHint(const std::string& hint, const std::string& value) {
    hints_[hint] = value;
}

std::string QueryContext::GetHint(const std::string& hint) const {
    auto it = hints_.find(hint);
    if (it != hints_.end()) {
        return it->second;
    }
    return "";
}

bool QueryContext::HasHint(const std::string& hint) const {
    return hints_.find(hint) != hints_.end();
}

void QueryContext::Cancel() {
    cancellation_requested_ = true;
    state_ = QueryState::CANCELLED;
}

std::string QueryContext::ToString() const {
    std::stringstream ss;
    
    ss << "QueryContext {" << std::endl;
    ss << "  ID: " << query_id_ << std::endl;
    ss << "  Query: " << query_string_ << std::endl;
    ss << "  State: " << static_cast<int>(state_) << std::endl;
    ss << "  Session: " << (session_ ? session_->GetSessionId() : "null") << std::endl;
    ss << "  Transaction: " << (transaction_ ? "active" : "none") << std::endl;
    ss << "  Affected Rows: " << affected_rows_ << std::endl;
    
    if (HasError()) {
        ss << "  Error: " << error_message_ << std::endl;
    }
    
    ss << "  Metrics: {" << std::endl;
    ss << "    Parse Time: " << metrics_.parse_time.count() << "ms" << std::endl;
    ss << "    Plan Time: " << metrics_.plan_time.count() << "ms" << std::endl;
    ss << "    Execution Time: " << metrics_.execution_time.count() << "ms" << std::endl;
    ss << "    Total Time: " << metrics_.total_time.count() << "ms" << std::endl;
    ss << "    Rows Examined: " << metrics_.rows_examined << std::endl;
    ss << "    Rows Returned: " << metrics_.rows_returned << std::endl;
    ss << "    Bytes Sent: " << metrics_.bytes_sent << std::endl;
    ss << "  }" << std::endl;
    
    ss << "}" << std::endl;
    
    return ss.str();
}

void QueryContext::LogMetrics() const {
    std::cout << "[Query Metrics] " << query_id_ << std::endl;
    std::cout << "  Parse Time: " << metrics_.parse_time.count() << "ms" << std::endl;
    std::cout << "  Plan Time: " << metrics_.plan_time.count() << "ms" << std::endl;
    std::cout << "  Execution Time: " << metrics_.execution_time.count() << "ms" << std::endl;
    std::cout << "  Total Time: " << metrics_.total_time.count() << "ms" << std::endl;
    std::cout << "  Rows Examined: " << metrics_.rows_examined << std::endl;
    std::cout << "  Rows Returned: " << metrics_.rows_returned << std::endl;
    
    if (metrics_.index_seeks > 0) {
        std::cout << "  Index Seeks: " << metrics_.index_seeks << std::endl;
    }
    if (metrics_.index_scans > 0) {
        std::cout << "  Index Scans: " << metrics_.index_scans << std::endl;
    }
    if (metrics_.table_scans > 0) {
        std::cout << "  Table Scans: " << metrics_.table_scans << std::endl;
    }
    if (metrics_.temp_tables_created > 0) {
        std::cout << "  Temp Tables: " << metrics_.temp_tables_created << std::endl;
    }
}

std::string QueryContext::GenerateQueryId() const {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 15);
    
    std::stringstream ss;
    ss << "Q-";
    ss << std::hex;
    
    for (int i = 0; i < 16; ++i) {
        ss << dis(gen);
    }
    
    return ss.str();
}

void QueryContext::ValidateState(QueryState new_state) const {
    // Validate state transitions
    switch (state_) {
        case QueryState::CREATED:
            if (new_state != QueryState::PARSING && 
                new_state != QueryState::FAILED &&
                new_state != QueryState::CANCELLED) {
                throw std::runtime_error("Invalid state transition from CREATED");
            }
            break;
            
        case QueryState::PARSING:
            if (new_state != QueryState::PLANNING && 
                new_state != QueryState::FAILED &&
                new_state != QueryState::CANCELLED) {
                throw std::runtime_error("Invalid state transition from PARSING");
            }
            break;
            
        case QueryState::PLANNING:
            if (new_state != QueryState::EXECUTING && 
                new_state != QueryState::FAILED &&
                new_state != QueryState::CANCELLED) {
                throw std::runtime_error("Invalid state transition from PLANNING");
            }
            break;
            
        case QueryState::EXECUTING:
            if (new_state != QueryState::COMPLETED && 
                new_state != QueryState::FAILED &&
                new_state != QueryState::CANCELLED) {
                throw std::runtime_error("Invalid state transition from EXECUTING");
            }
            break;
            
        case QueryState::COMPLETED:
        case QueryState::FAILED:
        case QueryState::CANCELLED:
            throw std::runtime_error("Cannot transition from terminal state");
            break;
    }
}

} // namespace SimpleRDBMS