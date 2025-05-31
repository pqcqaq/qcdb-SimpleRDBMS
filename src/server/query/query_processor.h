#pragma once

#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "execution/execution_engine.h"
#include "parser/parser.h"
#include "query_context.h"
#include "server/config/server_config.h"
#include "server/protocol/protocol_handler.h"

namespace SimpleRDBMS {

// Forward declarations
class Session;
class ExecutionEngine;
class TransactionManager;
class Catalog;

enum class QueryType {
    UNKNOWN = 0,
    SELECT,
    INSERT,
    UPDATE,
    DELETE,
    CREATE_TABLE,
    DROP_TABLE,
    CREATE_INDEX,
    DROP_INDEX,
    SHOW_TABLES,
    BEGIN_TRANSACTION,
    COMMIT_TRANSACTION,
    ROLLBACK_TRANSACTION,
    EXPLAIN
};

struct QueryPlan {
    QueryType type;
    std::unique_ptr<Statement> statement;
    std::chrono::system_clock::time_point parse_time;
    std::chrono::milliseconds parse_duration;
    size_t estimated_cost;
    bool is_cached;
};

struct QueryStats {
    size_t total_queries;
    size_t successful_queries;
    size_t failed_queries;
    std::chrono::milliseconds total_execution_time;
    std::chrono::milliseconds average_execution_time;
    std::chrono::milliseconds max_execution_time;
    std::chrono::milliseconds min_execution_time;
    std::unordered_map<QueryType, size_t> query_type_counts;
};

class QueryProcessor {
   public:
    explicit QueryProcessor(const ServerConfig& config);
    ~QueryProcessor();

    // Lifecycle management
    bool Initialize(ExecutionEngine* execution_engine,
                    TransactionManager* transaction_manager, Catalog* catalog);
    void Shutdown();
    bool IsInitialized() const { return initialized_; }

    // Query processing
    QueryResult ProcessQuery(Session* session, const std::string& query_string);
    QueryResult ProcessStatement(Session* session, Statement* statement);

    // Query planning
    std::unique_ptr<QueryPlan> CreateQueryPlan(const std::string& query_string);
    bool ValidateQuery(const std::string& query_string,
                       std::string* error_message = nullptr);

    // Query caching (simple implementation)
    void EnableQueryCache(bool enable) { query_cache_enabled_ = enable; }
    bool IsQueryCacheEnabled() const { return query_cache_enabled_; }
    void ClearQueryCache();

    // Statistics
    QueryStats GetStats() const;
    void ResetStats();

    // Configuration
    void UpdateConfig(const ServerConfig& config);
    const ServerConfig& GetConfig() const { return config_; }

    // Query timeout management
    void SetQueryTimeout(std::chrono::seconds timeout) {
        query_timeout_ = timeout;
    }
    std::chrono::seconds GetQueryTimeout() const { return query_timeout_; }

   private:
    ServerConfig config_;
    bool initialized_;

    // Core components
    ExecutionEngine* execution_engine_;
    TransactionManager* transaction_manager_;
    Catalog* catalog_;

    // Query planning and parsing
    // std::unique_ptr<Parser> parser_;
    std::unique_ptr<Statement> ParseQuery(const std::string& query_string);

    // Query caching
    bool query_cache_enabled_;
    std::unordered_map<std::string, std::unique_ptr<QueryPlan>> query_cache_;
    mutable std::mutex cache_mutex_;
    size_t max_cache_size_;

    // Statistics
    mutable std::mutex stats_mutex_;
    QueryStats stats_;

    // Configuration
    std::chrono::seconds query_timeout_;
    size_t max_query_length_;

    // Helper methods
    QueryType DetermineQueryType(const Statement* statement) const;
    std::string NormalizeQuery(const std::string& query) const;
    bool IsQueryCacheable(QueryType type) const;

    // Query execution helpers
    QueryResult ExecuteSelectStatement(Session* session, SelectStatement* stmt);
    QueryResult ExecuteInsertStatement(Session* session, InsertStatement* stmt);
    QueryResult ExecuteUpdateStatement(Session* session, UpdateStatement* stmt);
    QueryResult ExecuteDeleteStatement(Session* session, DeleteStatement* stmt);
    QueryResult ExecuteDDLStatement(Session* session, Statement* stmt);
    QueryResult ExecuteTransactionStatement(Session* session, Statement* stmt);
    QueryResult ExecuteShowTablesStatement(Session* session);
    QueryResult ExecuteExplainStatement(Session* session,
                                        ExplainStatement* stmt);

    // Error handling
    QueryResult CreateErrorResult(const std::string& error_message) const;
    QueryResult CreateSuccessResult(const std::vector<Tuple>& result_set,
                                    size_t affected_rows = 0) const;

    QueryResult DetermineQueryTypeFromString(const std::string& query) const;

    // Statistics helpers
    void UpdateQueryStats(QueryType type,
                          std::chrono::milliseconds execution_time,
                          bool success);

    // Cache management
    void AddToCache(const std::string& query, std::unique_ptr<QueryPlan> plan);
    QueryPlan* GetFromCache(const std::string& query) const;
    void EvictLRUCacheEntry();

    bool ValidateExecutionParameters(Statement* stmt,
                                     std::vector<Tuple>* result_set,
                                     Transaction* txn);
};

}  // namespace SimpleRDBMS