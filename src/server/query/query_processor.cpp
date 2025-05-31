#include "query_processor.h"

#include <iostream>
#include <memory>
#include <sstream>

#include "query_context.h"
#include "server/connection/session.h"

// Include necessary headers for database components
#include "catalog/catalog.h"
#include "execution/execution_engine.h"
#include "parser/parser.h"
#include "transaction/transaction_manager.h"

namespace SimpleRDBMS {

QueryProcessor::QueryProcessor(const ServerConfig& config)
    : config_(config),
      initialized_(false),
      execution_engine_(nullptr),
      transaction_manager_(nullptr),
      catalog_(nullptr),
      query_cache_enabled_(false),
      max_cache_size_(100),
      query_timeout_(60),
      max_query_length_(1024 * 1024) {
    ResetStats();
}

QueryProcessor::~QueryProcessor() { Shutdown(); }

bool QueryProcessor::Initialize(ExecutionEngine* execution_engine,
                                TransactionManager* transaction_manager,
                                Catalog* catalog) {
    if (initialized_) {
        return true;
    }
    if (!execution_engine || !transaction_manager || !catalog) {
        std::cerr << "QueryProcessor: Invalid dependencies provided"
                  << std::endl;
        return false;
    }
    execution_engine_ = execution_engine;
    transaction_manager_ = transaction_manager;
    catalog_ = catalog;

    // 不再创建parser，而是在每次使用时创建
    // try {
    //     parser_ = std::make_unique<Parser>();  // 假设Parser有默认构造函数
    //     // 如果Parser需要参数，请根据实际情况调整
    // } catch (const std::exception& e) {
    //     std::cerr << "Failed to create parser: " << e.what() << std::endl;
    //     return false;
    // }

    // Configure from config
    query_timeout_ = config_.GetQueryConfig().query_timeout;
    max_query_length_ = config_.GetQueryConfig().max_query_length;
    query_cache_enabled_ = config_.GetQueryConfig().enable_query_cache;
    max_cache_size_ = config_.GetQueryConfig().query_cache_size;
    initialized_ = true;
    std::cout << "[QueryProcessor] Initialized successfully" << std::endl;
    return true;
}

void QueryProcessor::Shutdown() {
    if (!initialized_) {
        return;
    }

    ClearQueryCache();
    // parser_.reset();

    execution_engine_ = nullptr;
    transaction_manager_ = nullptr;
    catalog_ = nullptr;

    initialized_ = false;
    std::cout << "[QueryProcessor] Shutdown complete" << std::endl;
}

QueryResult QueryProcessor::ProcessQuery(Session* session,
                                         const std::string& query_string) {
    if (!initialized_) {
        std::cout << "[ERROR] ProcessQuery: QueryProcessor not initialized"
                  << std::endl;
        return CreateErrorResult("QueryProcessor not initialized");
    }
    if (!session) {
        std::cout << "[ERROR] ProcessQuery: Invalid session" << std::endl;
        return CreateErrorResult("Invalid session");
    }
    // Validate query
    std::string error_message;
    if (!ValidateQuery(query_string, &error_message)) {
        std::cout << "[ERROR] ProcessQuery: Query validation failed: "
                  << error_message << std::endl;
        return CreateErrorResult(error_message);
    }
    std::cout << "[DEBUG] ProcessQuery: Starting to process query: "
              << query_string << std::endl;
    auto start_time = std::chrono::high_resolution_clock::now();
    try {
        // Create query context
        auto context = std::make_unique<QueryContext>(session, query_string);
        context->SetState(QueryState::PARSING);
        std::cout << "[DEBUG] ProcessQuery: Created query context" << std::endl;
        // Parse query - 每次创建新的parser
        auto parse_start = std::chrono::high_resolution_clock::now();
        std::unique_ptr<Parser> parser;
        std::unique_ptr<Statement> statement;
        try {
            std::cout << "[DEBUG] ProcessQuery: Creating parser for query: "
                      << query_string << std::endl;
            parser = std::make_unique<Parser>(query_string);
            std::cout << "[DEBUG] ProcessQuery: Parser created, starting parse"
                      << std::endl;
            statement = parser->Parse();
            std::cout << "[DEBUG] ProcessQuery: Parse completed" << std::endl;
        } catch (const std::exception& e) {
            std::cout << "[ERROR] ProcessQuery: Parse error: " << e.what()
                      << std::endl;
            return CreateErrorResult("Parse error: " + std::string(e.what()));
        }
        auto parse_end = std::chrono::high_resolution_clock::now();
        if (!statement) {
            std::cout << "[ERROR] ProcessQuery: Statement is null after parsing"
                      << std::endl;
            return CreateErrorResult(
                "Failed to parse query - statement is null");
        }
        context->GetMetrics().parse_time =
            std::chrono::duration_cast<std::chrono::milliseconds>(parse_end -
                                                                  parse_start);
        context->SetStatement(std::move(statement));
        context->SetState(QueryState::PLANNING);
        std::cout << "[DEBUG] ProcessQuery: Determining query type"
                  << std::endl;

        // 事务处理 - 确保session有活跃的事务
        auto current_transaction = session->GetCurrentTransaction();
        bool auto_commit = false;  // 标记是否需要自动提交

        if (current_transaction) {
            std::cout << "[DEBUG] ProcessQuery: Using existing transaction "
                      << current_transaction->GetTxnId() << std::endl;
            context->SetTransaction(current_transaction);
        } else {
            std::cout << "[DEBUG] ProcessQuery: No active transaction, "
                         "starting new transaction"
                      << std::endl;
            // 使用session的BeginTransaction方法，这样事务会正确设置到session中
            if (!session->BeginTransaction()) {
                std::cout << "[ERROR] ProcessQuery: Failed to begin transaction"
                          << std::endl;
                return CreateErrorResult("Failed to begin transaction");
            }
            current_transaction = session->GetCurrentTransaction();
            if (!current_transaction) {
                std::cout << "[ERROR] ProcessQuery: Transaction is still null "
                             "after BeginTransaction"
                          << std::endl;
                return CreateErrorResult("Failed to create transaction");
            }
            context->SetTransaction(current_transaction);
            auto_commit = true;  // 标记为自动事务，需要自动提交
            std::cout << "[DEBUG] ProcessQuery: Auto transaction created: "
                      << current_transaction->GetTxnId() << std::endl;
        }
        // 对于SHOW TABLES等系统查询，使用简化的事务处理
        QueryType query_type = DetermineQueryType(context->GetStatement());
        
        // Execute query
        context->SetState(QueryState::EXECUTING);
        std::cout << "[DEBUG] ProcessQuery: Starting statement execution"
                  << std::endl;
        QueryResult result = ProcessStatement(session, context->GetStatement());
        std::cout
            << "[DEBUG] ProcessQuery: Statement execution completed, success: "
            << result.success << std::endl;

        // 处理自动事务的提交或回滚
        if (auto_commit) {
            if (result.success) {
                std::cout << "[DEBUG] ProcessQuery: Auto-committing transaction"
                          << std::endl;
                if (!session->CommitTransaction()) {
                    std::cout << "[ERROR] ProcessQuery: Failed to auto-commit "
                                 "transaction"
                              << std::endl;
                    result.success = false;
                    result.error_message = "Failed to commit transaction";
                }
            } else {
                std::cout << "[DEBUG] ProcessQuery: Auto-rolling back "
                             "transaction due to error"
                          << std::endl;
                session->RollbackTransaction();
            }
        }

        if (!result.success) {
            std::cout << "[ERROR] ProcessQuery: Execution failed with error: "
                      << result.error_message << std::endl;
        }
        // Update context
        if (result.success) {
            context->SetState(QueryState::COMPLETED);
            context->SetResultSet(result.result_set);
            context->SetAffectedRows(result.affected_rows);
        } else {
            context->SetState(QueryState::FAILED);
            context->SetError(result.error_message);
        }
        auto end_time = std::chrono::high_resolution_clock::now();
        result.execution_time =
            std::chrono::duration_cast<std::chrono::milliseconds>(end_time -
                                                                  start_time);
        UpdateQueryStats(query_type, result.execution_time, result.success);
        std::cout << "[DEBUG] ProcessQuery: Query processing completed"
                  << std::endl;
        return result;
    } catch (const std::exception& e) {
        std::cout << "[ERROR] ProcessQuery: Exception: " << e.what()
                  << std::endl;
        auto end_time = std::chrono::high_resolution_clock::now();
        auto execution_time =
            std::chrono::duration_cast<std::chrono::milliseconds>(end_time -
                                                                  start_time);
        UpdateQueryStats(QueryType::UNKNOWN, execution_time, false);
        return CreateErrorResult("Query processing failed: " +
                                 std::string(e.what()));
    }
}

QueryResult QueryProcessor::ProcessStatement(Session* session,
                                             Statement* statement) {
    if (!session) {
        std::cout << "[ERROR] ProcessStatement: Session is null" << std::endl;
    }
    if (!statement) {
        std::cout << "[ERROR] ProcessStatement: Statement is null" << std::endl;
        return CreateErrorResult("Invalid statement");
    }

    std::cout << "[DEBUG] ProcessStatement: Processing statement" << std::endl;

    QueryType query_type = DetermineQueryType(statement);
    std::cout << "[DEBUG] ProcessStatement: Query type: "
              << static_cast<int>(query_type) << std::endl;

    try {
        switch (query_type) {
            case QueryType::SELECT:
                std::cout << "[DEBUG] ProcessStatement: Executing SELECT"
                          << std::endl;
                return ExecuteSelectStatement(
                    session, static_cast<SelectStatement*>(statement));
            case QueryType::INSERT:
                std::cout << "[DEBUG] ProcessStatement: Executing INSERT"
                          << std::endl;
                return ExecuteInsertStatement(
                    session, static_cast<InsertStatement*>(statement));
            case QueryType::UPDATE:
                std::cout << "[DEBUG] ProcessStatement: Executing UPDATE"
                          << std::endl;
                return ExecuteUpdateStatement(
                    session, static_cast<UpdateStatement*>(statement));
            case QueryType::DELETE:
                std::cout << "[DEBUG] ProcessStatement: Executing DELETE"
                          << std::endl;
                return ExecuteDeleteStatement(
                    session, static_cast<DeleteStatement*>(statement));
            case QueryType::CREATE_TABLE:
            case QueryType::DROP_TABLE:
            case QueryType::CREATE_INDEX:
            case QueryType::DROP_INDEX:
                std::cout << "[DEBUG] ProcessStatement: Executing DDL"
                          << std::endl;
                return ExecuteDDLStatement(session, statement);
            case QueryType::BEGIN_TRANSACTION:
            case QueryType::COMMIT_TRANSACTION:
            case QueryType::ROLLBACK_TRANSACTION:
                std::cout << "[DEBUG] ProcessStatement: Executing Transaction "
                             "statement"
                          << std::endl;
                return ExecuteTransactionStatement(session, statement);
            case QueryType::SHOW_TABLES:
                std::cout << "[DEBUG] ProcessStatement: Executing SHOW TABLES"
                          << std::endl;
                return ExecuteShowTablesStatement(session);
            case QueryType::EXPLAIN:
                std::cout << "[DEBUG] ProcessStatement: Executing EXPLAIN"
                          << std::endl;
                return ExecuteExplainStatement(
                    session, static_cast<ExplainStatement*>(statement));
            default:
                std::cout
                    << "[ERROR] ProcessStatement: Unsupported query type: "
                    << static_cast<int>(query_type) << std::endl;
                return CreateErrorResult("Unsupported query type");
        }
    } catch (const std::exception& e) {
        std::cout << "[ERROR] ProcessStatement: Exception: " << e.what()
                  << std::endl;
        return CreateErrorResult("Statement execution failed: " +
                                 std::string(e.what()));
    }
}

std::unique_ptr<QueryPlan> QueryProcessor::CreateQueryPlan(
    const std::string& query_string) {
    auto plan = std::make_unique<QueryPlan>();
    plan->parse_time = std::chrono::system_clock::now();
    try {
        // 每次创建新的parser
        auto parser = std::make_unique<Parser>(query_string);
        auto statement = parser->Parse();

        if (!statement) {
            return nullptr;
        }

        plan->type = DetermineQueryType(statement.get());
        plan->statement = std::move(statement);
        plan->estimated_cost = 100;  // Simplified cost estimation
        plan->is_cached = false;
        auto end_time = std::chrono::system_clock::now();
        plan->parse_duration =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                end_time - plan->parse_time);
        return plan;
    } catch (const std::exception& e) {
        std::cerr << "[QueryProcessor] Failed to create query plan: "
                  << e.what() << std::endl;
        return nullptr;
    }
}

bool QueryProcessor::ValidateQuery(const std::string& query_string,
                                   std::string* error_message) {
    if (query_string.empty()) {
        if (error_message) *error_message = "Empty query";
        return false;
    }

    if (query_string.length() > max_query_length_) {
        if (error_message) *error_message = "Query too long";
        return false;
    }

    // Additional validation can be added here
    return true;
}

void QueryProcessor::ClearQueryCache() {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    query_cache_.clear();
}

QueryStats QueryProcessor::GetStats() const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    auto stats = stats_;

    if (stats.total_queries > 0) {
        stats.average_execution_time = std::chrono::milliseconds(
            stats.total_execution_time.count() / stats.total_queries);
    }

    return stats;
}

void QueryProcessor::ResetStats() {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    stats_ = QueryStats{};
    stats_.min_execution_time = std::chrono::milliseconds::max();
}

void QueryProcessor::UpdateConfig(const ServerConfig& config) {
    config_ = config;
    query_timeout_ = config.GetQueryConfig().query_timeout;
    max_query_length_ = config.GetQueryConfig().max_query_length;

    bool cache_enabled = config.GetQueryConfig().enable_query_cache;
    if (cache_enabled != query_cache_enabled_) {
        query_cache_enabled_ = cache_enabled;
        if (!cache_enabled) {
            ClearQueryCache();
        }
    }

    max_cache_size_ = config.GetQueryConfig().query_cache_size;
}

QueryType QueryProcessor::DetermineQueryType(const Statement* statement) const {
    if (!statement) return QueryType::UNKNOWN;

    switch (statement->GetType()) {
        case Statement::StmtType::SELECT:
            return QueryType::SELECT;
        case Statement::StmtType::INSERT:
            return QueryType::INSERT;
        case Statement::StmtType::UPDATE:
            return QueryType::UPDATE;
        case Statement::StmtType::DELETE:
            return QueryType::DELETE;
        case Statement::StmtType::CREATE_TABLE:
            return QueryType::CREATE_TABLE;
        case Statement::StmtType::DROP_TABLE:
            return QueryType::DROP_TABLE;
        case Statement::StmtType::CREATE_INDEX:
            return QueryType::CREATE_INDEX;
        case Statement::StmtType::DROP_INDEX:
            return QueryType::DROP_INDEX;
        case Statement::StmtType::SHOW_TABLES:
            return QueryType::SHOW_TABLES;
        case Statement::StmtType::BEGIN_TXN:
            return QueryType::BEGIN_TRANSACTION;
        case Statement::StmtType::COMMIT_TXN:
            return QueryType::COMMIT_TRANSACTION;
        case Statement::StmtType::ROLLBACK_TXN:
            return QueryType::ROLLBACK_TRANSACTION;
        case Statement::StmtType::EXPLAIN:
            return QueryType::EXPLAIN;
        default:
            return QueryType::UNKNOWN;
    }
}

std::string QueryProcessor::NormalizeQuery(const std::string& query) const {
    // Simple normalization - remove extra whitespace and convert to uppercase
    std::string normalized;
    normalized.reserve(query.length());

    bool in_whitespace = false;
    for (char c : query) {
        if (std::isspace(c)) {
            if (!in_whitespace) {
                normalized += ' ';
                in_whitespace = true;
            }
        } else {
            normalized += std::toupper(c);
            in_whitespace = false;
        }
    }

    return normalized;
}

bool QueryProcessor::IsQueryCacheable(QueryType type) const {
    switch (type) {
        case QueryType::SELECT:
        case QueryType::SHOW_TABLES:
        case QueryType::EXPLAIN:
            return true;
        default:
            return false;
    }
}

QueryResult QueryProcessor::ExecuteSelectStatement(Session* session,
                                                   SelectStatement* stmt) {
    std::vector<Tuple> result_set;
    Transaction* txn = session ? session->GetCurrentTransaction() : nullptr;

    if (!ValidateExecutionParameters(stmt, &result_set, txn)) {
        return CreateErrorResult("Invalid execution parameters for SELECT");
    }

    std::cout << "[DEBUG] ExecuteSelectStatement: Starting SELECT execution"
              << std::endl;

    try {
        bool success = execution_engine_->Execute(stmt, &result_set, txn);
        std::cout
            << "[DEBUG] ExecuteSelectStatement: Execution engine returned: "
            << success << std::endl;

        if (success) {
            return CreateSuccessResult(result_set);
        } else {
            return CreateErrorResult("SELECT execution failed");
        }
    } catch (const std::exception& e) {
        std::cout << "[ERROR] ExecuteSelectStatement: Exception: " << e.what()
                  << std::endl;
        return CreateErrorResult("SELECT execution failed: " +
                                 std::string(e.what()));
    }
}

QueryResult QueryProcessor::ExecuteInsertStatement(Session* session,
                                                   InsertStatement* stmt) {
    if (!stmt) {
        std::cout << "[ERROR] ExecuteInsertStatement: Statement is null"
                  << std::endl;
        return CreateErrorResult("Invalid INSERT statement");
    }

    if (!execution_engine_) {
        std::cout << "[ERROR] ExecuteInsertStatement: Execution engine is null"
                  << std::endl;
        return CreateErrorResult("Execution engine not available");
    }

    std::cout << "[DEBUG] ExecuteInsertStatement: Starting INSERT execution"
              << std::endl;

    std::vector<Tuple> result_set;
    try {
        Transaction* txn = session ? session->GetCurrentTransaction() : nullptr;
        std::cout << "[DEBUG] ExecuteInsertStatement: Calling execution engine "
                     "with transaction: "
                  << (txn ? "active" : "null") << std::endl;

        bool success = execution_engine_->Execute(stmt, &result_set, txn);
        std::cout
            << "[DEBUG] ExecuteInsertStatement: Execution engine returned: "
            << success << std::endl;

        if (success) {
            size_t affected_rows = result_set.empty() ? 1 : result_set.size();
            return CreateSuccessResult(result_set, affected_rows);
        } else {
            return CreateErrorResult("INSERT execution failed");
        }
    } catch (const std::exception& e) {
        std::cout << "[ERROR] ExecuteInsertStatement: Exception: " << e.what()
                  << std::endl;
        return CreateErrorResult("INSERT execution failed: " +
                                 std::string(e.what()));
    }
}

QueryResult QueryProcessor::ExecuteUpdateStatement(Session* session,
                                                   UpdateStatement* stmt) {
    if (!stmt) {
        std::cout << "[ERROR] ExecuteUpdateStatement: Statement is null"
                  << std::endl;
        return CreateErrorResult("Invalid UPDATE statement");
    }

    if (!execution_engine_) {
        std::cout << "[ERROR] ExecuteUpdateStatement: Execution engine is null"
                  << std::endl;
        return CreateErrorResult("Execution engine not available");
    }

    std::cout << "[DEBUG] ExecuteUpdateStatement: Starting UPDATE execution"
              << std::endl;

    std::vector<Tuple> result_set;
    try {
        Transaction* txn = session ? session->GetCurrentTransaction() : nullptr;
        std::cout << "[DEBUG] ExecuteUpdateStatement: Calling execution engine "
                     "with transaction: "
                  << (txn ? "active" : "null") << std::endl;

        bool success = execution_engine_->Execute(stmt, &result_set, txn);
        std::cout
            << "[DEBUG] ExecuteUpdateStatement: Execution engine returned: "
            << success << std::endl;

        if (success) {
            size_t affected_rows =
                result_set.empty()
                    ? 0
                    : (result_set[0].GetValues().empty()
                           ? 0
                           : std::get<int32_t>(result_set[0].GetValue(0)));
            return CreateSuccessResult(result_set, affected_rows);
        } else {
            return CreateErrorResult("UPDATE execution failed");
        }
    } catch (const std::exception& e) {
        std::cout << "[ERROR] ExecuteUpdateStatement: Exception: " << e.what()
                  << std::endl;
        return CreateErrorResult("UPDATE execution failed: " +
                                 std::string(e.what()));
    }
}

QueryResult QueryProcessor::ExecuteDeleteStatement(Session* session,
                                                   DeleteStatement* stmt) {
    if (!stmt) {
        std::cout << "[ERROR] ExecuteDeleteStatement: Statement is null"
                  << std::endl;
        return CreateErrorResult("Invalid DELETE statement");
    }

    if (!execution_engine_) {
        std::cout << "[ERROR] ExecuteDeleteStatement: Execution engine is null"
                  << std::endl;
        return CreateErrorResult("Execution engine not available");
    }

    std::cout << "[DEBUG] ExecuteDeleteStatement: Starting DELETE execution"
              << std::endl;

    std::vector<Tuple> result_set;
    try {
        Transaction* txn = session ? session->GetCurrentTransaction() : nullptr;
        std::cout << "[DEBUG] ExecuteDeleteStatement: Calling execution engine "
                     "with transaction: "
                  << (txn ? "active" : "null") << std::endl;

        bool success = execution_engine_->Execute(stmt, &result_set, txn);
        std::cout
            << "[DEBUG] ExecuteDeleteStatement: Execution engine returned: "
            << success << std::endl;

        if (success) {
            size_t affected_rows =
                result_set.empty()
                    ? 0
                    : (result_set[0].GetValues().empty()
                           ? 0
                           : std::get<int32_t>(result_set[0].GetValue(0)));
            return CreateSuccessResult(result_set, affected_rows);
        } else {
            return CreateErrorResult("DELETE execution failed");
        }
    } catch (const std::exception& e) {
        std::cout << "[ERROR] ExecuteDeleteStatement: Exception: " << e.what()
                  << std::endl;
        return CreateErrorResult("DELETE execution failed: " +
                                 std::string(e.what()));
    }
}

QueryResult QueryProcessor::ExecuteDDLStatement(Session* session,
                                                Statement* stmt) {
    if (!stmt) {
        std::cout << "[ERROR] ExecuteDDLStatement: Statement is null"
                  << std::endl;
        return CreateErrorResult("Invalid DDL statement");
    }

    if (!execution_engine_) {
        std::cout << "[ERROR] ExecuteDDLStatement: Execution engine is null"
                  << std::endl;
        return CreateErrorResult("Execution engine not available");
    }

    std::cout << "[DEBUG] ExecuteDDLStatement: Starting DDL execution"
              << std::endl;

    std::vector<Tuple> result_set;
    try {
        Transaction* txn = session ? session->GetCurrentTransaction() : nullptr;
        std::cout << "[DEBUG] ExecuteDDLStatement: Calling execution engine "
                     "with transaction: "
                  << (txn ? "active" : "null") << std::endl;

        bool success = execution_engine_->Execute(stmt, &result_set, txn);
        std::cout << "[DEBUG] ExecuteDDLStatement: Execution engine returned: "
                  << success << std::endl;

        if (success) {
            return CreateSuccessResult(result_set);
        } else {
            return CreateErrorResult("DDL execution failed");
        }
    } catch (const std::exception& e) {
        std::cout << "[ERROR] ExecuteDDLStatement: Exception: " << e.what()
                  << std::endl;
        return CreateErrorResult("DDL execution failed: " +
                                 std::string(e.what()));
    }
}

QueryResult QueryProcessor::ExecuteTransactionStatement(Session* session,
                                                        Statement* stmt) {
    QueryType type = DetermineQueryType(stmt);

    switch (type) {
        case QueryType::BEGIN_TRANSACTION:
            if (session->BeginTransaction()) {
                return CreateSuccessResult({});
            } else {
                return CreateErrorResult("Failed to begin transaction");
            }
        case QueryType::COMMIT_TRANSACTION:
            if (session->CommitTransaction()) {
                return CreateSuccessResult({});
            } else {
                return CreateErrorResult("Failed to commit transaction");
            }
        case QueryType::ROLLBACK_TRANSACTION:
            if (session->RollbackTransaction()) {
                return CreateSuccessResult({});
            } else {
                return CreateErrorResult("Failed to rollback transaction");
            }
        default:
            return CreateErrorResult("Unknown transaction statement");
    }
}

std::unique_ptr<Statement> QueryProcessor::ParseQuery(
    const std::string& query_string) {
    try {
        auto parser = std::make_unique<Parser>(query_string);
        return parser->Parse();
    } catch (const std::exception& e) {
        std::cerr << "[QueryProcessor] Parse error: " << e.what() << std::endl;
        return nullptr;
    }
}

QueryResult QueryProcessor::ExecuteShowTablesStatement(Session* session) {
    std::cout
        << "[DEBUG] ExecuteShowTablesStatement: Starting SHOW TABLES execution"
        << std::endl;

    if (!catalog_) {
        std::cout
            << "[ERROR] ExecuteShowTablesStatement: Catalog not initialized"
            << std::endl;
        return CreateErrorResult("Catalog not initialized");
    }

    if (!execution_engine_) {
        std::cout << "[ERROR] ExecuteShowTablesStatement: Execution engine not "
                     "available"
                  << std::endl;
        return CreateErrorResult("Execution engine not available");
    }

    try {
        // 使用当前session的事务，而不是创建新的
        Transaction* txn = session ? session->GetCurrentTransaction() : nullptr;
        if (!txn) {
            std::cout
                << "[ERROR] ExecuteShowTablesStatement: No active transaction"
                << std::endl;
            return CreateErrorResult("No active transaction for SHOW TABLES");
        }

        std::cout
            << "[DEBUG] ExecuteShowTablesStatement: Using existing transaction "
            << txn->GetTxnId() << std::endl;

        // 每次创建新的parser
        auto parser = std::make_unique<Parser>("SHOW TABLES");
        auto statement = parser->Parse();
        if (!statement) {
            std::cout << "[ERROR] ExecuteShowTablesStatement: Failed to parse "
                         "SHOW TABLES statement"
                      << std::endl;
            return CreateErrorResult("Failed to parse SHOW TABLES statement");
        }

        std::cout << "[DEBUG] ExecuteShowTablesStatement: Parsed SHOW TABLES "
                     "statement successfully"
                  << std::endl;

        std::vector<Tuple> result_set;
        std::cout << "[DEBUG] ExecuteShowTablesStatement: Calling execution "
                     "engine with transaction: "
                  << txn->GetTxnId() << std::endl;

        bool success =
            execution_engine_->Execute(statement.get(), &result_set, txn);
        std::cout
            << "[DEBUG] ExecuteShowTablesStatement: Execution engine returned: "
            << success << std::endl;

        if (success) {
            return CreateSuccessResult(result_set);
        } else {
            return CreateErrorResult("Failed to execute SHOW TABLES");
        }
    } catch (const std::exception& e) {
        std::cout << "[ERROR] ExecuteShowTablesStatement: Exception: "
                  << e.what() << std::endl;
        return CreateErrorResult("SHOW TABLES execution failed: " +
                                 std::string(e.what()));
    }
}

QueryResult QueryProcessor::ExecuteExplainStatement(Session* session,
                                                    ExplainStatement* stmt) {
    std::vector<Tuple> result_set;

    // Create a simple execution plan explanation
    std::vector<Value> plan_step;
    plan_step.push_back(std::string("Sequential Scan"));
    result_set.emplace_back(plan_step, nullptr);

    plan_step.clear();
    plan_step.push_back(std::string("  -> Filter"));
    result_set.emplace_back(plan_step, nullptr);

    plan_step.clear();
    plan_step.push_back(std::string("  -> Project"));
    result_set.emplace_back(plan_step, nullptr);

    return CreateSuccessResult(result_set);
}

QueryResult QueryProcessor::CreateErrorResult(
    const std::string& error_message) const {
    QueryResult result;
    result.success = false;
    result.error_message = error_message;
    result.affected_rows = 0;
    result.execution_time = std::chrono::milliseconds(0);
    return result;
}

QueryResult QueryProcessor::CreateSuccessResult(
    const std::vector<Tuple>& result_set, size_t affected_rows) const {
    QueryResult result;
    result.success = true;
    result.result_set = result_set;
    result.affected_rows =
        affected_rows == 0 ? result_set.size() : affected_rows;
    result.execution_time = std::chrono::milliseconds(0);
    return result;
}

void QueryProcessor::UpdateQueryStats(QueryType type,
                                      std::chrono::milliseconds execution_time,
                                      bool success) {
    std::lock_guard<std::mutex> lock(stats_mutex_);

    stats_.total_queries++;
    if (success) {
        stats_.successful_queries++;
    } else {
        stats_.failed_queries++;
    }

    stats_.total_execution_time += execution_time;

    if (execution_time > stats_.max_execution_time) {
        stats_.max_execution_time = execution_time;
    }

    if (execution_time < stats_.min_execution_time) {
        stats_.min_execution_time = execution_time;
    }

    stats_.query_type_counts[type]++;
}

void QueryProcessor::AddToCache(const std::string& query,
                                std::unique_ptr<QueryPlan> plan) {
    if (!query_cache_enabled_) return;

    std::lock_guard<std::mutex> lock(cache_mutex_);

    std::string normalized = NormalizeQuery(query);

    // Check if cache is full
    if (query_cache_.size() >= max_cache_size_) {
        EvictLRUCacheEntry();
    }

    plan->is_cached = true;
    query_cache_[normalized] = std::move(plan);
}

QueryPlan* QueryProcessor::GetFromCache(const std::string& query) const {
    if (!query_cache_enabled_) return nullptr;

    std::lock_guard<std::mutex> lock(cache_mutex_);

    std::string normalized = NormalizeQuery(query);
    auto it = query_cache_.find(normalized);
    if (it != query_cache_.end()) {
        return it->second.get();
    }

    return nullptr;
}

void QueryProcessor::EvictLRUCacheEntry() {
    // Simple eviction - remove first entry
    // In a real implementation, this would track LRU order
    if (!query_cache_.empty()) {
        query_cache_.erase(query_cache_.begin());
    }
}

bool QueryProcessor::ValidateExecutionParameters(Statement* stmt,
                                                 std::vector<Tuple>* result_set,
                                                 Transaction* txn) {
    if (!stmt) {
        std::cout << "[ERROR] ValidateExecutionParameters: Statement is null"
                  << std::endl;
        return false;
    }
    if (!result_set) {
        std::cout
            << "[ERROR] ValidateExecutionParameters: Result set pointer is null"
            << std::endl;
        return false;
    }
    if (!execution_engine_) {
        std::cout
            << "[ERROR] ValidateExecutionParameters: Execution engine is null"
            << std::endl;
        return false;
    }
    if (!txn) {
        std::cout << "[ERROR] ValidateExecutionParameters: Transaction is null"
                  << std::endl;
        return false;
    }
    std::cout << "[DEBUG] ValidateExecutionParameters: All parameters are valid"
              << std::endl;
    std::cout << "[DEBUG] ValidateExecutionParameters: Statement type: "
              << static_cast<int>(stmt->GetType()) << std::endl;
    std::cout << "[DEBUG] ValidateExecutionParameters: Transaction ID: "
              << txn->GetTxnId() << std::endl;
    return true;
}

}  // namespace SimpleRDBMS