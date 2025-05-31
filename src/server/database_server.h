#pragma once

#include "config/server_config.h"
#include "connection/connection_manager.h"
#include "thread/thread_pool.h"
#include "query/query_processor.h"
#include "protocol/protocol_handler.h"

// Database core components
#include "storage/disk_manager.h"
#include "buffer/buffer_pool_manager.h"
#include "buffer/lru_replacer.h"
#include "catalog/catalog.h"
#include "execution/execution_engine.h"
#include "transaction/transaction_manager.h"
#include "transaction/lock_manager.h"
#include "recovery/log_manager.h"
#include "recovery/recovery_manager.h"

#include <memory>
#include <thread>
#include <atomic>
#include <mutex>
#include <chrono>
#include <condition_variable>
#include <sys/socket.h>
#include <netinet/in.h>
#include <signal.h>

namespace SimpleRDBMS {

enum class ServerState {
    STOPPED = 0,
    STARTING,
    RUNNING,
    STOPPING,
    ERROR
};

struct ServerStats {
    std::chrono::system_clock::time_point start_time;
    std::chrono::milliseconds uptime;
    size_t total_connections;
    size_t active_connections;
    size_t total_queries;
    size_t successful_queries;
    size_t failed_queries;
    size_t active_transactions;
    size_t committed_transactions;
    size_t aborted_transactions;
    double cpu_usage;
    size_t memory_usage;
    size_t disk_usage;
};

class DatabaseServer {
public:
    explicit DatabaseServer(const ServerConfig& config = ServerConfig{});
    ~DatabaseServer();
    
    // Server lifecycle
    bool Initialize();
    bool Start();
    void Stop();
    void Shutdown();
    
    // Server state
    ServerState GetState() const { return state_; }
    bool IsRunning() const { return state_ == ServerState::RUNNING; }
    bool IsStopped() const { return state_ == ServerState::STOPPED; }
    
    // Configuration management
    bool LoadConfig(const std::string& config_file);
    bool LoadConfig(int argc, char* argv[]);
    void UpdateConfig(const ServerConfig& config);
    const ServerConfig& GetConfig() const { return config_; }
    
    // Statistics and monitoring
    ServerStats GetStats();
    void PrintStats();
    void ResetStats();
    
    // Component access (for testing or advanced usage)
    ConnectionManager* GetConnectionManager() const { return connection_manager_.get(); }
    QueryProcessor* GetQueryProcessor() const { return query_processor_.get(); }
    ExecutionEngine* GetExecutionEngine() const { return execution_engine_.get(); }
    Catalog* GetCatalog() const { return catalog_.get(); }
    
    // Signal handling
    static void SetupSignalHandlers();
    static void SignalHandler(int signal);

private:
    ServerConfig config_;
    ServerState state_;
    
    // Network components
    int server_socket_;
    std::unique_ptr<std::thread> accept_thread_;
    std::atomic<bool> accepting_connections_;
    
    // Core database components
    std::unique_ptr<DiskManager> disk_manager_;
    std::unique_ptr<DiskManager> log_disk_manager_;
    std::unique_ptr<LRUReplacer> replacer_;
    std::unique_ptr<BufferPoolManager> buffer_pool_manager_;
    std::unique_ptr<LogManager> log_manager_;
    std::unique_ptr<LockManager> lock_manager_;
    std::unique_ptr<TransactionManager> transaction_manager_;
    std::unique_ptr<Catalog> catalog_;
    std::unique_ptr<ExecutionEngine> execution_engine_;
    std::unique_ptr<RecoveryManager> recovery_manager_;
    
    // Server components
    std::unique_ptr<ConnectionManager> connection_manager_;
    std::unique_ptr<ThreadPool> connection_thread_pool_;
    std::unique_ptr<ThreadPool> query_thread_pool_;
    std::unique_ptr<QueryProcessor> query_processor_;
    
    // Statistics
    mutable std::mutex stats_mutex_;
    ServerStats stats_;
    
    // Synchronization
    mutable std::mutex state_mutex_;
    std::condition_variable state_cv_;
    
    // Global server instance for signal handling
    static DatabaseServer* global_instance_;
    
    // Initialization methods
    bool InitializeNetworking();
    bool InitializeDatabaseCore();
    bool InitializeServerComponents();
    bool InitializeLogging();
    
    // Network methods
    void AcceptLoop();
    bool HandleNewConnection(int client_socket, const sockaddr_in& client_addr);
    void ProcessConnection(std::shared_ptr<Connection> connection);
    
    // Cleanup methods
    void CleanupNetworking();
    void CleanupDatabaseCore();
    void CleanupServerComponents();
    
    // State management
    void SetState(ServerState new_state);
    bool WaitForState(ServerState expected_state, std::chrono::milliseconds timeout);
    
    // Statistics helpers
    void UpdateServerStats();
    void InitializeStats();
    
    // Error handling
    void HandleError(const std::string& error_message);
    void LogError(const std::string& error_message) const;
    void LogInfo(const std::string& message) const;
    
    // Signal handling helpers
    void HandleShutdownSignal();
    void HandleReloadSignal();
    
    // Utility methods
    std::string GetServerInfo() const;
    bool ValidateConfig() const;
    void PrintStartupBanner() const;
};

} // namespace SimpleRDBMS