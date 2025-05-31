#pragma once

#include <string>
#include <chrono>
#include <unordered_map>

namespace SimpleRDBMS {

struct NetworkConfig {
    std::string host = "localhost";
    int port = 15432;  // 监听的端口
    int backlog = 128;
    int max_connections = 100;
    std::chrono::seconds connection_timeout{30};
    std::chrono::seconds idle_timeout{300};
};

struct ThreadConfig {
    int worker_threads = 4;
    int query_threads = 8;
    int io_threads = 2;
    size_t max_queue_size = 1000;
};

struct DatabaseConfig {
    std::string database_file = "simpledb.db";
    std::string log_file = "simpledb.log";
    size_t buffer_pool_size = 1000;
    size_t log_buffer_size = 1024 * 1024; // 1MB
    bool enable_logging = true;
    bool enable_recovery = true;
};

struct QueryConfig {
    std::chrono::seconds query_timeout{60};
    size_t max_query_length = 1024 * 1024; // 1MB
    bool enable_query_cache = false;
    size_t query_cache_size = 100;
};

class ServerConfig {
public:
    ServerConfig() = default;
    
    // Load configuration from file
    bool LoadFromFile(const std::string& config_file);
    
    // Load configuration from command line arguments
    bool LoadFromArgs(int argc, char* argv[]);
    
    // Load configuration from environment variables
    void LoadFromEnv();
    
    // Validate configuration
    bool Validate() const;
    
    // Print configuration
    void Print() const;
    
    // Getters
    const NetworkConfig& GetNetworkConfig() const { return network_config_; }
    const ThreadConfig& GetThreadConfig() const { return thread_config_; }
    const DatabaseConfig& GetDatabaseConfig() const { return database_config_; }
    const QueryConfig& GetQueryConfig() const { return query_config_; }
    
    // Setters
    NetworkConfig& GetNetworkConfig() { return network_config_; }
    ThreadConfig& GetThreadConfig() { return thread_config_; }
    DatabaseConfig& GetDatabaseConfig() { return database_config_; }
    QueryConfig& GetQueryConfig() { return query_config_; }

private:
    NetworkConfig network_config_;
    ThreadConfig thread_config_;
    DatabaseConfig database_config_;
    QueryConfig query_config_;
    
    // Helper methods
    void SetDefaultValues();
    bool ParseConfigLine(const std::string& line);
    std::string Trim(const std::string& str) const;
};

} // namespace SimpleRDBMS