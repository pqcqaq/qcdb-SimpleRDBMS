#pragma once

#include "connection.h"
#include "server/config/server_config.h"
#include <memory>
#include <vector>
#include <unordered_map>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <chrono>
#include <shared_mutex>

namespace SimpleRDBMS {

// Forward declarations
class ThreadPool;

struct ConnectionStats {
    size_t total_connections;
    size_t active_connections;
    size_t authenticated_connections;
    size_t idle_connections;
    size_t peak_connections;
    std::chrono::system_clock::time_point last_connection_time;
    size_t total_bytes_sent;
    size_t total_bytes_received;
    size_t total_queries_processed;
};

class ConnectionManager {
public:
    explicit ConnectionManager(const ServerConfig& config);
    ~ConnectionManager();
    
    // Lifecycle management
    bool Initialize();
    void Shutdown();
    bool IsRunning() const { return running_; }
    
    // Connection management
    std::shared_ptr<Connection> AcceptConnection(int client_socket, 
                                               const std::string& client_address,
                                               int client_port);
    bool RemoveConnection(const std::shared_ptr<Connection>& connection);
    bool RemoveConnection(int socket_fd);
    
    // Connection lookup
    std::shared_ptr<Connection> GetConnection(int socket_fd) const;
    std::vector<std::shared_ptr<Connection>> GetAllConnections() const;
    std::vector<std::shared_ptr<Connection>> GetActiveConnections() const;
    std::vector<std::shared_ptr<Connection>> GetIdleConnections() const;
    
    // Connection limits
    bool CanAcceptNewConnection() const;
    size_t GetConnectionCount() const;
    size_t GetMaxConnections() const { return config_.GetNetworkConfig().max_connections; }
    
    // Connection monitoring
    void StartMonitoring();
    void StopMonitoring();
    
    // Statistics
    ConnectionStats GetStats() const;
    void UpdateStats();
    void ResetStats();
    
    // Configuration
    void UpdateConfig(const ServerConfig& config);
    const ServerConfig& GetConfig() const { return config_; }
    
    // Connection cleanup
    size_t CleanupIdleConnections();
    size_t CleanupTimedOutConnections();
    size_t ForceCloseConnections();

private:
    ServerConfig config_;
    
    // Connection storage
    std::unordered_map<int, std::shared_ptr<Connection>> connections_;
    mutable std::shared_mutex connections_mutex_;
    
    // Statistics
    ConnectionStats stats_;
    mutable std::mutex stats_mutex_;
    
    // Monitoring
    std::atomic<bool> running_;
    std::atomic<bool> monitoring_;
    std::unique_ptr<std::thread> monitor_thread_;
    std::condition_variable monitor_cv_;
    std::mutex monitor_mutex_;
    
    // Connection cleanup
    std::chrono::seconds cleanup_interval_{30};
    
    // Helper methods
    void MonitorLoop();
    void CleanupConnections();
    bool IsConnectionValid(const std::shared_ptr<Connection>& connection) const;
    void UpdateConnectionStats();
    void LogConnectionEvent(const std::string& event, 
                          const std::shared_ptr<Connection>& connection) const;
};

} // namespace SimpleRDBMS