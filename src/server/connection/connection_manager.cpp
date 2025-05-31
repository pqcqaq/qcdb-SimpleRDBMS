#include "server/connection/connection_manager.h"

#include <algorithm>
#include <iostream>
#include <sstream>

#include "server/connection/connection.h"
#include "server/thread/thread_pool.h"

namespace SimpleRDBMS {

ConnectionManager::ConnectionManager(const ServerConfig& config)
    : config_(config), running_(false), monitoring_(false) {
    // Initialize statistics
    stats_.total_connections = 0;
    stats_.active_connections = 0;
    stats_.authenticated_connections = 0;
    stats_.idle_connections = 0;
    stats_.peak_connections = 0;
    stats_.last_connection_time = std::chrono::system_clock::now();
    stats_.total_bytes_sent = 0;
    stats_.total_bytes_received = 0;
    stats_.total_queries_processed = 0;
}

ConnectionManager::~ConnectionManager() { Shutdown(); }

bool ConnectionManager::Initialize() {
    std::unique_lock<std::shared_mutex> lock(connections_mutex_);

    if (running_) {
        return true;  // Already initialized
    }

    running_ = true;

    // Start monitoring thread
    StartMonitoring();

    return true;
}

void ConnectionManager::Shutdown() {
    // Stop accepting new connections
    running_ = false;

    // Stop monitoring
    StopMonitoring();

    // Force close all connections
    ForceCloseConnections();

    // Clear connections map
    {
        std::unique_lock<std::shared_mutex> lock(connections_mutex_);
        connections_.clear();
    }
}

std::shared_ptr<Connection> ConnectionManager::AcceptConnection(
    int client_socket, const std::string& client_address, int client_port) {
    if (!running_) {
        return nullptr;
    }

    // Check if we can accept new connections
    if (!CanAcceptNewConnection()) {
        LogConnectionEvent("Connection rejected - max connections reached",
                           nullptr);
        return nullptr;
    }

    // Create new connection
    auto connection = std::make_shared<Connection>(client_socket,
                                                   client_address, client_port);

    // Initialize connection
    if (!connection->Initialize()) {
        LogConnectionEvent("Connection initialization failed", connection);
        return nullptr;
    }

    // Add to connections map
    {
        std::unique_lock<std::shared_mutex> lock(connections_mutex_);
        connections_[client_socket] = connection;
    }

    // Update statistics
    {
        std::lock_guard<std::mutex> stats_lock(stats_mutex_);
        stats_.total_connections++;
        stats_.active_connections++;
        stats_.last_connection_time = std::chrono::system_clock::now();

        if (stats_.active_connections > stats_.peak_connections) {
            stats_.peak_connections = stats_.active_connections;
        }
    }

    LogConnectionEvent("New connection accepted", connection);

    return connection;
}

bool ConnectionManager::RemoveConnection(
    const std::shared_ptr<Connection>& connection) {
    if (!connection) {
        return false;
    }

    return RemoveConnection(connection->GetSocketFd());
}

bool ConnectionManager::RemoveConnection(int socket_fd) {
    std::shared_ptr<Connection> connection;

    // Remove from map
    {
        std::unique_lock<std::shared_mutex> lock(connections_mutex_);
        auto it = connections_.find(socket_fd);
        if (it == connections_.end()) {
            return false;
        }
        connection = it->second;
        connections_.erase(it);
    }

    // Close connection
    if (connection) {
        connection->Close();
    }

    // Update statistics
    {
        std::lock_guard<std::mutex> stats_lock(stats_mutex_);
        if (stats_.active_connections > 0) {
            stats_.active_connections--;
        }
    }

    LogConnectionEvent("Connection removed", connection);

    return true;
}

std::shared_ptr<Connection> ConnectionManager::GetConnection(
    int socket_fd) const {
    std::shared_lock<std::shared_mutex> lock(connections_mutex_);

    auto it = connections_.find(socket_fd);
    if (it != connections_.end()) {
        return it->second;
    }

    return nullptr;
}

std::vector<std::shared_ptr<Connection>> ConnectionManager::GetAllConnections()
    const {
    std::shared_lock<std::shared_mutex> lock(connections_mutex_);

    std::vector<std::shared_ptr<Connection>> result;
    result.reserve(connections_.size());

    for (const auto& [fd, conn] : connections_) {
        result.push_back(conn);
    }

    return result;
}

std::vector<std::shared_ptr<Connection>>
ConnectionManager::GetActiveConnections() const {
    std::shared_lock<std::shared_mutex> lock(connections_mutex_);

    std::vector<std::shared_ptr<Connection>> result;

    for (const auto& [fd, conn] : connections_) {
        if (conn && conn->IsConnected() &&
            !conn->IsIdle(config_.GetNetworkConfig().idle_timeout)) {
            result.push_back(conn);
        }
    }

    return result;
}

std::vector<std::shared_ptr<Connection>> ConnectionManager::GetIdleConnections()
    const {
    std::shared_lock<std::shared_mutex> lock(connections_mutex_);

    std::vector<std::shared_ptr<Connection>> result;

    for (const auto& [fd, conn] : connections_) {
        if (conn && conn->IsConnected() &&
            conn->IsIdle(config_.GetNetworkConfig().idle_timeout)) {
            result.push_back(conn);
        }
    }

    return result;
}

bool ConnectionManager::CanAcceptNewConnection() const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    return stats_.active_connections <
           static_cast<size_t>(config_.GetNetworkConfig().max_connections);
}

size_t ConnectionManager::GetConnectionCount() const {
    std::shared_lock<std::shared_mutex> lock(connections_mutex_);
    return connections_.size();
}

void ConnectionManager::StartMonitoring() {
    if (monitoring_) {
        return;
    }

    monitoring_ = true;
    monitor_thread_ =
        std::make_unique<std::thread>(&ConnectionManager::MonitorLoop, this);
}

void ConnectionManager::StopMonitoring() {
    if (!monitoring_) {
        return;
    }

    monitoring_ = false;
    monitor_cv_.notify_all();

    if (monitor_thread_ && monitor_thread_->joinable()) {
        monitor_thread_->join();
    }

    monitor_thread_.reset();
}

ConnectionStats ConnectionManager::GetStats() const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    return stats_;
}

void ConnectionManager::UpdateStats() { UpdateConnectionStats(); }

void ConnectionManager::ResetStats() {
    std::lock_guard<std::mutex> lock(stats_mutex_);

    // Keep current connection counts
    size_t current_active = stats_.active_connections;

    // Reset stats
    stats_ = ConnectionStats{};
    stats_.active_connections = current_active;
    stats_.last_connection_time = std::chrono::system_clock::now();
}

void ConnectionManager::UpdateConfig(const ServerConfig& config) {
    config_ = config;
}

size_t ConnectionManager::CleanupIdleConnections() {
    auto idle_connections = GetIdleConnections();
    size_t cleaned = 0;

    for (const auto& conn : idle_connections) {
        if (RemoveConnection(conn)) {
            cleaned++;
        }
    }

    if (cleaned > 0) {
        std::cout << "Cleaned up " << cleaned << " idle connections"
                  << std::endl;
    }

    return cleaned;
}

size_t ConnectionManager::CleanupTimedOutConnections() {
    std::vector<int> to_remove;

    {
        std::shared_lock<std::shared_mutex> lock(connections_mutex_);

        for (const auto& [fd, conn] : connections_) {
            if (conn && conn->IsTimedOut(
                            config_.GetNetworkConfig().connection_timeout)) {
                to_remove.push_back(fd);
            }
        }
    }

    size_t cleaned = 0;
    for (int fd : to_remove) {
        if (RemoveConnection(fd)) {
            cleaned++;
        }
    }

    if (cleaned > 0) {
        std::cout << "Cleaned up " << cleaned << " timed out connections"
                  << std::endl;
    }

    return cleaned;
}

size_t ConnectionManager::ForceCloseConnections() {
    std::vector<int> all_fds;

    {
        std::shared_lock<std::shared_mutex> lock(connections_mutex_);
        for (const auto& [fd, conn] : connections_) {
            all_fds.push_back(fd);
        }
    }

    size_t closed = 0;
    for (int fd : all_fds) {
        if (RemoveConnection(fd)) {
            closed++;
        }
    }

    return closed;
}

void ConnectionManager::MonitorLoop() {
    while (monitoring_) {
        // Wait for cleanup interval
        std::unique_lock<std::mutex> lock(monitor_mutex_);
        monitor_cv_.wait_for(lock, cleanup_interval_,
                             [this] { return !monitoring_; });

        if (!monitoring_) {
            break;
        }

        // Perform cleanup
        CleanupConnections();

        // Update statistics
        UpdateConnectionStats();
    }
}

void ConnectionManager::CleanupConnections() {
    // Clean up idle connections
    CleanupIdleConnections();

    // Clean up timed out connections
    CleanupTimedOutConnections();

    // Remove invalid connections
    std::vector<int> invalid_connections;

    {
        std::shared_lock<std::shared_mutex> lock(connections_mutex_);

        for (const auto& [fd, conn] : connections_) {
            if (!IsConnectionValid(conn)) {
                invalid_connections.push_back(fd);
            }
        }
    }

    for (int fd : invalid_connections) {
        RemoveConnection(fd);
    }
}

bool ConnectionManager::IsConnectionValid(
    const std::shared_ptr<Connection>& connection) const {
    return connection && connection->IsValid();
}

void ConnectionManager::UpdateConnectionStats() {
    std::lock_guard<std::mutex> stats_lock(stats_mutex_);

    size_t authenticated = 0;
    size_t idle = 0;
    size_t bytes_sent = 0;
    size_t bytes_received = 0;

    {
        std::shared_lock<std::shared_mutex> lock(connections_mutex_);

        for (const auto& [fd, conn] : connections_) {
            if (!conn) continue;

            if (conn->IsAuthenticated()) {
                authenticated++;
            }

            if (conn->IsIdle(config_.GetNetworkConfig().idle_timeout)) {
                idle++;
            }

            const auto& info = conn->GetConnectionInfo();
            bytes_sent += info.bytes_sent;
            bytes_received += info.bytes_received;
        }
    }

    stats_.authenticated_connections = authenticated;
    stats_.idle_connections = idle;
    stats_.total_bytes_sent = bytes_sent;
    stats_.total_bytes_received = bytes_received;
}

void ConnectionManager::LogConnectionEvent(
    const std::string& event,
    const std::shared_ptr<Connection>& connection) const {
    std::stringstream ss;
    ss << "[ConnectionManager] " << event;

    if (connection) {
        ss << " - Client: " << connection->GetClientAddress()
           << ", Socket: " << connection->GetSocketFd();
    }

    std::cout << ss.str() << std::endl;
}

}  // namespace SimpleRDBMS