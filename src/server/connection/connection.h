#pragma once

#include "session.h"
#include "server/protocol/protocol_handler.h"
#include <memory>
#include <string>
#include <chrono>
#include <mutex>
#include <atomic>
#include <sys/socket.h>
#include <netinet/in.h>

namespace SimpleRDBMS {

// Forward declarations
class ProtocolHandler;

enum class ConnectionState {
    INVALID = 0,
    CONNECTING,     // 正在建立连接
    AUTHENTICATING, // 认证中
    CONNECTED,      // 已连接
    PROCESSING,     // 处理请求中
    CLOSING,        // 正在关闭
    CLOSED          // 已关闭
};

struct ConnectionInfo {
    int socket_fd;
    std::string client_address;
    int client_port;
    std::chrono::system_clock::time_point connect_time;
    std::chrono::system_clock::time_point last_activity;
    ConnectionState state;
    size_t total_requests;
    size_t bytes_sent;
    size_t bytes_received;
};

class Connection {
public:
    explicit Connection(int socket_fd, const std::string& client_address, int client_port);
    ~Connection();
    
    // Connection lifecycle
    bool Initialize();
    void Close();
    bool IsValid() const;
    bool IsConnected() const { return state_ == ConnectionState::CONNECTED; }
    bool SetNonBlocking(bool non_blocking);
    
    // State management
    ConnectionState GetState() const { return state_; }
    void SetState(ConnectionState state);
    
    // Connection info
    const ConnectionInfo& GetConnectionInfo() const { return connection_info_; }
    int GetSocketFd() const { return connection_info_.socket_fd; }
    std::string GetClientAddress() const { return connection_info_.client_address; }
    
    // Session management
    Session* GetSession() const { return session_.get(); }
    bool CreateSession();
    
    // Data I/O
    ssize_t SendData(const char* data, size_t length);
    ssize_t SendData(const std::string& data);
    ssize_t ReceiveData(char* buffer, size_t buffer_size);
    std::string ReceiveLine(); // Receive until '\n'
    
    // Protocol handling
    void SetProtocolHandler(std::unique_ptr<ProtocolHandler> handler);
    ProtocolHandler* GetProtocolHandler() const { return protocol_handler_.get(); }
    
    // Request processing
    bool ProcessRequest();
    bool HandleQuery(const std::string& query);
    
    // Authentication
    bool Authenticate(const std::string& username, const std::string& password);
    bool IsAuthenticated() const { return authenticated_; }
    const std::string& GetUsername() const { return username_; }
    
    // Activity tracking
    void UpdateLastActivity();
    bool IsIdle(std::chrono::seconds timeout) const;
    bool IsTimedOut(std::chrono::seconds timeout) const;
    
    // Statistics
    void IncrementRequests() { connection_info_.total_requests++; }
    void AddBytesSent(size_t bytes);
    void AddBytesReceived(size_t bytes);
    
    // Error handling
    bool HasError() const { return has_error_; }
    std::string GetLastError() const { return last_error_; }
    void SetError(const std::string& error);
    void ClearError();

private:
    ConnectionInfo connection_info_;
    ConnectionState state_;
    
    // Session
    std::unique_ptr<Session> session_;
    
    // Protocol handling
    std::unique_ptr<ProtocolHandler> protocol_handler_;
    
    // Authentication
    bool authenticated_;
    std::string username_;
    
    // Error state
    std::atomic<bool> has_error_;
    std::string last_error_;
    
    // Thread safety
    mutable std::mutex connection_mutex_;
    
    // Buffer for I/O operations
    static constexpr size_t BUFFER_SIZE = 8192;
    char receive_buffer_[BUFFER_SIZE];
    
    // Helper methods
    bool SetSocketOptions();
    ssize_t SafeSend(const char* data, size_t length);
    ssize_t SafeReceive(char* buffer, size_t buffer_size);
    std::string GenerateSessionId() const;
};

} // namespace SimpleRDBMS