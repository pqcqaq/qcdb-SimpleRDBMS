#include "server/connection/connection.h"
#include "server/connection/session.h"
#include "server/protocol/protocol_handler.h"
#include "server/protocol/simple_protocol.h"
#include "common/debug.h"
#include <sys/socket.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <cstring>
#include <iostream>
#include <sstream>
#include <random>

namespace SimpleRDBMS {

Connection::Connection(int socket_fd, const std::string& client_address, int client_port)
    : state_(ConnectionState::CONNECTING),
      authenticated_(false),
      has_error_(false) {
    
    connection_info_.socket_fd = socket_fd;
    connection_info_.client_address = client_address;
    connection_info_.client_port = client_port;
    connection_info_.connect_time = std::chrono::system_clock::now();
    connection_info_.last_activity = connection_info_.connect_time;
    connection_info_.state = state_;
    connection_info_.total_requests = 0;
    connection_info_.bytes_sent = 0;
    connection_info_.bytes_received = 0;
    
    std::memset(receive_buffer_, 0, BUFFER_SIZE);
}

Connection::~Connection() {
    Close();
}

bool Connection::Initialize() {
    std::lock_guard<std::mutex> lock(connection_mutex_);
    
    if (state_ != ConnectionState::CONNECTING) {
        SetError("Invalid connection state for initialization");
        return false;
    }
    
    // Set socket options
    if (!SetSocketOptions()) {
        SetError("Failed to set socket options");
        return false;
    }
    
    // Set non-blocking mode
    if (!SetNonBlocking(false)) {  // Start with blocking mode
        SetError("Failed to set socket mode");
        return false;
    }
    
    state_ = ConnectionState::AUTHENTICATING;
    connection_info_.state = state_;
    
    return true;
}

void Connection::Close() {
    std::lock_guard<std::mutex> lock(connection_mutex_);
    
    if (state_ == ConnectionState::CLOSED) {
        return;
    }
    
    state_ = ConnectionState::CLOSING;
    connection_info_.state = state_;
    
    // Close the socket
    if (connection_info_.socket_fd >= 0) {
        shutdown(connection_info_.socket_fd, SHUT_RDWR);
        close(connection_info_.socket_fd);
        connection_info_.socket_fd = -1;
    }
    
    // Clean up session
    session_.reset();
    
    // Clean up protocol handler
    protocol_handler_.reset();
    
    state_ = ConnectionState::CLOSED;
    connection_info_.state = state_;
}

bool Connection::IsValid() const {
    std::lock_guard<std::mutex> lock(connection_mutex_);
    return state_ != ConnectionState::INVALID && 
           state_ != ConnectionState::CLOSED &&
           state_ != ConnectionState::CLOSING &&
           connection_info_.socket_fd >= 0;
}

void Connection::SetState(ConnectionState state) {
    std::lock_guard<std::mutex> lock(connection_mutex_);
    state_ = state;
    connection_info_.state = state;
}

bool Connection::CreateSession() {
    std::lock_guard<std::mutex> lock(connection_mutex_);
    
    if (session_) {
        return true;  // Session already exists
    }
    
    std::string session_id = GenerateSessionId();
    session_ = std::make_unique<Session>(session_id, connection_info_.client_address);
    
    return session_->Initialize();
}

ssize_t Connection::SendData(const char* data, size_t length) {
    if (!IsValid()) {
        SetError("Connection is not valid");
        return -1;
    }
    
    ssize_t total_sent = 0;
    while (total_sent < static_cast<ssize_t>(length)) {
        ssize_t sent = SafeSend(data + total_sent, length - total_sent);
        if (sent < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // Would block, return what we've sent so far
                break;
            }
            SetError("Send failed: " + std::string(strerror(errno)));
            return -1;
        }
        total_sent += sent;
    }
    
    AddBytesSent(total_sent);
    UpdateLastActivity();
    
    return total_sent;
}

ssize_t Connection::SendData(const std::string& data) {
    return SendData(data.c_str(), data.length());
}

ssize_t Connection::ReceiveData(char* buffer, size_t buffer_size) {
    if (!IsValid()) {
        SetError("Connection is not valid");
        return -1;
    }
    
    ssize_t received = SafeReceive(buffer, buffer_size);
    if (received < 0) {
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            SetError("Receive failed: " + std::string(strerror(errno)));
        }
        return -1;
    }
    
    if (received == 0) {
        // Connection closed by peer
        SetState(ConnectionState::CLOSING);
        return 0;
    }
    
    AddBytesReceived(received);
    UpdateLastActivity();
    
    return received;
}

std::string Connection::ReceiveLine() {
    std::string line;
    char ch;
    
    while (true) {
        ssize_t received = ReceiveData(&ch, 1);
        if (received <= 0) {
            break;
        }
        
        if (ch == '\n') {
            break;
        }
        
        if (ch != '\r') {  // Skip carriage return
            line += ch;
        }
    }
    
    return line;
}

void Connection::SetProtocolHandler(std::unique_ptr<ProtocolHandler> handler) {
    std::lock_guard<std::mutex> lock(connection_mutex_);
    protocol_handler_ = std::move(handler);
}

bool Connection::ProcessRequest() {
    if (!IsValid() || !protocol_handler_) {
        std::cerr << "[WARN] ProcessRequest: Invalid connection or no protocol handler set" << std::endl;
        SetError("Invalid connection or no protocol handler");
        return false;
    }
    
    // 设置接收超时
    struct timeval timeout;
    timeout.tv_sec = 30;  // 30秒超时
    timeout.tv_usec = 0;
    setsockopt(connection_info_.socket_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    
    // Read data from socket
    ssize_t received = ReceiveData(receive_buffer_, BUFFER_SIZE - 1);
    if (received <= 0) {
        if (received < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            // No data available right now, continue
            return true;
        }
        if (received == 0) {
            std::cout << "[DEBUG] ProcessRequest: Client closed connection" << std::endl;
        } else {
            std::cout << "[DEBUG] ProcessRequest: Receive error: " << strerror(errno) << std::endl;
        }
        return false;
    }
    
    std::cout << "[DEBUG] ProcessRequest: Received " << received << " bytes" << std::endl;
    receive_buffer_[received] = '\0';
    std::string data(receive_buffer_, received);
    
    // 清理数据，移除可能的空白字符
    size_t start = data.find_first_not_of(" \t\r\n");
    if (start == std::string::npos) {
        // 只有空白字符，继续等待更多数据
        return true;
    }
    data = data.substr(start);
    
    std::cout << "[DEBUG] ProcessRequest: Processing data: '" << data << "'" << std::endl;
    
    // Parse message
    auto message = protocol_handler_->ParseMessage(data);
    if (!message) {
        SetError("Failed to parse message");
        std::string error_resp = protocol_handler_->FormatError("Invalid message format");
        SendData(error_resp);
        return true; // 不要断开连接，继续处理
    }
    
    std::cout << "[DEBUG] ProcessRequest: Parsed message of type " 
              << static_cast<int>(message->type) << " with content: '" 
              << message->content << "'" << std::endl;
    
    // Handle message based on type
    bool success = false;
    try {
        auto start_time = std::chrono::high_resolution_clock::now();
        
        switch (message->type) {
            case MessageType::AUTHENTICATION:
                std::cout << "[DEBUG] Processing AUTHENTICATION message" << std::endl;
                success = protocol_handler_->HandleAuthentication(this, *message);
                break;
            case MessageType::QUERY:
                std::cout << "[DEBUG] Processing QUERY message" << std::endl;
                if (!IsAuthenticated()) {
                    std::string error_resp = protocol_handler_->FormatError("Not authenticated");
                    SendData(error_resp);
                    return true; // 继续保持连接
                }
                success = protocol_handler_->HandleQuery(this, *message);
                break;
            case MessageType::COMMAND:
                std::cout << "[DEBUG] Processing COMMAND message" << std::endl;
                if (!IsAuthenticated()) {
                    std::string error_resp = protocol_handler_->FormatError("Not authenticated");
                    SendData(error_resp);
                    return true; // 继续保持连接
                }
                success = static_cast<SimpleProtocolHandler*>(protocol_handler_.get())->HandleCommand(this, *message);
                break;
            case MessageType::HEARTBEAT:
                std::cout << "[DEBUG] Processing HEARTBEAT message" << std::endl;
                SendData(protocol_handler_->FormatOkMessage("PONG"));
                success = true;
                break;
            case MessageType::CLOSE:
                std::cout << "[DEBUG] Processing CLOSE message" << std::endl;
                SetState(ConnectionState::CLOSING);
                return false;
            default:
                std::cout << "[DEBUG] Processing UNKNOWN message type" << std::endl;
                SetError("Unknown message type");
                std::string error_resp = protocol_handler_->FormatError("Unknown message type");
                SendData(error_resp);
                success = true; // 即使消息类型未知，也保持连接
                break;
        }
        
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        std::cout << "[DEBUG] ProcessRequest: Message processing took " << duration.count() << "ms" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "[ERROR] Exception in ProcessRequest: " << e.what() << std::endl;
        std::string error_resp = protocol_handler_->FormatError("Internal server error");
        SendData(error_resp);
        success = true; // 发生异常时也保持连接
    }
    
    IncrementRequests();
    return success;
}

bool Connection::HandleQuery(const std::string& query) {
    if (!session_) {
        SetError("No session available");
        return false;
    }
    
    std::vector<Tuple> result_set;
    bool success = session_->ExecuteQuery(query, &result_set);
    
    if (!success) {
        return false;
    }
    
    // The protocol handler will format and send the response
    return true;
}

bool Connection::Authenticate(const std::string& username, const std::string& password) {
    // Simple authentication - in real system, check against user table
    if (username.empty()) {
        SetError("Username cannot be empty");
        return false;
    }
    
    // For now, accept any non-empty username/password
    authenticated_ = true;
    username_ = username;
    
    // Create session after successful authentication
    if (!CreateSession()) {
        authenticated_ = false;
        SetError("Failed to create session");
        return false;
    }
    
    SetState(ConnectionState::CONNECTED);
    
    return true;
}

void Connection::UpdateLastActivity() {
    connection_info_.last_activity = std::chrono::system_clock::now();
}

bool Connection::IsIdle(std::chrono::seconds timeout) const {
    auto now = std::chrono::system_clock::now();
    auto idle_time = std::chrono::duration_cast<std::chrono::seconds>(
        now - connection_info_.last_activity);
    return idle_time >= timeout;
}

bool Connection::IsTimedOut(std::chrono::seconds timeout) const {
    auto now = std::chrono::system_clock::now();
    auto connection_time = std::chrono::duration_cast<std::chrono::seconds>(
        now - connection_info_.connect_time);
    return connection_time >= timeout;
}

void Connection::AddBytesSent(size_t bytes) {
    connection_info_.bytes_sent += bytes;
}

void Connection::AddBytesReceived(size_t bytes) {
    connection_info_.bytes_received += bytes;
}

void Connection::SetError(const std::string& error) {
    has_error_ = true;
    last_error_ = error;
}

void Connection::ClearError() {
    has_error_ = false;
    last_error_.clear();
}

bool Connection::SetSocketOptions() {
    int yes = 1;
    
    // Allow socket reuse
    if (setsockopt(connection_info_.socket_fd, SOL_SOCKET, SO_REUSEADDR, 
                   &yes, sizeof(yes)) < 0) {
        std::cerr << "[WARN] Failed to set SO_REUSEADDR: " << strerror(errno) << std::endl;
        // 不要失败，继续设置其他选项
    }
    
    // Set socket keep-alive
    if (setsockopt(connection_info_.socket_fd, SOL_SOCKET, SO_KEEPALIVE,
                   &yes, sizeof(yes)) < 0) {
        std::cerr << "[WARN] Failed to set SO_KEEPALIVE: " << strerror(errno) << std::endl;
        // 不要失败，继续设置其他选项
    }
    
    // 设置更短的超时时间，避免阻塞
    struct timeval timeout;
    timeout.tv_sec = 5;   // 5秒超时而不是30秒
    timeout.tv_usec = 0;
    
    if (setsockopt(connection_info_.socket_fd, SOL_SOCKET, SO_RCVTIMEO,
                   &timeout, sizeof(timeout)) < 0) {
        std::cerr << "[WARN] Failed to set SO_RCVTIMEO: " << strerror(errno) << std::endl;
        // 不要失败，继续设置其他选项
    }
    
    if (setsockopt(connection_info_.socket_fd, SOL_SOCKET, SO_SNDTIMEO,
                   &timeout, sizeof(timeout)) < 0) {
        std::cerr << "[WARN] Failed to set SO_SNDTIMEO: " << strerror(errno) << std::endl;
        // 不要失败，继续设置其他选项
    }
    
    // 设置socket为非阻塞模式
    int flags = fcntl(connection_info_.socket_fd, F_GETFL, 0);
    if (flags >= 0) {
        flags |= O_NONBLOCK;
        if (fcntl(connection_info_.socket_fd, F_SETFL, flags) < 0) {
            std::cerr << "[WARN] Failed to set non-blocking mode: " << strerror(errno) << std::endl;
        }
    }
    
    return true; // 即使某些选项设置失败也返回true
}

bool Connection::SetNonBlocking(bool non_blocking) {
    int flags = fcntl(connection_info_.socket_fd, F_GETFL, 0);
    if (flags < 0) {
        return false;
    }
    
    if (non_blocking) {
        flags |= O_NONBLOCK;
    } else {
        flags &= ~O_NONBLOCK;
    }
    
    return fcntl(connection_info_.socket_fd, F_SETFL, flags) >= 0;
}

ssize_t Connection::SafeSend(const char* data, size_t length) {
    return send(connection_info_.socket_fd, data, length, MSG_NOSIGNAL);
}

ssize_t Connection::SafeReceive(char* buffer, size_t buffer_size) {
    return recv(connection_info_.socket_fd, buffer, buffer_size, 0);
}

std::string Connection::GenerateSessionId() const {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 15);
    
    std::stringstream ss;
    ss << std::hex;
    
    for (int i = 0; i < 32; ++i) {
        ss << dis(gen);
    }
    
    return ss.str();
}

} // namespace SimpleRDBMS