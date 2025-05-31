#pragma once

#include "record/tuple.h"
#include <string>
#include <vector>
#include <memory>
#include <chrono>

namespace SimpleRDBMS {

// Forward declarations
class Connection;
class Session;

enum class MessageType {
    UNKNOWN = 0,
    QUERY,              // SQL查询
    COMMAND,            // 命令
    AUTHENTICATION,     // 认证
    HEARTBEAT,          // 心跳
    CLOSE,              // 关闭连接
    ERROR,              // 错误消息
    RESULT,             // 查询结果
    OK,                 // 成功响应
    READY               // 准备就绪
};

struct Message {
    MessageType type;
    std::string content;
    std::vector<std::string> parameters;
    size_t length;
    
    Message() : type(MessageType::UNKNOWN), length(0) {}
    Message(MessageType t, const std::string& c) 
        : type(t), content(c), length(c.length()) {}
};

struct QueryResult {
    bool success;
    std::string error_message;
    std::vector<Tuple> result_set;
    std::vector<std::string> column_names;
    size_t affected_rows;
    std::chrono::milliseconds execution_time;
    
    QueryResult() : success(false), affected_rows(0), execution_time(0) {}
};

class ProtocolHandler {
public:
    ProtocolHandler() = default;
    virtual ~ProtocolHandler() = default;
    
    // Protocol identification
    virtual std::string GetProtocolName() const = 0;
    virtual std::string GetProtocolVersion() const = 0;
    
    // Connection handling
    virtual bool HandleConnection(Connection* connection) = 0;
    virtual bool HandleDisconnection(Connection* connection) = 0;
    
    // Message parsing and formatting
    virtual std::unique_ptr<Message> ParseMessage(const std::string& raw_data) = 0;
    virtual std::string FormatMessage(const Message& message) = 0;
    
    // Authentication
    virtual bool HandleAuthentication(Connection* connection, const Message& message) = 0;
    virtual std::string FormatAuthenticationChallenge() = 0;
    virtual std::string FormatAuthenticationResponse(bool success, const std::string& message = "") = 0;
    
    // Query handling
    virtual bool HandleQuery(Connection* connection, const Message& message) = 0;
    virtual std::string FormatQueryResult(const QueryResult& result) = 0;
    virtual std::string FormatError(const std::string& error_message) = 0;
    
    // Status messages
    virtual std::string FormatReadyMessage() = 0;
    virtual std::string FormatOkMessage(const std::string& message = "") = 0;
    
    // Utility methods
    virtual bool IsComplete(const std::string& buffer) const = 0;
    virtual size_t GetMessageLength(const std::string& buffer) const = 0;

protected:
    // Helper methods for subclasses
    std::string EscapeString(const std::string& str) const;
    std::string UnescapeString(const std::string& str) const;
    bool ValidateMessage(const Message& message) const;
    QueryResult ExecuteQueryOnSession(Session* session, const std::string& query);
};

// Factory for creating protocol handlers
class ProtocolHandlerFactory {
public:
    enum class ProtocolType {
        SIMPLE_TEXT,
        POSTGRESQL,
        MYSQL
    };
    
    static std::unique_ptr<ProtocolHandler> CreateHandler(ProtocolType type);
    static ProtocolType DetectProtocol(const std::string& initial_data);
};

} // namespace SimpleRDBMS