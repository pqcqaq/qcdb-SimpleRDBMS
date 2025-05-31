#pragma once

#include "protocol_handler.h"
#include <sstream>

namespace SimpleRDBMS {

/**
 * Simple text-based protocol for SimpleRDBMS
 * 
 * Protocol format:
 * - Commands are line-based, terminated by '\n'
 * - Authentication: AUTH <username> <password>
 * - Query: QUERY <sql_statement>
 * - Command: CMD <command>
 * - Close: CLOSE
 * 
 * Response format:
 * - Success: OK [message]
 * - Error: ERROR <error_message>
 * - Result: RESULT <rows>\n<column_names>\n<data_rows>
 * - Ready: READY
   */
    class SimpleProtocolHandler : public ProtocolHandler {
    public:
    SimpleProtocolHandler();
    ~SimpleProtocolHandler() override = default;
    
    // Protocol identification
    std::string GetProtocolName() const override { return "SimpleText"; }
    std::string GetProtocolVersion() const override { return "1.0"; }
    
    // Connection handling
    bool HandleConnection(Connection* connection) override;
    bool HandleDisconnection(Connection* connection) override;

    bool HandleCommand(Connection* connection, const Message& message);
    
    // Message parsing and formatting
    std::unique_ptr<Message> ParseMessage(const std::string& raw_data) override;
    std::string FormatMessage(const Message& message) override;
    
    // Authentication
    bool HandleAuthentication(Connection* connection, const Message& message) override;
    std::string FormatAuthenticationChallenge() override;
    std::string FormatAuthenticationResponse(bool success, const std::string& message = "") override;
    
    // Query handling
    bool HandleQuery(Connection* connection, const Message& message) override;
    std::string FormatQueryResult(const QueryResult& result) override;
    std::string FormatError(const std::string& error_message) override;
    
    // Status messages
    std::string FormatReadyMessage() override;
    std::string FormatOkMessage(const std::string& message = "") override;
    
    // Utility methods
    bool IsComplete(const std::string& buffer) const override;
    size_t GetMessageLength(const std::string& buffer) const override;

private:
    // Protocol constants
    static constexpr const char* CMD_AUTH = "AUTH";
    static constexpr const char* CMD_QUERY = "QUERY";
    static constexpr const char* CMD_COMMAND = "CMD";
    static constexpr const char* CMD_CLOSE = "CLOSE";
    static constexpr const char* CMD_PING = "PING";
    
    static constexpr const char* RESP_OK = "OK";
    static constexpr const char* RESP_ERROR = "ERROR";
    static constexpr const char* RESP_RESULT = "RESULT";
    static constexpr const char* RESP_READY = "READY";
    static constexpr const char* RESP_PONG = "PONG";
    
    // Helper methods
    std::vector<std::string> SplitCommand(const std::string& command) const;
    std::string JoinStrings(const std::vector<std::string>& strings, const std::string& delimiter) const;
    std::string FormatTuple(const Tuple& tuple) const;
    std::string FormatValue(const Value& value) const;
    MessageType ParseMessageType(const std::string& command) const;
    bool HandlePing(Connection* connection);
    
    // Command handlers
    bool ProcessShowTablesCommand(Connection* connection);
    bool ProcessDescribeCommand(Connection* connection, const std::string& table_name);
    bool ProcessStatusCommand(Connection* connection);
    bool ProcessHelpCommand(Connection* connection);
};

} // namespace SimpleRDBMS