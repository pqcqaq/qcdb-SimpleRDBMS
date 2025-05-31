#include "server/protocol/simple_protocol.h"
#include "server/connection/connection.h"
#include "server/connection/session.h"
#include "server/query/query_processor.h"
#include <sstream>
#include <algorithm>
#include <iomanip>
#include <iostream>

namespace SimpleRDBMS {

SimpleProtocolHandler::SimpleProtocolHandler() {
}

bool SimpleProtocolHandler::HandleConnection(Connection* connection) {
    if (!connection) {
        return false;
    }
    
    // Send authentication challenge
    std::string challenge = FormatAuthenticationChallenge();
    connection->SendData(challenge);
    
    return true;
}

bool SimpleProtocolHandler::HandleDisconnection(Connection* connection) {
    // Cleanup any protocol-specific state
    return true;
}

std::unique_ptr<Message> SimpleProtocolHandler::ParseMessage(const std::string& raw_data) {
    auto message = std::make_unique<Message>();
    if (raw_data.empty()) {
        message->type = MessageType::UNKNOWN;
        return message;
    }
    
    // 清理输入数据
    std::string trimmed_data = raw_data;
    // 移除末尾的换行符和回车符
    while (!trimmed_data.empty() && 
           (trimmed_data.back() == '\n' || trimmed_data.back() == '\r')) {
        trimmed_data.pop_back();
    }
    
    if (trimmed_data.empty()) {
        message->type = MessageType::UNKNOWN;
        return message;
    }
    
    std::cout << "[DEBUG] ParseMessage: Processing: '" << trimmed_data << "'" << std::endl;
    
    // Split command and arguments
    std::vector<std::string> parts = SplitCommand(trimmed_data);
    if (parts.empty()) {
        message->type = MessageType::UNKNOWN;
        return message;
    }
    
    // Convert command to uppercase for comparison
    std::string command = parts[0];
    std::transform(command.begin(), command.end(), command.begin(), ::toupper);
    
    std::cout << "[DEBUG] ParseMessage: Command: '" << command << "'" << std::endl;
    
    // Parse message type
    message->type = ParseMessageType(command);
    
    // Handle different message types
    switch (message->type) {
        case MessageType::AUTHENTICATION:
            if (parts.size() >= 3) {
                message->parameters.push_back(parts[1]);  // username
                message->parameters.push_back(parts[2]);  // password
            }
            break;
        case MessageType::QUERY:
            // Reconstruct query from remaining parts
            if (parts.size() > 1) {
                message->content = trimmed_data.substr(parts[0].length());
                // 移除前导空格
                size_t start = message->content.find_first_not_of(" \t");
                if (start != std::string::npos) {
                    message->content = message->content.substr(start);
                }
            } else {
                // 如果只有QUERY命令没有内容，检查是否是简单的SQL语句
                if (command != "QUERY") {
                    message->type = MessageType::QUERY;
                    message->content = trimmed_data;
                }
            }
            break;
        case MessageType::COMMAND:
            if (parts.size() > 1) {
                message->content = parts[1];
                for (size_t i = 2; i < parts.size(); ++i) {
                    message->parameters.push_back(parts[i]);
                }
            }
            break;
        case MessageType::HEARTBEAT:
        case MessageType::CLOSE:
            // No additional data needed
            break;
        default:
            // 如果不是已知命令，当作查询处理
            std::cout << "[DEBUG] ParseMessage: Unknown command, treating as query" << std::endl;
            message->type = MessageType::QUERY;
            message->content = trimmed_data;
            break;
    }
    
    message->length = raw_data.length();
    std::cout << "[DEBUG] ParseMessage: Final type: " << static_cast<int>(message->type) 
              << ", content: '" << message->content << "'" << std::endl;
    return message;
}

std::string SimpleProtocolHandler::FormatMessage(const Message& message) {
    std::stringstream ss;
    
    switch (message.type) {
        case MessageType::OK:
            ss << RESP_OK;
            if (!message.content.empty()) {
                ss << " " << message.content;
            }
            break;
            
        case MessageType::ERROR:
            ss << RESP_ERROR << " " << message.content;
            break;
            
        case MessageType::RESULT:
            ss << RESP_RESULT << " " << message.content;
            break;
            
        case MessageType::READY:
            ss << RESP_READY;
            break;
            
        default:
            ss << message.content;
            break;
    }
    
    ss << "\n";
    return ss.str();
}

bool SimpleProtocolHandler::HandleAuthentication(Connection* connection, const Message& message) {
    if (!connection || message.parameters.size() < 2) {
        std::string error = FormatAuthenticationResponse(false, "Invalid authentication data");
        connection->SendData(error);
        return false;
    }
    
    const std::string& username = message.parameters[0];
    const std::string& password = message.parameters[1];
    
    // Perform authentication
    bool success = connection->Authenticate(username, password);
    
    // Send response
    std::string response = FormatAuthenticationResponse(success, 
        success ? "Authentication successful" : "Authentication failed");
    connection->SendData(response);
    
    return success;
}

std::string SimpleProtocolHandler::FormatAuthenticationChallenge() {
    return std::string(RESP_READY) + " Authentication required. Use: AUTH <username> <password>\n";
}

std::string SimpleProtocolHandler::FormatAuthenticationResponse(bool success, const std::string& message) {
    if (success) {
        return std::string(RESP_OK) + " " + message + "\n";
    } else {
        return std::string(RESP_ERROR) + " " + message + "\n";
    }
}

bool SimpleProtocolHandler::HandleQuery(Connection* connection, const Message& message) {
    if (!connection || !connection->GetSession()) {
        std::string error = FormatError("No session available");
        connection->SendData(error);
        return false;
    }
    
    std::cout << "[DEBUG] HandleQuery: Starting query execution: " << message.content << std::endl;
    
    try {
        // 设置超时机制
        auto start_time = std::chrono::high_resolution_clock::now();
        const auto timeout = std::chrono::seconds(30); // 30秒超时
        
        // Execute query
        QueryResult result = ExecuteQueryOnSession(connection->GetSession(), message.content);
        
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time);
        
        std::cout << "[DEBUG] HandleQuery: Query completed in " << duration.count() << " seconds" << std::endl;
        
        // Format and send response
        std::string response;
        if (result.success) {
            response = FormatQueryResult(result);
            std::cout << "[DEBUG] HandleQuery: Formatted successful response" << std::endl;
        } else {
            response = FormatError(result.error_message);
            std::cout << "[DEBUG] HandleQuery: Formatted error response: " << result.error_message << std::endl;
        }
        
        ssize_t sent = connection->SendData(response);
        if (sent <= 0) {
            std::cerr << "[ERROR] Failed to send query response" << std::endl;
            return false;
        }
        
        std::cout << "[DEBUG] Query response sent successfully, bytes: " << sent << std::endl;
        return true; // 总是返回true以保持连接
        
    } catch (const std::exception& e) {
        std::cerr << "[ERROR] Exception in HandleQuery: " << e.what() << std::endl;
        std::string error_resp = FormatError("Query execution failed: " + std::string(e.what()));
        connection->SendData(error_resp);
        return true; // 即使发生异常也保持连接
    }
}

std::string SimpleProtocolHandler::FormatQueryResult(const QueryResult& result) {
    std::stringstream ss;
    
    if (result.result_set.empty()) {
        ss << RESP_OK << " Query executed successfully. ";
        ss << result.affected_rows << " row(s) affected.\n";
        return ss.str();
    }
    
    // Format result set
    ss << RESP_RESULT << " " << result.result_set.size() << "\n";
    
    // Get column count from first tuple
    if (!result.result_set.empty()) {
        size_t column_count = result.result_set[0].GetValues().size();
        
        // Column names (simplified - use indices for now)
        for (size_t i = 0; i < column_count; ++i) {
            if (i > 0) ss << ",";
            ss << "col" << i;
        }
        ss << "\n";
        
        // Data rows
        for (const auto& tuple : result.result_set) {
            ss << FormatTuple(tuple) << "\n";
        }
    }
    
    return ss.str();
}

std::string SimpleProtocolHandler::FormatError(const std::string& error_message) {
    return std::string(RESP_ERROR) + " " + error_message + "\n";
}

std::string SimpleProtocolHandler::FormatReadyMessage() {
    return std::string(RESP_READY) + " SimpleRDBMS Server ready for queries.\n";
}

std::string SimpleProtocolHandler::FormatOkMessage(const std::string& message) {
    std::stringstream ss;
    ss << RESP_OK;
    if (!message.empty()) {
        ss << " " << message;
    }
    ss << "\n";
    return ss.str();
}

bool SimpleProtocolHandler::IsComplete(const std::string& buffer) const {
    // Messages are complete when they contain a newline
    return buffer.find('\n') != std::string::npos;
}

size_t SimpleProtocolHandler::GetMessageLength(const std::string& buffer) const {
    size_t pos = buffer.find('\n');
    if (pos != std::string::npos) {
        return pos + 1;
    }
    return 0;
}

std::vector<std::string> SimpleProtocolHandler::SplitCommand(const std::string& command) const {
    std::vector<std::string> parts;
    std::istringstream iss(command);
    std::string part;
    
    // First part is the command
    if (iss >> part) {
        parts.push_back(part);
    }
    
    // Rest is treated as a single string (for SQL queries)
    std::string remainder;
    if (std::getline(iss, remainder)) {
        // Trim leading whitespace
        size_t start = remainder.find_first_not_of(" \t");
        if (start != std::string::npos) {
            remainder = remainder.substr(start);
        }
        
        // For AUTH command, split username and password
        if (!parts.empty() && parts[0] == CMD_AUTH) {
            std::istringstream auth_iss(remainder);
            std::string username, password;
            if (auth_iss >> username >> password) {
                parts.push_back(username);
                parts.push_back(password);
            }
        } else if (!remainder.empty()) {
            parts.push_back(remainder);
        }
    }
    
    return parts;
}

std::string SimpleProtocolHandler::JoinStrings(const std::vector<std::string>& strings, 
                                              const std::string& delimiter) const {
    if (strings.empty()) {
        return "";
    }
    
    std::stringstream ss;
    for (size_t i = 0; i < strings.size(); ++i) {
        if (i > 0) {
            ss << delimiter;
        }
        ss << strings[i];
    }
    
    return ss.str();
}

std::string SimpleProtocolHandler::FormatTuple(const Tuple& tuple) const {
    std::stringstream ss;
    const auto& values = tuple.GetValues();
    
    for (size_t i = 0; i < values.size(); ++i) {
        if (i > 0) {
            ss << ",";
        }
        ss << FormatValue(values[i]);
    }
    
    return ss.str();
}

std::string SimpleProtocolHandler::FormatValue(const Value& value) const {
    std::stringstream ss;
    
    if (std::holds_alternative<bool>(value)) {
        ss << (std::get<bool>(value) ? "true" : "false");
    } else if (std::holds_alternative<int8_t>(value)) {
        ss << static_cast<int>(std::get<int8_t>(value));
    } else if (std::holds_alternative<int16_t>(value)) {
        ss << std::get<int16_t>(value);
    } else if (std::holds_alternative<int32_t>(value)) {
        ss << std::get<int32_t>(value);
    } else if (std::holds_alternative<int64_t>(value)) {
        ss << std::get<int64_t>(value);
    } else if (std::holds_alternative<float>(value)) {
        ss << std::fixed << std::setprecision(2) << std::get<float>(value);
    } else if (std::holds_alternative<double>(value)) {
        ss << std::fixed << std::setprecision(2) << std::get<double>(value);
    } else if (std::holds_alternative<std::string>(value)) {
        // Escape special characters in strings
        ss << "\"" << EscapeString(std::get<std::string>(value)) << "\"";
    } else {
        ss << "NULL";
    }
    
    return ss.str();
}

MessageType SimpleProtocolHandler::ParseMessageType(const std::string& command) const {
    if (command == CMD_AUTH) {
        return MessageType::AUTHENTICATION;
    } else if (command == CMD_QUERY) {
        return MessageType::QUERY;
    } else if (command == CMD_COMMAND) {
        return MessageType::COMMAND;
    } else if (command == CMD_CLOSE) {
        return MessageType::CLOSE;
    } else if (command == CMD_PING) {
        return MessageType::HEARTBEAT;
    } else {
        // 默认当作查询处理
        return MessageType::QUERY;
    }
}

bool SimpleProtocolHandler::HandleCommand(Connection* connection, const Message& message) {
    if (!connection || message.content.empty()) {
        return false;
    }
    
    std::cout << "[DEBUG] HandleCommand: Processing command: " << message.content << std::endl;
    
    std::string command = message.content;
    std::transform(command.begin(), command.end(), command.begin(), ::toupper);
    
    if (command == "SHOW TABLES") {
        return ProcessShowTablesCommand(connection);
    } else if (command.substr(0, 8) == "DESCRIBE" && message.parameters.size() > 0) {
        return ProcessDescribeCommand(connection, message.parameters[0]);
    } else if (command == "STATUS") {
        return ProcessStatusCommand(connection);
    } else if (command == "HELP") {
        return ProcessHelpCommand(connection);
    } else {
        std::string error = FormatError("Unknown command: " + command);
        connection->SendData(error);
        return false;
    }
}

bool SimpleProtocolHandler::HandlePing(Connection* connection) {
    connection->SendData(std::string(RESP_PONG) + "\n");
    return true;
}

bool SimpleProtocolHandler::ProcessShowTablesCommand(Connection* connection) {
    std::cout << "[DEBUG] ProcessShowTablesCommand: Executing SHOW TABLES" << std::endl;
    
    if (!connection || !connection->GetSession()) {
        std::string error = FormatError("Invalid session for SHOW TABLES");
        connection->SendData(error);
        return false;
    }
    
    Session* session = connection->GetSession();
    
    // 直接调用QueryProcessor，避免通过ExecuteQueryOnSession的重复调用
    if (!session->GetQueryProcessor()) {
        std::string error = FormatError("Query processor not available");
        connection->SendData(error);
        return false;
    }
    
    try {
        QueryResult result = session->GetQueryProcessor()->ProcessQuery(session, "SHOW TABLES");
        
        // Format and send response
        std::string response;
        if (result.success) {
            response = FormatQueryResult(result);
        } else {
            response = FormatError("Failed to retrieve table list: " + result.error_message);
        }
        connection->SendData(response);
        return result.success;
    } catch (const std::exception& e) {
        std::string error = FormatError("SHOW TABLES failed: " + std::string(e.what()));
        connection->SendData(error);
        return false;
    }
}

bool SimpleProtocolHandler::ProcessDescribeCommand(Connection* connection, const std::string& table_name) {
    // Execute DESCRIBE TABLE query
    std::string query = "DESCRIBE " + table_name + ";";
    QueryResult result = ExecuteQueryOnSession(connection->GetSession(), query);
    
    // Format and send response
    std::string response;
    if (result.success) {
        response = FormatQueryResult(result);
    } else {
        response = FormatError("Failed to describe table: " + table_name);
    }
    
    connection->SendData(response);
    return result.success;
}

bool SimpleProtocolHandler::ProcessStatusCommand(Connection* connection) {
    std::stringstream ss;
    ss << RESP_OK << " Server Status\n";
    ss << "Protocol: " << GetProtocolName() << " " << GetProtocolVersion() << "\n";
    ss << "Connection: " << connection->GetClientAddress() << "\n";
    ss << "Session ID: " << (connection->GetSession() ? 
                           connection->GetSession()->GetSessionId() : 
                           "N/A") << "\n";
    ss << "Authenticated: " << (connection->IsAuthenticated() ? "Yes" : "No") << "\n";
    
    connection->SendData(ss.str());
    return true;
}

bool SimpleProtocolHandler::ProcessHelpCommand(Connection* connection) {
    std::stringstream ss;
    ss << RESP_OK << " Available Commands:\n";
    ss << "AUTH <username> <password> - Authenticate to the server\n";
    ss << "QUERY <sql_statement> - Execute SQL query\n";
    ss << "CMD <command> [args] - Execute server command\n";
    ss << "  SHOW TABLES - List all tables\n";
    ss << "  DESCRIBE <table> - Show table structure\n";
    ss << "  STATUS - Show connection status\n";
    ss << "  HELP - Show this help message\n";
    ss << "PING - Test connection\n";
    ss << "CLOSE - Close connection\n";
    
    connection->SendData(ss.str());
    return true;
}

} // namespace SimpleRDBMS