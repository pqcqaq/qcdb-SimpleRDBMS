// namespace SimpleRDBMS
#include <algorithm>
#include <sstream>
#include <iostream>

#include "server/connection/connection.h"
#include "server/connection/session.h"
#include "server/protocol/simple_protocol.h"

namespace SimpleRDBMS {

std::string ProtocolHandler::EscapeString(const std::string& str) const {
    std::string result;
    result.reserve(str.length() * 2);

    for (char ch : str) {
        switch (ch) {
            case '\n':
                result += "\\n";
                break;
            case '\r':
                result += "\\r";
                break;
            case '\t':
                result += "\\t";
                break;
            case '\\':
                result += "\\\\";
                break;
            case '"':
                result += "\\\"";
                break;
            default:
                result += ch;
                break;
        }
    }

    return result;
}

std::string ProtocolHandler::UnescapeString(const std::string& str) const {
    std::string result;
    result.reserve(str.length());

    bool escape = false;
    for (char ch : str) {
        if (escape) {
            switch (ch) {
                case 'n':
                    result += '\n';
                    break;
                case 'r':
                    result += '\r';
                    break;
                case 't':
                    result += '\t';
                    break;
                case '\\':
                    result += '\\';
                    break;
                case '"':
                    result += '"';
                    break;
                default:
                    result += ch;
                    break;
            }
            escape = false;
        } else if (ch == '\\') {
            escape = true;
        } else {
            result += ch;
        }
    }

    return result;
}

bool ProtocolHandler::ValidateMessage(const Message& message) const {
    if (message.type == MessageType::UNKNOWN) {
        return false;
    }

    if (message.content.empty() && message.type != MessageType::HEARTBEAT &&
        message.type != MessageType::CLOSE) {
        return false;
    }

    return true;
}

QueryResult ProtocolHandler::ExecuteQueryOnSession(Session* session, const std::string& query) {
    QueryResult result;
    if (!session) {
        result.success = false;
        result.error_message = "No session available";
        return result;
    }
    
    std::cout << "[DEBUG] ExecuteQueryOnSession: Starting query execution" << std::endl;
    
    auto start_time = std::chrono::high_resolution_clock::now();
    
    // Execute query
    std::vector<Tuple> result_set;
    try {
        result.success = session->ExecuteQuery(query, &result_set);
        std::cout << "[DEBUG] ExecuteQueryOnSession: Session ExecuteQuery returned: " << result.success << std::endl;
        
        if (result.success) {
            result.result_set = std::move(result_set);
            result.affected_rows = result.result_set.size();
        } else {
            result.error_message = "Query execution failed";
        }
    } catch (const std::exception& e) {
        std::cout << "[ERROR] ExecuteQueryOnSession: Exception: " << e.what() << std::endl;
        result.success = false;
        result.error_message = "Query execution failed: " + std::string(e.what());
    }
    
    auto end_time = std::chrono::high_resolution_clock::now();
    result.execution_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    
    std::cout << "[DEBUG] ExecuteQueryOnSession: Completed in " << result.execution_time.count() << "ms" << std::endl;
    
    return result;
}

// ProtocolHandlerFactory implementation
std::unique_ptr<ProtocolHandler> ProtocolHandlerFactory::CreateHandler(
    ProtocolType type) {
    switch (type) {
        case ProtocolType::SIMPLE_TEXT:
            return std::make_unique<SimpleProtocolHandler>();
        case ProtocolType::POSTGRESQL:
            // Not implemented yet
            return nullptr;
        case ProtocolType::MYSQL:
            // Not implemented yet
            return nullptr;
        default:
            return nullptr;
    }
}

ProtocolHandlerFactory::ProtocolType ProtocolHandlerFactory::DetectProtocol(
    const std::string& initial_data) {
    // Simple detection based on initial bytes
    if (initial_data.empty()) {
        return ProtocolType::SIMPLE_TEXT;
    }

    // PostgreSQL protocol starts with specific bytes
    if (initial_data.size() >= 8) {
        // Check for PostgreSQL startup message
        // This is a simplified check
        if (initial_data[4] == 0x00 && initial_data[5] == 0x03) {
            return ProtocolType::POSTGRESQL;
        }
    }

    // MySQL protocol detection would go here

    // Default to simple text protocol
    return ProtocolType::SIMPLE_TEXT;
}

}  // namespace SimpleRDBMS