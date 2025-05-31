#include "server/config/server_config.h"
#include <fstream>
#include <sstream>
#include <iostream>
#include <cstdlib>
#include <algorithm>
#include <getopt.h>

namespace SimpleRDBMS {

bool ServerConfig::LoadFromFile(const std::string& config_file) {
    std::ifstream file(config_file);
    if (!file.is_open()) {
        std::cerr << "Failed to open config file: " << config_file << std::endl;
        return false;
    }

    std::string line;
    while (std::getline(file, line)) {
        if (!ParseConfigLine(line)) {
            std::cerr << "Failed to parse config line: " << line << std::endl;
        }
    }

    file.close();
    return Validate();
}

bool ServerConfig::LoadFromArgs(int argc, char* argv[]) {
    static struct option long_options[] = {
        {"host", required_argument, 0, 'h'},
        {"port", required_argument, 0, 'p'},
        {"database", required_argument, 0, 'd'},
        {"workers", required_argument, 0, 'w'},
        {"max-connections", required_argument, 0, 'c'},
        {"config", required_argument, 0, 'f'},
        {0, 0, 0, 0}
    };

    int opt;
    int option_index = 0;
    std::string config_file;

    while ((opt = getopt_long(argc, argv, "h:p:d:w:c:f:", long_options, &option_index)) != -1) {
        switch (opt) {
            case 'h':
                network_config_.host = optarg;
                break;
            case 'p':
                network_config_.port = std::stoi(optarg);
                break;
            case 'd':
                database_config_.database_file = optarg;
                break;
            case 'w':
                thread_config_.worker_threads = std::stoi(optarg);
                break;
            case 'c':
                network_config_.max_connections = std::stoi(optarg);
                break;
            case 'f':
                config_file = optarg;
                break;
            default:
                return false;
        }
    }

    // Load from config file if specified
    if (!config_file.empty()) {
        LoadFromFile(config_file);
    }

    return Validate();
}

void ServerConfig::LoadFromEnv() {
    // Network config
    if (const char* host = std::getenv("SIMPLEDB_HOST")) {
        network_config_.host = host;
    }
    if (const char* port = std::getenv("SIMPLEDB_PORT")) {
        network_config_.port = std::stoi(port);
    }
    
    // Database config
    if (const char* db_file = std::getenv("SIMPLEDB_DATABASE")) {
        database_config_.database_file = db_file;
    }
    
    // Thread config
    if (const char* workers = std::getenv("SIMPLEDB_WORKERS")) {
        thread_config_.worker_threads = std::stoi(workers);
    }
}

bool ServerConfig::Validate() const {
    // Validate network config
    if (network_config_.port < 1 || network_config_.port > 65535) {
        std::cerr << "Invalid port number: " << network_config_.port << std::endl;
        return false;
    }
    if (network_config_.max_connections < 1) {
        std::cerr << "Invalid max connections: " << network_config_.max_connections << std::endl;
        return false;
    }
    
    // Validate thread config
    if (thread_config_.worker_threads < 1) {
        std::cerr << "Invalid worker threads: " << thread_config_.worker_threads << std::endl;
        return false;
    }
    
    // Validate database config
    if (database_config_.database_file.empty()) {
        std::cerr << "Database file not specified" << std::endl;
        return false;
    }
    
    return true;
}

void ServerConfig::Print() const {
    std::cout << "=== Server Configuration ===" << std::endl;
    std::cout << "Network:" << std::endl;
    std::cout << "  Host: " << network_config_.host << std::endl;
    std::cout << "  Port: " << network_config_.port << std::endl;
    std::cout << "  Max Connections: " << network_config_.max_connections << std::endl;
    std::cout << "  Connection Timeout: " << network_config_.connection_timeout.count() << "s" << std::endl;
    
    std::cout << "Thread Pool:" << std::endl;
    std::cout << "  Worker Threads: " << thread_config_.worker_threads << std::endl;
    std::cout << "  Query Threads: " << thread_config_.query_threads << std::endl;
    std::cout << "  I/O Threads: " << thread_config_.io_threads << std::endl;
    
    std::cout << "Database:" << std::endl;
    std::cout << "  Database File: " << database_config_.database_file << std::endl;
    std::cout << "  Log File: " << database_config_.log_file << std::endl;
    std::cout << "  Buffer Pool Size: " << database_config_.buffer_pool_size << std::endl;
    
    std::cout << "Query:" << std::endl;
    std::cout << "  Query Timeout: " << query_config_.query_timeout.count() << "s" << std::endl;
    std::cout << "  Max Query Length: " << query_config_.max_query_length << " bytes" << std::endl;
    std::cout << "=========================" << std::endl;
}

void ServerConfig::SetDefaultValues() {
    // Already initialized in header with default values
}

bool ServerConfig::ParseConfigLine(const std::string& line) {
    std::string trimmed = Trim(line);
    
    // Skip empty lines and comments
    if (trimmed.empty() || trimmed[0] == '#' || trimmed[0] == ';') {
        return true;
    }
    
    size_t eq_pos = trimmed.find('=');
    if (eq_pos == std::string::npos) {
        return false;
    }
    
    std::string key = Trim(trimmed.substr(0, eq_pos));
    std::string value = Trim(trimmed.substr(eq_pos + 1));
    
    // Remove quotes if present
    if (value.size() >= 2 && value.front() == '"' && value.back() == '"') {
        value = value.substr(1, value.size() - 2);
    }
    
    // Network config
    if (key == "network.host") {
        network_config_.host = value;
    } else if (key == "network.port") {
        network_config_.port = std::stoi(value);
    } else if (key == "network.max_connections") {
        network_config_.max_connections = std::stoi(value);
    } else if (key == "network.connection_timeout") {
        network_config_.connection_timeout = std::chrono::seconds(std::stoi(value));
    }
    // Thread config
    else if (key == "thread.worker_threads") {
        thread_config_.worker_threads = std::stoi(value);
    } else if (key == "thread.query_threads") {
        thread_config_.query_threads = std::stoi(value);
    } else if (key == "thread.max_queue_size") {
        thread_config_.max_queue_size = std::stoul(value);
    }
    // Database config
    else if (key == "database.file") {
        database_config_.database_file = value;
    } else if (key == "database.log_file") {
        database_config_.log_file = value;
    } else if (key == "database.buffer_pool_size") {
        database_config_.buffer_pool_size = std::stoul(value);
    }
    // Query config
    else if (key == "query.timeout") {
        query_config_.query_timeout = std::chrono::seconds(std::stoi(value));
    } else if (key == "query.max_length") {
        query_config_.max_query_length = std::stoul(value);
    }
    
    return true;
}

std::string ServerConfig::Trim(const std::string& str) const {
    const std::string whitespace = " \t\n\r";
    size_t start = str.find_first_not_of(whitespace);
    if (start == std::string::npos) {
        return "";
    }
    size_t end = str.find_last_not_of(whitespace);
    return str.substr(start, end - start + 1);
}

} // namespace SimpleRDBMS