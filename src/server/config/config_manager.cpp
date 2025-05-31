#include "config_manager.h"
#include <iostream>
#include <fstream>
#include <cstdlib>

namespace SimpleRDBMS {

ConfigManager& ConfigManager::Instance() {
    static ConfigManager instance;
    return instance;
}

bool ConfigManager::LoadConfig(const std::string& config_file) {
    config_file_ = config_file;
    return server_config_.LoadFromFile(config_file);
}

bool ConfigManager::LoadConfig(int argc, char* argv[]) {
    return server_config_.LoadFromArgs(argc, argv);
}

void ConfigManager::LoadFromEnvironment() {
    server_config_.LoadFromEnv();
}

bool ConfigManager::ValidateConfig() const {
    return server_config_.Validate();
}

void ConfigManager::ReloadConfig() {
    if (!config_file_.empty()) {
        LoadConfig(config_file_);
    }
    LoadFromEnvironment();
}

void ConfigManager::UpdateNetworkConfig(const NetworkConfig& config) {
    auto& net_config = server_config_.GetNetworkConfig();
    std::string old_host = net_config.host;
    int old_port = net_config.port;
    
    net_config = config;
    
    // Notify changes
    NotifyConfigChange("network.host", old_host, config.host);
    NotifyConfigChange("network.port", std::to_string(old_port), std::to_string(config.port));
}

void ConfigManager::UpdateThreadConfig(const ThreadConfig& config) {
    auto& thread_config = server_config_.GetThreadConfig();
    int old_workers = thread_config.worker_threads;
    
    thread_config = config;
    
    NotifyConfigChange("thread.worker_threads", std::to_string(old_workers), 
                      std::to_string(config.worker_threads));
}

void ConfigManager::UpdateDatabaseConfig(const DatabaseConfig& config) {
    auto& db_config = server_config_.GetDatabaseConfig();
    std::string old_file = db_config.database_file;
    
    db_config = config;
    
    NotifyConfigChange("database.file", old_file, config.database_file);
}

void ConfigManager::UpdateQueryConfig(const QueryConfig& config) {
    auto& query_config = server_config_.GetQueryConfig();
    auto old_timeout = query_config.query_timeout;
    
    query_config = config;
    
    NotifyConfigChange("query.timeout", std::to_string(old_timeout.count()), 
                      std::to_string(config.query_timeout.count()));
}

bool ConfigManager::SaveConfig(const std::string& config_file) const {
    std::string file_to_use = config_file.empty() ? config_file_ : config_file;
    if (file_to_use.empty()) {
        return false;
    }
    
    std::ofstream file(file_to_use);
    if (!file.is_open()) {
        return false;
    }
    
    const auto& net_config = server_config_.GetNetworkConfig();
    const auto& thread_config = server_config_.GetThreadConfig();
    const auto& db_config = server_config_.GetDatabaseConfig();
    const auto& query_config = server_config_.GetQueryConfig();
    
    file << "# SimpleRDBMS Configuration File\n\n";
    
    file << "# Network Configuration\n";
    file << "network.host=" << net_config.host << "\n";
    file << "network.port=" << net_config.port << "\n";
    file << "network.max_connections=" << net_config.max_connections << "\n";
    file << "network.connection_timeout=" << net_config.connection_timeout.count() << "\n\n";
    
    file << "# Thread Configuration\n";
    file << "thread.worker_threads=" << thread_config.worker_threads << "\n";
    file << "thread.query_threads=" << thread_config.query_threads << "\n";
    file << "thread.max_queue_size=" << thread_config.max_queue_size << "\n\n";
    
    file << "# Database Configuration\n";
    file << "database.file=" << db_config.database_file << "\n";
    file << "database.log_file=" << db_config.log_file << "\n";
    file << "database.buffer_pool_size=" << db_config.buffer_pool_size << "\n\n";
    
    file << "# Query Configuration\n";
    file << "query.timeout=" << query_config.query_timeout.count() << "\n";
    file << "query.max_length=" << query_config.max_query_length << "\n";
    
    return true;
}

void ConfigManager::RegisterChangeCallback(const std::string& key, ConfigChangeCallback callback) {
    change_callbacks_[key] = callback;
}

void ConfigManager::UnregisterChangeCallback(const std::string& key) {
    change_callbacks_.erase(key);
}

void ConfigManager::NotifyConfigChange(const std::string& key, const std::string& old_value, const std::string& new_value) {
    auto it = change_callbacks_.find(key);
    if (it != change_callbacks_.end() && it->second) {
        it->second(key, old_value, new_value);
    }
}

} // namespace SimpleRDBMS