#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include "server_config.h"
#include <functional>

namespace SimpleRDBMS {

class ConfigManager {
public:
    static ConfigManager& Instance();
    
    // Configuration loading
    bool LoadConfig(const std::string& config_file);
    bool LoadConfig(int argc, char* argv[]);
    void LoadFromEnvironment();
    
    // Configuration access
    const ServerConfig& GetServerConfig() const { return server_config_; }
    ServerConfig& GetServerConfig() { return server_config_; }
    
    // Specific config getters
    const NetworkConfig& GetNetworkConfig() const { return server_config_.GetNetworkConfig(); }
    const ThreadConfig& GetThreadConfig() const { return server_config_.GetThreadConfig(); }
    const DatabaseConfig& GetDatabaseConfig() const { return server_config_.GetDatabaseConfig(); }
    const QueryConfig& GetQueryConfig() const { return server_config_.GetQueryConfig(); }
    
    // Configuration validation and updates
    bool ValidateConfig() const;
    void ReloadConfig();
    void SetConfigFile(const std::string& config_file) { config_file_ = config_file; }
    
    // Runtime configuration updates
    void UpdateNetworkConfig(const NetworkConfig& config);
    void UpdateThreadConfig(const ThreadConfig& config);
    void UpdateDatabaseConfig(const DatabaseConfig& config);
    void UpdateQueryConfig(const QueryConfig& config);
    
    // Configuration persistence
    bool SaveConfig(const std::string& config_file = "") const;
    
    // Configuration properties access
    template<typename T>
    T GetProperty(const std::string& key, const T& default_value = T{}) const;
    
    template<typename T>
    void SetProperty(const std::string& key, const T& value);
    
    // Configuration change notifications
    using ConfigChangeCallback = std::function<void(const std::string& key, const std::string& old_value, const std::string& new_value)>;
    void RegisterChangeCallback(const std::string& key, ConfigChangeCallback callback);
    void UnregisterChangeCallback(const std::string& key);

private:
    ConfigManager() = default;
    ~ConfigManager() = default;
    ConfigManager(const ConfigManager&) = delete;
    ConfigManager& operator=(const ConfigManager&) = delete;
    
    ServerConfig server_config_;
    std::string config_file_;
    std::unordered_map<std::string, std::string> properties_;
    std::unordered_map<std::string, ConfigChangeCallback> change_callbacks_;
    
    void NotifyConfigChange(const std::string& key, const std::string& old_value, const std::string& new_value);
};

// Template implementations
template<typename T>
T ConfigManager::GetProperty(const std::string& key, const T& default_value) const {
    auto it = properties_.find(key);
    if (it != properties_.end()) {
        // Convert string to T - this is a simplified implementation
        if constexpr (std::is_same_v<T, std::string>) {
            return it->second;
        } else if constexpr (std::is_same_v<T, int>) {
            return std::stoi(it->second);
        } else if constexpr (std::is_same_v<T, bool>) {
            return it->second == "true" || it->second == "1";
        } else if constexpr (std::is_same_v<T, double>) {
            return std::stod(it->second);
        }
    }
    return default_value;
}

template<typename T>
void ConfigManager::SetProperty(const std::string& key, const T& value) {
    std::string old_value;
    auto it = properties_.find(key);
    if (it != properties_.end()) {
        old_value = it->second;
    }
    
    std::string new_value;
    if constexpr (std::is_same_v<T, std::string>) {
        new_value = value;
    } else if constexpr (std::is_arithmetic_v<T>) {
        new_value = std::to_string(value);
    } else if constexpr (std::is_same_v<T, bool>) {
        new_value = value ? "true" : "false";
    }
    
    properties_[key] = new_value;
    NotifyConfigChange(key, old_value, new_value);
}

} // namespace SimpleRDBMS