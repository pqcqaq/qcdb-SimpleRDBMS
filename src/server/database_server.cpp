#include "database_server.h"

#include <arpa/inet.h>
#include <signal.h>
#include <fcntl.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cstring>
#include <cerrno>

#include <iomanip>
#include <iostream>

#include "protocol/simple_protocol.h"

namespace SimpleRDBMS {

// Global instance for signal handling
DatabaseServer* DatabaseServer::global_instance_ = nullptr;

DatabaseServer::DatabaseServer(const ServerConfig& config)
    : config_(config),
      state_(ServerState::STOPPED),
      server_socket_(-1),
      accepting_connections_(false) {
    global_instance_ = this;
    InitializeStats();
}

DatabaseServer::~DatabaseServer() {
    Shutdown();
    global_instance_ = nullptr;
}

bool DatabaseServer::Initialize() {
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        if (state_ != ServerState::STOPPED) {
            LogError("Server is already initialized");
            return false;
        }
        state_ = ServerState::STARTING; // 直接设置状态，避免死锁
    }
    state_cv_.notify_all(); // 在锁外通知
    
    LogInfo("Initializing database server...");
    
    // Initialize logging
    if (!InitializeLogging()) {
        LogError("Failed to initialize logging");
        SetState(ServerState::ERROR);
        return false;
    }
    
    // Initialize database core components
    if (!InitializeDatabaseCore()) {
        LogError("Failed to initialize database core");
        SetState(ServerState::ERROR);
        return false;
    }
    
    // Initialize server components
    if (!InitializeServerComponents()) {
        LogError("Failed to initialize server components");
        SetState(ServerState::ERROR);
        return false;
    }
    
    // Initialize networking
    if (!InitializeNetworking()) {
        LogError("Failed to initialize networking");
        SetState(ServerState::ERROR);
        return false;
    }
    
    LogInfo("Database server initialized successfully");
    return true;
}

bool DatabaseServer::Start() {
    // 检查当前状态而不是等待
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        if (state_ != ServerState::STARTING) {
            LogError("Server not in STARTING state, current state: " + 
                    std::to_string(static_cast<int>(state_)));
            return false;
        }
    }
    
    LogInfo("Starting database server...");
    PrintStartupBanner();
    
    // Start accepting connections
    accepting_connections_ = true;
    accept_thread_ =
        std::make_unique<std::thread>(&DatabaseServer::AcceptLoop, this);
    
    SetState(ServerState::RUNNING);
    LogInfo("Database server started successfully");
    return true;
}

void DatabaseServer::Stop() {
    if (state_ != ServerState::RUNNING) {
        return;
    }

    LogInfo("Stopping database server...");
    SetState(ServerState::STOPPING);

    // Stop accepting new connections
    accepting_connections_ = false;

    // Close server socket to interrupt accept()
    if (server_socket_ >= 0) {
        shutdown(server_socket_, SHUT_RDWR);
        close(server_socket_);
        server_socket_ = -1;
    }

    // Wait for accept thread to finish
    if (accept_thread_ && accept_thread_->joinable()) {
        accept_thread_->join();
    }

    // Stop connection manager
    if (connection_manager_) {
        connection_manager_->Shutdown();
    }

    // Wait for all queries to complete
    if (query_thread_pool_) {
        query_thread_pool_->Shutdown();
    }

    if (connection_thread_pool_) {
        connection_thread_pool_->Shutdown();
    }

    SetState(ServerState::STOPPED);
    LogInfo("Database server stopped");
}

void DatabaseServer::Shutdown() {
    Stop();

    LogInfo("Shutting down database server...");

    // Cleanup components in reverse order
    CleanupServerComponents();
    CleanupDatabaseCore();
    CleanupNetworking();

    LogInfo("Database server shutdown complete");
}

bool DatabaseServer::LoadConfig(const std::string& config_file) {
    return config_.LoadFromFile(config_file);
}

bool DatabaseServer::LoadConfig(int argc, char* argv[]) {
    return config_.LoadFromArgs(argc, argv);
}

void DatabaseServer::UpdateConfig(const ServerConfig& config) {
    config_ = config;

    // Update components with new config
    if (connection_manager_) {
        connection_manager_->UpdateConfig(config);
    }

    if (query_processor_) {
        query_processor_->UpdateConfig(config);
    }
}

ServerStats DatabaseServer::GetStats() {
    std::lock_guard<std::mutex> lock(stats_mutex_);

    UpdateServerStats();

    // Calculate uptime
    auto now = std::chrono::system_clock::now();
    stats_.uptime = std::chrono::duration_cast<std::chrono::milliseconds>(
        now - stats_.start_time);

    return stats_;
}

void DatabaseServer::PrintStats() {
    auto stats = GetStats();

    std::cout << "\n=== Server Statistics ===" << std::endl;
    std::cout << "Uptime: " << stats.uptime.count() / 1000 << " seconds"
              << std::endl;
    std::cout << "Total Connections: " << stats.total_connections << std::endl;
    std::cout << "Active Connections: " << stats.active_connections
              << std::endl;
    std::cout << "Total Queries: " << stats.total_queries << std::endl;
    std::cout << "Successful Queries: " << stats.successful_queries
              << std::endl;
    std::cout << "Failed Queries: " << stats.failed_queries << std::endl;
    std::cout << "Active Transactions: " << stats.active_transactions
              << std::endl;
    std::cout << "Committed Transactions: " << stats.committed_transactions
              << std::endl;
    std::cout << "Aborted Transactions: " << stats.aborted_transactions
              << std::endl;
    std::cout << "========================" << std::endl;
}

void DatabaseServer::ResetStats() {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    InitializeStats();
}

void DatabaseServer::SetupSignalHandlers() {
    signal(SIGINT, SignalHandler);
    signal(SIGTERM, SignalHandler);
    signal(SIGHUP, SignalHandler);
    signal(SIGPIPE, SIG_IGN);  // Ignore broken pipe
}

void DatabaseServer::SignalHandler(int signal) {
    if (!global_instance_) {
        return;
    }

    switch (signal) {
        case SIGINT:
        case SIGTERM:
            global_instance_->HandleShutdownSignal();
            break;
        case SIGHUP:
            global_instance_->HandleReloadSignal();
            break;
        default:
            break;
    }
}

bool DatabaseServer::InitializeNetworking() {
    LogInfo("Initializing networking...");
    
    // Create server socket
    server_socket_ = socket(AF_INET, SOCK_STREAM, 0);
    if (server_socket_ < 0) {
        LogError("Failed to create socket: " + std::string(strerror(errno)));
        return false;
    }

    // Set socket options
    int yes = 1;
    if (setsockopt(server_socket_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0) {
        LogError("Failed to set SO_REUSEADDR: " + std::string(strerror(errno)));
        close(server_socket_);
        server_socket_ = -1;
        return false;
    }

    // Also set SO_REUSEPORT if available (Linux/BSD)
    #ifdef SO_REUSEPORT
    if (setsockopt(server_socket_, SOL_SOCKET, SO_REUSEPORT, &yes, sizeof(yes)) < 0) {
        LogInfo("Warning: Failed to set SO_REUSEPORT (not critical): " + std::string(strerror(errno)));
    }
    #endif

    // Set non-blocking mode for the server socket
    int flags = fcntl(server_socket_, F_GETFL, 0);
    if (flags >= 0) {
        fcntl(server_socket_, F_SETFL, flags | O_NONBLOCK);
    }

    // Bind socket with retry logic
    struct sockaddr_in server_addr;
    std::memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    
    // Parse host address
    if (config_.GetNetworkConfig().host == "localhost" || 
        config_.GetNetworkConfig().host == "127.0.0.1") {
        server_addr.sin_addr.s_addr = INADDR_ANY;
    } else {
        if (inet_pton(AF_INET, config_.GetNetworkConfig().host.c_str(), 
                     &server_addr.sin_addr) <= 0) {
            LogError("Invalid host address: " + config_.GetNetworkConfig().host);
            close(server_socket_);
            server_socket_ = -1;
            return false;
        }
    }
    
    int original_port = config_.GetNetworkConfig().port;
    int current_port = original_port;
    bool bound = false;
    
    // Try to bind to the specified port, then try nearby ports if needed
    for (int attempt = 0; attempt < 10 && !bound; ++attempt) {
        server_addr.sin_port = htons(current_port);
        
        if (bind(server_socket_, (struct sockaddr*)&server_addr, sizeof(server_addr)) == 0) {
            bound = true;
            if (current_port != original_port) {
                LogInfo("Original port " + std::to_string(original_port) + 
                       " was in use, bound to port " + std::to_string(current_port));
                // Update config to reflect actual port used
                const_cast<NetworkConfig&>(config_.GetNetworkConfig()).port = current_port;
            }
        } else {
            if (errno == EADDRINUSE) {
                LogInfo("Port " + std::to_string(current_port) + " is in use, trying next port...");
                current_port++;
            } else {
                LogError("Failed to bind to port " + std::to_string(current_port) + 
                        ": " + std::string(strerror(errno)));
                break;
            }
        }
    }
    
    if (!bound) {
        LogError("Failed to bind to any port starting from " + std::to_string(original_port));
        close(server_socket_);
        server_socket_ = -1;
        return false;
    }

    // Start listening
    if (listen(server_socket_, config_.GetNetworkConfig().backlog) < 0) {
        LogError("Failed to listen on socket: " + std::string(strerror(errno)));
        close(server_socket_);
        server_socket_ = -1;
        return false;
    }

    LogInfo("Server listening on " + config_.GetNetworkConfig().host + 
           ":" + std::to_string(current_port));
    return true;
}

bool DatabaseServer::InitializeDatabaseCore() {
    try {
        LogInfo("Initializing database core components...");
        
        // Initialize disk managers
        LogInfo("Creating disk managers...");
        disk_manager_ = std::make_unique<DiskManager>(
            config_.GetDatabaseConfig().database_file);
        log_disk_manager_ =
            std::make_unique<DiskManager>(config_.GetDatabaseConfig().log_file);
        
        // Initialize buffer pool
        LogInfo("Creating buffer pool...");
        replacer_ = std::make_unique<LRUReplacer>(
            config_.GetDatabaseConfig().buffer_pool_size);
        buffer_pool_manager_ = std::make_unique<BufferPoolManager>(
            config_.GetDatabaseConfig().buffer_pool_size,
            std::move(disk_manager_), std::move(replacer_));
        
        // Initialize log manager
        LogInfo("Creating log manager...");
        log_manager_ = std::make_unique<LogManager>(log_disk_manager_.get());
        
        // Initialize lock manager
        LogInfo("Creating lock manager...");
        lock_manager_ = std::make_unique<LockManager>();
        
        // Initialize transaction manager
        LogInfo("Creating transaction manager...");
        transaction_manager_ = std::make_unique<TransactionManager>(
            lock_manager_.get(), log_manager_.get());
        
        // Initialize catalog
        LogInfo("Creating catalog...");
        catalog_ = std::make_unique<Catalog>(buffer_pool_manager_.get());
        
        // Initialize execution engine
        LogInfo("Creating execution engine...");
        execution_engine_ = std::make_unique<ExecutionEngine>(
            buffer_pool_manager_.get(), catalog_.get(),
            transaction_manager_.get());
        
        // Initialize recovery manager
        LogInfo("Creating recovery manager...");
        recovery_manager_ = std::make_unique<RecoveryManager>(
            buffer_pool_manager_.get(), catalog_.get(), log_manager_.get(),
            lock_manager_.get());
        
        // Perform recovery if needed
        if (config_.GetDatabaseConfig().enable_recovery) {
            LogInfo("Performing recovery...");
            recovery_manager_->Recover();
            LogInfo("Recovery completed");
        }
        
        LogInfo("Database core initialization completed");
        return true;
    } catch (const std::exception& e) {
        LogError("Exception during database core initialization: " +
                 std::string(e.what()));
        return false;
    } catch (...) {
        LogError("Unknown exception during database core initialization");
        return false;
    }
}

bool DatabaseServer::InitializeServerComponents() {
    try {
        LogInfo("Initializing server components...");
        
        // Initialize thread pools
        LogInfo("Creating connection thread pool...");
        ThreadPoolConfig conn_pool_config;
        conn_pool_config.min_threads = config_.GetThreadConfig().io_threads;
        conn_pool_config.max_threads = config_.GetThreadConfig().io_threads * 2;
        connection_thread_pool_ =
            std::make_unique<ThreadPool>(conn_pool_config);
        if (!connection_thread_pool_->Initialize()) {
            LogError("Failed to initialize connection thread pool");
            return false;
        }
        
        LogInfo("Creating query thread pool...");
        ThreadPoolConfig query_pool_config;
        query_pool_config.min_threads = config_.GetThreadConfig().query_threads;
        query_pool_config.max_threads =
            config_.GetThreadConfig().query_threads * 2;
        query_thread_pool_ = std::make_unique<ThreadPool>(query_pool_config);
        if (!query_thread_pool_->Initialize()) {
            LogError("Failed to initialize query thread pool");
            return false;
        }
        
        // Initialize connection manager
        LogInfo("Creating connection manager...");
        connection_manager_ = std::make_unique<ConnectionManager>(config_);
        if (!connection_manager_->Initialize()) {
            LogError("Failed to initialize connection manager");
            return false;
        }
        
        // Initialize query processor
        LogInfo("Creating query processor...");
        query_processor_ = std::make_unique<QueryProcessor>(config_);
        if (!query_processor_->Initialize(execution_engine_.get(),
                                         transaction_manager_.get(),
                                         catalog_.get())) {
            LogError("Failed to initialize query processor");
            return false;
        }
        
        LogInfo("Server components initialization completed");
        return true;
    } catch (const std::exception& e) {
        LogError("Exception during server components initialization: " +
                 std::string(e.what()));
        return false;
    } catch (...) {
        LogError("Unknown exception during server components initialization");
        return false;
    }
}

bool DatabaseServer::InitializeLogging() {
    // In a real implementation, initialize logging framework here
    return true;
}

void DatabaseServer::AcceptLoop() {
    LogInfo("Starting accept loop");
    
    // Reset server socket to blocking mode for accept
    if (server_socket_ >= 0) {
        int flags = fcntl(server_socket_, F_GETFL, 0);
        if (flags >= 0) {
            fcntl(server_socket_, F_SETFL, flags & ~O_NONBLOCK);
        }
    }
    
    while (accepting_connections_ && server_socket_ >= 0) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        
        // Accept new connection
        int client_socket = accept(server_socket_, (struct sockaddr*)&client_addr, &client_len);
        
        if (client_socket < 0) {
            if (accepting_connections_) {
                if (errno == EINTR) {
                    // Interrupted by signal, continue
                    continue;
                } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    // Would block, wait a bit and retry
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                    continue;
                } else {
                    LogError("Failed to accept connection: " + std::string(strerror(errno)));
                }
            }
            continue;
        }
        
        // Handle new connection
        if (!HandleNewConnection(client_socket, client_addr)) {
            close(client_socket);
        }
    }
    LogInfo("Accept loop terminated");
}

bool DatabaseServer::HandleNewConnection(int client_socket,
                                         const sockaddr_in& client_addr) {
    // Get client address
    char client_ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, INET_ADDRSTRLEN);
    int client_port = ntohs(client_addr.sin_port);
    
    LogInfo("New connection from " + std::string(client_ip) + ":" +
            std::to_string(client_port));
    
    // Create connection
    auto connection = connection_manager_->AcceptConnection(
        client_socket, std::string(client_ip), client_port);
    if (!connection) {
        LogError("Failed to create connection");
        return false;
    }
    
    // Set protocol handler
    connection->SetProtocolHandler(std::make_unique<SimpleProtocolHandler>());
    
    // Set dependencies on session - 使用安全的方式
    if (connection->CreateSession()) {
        auto* session = connection->GetSession();
        if (session) {
            // 只有在组件存在时才设置
            if (transaction_manager_) {
                session->SetTransactionManager(transaction_manager_.get());
            }
            if (query_processor_) {
                session->SetQueryProcessor(query_processor_.get());
            }
            LogInfo("Session created and configured for connection");
        } else {
            LogError("Failed to get session after creation");
        }
    } else {
        LogError("Failed to create session for connection");
    }
    
    // Send initial greeting
    auto* handler = connection->GetProtocolHandler();
    if (handler) {
        std::string greeting = handler->FormatReadyMessage();
        ssize_t sent = connection->SendData(greeting);
        if (sent <= 0) {
            LogError("Failed to send greeting message");
        } else {
            LogInfo("Greeting sent successfully");
        }
    }
    
    // Process connection in thread pool
    if (connection_thread_pool_) {
        connection_thread_pool_->Enqueue(&DatabaseServer::ProcessConnection, this,
                                         connection);
    } else {
        // 如果线程池不可用，直接在当前线程处理
        LogInfo("Thread pool not available, processing connection directly");
        ProcessConnection(connection);
    }
    
    return true;
}

void DatabaseServer::ProcessConnection(std::shared_ptr<Connection> connection) {
    if (!connection) {
        return;
    }
    
    LogInfo("Processing connection from " + connection->GetClientAddress());
    
    // 设置连接为阻塞模式以便更好地处理I/O
    connection->SetNonBlocking(false);
    
    while (connection->IsValid() && IsRunning()) {
        try {
            // Process one request
            if (!connection->ProcessRequest()) {
                // 只有在明确需要关闭连接时才退出
                if (connection->GetState() == ConnectionState::CLOSING ||
                    connection->GetState() == ConnectionState::CLOSED) {
                    LogInfo("Connection closing normally: " + connection->GetClientAddress());
                    break;
                }
                // 其他情况下，稍等一下再继续
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                continue;
            }
        } catch (const std::exception& e) {
            LogError("Exception processing request: " + std::string(e.what()));
            // 发生异常时发送错误响应但不关闭连接
            if (connection->GetProtocolHandler()) {
                std::string error_resp = connection->GetProtocolHandler()->FormatError(
                    "Server error: " + std::string(e.what()));
                connection->SendData(error_resp);
            }
        }
    }
    
    // Remove connection
    connection_manager_->RemoveConnection(connection);
    LogInfo("Connection closed: " + connection->GetClientAddress());
}

void DatabaseServer::CleanupNetworking() {
    if (server_socket_ >= 0) {
        close(server_socket_);
        server_socket_ = -1;
    }
}

void DatabaseServer::CleanupDatabaseCore() {
    // Cleanup in reverse order of initialization
    recovery_manager_.reset();
    execution_engine_.reset();
    catalog_.reset();
    transaction_manager_.reset();
    lock_manager_.reset();
    log_manager_.reset();
    buffer_pool_manager_.reset();
    replacer_.reset();
    log_disk_manager_.reset();
    disk_manager_.reset();
}

void DatabaseServer::CleanupServerComponents() {
    query_processor_.reset();
    connection_manager_.reset();
    query_thread_pool_.reset();
    connection_thread_pool_.reset();
}

void DatabaseServer::SetState(ServerState new_state) {
    std::lock_guard<std::mutex> lock(state_mutex_);
    state_ = new_state;
    state_cv_.notify_all();
}

bool DatabaseServer::WaitForState(ServerState expected_state,
                                  std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(state_mutex_);
    return state_cv_.wait_for(lock, timeout, [this, expected_state] {
        return state_ == expected_state;
    });
}

void DatabaseServer::UpdateServerStats() {
    if (!connection_manager_) return;

    auto conn_stats = connection_manager_->GetStats();
    stats_.total_connections = conn_stats.total_connections;
    stats_.active_connections = conn_stats.active_connections;

    // Other stats would be updated from query processor, transaction manager,
    // etc.
}

void DatabaseServer::InitializeStats() {
    stats_.start_time = std::chrono::system_clock::now();
    stats_.uptime = std::chrono::milliseconds(0);
    stats_.total_connections = 0;
    stats_.active_connections = 0;
    stats_.total_queries = 0;
    stats_.successful_queries = 0;
    stats_.failed_queries = 0;
    stats_.active_transactions = 0;
    stats_.committed_transactions = 0;
    stats_.aborted_transactions = 0;
    stats_.cpu_usage = 0.0;
    stats_.memory_usage = 0;
    stats_.disk_usage = 0;
}

void DatabaseServer::HandleError(const std::string& error_message) {
    LogError(error_message);
    SetState(ServerState::ERROR);
}

void DatabaseServer::LogError(const std::string& error_message) const {
    std::cerr << "[ERROR] " << error_message << std::endl;
}

void DatabaseServer::LogInfo(const std::string& message) const {
    std::cout << "[INFO] " << message << std::endl;
}

void DatabaseServer::HandleShutdownSignal() {
    LogInfo("Received shutdown signal");
    Stop();
}

void DatabaseServer::HandleReloadSignal() {
    LogInfo("Received reload signal");
    // In a real implementation, reload configuration
}

std::string DatabaseServer::GetServerInfo() const {
    std::stringstream ss;
    ss << "SimpleRDBMS Server v1.0\n";
    ss << "Host: " << config_.GetNetworkConfig().host << "\n";
    ss << "Port: " << config_.GetNetworkConfig().port << "\n";
    ss << "Database: " << config_.GetDatabaseConfig().database_file << "\n";
    ss << "Worker Threads: " << config_.GetThreadConfig().worker_threads << "\n";
    return ss.str();
}

bool DatabaseServer::ValidateConfig() const { return config_.Validate(); }

void DatabaseServer::PrintStartupBanner() const {
    std::cout << "\n";
    std::cout << "================================================\n";
    std::cout << "       SimpleRDBMS Database Server v1.0         \n";
    std::cout << "================================================\n";
    std::cout << GetServerInfo();
    std::cout << "================================================\n";
    std::cout << "\n";
}

} 