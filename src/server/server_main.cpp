#include <signal.h>

#include <iostream>

#include "config/config_manager.h"
#include "database_server.h"

using namespace SimpleRDBMS;

int main(int argc, char* argv[]) {
    setenv("SIMPLEDB_DEBUG_LEVEL", "4", 1);  // DEBUG级别
    try {
        // Setup signal handlers
        DatabaseServer::SetupSignalHandlers();

        // Load configuration
        ServerConfig config;
        if (argc > 1) {
            // Load from command line arguments
            if (!config.LoadFromArgs(argc, argv)) {
                std::cerr << "Failed to load configuration from arguments"
                          << std::endl;
                return 1;
            }
        } else {
            // Load from default config file or use defaults
            config.LoadFromEnv();
        }

        // Validate configuration
        if (!config.Validate()) {
            std::cerr << "Invalid configuration" << std::endl;
            return 1;
        }

        // Print configuration
        config.Print();

        // Create and start server
        DatabaseServer server(config);

        if (!server.Initialize()) {
            std::cerr << "Failed to initialize server" << std::endl;
            return 1;
        }

        if (!server.Start()) {
            std::cerr << "Failed to start server" << std::endl;
            return 1;
        }

        std::cout << "Server started successfully. Press Ctrl+C to stop."
                  << std::endl;

        // Wait for shutdown signal
        while (server.IsRunning()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        std::cout << "Server stopping..." << std::endl;
        server.Shutdown();
        std::cout << "Server stopped." << std::endl;

        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Server error: " << e.what() << std::endl;
        return 1;
    } catch (...) {
        std::cerr << "Unknown server error" << std::endl;
        return 1;
    }
}