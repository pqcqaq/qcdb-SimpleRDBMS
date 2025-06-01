#pragma once

#include <iostream>
#include <string>
#include <sstream>
#include <cstdlib>
#include <iomanip>

// Platform-specific includes for stack trace
#ifdef __linux__
#include <execinfo.h>
#include <cxxabi.h>
#include <dlfcn.h>
#endif

namespace SimpleRDBMS {

// Debug levels
enum class DebugLevel {
    NONE = 0,
    ERROR = 1,
    WARN = 2,
    INFO = 3,
    DEBUG = 4,
    TRACE = 5
};

// Global debug level (can be set via environment variable SIMPLEDB_DEBUG_LEVEL)
inline DebugLevel GetDebugLevel() {
    static DebugLevel level = []() {
        const char* env = std::getenv("SIMPLEDB_DEBUG_LEVEL");
        if (env) {
            int val = std::atoi(env);
            return static_cast<DebugLevel>(val);
        }
        return DebugLevel::WARN;
    }();
    return level;
}

// Color codes for terminal output
#define DEBUG_COLOR_RED     "\033[0;31m"
#define DEBUG_COLOR_GREEN   "\033[0;32m"
#define DEBUG_COLOR_YELLOW  "\033[0;33m"
#define DEBUG_COLOR_BLUE    "\033[0;34m"
#define DEBUG_COLOR_MAGENTA "\033[0;35m"
#define DEBUG_COLOR_CYAN    "\033[0;36m"
#define DEBUG_COLOR_RESET   "\033[0m"

// Debug macros
#define DEBUG_LOG(level, msg) do { \
    if (static_cast<int>(SimpleRDBMS::GetDebugLevel()) >= static_cast<int>(level)) { \
        std::ostringstream oss; \
        oss << msg; \
        std::cerr << SimpleRDBMS::GetDebugPrefix(level) << " [" << __FILE__ << ":" << __LINE__ << " " << __FUNCTION__ << "] " \
                  << oss.str() << DEBUG_COLOR_RESET << std::endl; \
    } \
} while(0)

#define LOG_ERROR(msg)   DEBUG_LOG(SimpleRDBMS::DebugLevel::ERROR, msg)
#define LOG_WARN(msg)    DEBUG_LOG(SimpleRDBMS::DebugLevel::WARN, msg)
#define LOG_INFO(msg)    DEBUG_LOG(SimpleRDBMS::DebugLevel::INFO, msg)
#define LOG_DEBUG(msg)   DEBUG_LOG(SimpleRDBMS::DebugLevel::DEBUG, msg)
#define LOG_TRACE(msg)   DEBUG_LOG(SimpleRDBMS::DebugLevel::TRACE, msg)

// Get debug prefix with color
inline std::string GetDebugPrefix(DebugLevel level) {
    switch (level) {
        case DebugLevel::ERROR:
            return std::string(DEBUG_COLOR_RED) + "[ERROR]";
        case DebugLevel::WARN:
            return std::string(DEBUG_COLOR_YELLOW) + "[WARN ]";
        case DebugLevel::INFO:
            return std::string(DEBUG_COLOR_GREEN) + "[INFO ]";
        case DebugLevel::DEBUG:
            return std::string(DEBUG_COLOR_CYAN) + "[DEBUG]";
        case DebugLevel::TRACE:
            return std::string(DEBUG_COLOR_MAGENTA) + "[TRACE]";
        default:
            return "[?????]";
    }
}

// Improved stack trace utility
class Debug {
public:
    static std::string GetStackTrace(int skip = 1) {
        std::stringstream ss;
        
#ifdef __linux__
        const int max_frames = 128;
        void* buffer[max_frames];
        int nptrs = backtrace(buffer, max_frames);
        
        if (nptrs > skip) {
            ss << "Stack trace:\n";
            
            for (int i = skip; i < nptrs; i++) {
                Dl_info info;
                if (dladdr(buffer[i], &info) && info.dli_sname) {
                    // Attempt to demangle C++ symbols
                    int status;
                    char* demangled = abi::__cxa_demangle(info.dli_sname, nullptr, nullptr, &status);
                    
                    ss << "  #" << std::setw(2) << (i - skip) << " ";
                    
                    if (info.dli_fname) {
                        // Extract just the filename without path
                        std::string fname(info.dli_fname);
                        size_t pos = fname.find_last_of("/\\");
                        if (pos != std::string::npos) {
                            fname = fname.substr(pos + 1);
                        }
                        ss << fname << " ";
                    }
                    
                    if (status == 0 && demangled) {
                        ss << demangled;
                        free(demangled);
                    } else if (info.dli_sname) {
                        ss << info.dli_sname;
                    } else {
                        ss << "???";
                    }
                    
                    // Add offset
                    if (info.dli_saddr) {
                        ss << " + " << std::hex << "0x" 
                           << ((char*)buffer[i] - (char*)info.dli_saddr) << std::dec;
                    }
                    
                    ss << "\n";
                } else {
                    // Fallback to backtrace_symbols
                    char** symbols = backtrace_symbols(&buffer[i], 1);
                    if (symbols) {
                        ss << "  #" << std::setw(2) << (i - skip) << " " << symbols[0] << "\n";
                        free(symbols);
                    }
                }
            }
        } else {
            ss << "Stack trace not available\n";
        }
#else
        ss << "Stack trace not available on this platform\n";
#endif
        
        return ss.str();
    }
};

}  // namespace SimpleRDBMS