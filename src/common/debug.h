/*
 * 文件: debug.h
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述: 调试工具和日志系统，提供分级日志输出、彩色终端显示和堆栈追踪功能
 */

#pragma once

#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

// 根据不同平台包含堆栈追踪相关的头文件
// Linux系统下可以获取详细的函数调用栈信息
#ifdef __linux__
#include <cxxabi.h>    // 提供C++符号解析功能
#include <dlfcn.h>     // 提供动态链接信息
#include <execinfo.h>  // 提供backtrace函数
#endif

namespace SimpleRDBMS {

// ==================== 调试级别定义 ====================
// 按照严重程度递增的顺序定义调试级别
// 级别越高，输出的信息越详细
enum class DebugLevel {
    NONE = 0,   // 不输出任何调试信息
    ERROR = 1,  // 只输出错误信息
    WARN = 2,   // 输出警告和错误
    INFO = 3,   // 输出一般信息、警告和错误
    DEBUG = 4,  // 输出调试信息及以上级别
    TRACE = 5   // 输出所有信息，包括函数调用追踪
};

// ==================== 全局调试级别管理 ====================
// 通过环境变量SIMPLEDB_DEBUG_LEVEL来控制调试输出级别
// 这样可以在不重新编译的情况下调整调试输出的详细程度
inline DebugLevel GetDebugLevel() {
    // 使用static变量确保只初始化一次，提高性能
    static DebugLevel level = []() {
        const char* env = std::getenv("SIMPLEDB_DEBUG_LEVEL");
        if (env) {
            int val = std::atoi(env);
            return static_cast<DebugLevel>(val);
        }
        // 默认级别设为WARN，既能看到重要信息又不会太吵
        return DebugLevel::WARN;
    }();
    return level;
}

// ==================== 终端颜色代码定义 ====================
// ANSI颜色代码，让终端输出更清晰易读
// 不同级别的日志用不同颜色显示，方便快速识别
#define DEBUG_COLOR_RED "\033[0;31m"      // 红色 - 错误
#define DEBUG_COLOR_GREEN "\033[0;32m"    // 绿色 - 信息
#define DEBUG_COLOR_YELLOW "\033[0;33m"   // 黄色 - 警告
#define DEBUG_COLOR_BLUE "\033[0;34m"     // 蓝色 - 调试
#define DEBUG_COLOR_MAGENTA "\033[0;35m"  // 紫色 - 追踪
#define DEBUG_COLOR_CYAN "\033[0;36m"     // 青色 - 调试
#define DEBUG_COLOR_RESET "\033[0m"       // 重置颜色

// ==================== 核心调试宏定义 ====================
// 主要的调试输出宏，支持流式语法和自动的文件名、行号、函数名输出
// 只有当前设置的调试级别大于等于指定级别时才会输出
#define DEBUG_LOG(level, msg)                                                 \
    do {                                                                      \
        if (static_cast<int>(SimpleRDBMS::GetDebugLevel()) >=                 \
            static_cast<int>(level)) {                                        \
            std::ostringstream oss;                                           \
            oss << msg;                                                       \
            std::cerr << SimpleRDBMS::GetDebugPrefix(level) << " ["           \
                      << __FILE__ << ":" << __LINE__ << " " << __FUNCTION__   \
                      << "] " << oss.str() << DEBUG_COLOR_RESET << std::endl; \
        }                                                                     \
    } while (0)

// ==================== 便捷的日志宏 ====================
// 为每个调试级别提供简化的宏，使用起来更方便
#define LOG_ERROR(msg) DEBUG_LOG(SimpleRDBMS::DebugLevel::ERROR, msg)
#define LOG_WARN(msg) DEBUG_LOG(SimpleRDBMS::DebugLevel::WARN, msg)
#define LOG_INFO(msg) DEBUG_LOG(SimpleRDBMS::DebugLevel::INFO, msg)
#define LOG_DEBUG(msg) DEBUG_LOG(SimpleRDBMS::DebugLevel::DEBUG, msg)
#define LOG_TRACE(msg) DEBUG_LOG(SimpleRDBMS::DebugLevel::TRACE, msg)

// ==================== 调试前缀格式化 ====================
// 为每个调试级别生成带颜色的前缀标签
// 让用户一眼就能看出这条日志的重要程度
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

// ==================== 高级调试工具类 ====================
// 提供堆栈追踪等高级调试功能
// 主要用于程序崩溃时的错误诊断
class Debug {
   public:
    // 获取当前函数调用栈的详细信息
    // skip参数表示跳过栈顶的几层调用（通常跳过当前函数本身）
    static std::string GetStackTrace(int skip = 1) {
        std::stringstream ss;

#ifdef __linux__
        // Linux下使用backtrace系列函数获取调用栈
        const int max_frames = 128;  // 最多追踪128层调用
        void* buffer[max_frames];

        // 获取当前的函数调用栈地址
        int nptrs = backtrace(buffer, max_frames);

        if (nptrs > skip) {
            ss << "Stack trace:\n";

            // 遍历每一层调用栈，尝试解析出函数名和文件信息
            for (int i = skip; i < nptrs; i++) {
                Dl_info info;

                // 尝试获取地址对应的符号信息
                if (dladdr(buffer[i], &info) && info.dli_sname) {
                    // 尝试解析C++的修饰符号名（demangle）
                    // C++编译器会把函数名编码，需要解码才能看懂
                    int status;
                    char* demangled = abi::__cxa_demangle(
                        info.dli_sname, nullptr, nullptr, &status);

                    ss << "  #" << std::setw(2) << (i - skip) << " ";

                    // 输出文件名（只保留文件名部分，去掉路径）
                    if (info.dli_fname) {
                        std::string fname(info.dli_fname);
                        size_t pos = fname.find_last_of("/\\");
                        if (pos != std::string::npos) {
                            fname = fname.substr(pos + 1);
                        }
                        ss << fname << " ";
                    }

                    // 输出函数名（解码后的）
                    if (status == 0 && demangled) {
                        ss << demangled;
                        free(demangled);
                    } else if (info.dli_sname) {
                        ss << info.dli_sname;
                    } else {
                        ss << "???";
                    }

                    // 输出相对于函数起始地址的偏移量
                    if (info.dli_saddr) {
                        ss << " + " << std::hex << "0x"
                           << ((char*)buffer[i] - (char*)info.dli_saddr)
                           << std::dec;
                    }

                    ss << "\n";
                } else {
                    // 如果dladdr失败，使用backtrace_symbols作为后备方案
                    char** symbols = backtrace_symbols(&buffer[i], 1);
                    if (symbols) {
                        ss << "  #" << std::setw(2) << (i - skip) << " "
                           << symbols[0] << "\n";
                        free(symbols);
                    }
                }
            }
        } else {
            ss << "Stack trace not available\n";
        }
#else
        // 非Linux平台暂时不支持堆栈追踪
        ss << "Stack trace not available on this platform\n";
#endif

        return ss.str();
    }
};

}  // namespace SimpleRDBMS