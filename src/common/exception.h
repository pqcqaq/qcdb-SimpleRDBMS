/*
 * 文件: exception.h
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述: 异常处理系统，提供分层次的异常类型和增强的错误追踪功能
 */

#pragma once

#include <exception>
#include <sstream>
#include <string>

#include "common/debug.h"

namespace SimpleRDBMS {

// ==================== 基础异常类 ====================
// 所有自定义异常的基类，继承自标准库的exception
// 提供了两种构造方式：简单消息和详细位置信息
class Exception : public std::exception {
   public:
    // 简单构造函数，只接受错误消息
    // 适用于不需要详细位置信息的场景
    explicit Exception(const std::string& message) : message_(message) {}

    // 增强构造函数，包含文件名、行号、函数名等详细信息
    // 这样当异常发生时，可以精确定位到出错的代码位置
    Exception(const std::string& message, const std::string& file, int line,
              const std::string& func) {
        std::ostringstream oss;
        oss << message << "\n  at " << file << ":" << line << " in " << func;
        message_ = oss.str();
        // 同时记录到日志系统中，方便调试
        LOG_ERROR("Exception created: " << message_);
    }

    // 重写标准库的what()方法，返回错误消息
    const char* what() const noexcept override { return message_.c_str(); }

   protected:
    std::string message_;  // 存储完整的错误消息
};

// ==================== 缓冲池异常类 ====================
// 专门处理缓冲池相关的错误，比如页面固定失败、LRU替换失败等
class BufferPoolException : public Exception {
   public:
    // 自动在错误消息前加上"BufferPool:"前缀，方便识别错误来源
    explicit BufferPoolException(const std::string& message)
        : Exception("BufferPool: " + message) {}

    // 带位置信息的构造函数
    BufferPoolException(const std::string& message, const std::string& file,
                        int line, const std::string& func)
        : Exception("BufferPool: " + message, file, line, func) {}
};

// ==================== 存储异常类 ====================
// 处理磁盘I/O、页面分配、文件操作等存储层面的错误
class StorageException : public Exception {
   public:
    explicit StorageException(const std::string& message)
        : Exception("Storage: " + message) {}

    StorageException(const std::string& message, const std::string& file,
                     int line, const std::string& func)
        : Exception("Storage: " + message, file, line, func) {}
};

// ==================== 事务异常类 ====================
// 处理事务管理相关的错误，如死锁、隔离级别冲突、日志记录失败等
class TransactionException : public Exception {
   public:
    explicit TransactionException(const std::string& message)
        : Exception("Transaction: " + message) {}

    TransactionException(const std::string& message, const std::string& file,
                         int line, const std::string& func)
        : Exception("Transaction: " + message, file, line, func) {}
};

// ==================== 执行异常类 ====================
// 处理SQL执行过程中的错误，如语法错误、类型不匹配、表不存在等
class ExecutionException : public Exception {
   public:
    explicit ExecutionException(const std::string& message)
        : Exception("Execution: " + message) {}

    ExecutionException(const std::string& message, const std::string& file,
                       int line, const std::string& func)
        : Exception("Execution: " + message, file, line, func) {}
};

// ==================== 增强的异常抛出宏 ====================
// 这个宏大大简化了异常抛出的代码，自动添加了以下功能：
// 1. 支持流式语法构建错误消息
// 2. 自动记录错误日志
// 3. 自动获取堆栈追踪信息
// 4. 自动填充文件名、行号、函数名
#define THROW_EXCEPTION_WITH_TRACE(ExceptionClass, msg)                        \
    do {                                                                       \
        std::ostringstream oss;                                                \
        oss << msg;                                                            \
        std::string message = oss.str();                                       \
        LOG_ERROR("Throwing " << #ExceptionClass << ": " << message);          \
        std::string stack_trace = SimpleRDBMS::Debug::GetStackTrace();         \
        throw ExceptionClass(message + "\n" + stack_trace, __FILE__, __LINE__, \
                             __FUNCTION__);                                    \
    } while (0)

// ==================== 便捷的异常抛出宏 ====================
// 为每种异常类型提供专门的宏，使用起来更方便
// 用法示例：THROW_STORAGE_EXCEPTION("Failed to read page " << page_id);
#define THROW_STORAGE_EXCEPTION(msg) \
    THROW_EXCEPTION_WITH_TRACE(StorageException, msg)
#define THROW_BUFFER_EXCEPTION(msg) \
    THROW_EXCEPTION_WITH_TRACE(BufferPoolException, msg)
#define THROW_EXECUTION_EXCEPTION(msg) \
    THROW_EXCEPTION_WITH_TRACE(ExecutionException, msg)
#define THROW_TRANSACTION_EXCEPTION(msg) \
    THROW_EXCEPTION_WITH_TRACE(TransactionException, msg)
#define THROW_EXCEPTION(msg) THROW_EXCEPTION_WITH_TRACE(Exception, msg)

}  // namespace SimpleRDBMS