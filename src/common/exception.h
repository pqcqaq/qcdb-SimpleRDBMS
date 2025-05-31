#pragma once

#include <exception>
#include <string>
#include <sstream>
#include "common/debug.h"

namespace SimpleRDBMS {

class Exception : public std::exception {
public:
    explicit Exception(const std::string& message) : message_(message) {
    }
    
    Exception(const std::string& message, const std::string& file, int line, const std::string& func) {
        std::ostringstream oss;
        oss << message << "\n  at " << file << ":" << line << " in " << func;
        message_ = oss.str();
        LOG_ERROR("Exception created: " << message_);
    }
    
    const char* what() const noexcept override {
        return message_.c_str();
    }

protected:
    std::string message_;
};

class BufferPoolException : public Exception {
public:
    explicit BufferPoolException(const std::string& message)
        : Exception("BufferPool: " + message) {}
        
    BufferPoolException(const std::string& message, const std::string& file, int line, const std::string& func)
        : Exception("BufferPool: " + message, file, line, func) {}
};

class StorageException : public Exception {
public:
    explicit StorageException(const std::string& message)
        : Exception("Storage: " + message) {}
        
    StorageException(const std::string& message, const std::string& file, int line, const std::string& func)
        : Exception("Storage: " + message, file, line, func) {}
};

class TransactionException : public Exception {
public:
    explicit TransactionException(const std::string& message)
        : Exception("Transaction: " + message) {}
        
    TransactionException(const std::string& message, const std::string& file, int line, const std::string& func)
        : Exception("Transaction: " + message, file, line, func) {}
};

class ExecutionException : public Exception {
public:
    explicit ExecutionException(const std::string& message)
        : Exception("Execution: " + message) {}
        
    ExecutionException(const std::string& message, const std::string& file, int line, const std::string& func)
        : Exception("Execution: " + message, file, line, func) {}
};

// Enhanced exception throwing macros with stack trace
#define THROW_EXCEPTION_WITH_TRACE(ExceptionClass, msg) \
    do { \
        std::ostringstream oss; \
        oss << msg; \
        std::string message = oss.str(); \
        LOG_ERROR("Throwing " << #ExceptionClass << ": " << message); \
        std::string stack_trace = SimpleRDBMS::Debug::GetStackTrace(); \
        throw ExceptionClass(message + "\n" + stack_trace, __FILE__, __LINE__, __FUNCTION__); \
    } while(0)

#define THROW_STORAGE_EXCEPTION(msg) THROW_EXCEPTION_WITH_TRACE(StorageException, msg)
#define THROW_BUFFER_EXCEPTION(msg) THROW_EXCEPTION_WITH_TRACE(BufferPoolException, msg)
#define THROW_EXECUTION_EXCEPTION(msg) THROW_EXCEPTION_WITH_TRACE(ExecutionException, msg)
#define THROW_TRANSACTION_EXCEPTION(msg) THROW_EXCEPTION_WITH_TRACE(TransactionException, msg)
#define THROW_EXCEPTION(msg) THROW_EXCEPTION_WITH_TRACE(Exception, msg)

}  // namespace SimpleRDBMS