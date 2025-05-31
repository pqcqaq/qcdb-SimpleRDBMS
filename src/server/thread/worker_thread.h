#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <condition_variable>

namespace SimpleRDBMS {

enum class WorkerThreadState {
    CREATED = 0,
    RUNNING,
    IDLE,
    BUSY,
    STOPPING,
    STOPPED
};

struct WorkerThreadStats {
    size_t tasks_processed;
    size_t tasks_failed;
    std::chrono::milliseconds total_execution_time;
    std::chrono::milliseconds average_execution_time;
    std::chrono::system_clock::time_point last_task_time;
    std::chrono::system_clock::time_point start_time;
};

class WorkerThread {
public:
    using TaskFunction = std::function<void()>;
    
    explicit WorkerThread(const std::string& name = "");
    ~WorkerThread();
    
    // Thread lifecycle
    bool Start();
    void Stop();
    void Join();
    bool IsRunning() const { return state_ == WorkerThreadState::RUNNING || 
                                   state_ == WorkerThreadState::IDLE || 
                                   state_ == WorkerThreadState::BUSY; }
    
    // Task execution
    bool ExecuteTask(TaskFunction task);
    void SetIdleTimeout(std::chrono::seconds timeout) { idle_timeout_ = timeout; }
    
    // Thread information
    std::string GetName() const { return name_; }
    std::thread::id GetThreadId() const;
    WorkerThreadState GetState() const { return state_; }
    
    // Statistics
    WorkerThreadStats GetStats() const;
    void ResetStats();
    
    // Health checking
    bool IsHealthy() const;
    std::chrono::milliseconds GetIdleTime() const;
    
private:
    std::string name_;
    std::unique_ptr<std::thread> thread_;
    std::atomic<WorkerThreadState> state_;
    
    // Task execution
    TaskFunction current_task_;
    std::mutex task_mutex_;
    std::condition_variable task_cv_;
    std::atomic<bool> has_task_;
    
    // Configuration
    std::chrono::seconds idle_timeout_;
    
    // Statistics
    mutable std::mutex stats_mutex_;
    WorkerThreadStats stats_;
    
    // Thread main loop
    void WorkerLoop();
    
    // State management
    void SetState(WorkerThreadState state);
    
    // Statistics helpers
    void UpdateTaskStats(bool success, std::chrono::milliseconds execution_time);
};

} // namespace SimpleRDBMS