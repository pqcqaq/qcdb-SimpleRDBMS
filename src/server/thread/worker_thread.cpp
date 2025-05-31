#include "worker_thread.h"
#include <iostream>

namespace SimpleRDBMS {

WorkerThread::WorkerThread(const std::string& name)
    : name_(name.empty() ? "WorkerThread" : name),
      state_(WorkerThreadState::CREATED),
      has_task_(false),
      idle_timeout_(std::chrono::seconds(60)) {
    ResetStats();
}

WorkerThread::~WorkerThread() {
    Stop();
    Join();
}

bool WorkerThread::Start() {
    if (state_ != WorkerThreadState::CREATED) {
        return false;
    }
    
    try {
        thread_ = std::make_unique<std::thread>(&WorkerThread::WorkerLoop, this);
        SetState(WorkerThreadState::RUNNING);
        
        // Update start time
        {
            std::lock_guard<std::mutex> lock(stats_mutex_);
            stats_.start_time = std::chrono::system_clock::now();
        }
        
        return true;
    } catch (const std::exception& e) {
        std::cerr << "[WorkerThread:" << name_ << "] Failed to start: " << e.what() << std::endl;
        return false;
    }
}

void WorkerThread::Stop() {
    if (state_ == WorkerThreadState::STOPPING || state_ == WorkerThreadState::STOPPED) {
        return;
    }
    
    SetState(WorkerThreadState::STOPPING);
    
    // Wake up the worker thread
    {
        std::lock_guard<std::mutex> lock(task_mutex_);
        task_cv_.notify_all();
    }
}

void WorkerThread::Join() {
    if (thread_ && thread_->joinable()) {
        thread_->join();
    }
    SetState(WorkerThreadState::STOPPED);
}

bool WorkerThread::ExecuteTask(TaskFunction task) {
    if (!IsRunning() || !task) {
        return false;
    }
    
    {
        std::unique_lock<std::mutex> lock(task_mutex_);
        
        // Wait if there's already a task
        if (has_task_.load()) {
            return false; // Can't accept new task
        }
        
        current_task_ = task;
        has_task_ = true;
    }
    
    task_cv_.notify_one();
    return true;
}

std::thread::id WorkerThread::GetThreadId() const {
    if (thread_) {
        return thread_->get_id();
    }
    return std::thread::id{};
}

WorkerThreadStats WorkerThread::GetStats() const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    auto stats = stats_;
    
    if (stats.tasks_processed > 0) {
        stats.average_execution_time = std::chrono::milliseconds(
            stats.total_execution_time.count() / stats.tasks_processed);
    }
    
    return stats;
}

void WorkerThread::ResetStats() {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    stats_ = WorkerThreadStats{};
    stats_.start_time = std::chrono::system_clock::now();
}

bool WorkerThread::IsHealthy() const {
    // Consider thread healthy if it's running and not stuck
    return IsRunning() && GetIdleTime() < idle_timeout_;
}

std::chrono::milliseconds WorkerThread::GetIdleTime() const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    auto now = std::chrono::system_clock::now();
    
    if (stats_.last_task_time.time_since_epoch().count() == 0) {
        // No tasks executed yet
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            now - stats_.start_time);
    }
    
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        now - stats_.last_task_time);
}

void WorkerThread::WorkerLoop() {
    SetState(WorkerThreadState::IDLE);
    
    while (state_ != WorkerThreadState::STOPPING) {
        TaskFunction task_to_execute;
        
        // Wait for task or timeout
        {
            std::unique_lock<std::mutex> lock(task_mutex_);
            
            task_cv_.wait_for(lock, idle_timeout_, [this] {
                return has_task_.load() || state_ == WorkerThreadState::STOPPING;
            });
            
            if (state_ == WorkerThreadState::STOPPING) {
                break;
            }
            
            if (has_task_.load()) {
                task_to_execute = current_task_;
                current_task_ = nullptr;
                has_task_ = false;
                SetState(WorkerThreadState::BUSY);
            }
        }
        
        // Execute task if we have one
        if (task_to_execute) {
            auto start_time = std::chrono::high_resolution_clock::now();
            bool success = true;
            
            try {
                task_to_execute();
            } catch (const std::exception& e) {
                std::cerr << "[WorkerThread:" << name_ << "] Task execution failed: " 
                         << e.what() << std::endl;
                success = false;
            } catch (...) {
                std::cerr << "[WorkerThread:" << name_ << "] Task execution failed with unknown exception" << std::endl;
                success = false;
            }
            
            auto end_time = std::chrono::high_resolution_clock::now();
            auto execution_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                end_time - start_time);
            
            UpdateTaskStats(success, execution_time);
            SetState(WorkerThreadState::IDLE);
        }
    }
    
    SetState(WorkerThreadState::STOPPED);
}

void WorkerThread::SetState(WorkerThreadState state) {
    state_ = state;
}

void WorkerThread::UpdateTaskStats(bool success, std::chrono::milliseconds execution_time) {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    
    stats_.tasks_processed++;
    if (!success) {
        stats_.tasks_failed++;
    }
    
    stats_.total_execution_time += execution_time;
    stats_.last_task_time = std::chrono::system_clock::now();
}

} // namespace SimpleRDBMS