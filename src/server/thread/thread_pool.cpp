#include "thread_pool.h"
#include "worker_thread.h"
#include <iostream>
#include <algorithm>

namespace SimpleRDBMS {

ThreadPool::ThreadPool(const ThreadPoolConfig& config)
    : config_(config),
      active_threads_(0),
      running_(false) {
    ResetStats();
}

ThreadPool::~ThreadPool() {
    Shutdown();
}

bool ThreadPool::Initialize() {
    if (running_) {
        return true;
    }
    
    try {
        // Validate configuration
        if (config_.min_threads > config_.max_threads) {
            std::cerr << "[ThreadPool] Invalid configuration: min_threads > max_threads" << std::endl;
            return false;
        }
        
        // Start with minimum number of threads
        for (size_t i = 0; i < config_.min_threads; ++i) {
            AddWorkerThread();
        }
        
        running_ = true;
        
        std::cout << "[ThreadPool] Initialized with " << config_.min_threads 
                  << " threads (max: " << config_.max_threads << ")" << std::endl;
        return true;
        
    } catch (const std::exception& e) {
        std::cerr << "[ThreadPool] Initialization failed: " << e.what() << std::endl;
        return false;
    }
}

void ThreadPool::Shutdown() {
    if (!running_) {
        return;
    }
    
    running_ = false;
    
    // Notify all threads to stop
    condition_.notify_all();
    
    // Stop and join all worker threads
    for (auto& thread : worker_threads_) {
        if (thread && thread->joinable()) {
            thread->join();
        }
    }
    
    worker_threads_.clear();
    active_threads_ = 0;
    
    // Clear remaining tasks
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        while (!task_queue_.empty()) {
            task_queue_.pop();
        }
    }
    
    std::cout << "[ThreadPool] Shutdown complete" << std::endl;
}

void ThreadPool::SetPoolSize(size_t min_threads, size_t max_threads) {
    if (min_threads > max_threads) {
        return;
    }
    
    config_.min_threads = min_threads;
    config_.max_threads = max_threads;
    
    // Adjust current pool size
    size_t current_size = worker_threads_.size();
    if (current_size < min_threads) {
        // Add threads
        for (size_t i = current_size; i < min_threads; ++i) {
            AddWorkerThread();
        }
    } else if (current_size > max_threads) {
        // Remove excess threads
        for (size_t i = max_threads; i < current_size; ++i) {
            RemoveWorkerThread();
        }
    }
}

size_t ThreadPool::GetQueueSize() const {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    return task_queue_.size();
}

TaskStats ThreadPool::GetStats() const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    auto stats = stats_;
    
    {
        std::lock_guard<std::mutex> queue_lock(queue_mutex_);
        stats.pending_tasks = task_queue_.size();
    }
    
    if (stats.completed_tasks > 0) {
        stats.average_execution_time = std::chrono::milliseconds(
            stats.total_execution_time.count() / stats.completed_tasks);
    }
    
    return stats;
}

void ThreadPool::ResetStats() {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    stats_ = TaskStats{};
}

void ThreadPool::UpdateConfig(const ThreadPoolConfig& config) {
    config_ = config;
    SetPoolSize(config.min_threads, config.max_threads);
}

bool ThreadPool::IsHealthy() const {
    if (!running_) {
        return false;
    }
    
    // Check if we have active threads
    if (active_threads_ == 0 && config_.min_threads > 0) {
        return false;
    }
    
    // Check queue size
    size_t queue_size = GetQueueSize();
    if (queue_size > config_.max_queue_size) {
        return false;
    }
    
    return true;
}

double ThreadPool::GetUtilization() const {
    size_t total_threads = worker_threads_.size();
    if (total_threads == 0) {
        return 0.0;
    }
    
    return static_cast<double>(active_threads_) / total_threads;
}

void ThreadPool::WorkerLoop() {
    while (running_) {
        std::function<void()> task;
        
        // Get task from queue
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            
            condition_.wait(lock, [this] {
                return !task_queue_.empty() || !running_;
            });
            
            if (!running_) {
                break;
            }
            
            if (!task_queue_.empty()) {
                task = std::move(task_queue_.front());
                task_queue_.pop();
            }
        }
        
        // Execute task
        if (task) {
            active_threads_++;
            
            auto start_time = std::chrono::high_resolution_clock::now();
            bool success = true;
            
            try {
                task();
            } catch (const std::exception& e) {
                std::cerr << "[ThreadPool] Task execution failed: " << e.what() << std::endl;
                success = false;
            } catch (...) {
                std::cerr << "[ThreadPool] Task execution failed with unknown exception" << std::endl;
                success = false;
            }
            
            auto end_time = std::chrono::high_resolution_clock::now();
            auto execution_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                end_time - start_time);
            
            UpdateTaskStats(success, execution_time);
            active_threads_--;
        }
        
        // Dynamic thread management
        if (ShouldRemoveThread()) {
            break; // This thread will exit
        }
    }
}

void ThreadPool::AddWorkerThread() {
    if (!running_ || worker_threads_.size() >= config_.max_threads) {
        return;
    }
    
    try {
        auto thread = std::make_unique<std::thread>(&ThreadPool::WorkerLoop, this);
        worker_threads_.push_back(std::move(thread));
    } catch (const std::exception& e) {
        std::cerr << "[ThreadPool] Failed to add worker thread: " << e.what() << std::endl;
    }
}

void ThreadPool::RemoveWorkerThread() {
    if (worker_threads_.size() <= config_.min_threads) {
        return;
    }
    
    // Mark for removal by decreasing max threads temporarily
    // The actual removal happens when threads exit naturally
    condition_.notify_one();
}

bool ThreadPool::ShouldAddThread() const {
    if (!running_ || worker_threads_.size() >= config_.max_threads) {
        return false;
    }
    
    // Add thread if queue is growing and we have capacity
    size_t queue_size = GetQueueSize();
    size_t thread_count = worker_threads_.size();
    
    return queue_size > thread_count && thread_count < config_.max_threads;
}

bool ThreadPool::ShouldRemoveThread() const {
    if (!running_ || worker_threads_.size() <= config_.min_threads) {
        return false;
    }
    
    // Remove thread if we have too many idle threads
    size_t queue_size = GetQueueSize();
    size_t thread_count = worker_threads_.size();
    
    return queue_size == 0 && thread_count > config_.min_threads;
}

void ThreadPool::UpdateTaskStats(bool success, std::chrono::milliseconds execution_time) {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    
    stats_.total_tasks++;
    if (success) {
        stats_.completed_tasks++;
    } else {
        stats_.failed_tasks++;
    }
    
    stats_.total_execution_time += execution_time;
    stats_.last_task_time = std::chrono::system_clock::now();
}

} // namespace SimpleRDBMS