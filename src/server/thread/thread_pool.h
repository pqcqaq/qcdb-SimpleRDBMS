#pragma once

#include <vector>
#include <queue>
#include <memory>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <future>
#include <functional>
#include <stdexcept>
#include <atomic>
#include <chrono>

namespace SimpleRDBMS {

// Task statistics
struct TaskStats {
    size_t total_tasks;
    size_t completed_tasks;
    size_t failed_tasks;
    size_t pending_tasks;
    std::chrono::milliseconds total_execution_time;
    std::chrono::milliseconds average_execution_time;
    std::chrono::system_clock::time_point last_task_time;
};

// Thread pool configuration
struct ThreadPoolConfig {
    size_t min_threads = 2;
    size_t max_threads = 8;
    size_t max_queue_size = 1000;
    std::chrono::seconds idle_timeout{60};
    bool allow_core_thread_timeout = false;
};

class ThreadPool {
public:
    explicit ThreadPool(const ThreadPoolConfig& config = ThreadPoolConfig{});
    ~ThreadPool();
    
    // Lifecycle management
    bool Initialize();
    void Shutdown();
    bool IsRunning() const { return running_; }
    
    // Task submission
    template<class F, class... Args>
    auto Enqueue(F&& f, Args&&... args) 
        -> std::future<typename std::result_of<F(Args...)>::type>;
    
    // Pool management
    void SetPoolSize(size_t min_threads, size_t max_threads);
    size_t GetActiveThreads() const { return active_threads_; }
    size_t GetTotalThreads() const { return worker_threads_.size(); }
    size_t GetQueueSize() const;
    
    // Statistics
    TaskStats GetStats() const;
    void ResetStats();
    
    // Configuration
    void UpdateConfig(const ThreadPoolConfig& config);
    const ThreadPoolConfig& GetConfig() const { return config_; }
    
    // Health monitoring
    bool IsHealthy() const;
    double GetUtilization() const;

private:
    ThreadPoolConfig config_;
    
    // Thread management
    std::vector<std::unique_ptr<std::thread>> worker_threads_;
    std::atomic<size_t> active_threads_;
    std::atomic<bool> running_;
    
    // Task queue
    std::queue<std::function<void()>> task_queue_;
    mutable std::mutex queue_mutex_;
    std::condition_variable condition_;
    
    // Statistics
    mutable std::mutex stats_mutex_;
    TaskStats stats_;
    
    // Worker thread function
    void WorkerLoop();
    
    // Thread pool management
    void AddWorkerThread();
    void RemoveWorkerThread();
    bool ShouldAddThread() const;
    bool ShouldRemoveThread() const;
    
    // Statistics helpers
    void UpdateTaskStats(bool success, std::chrono::milliseconds execution_time);
};

// Template implementation
template<class F, class... Args>
auto ThreadPool::Enqueue(F&& f, Args&&... args) 
    -> std::future<typename std::result_of<F(Args...)>::type> {
    
    using return_type = typename std::result_of<F(Args...)>::type;
    
    if (!running_) {
        throw std::runtime_error("ThreadPool is not running");
    }
    
    auto task = std::make_shared<std::packaged_task<return_type()>>(
        std::bind(std::forward<F>(f), std::forward<Args>(args)...)
    );
    
    std::future<return_type> result = task->get_future();
    
    {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        
        // Check queue size limit
        if (task_queue_.size() >= config_.max_queue_size) {
            throw std::runtime_error("Task queue is full");
        }
        
        task_queue_.emplace([task](){ 
            auto start_time = std::chrono::high_resolution_clock::now();
            bool success = true;
            
            try {
                (*task)();
            } catch (...) {
                success = false;
                throw;
            }
            
            auto end_time = std::chrono::high_resolution_clock::now();
            auto execution_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                end_time - start_time);
            
            // Update stats in the thread pool instance
            // Note: This requires access to the ThreadPool instance
            // In practice, you might want to pass a callback or use a different approach
        });
        
        // Update pending tasks count
        {
            std::lock_guard<std::mutex> stats_lock(stats_mutex_);
            stats_.total_tasks++;
            stats_.pending_tasks++;
        }
    }
    
    condition_.notify_one();
    
    // Consider adding more threads if needed
    if (ShouldAddThread()) {
        AddWorkerThread();
    }
    
    return result;
}

} // namespace SimpleRDBMS