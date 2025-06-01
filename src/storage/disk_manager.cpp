/*
 * 文件: disk_manager.cpp
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述: 磁盘管理器实现，负责database文件的读写操作和页面管理
 *       提供页面级别的I/O接口，管理页面分配和回收
 */

#include "storage/disk_manager.h"

#include <sys/stat.h>
#include <unistd.h>

#include <cstring>

#include "common/exception.h"
#include "stat/stat.h"

namespace SimpleRDBMS {

/**
 * 构造函数 - 初始化磁盘管理器
 * @param db_file database文件路径
 *
 * 实现思路：
 * 1. 尝试打开已存在的database文件
 * 2. 如果文件不存在，创建新文件
 * 3. 通过stat系统调用获取文件大小，计算已有页面数量
 * 4. 设置next_page_id为下一个可用的page ID
 */
DiskManager::DiskManager(const std::string& db_file)
    : db_file_name_(db_file), num_pages_(0), next_page_id_(0) {
    // 先尝试以读写模式打开existing文件
    db_file_.open(db_file_name_,
                  std::ios::binary | std::ios::in | std::ios::out);

    // 如果文件不存在，创建新文件
    if (!db_file_.is_open()) {
        db_file_.clear();
        // 创建空文件
        db_file_.open(db_file_name_,
                      std::ios::binary | std::ios::trunc | std::ios::out);
        db_file_.close();
        // 重新以读写模式打开
        db_file_.open(db_file_name_,
                      std::ios::binary | std::ios::in | std::ios::out);
        if (!db_file_.is_open()) {
            throw StorageException("Cannot open database file: " +
                                   db_file_name_);
        }
    }

    // 获取文件统计信息，计算已有页面数量
    struct stat file_stat;
    if (stat(db_file_name_.c_str(), &file_stat) == 0) {
        num_pages_ = file_stat.st_size / PAGE_SIZE;
        next_page_id_ = std::max(0, static_cast<int>(num_pages_));
    }
}

/**
 * 析构函数 - 清理资源
 * 确保文件正确关闭
 */
DiskManager::~DiskManager() {
    if (db_file_.is_open()) {
        db_file_.close();
    }
}

/**
 * 从磁盘读取指定页面
 * @param page_id 要读取的页面ID
 * @param page_data 存储读取数据的buffer（调用者负责分配内存）
 *
 * 实现思路：
 * 1. 加锁保证线程安全
 * 2. 验证page_id的有效性
 * 3. 计算文件offset = page_id * PAGE_SIZE
 * 4. 使用seekg定位到指定位置
 * 5. 读取PAGE_SIZE字节的数据
 * 6. 如果读取不足，用0填充剩余部分
 */
void DiskManager::ReadPage(page_id_t page_id, char* page_data) {
    std::lock_guard<std::mutex> lock(latch_);

    // 验证page_id范围
    if (page_id < 0 || page_id >= num_pages_) {
        throw StorageException("Invalid page id: " + std::to_string(page_id));
    }

    // 计算文件中的offset位置
    size_t offset = static_cast<size_t>(page_id) * PAGE_SIZE;
    db_file_.seekg(offset);
    db_file_.read(page_data, PAGE_SIZE);

    if (db_file_.bad()) {
        throw StorageException("Failed to read page: " +
                               std::to_string(page_id));
    }

    // 如果实际读取的字节数不足PAGE_SIZE，用0填充
    size_t read_count = db_file_.gcount();
    if (read_count < PAGE_SIZE) {
        std::memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }

    STATS.RecordDiskRead(PAGE_SIZE);
}

/**
 * 将页面数据写入磁盘
 * @param page_id 要写入的页面ID
 * @param page_data 要写入的数据buffer
 *
 * 实现思路：
 * 1. 加锁保证线程安全
 * 2. 验证page_id有效性（允许写入新页面）
 * 3. 如果需要，扩展文件大小以容纳新页面
 * 4. 写入页面数据到指定offset
 * 5. 强制flush到磁盘，确保持久化
 * 6. 在Unix系统上调用fsync进一步确保数据写入
 * 7. 更新metadata（页面数量等）
 */
void DiskManager::WritePage(page_id_t page_id, const char* page_data) {
    std::lock_guard<std::mutex> lock(latch_);

    if (page_id < 0) {
        throw StorageException("Invalid page id: " + std::to_string(page_id));
    }

    size_t offset = static_cast<size_t>(page_id) * PAGE_SIZE;

    // 检查并扩展文件大小
    db_file_.seekp(0, std::ios::end);
    size_t current_size = db_file_.tellp();
    size_t required_size = offset + PAGE_SIZE;

    if (current_size < required_size) {
        // 通过写入一个字节来扩展文件到所需大小
        db_file_.seekp(required_size - 1);
        db_file_.write("", 1);
        LOG_DEBUG("Extended file size to accommodate page " << page_id);
    }

    // 写入页面数据
    db_file_.seekp(offset);
    db_file_.write(page_data, PAGE_SIZE);
    LOG_DEBUG("Writing page " << page_id << " to disk at offset " << offset);

    if (db_file_.bad()) {
        throw StorageException("Failed to write page: " +
                               std::to_string(page_id));
    }

    // 强制刷新到磁盘缓冲区
    db_file_.flush();
    LOG_DEBUG("Flushed page " << page_id << " to disk");

    // 在Unix系统上调用fsync确保数据真正写入磁盘
// #ifdef __unix__
//     FILE* file = fopen(db_file_name_.c_str(), "r+");
//     if (file != nullptr) {
//         int fd = fileno(file);
//         if (fd != -1) {
//             LOG_DEBUG("Calling fsync on file descriptor " << fd);
//             fsync(fd);
//         }
//         fclose(file);
//     }
// #endif

    // 更新页面数量metadata
    if (page_id >= num_pages_) {
        num_pages_ = page_id + 1;
        if (next_page_id_ <= page_id) {
            next_page_id_ = page_id + 1;
        }
    }

    STATS.RecordDiskWrite(PAGE_SIZE);

    LOG_DEBUG("Successfully wrote page " << page_id << " to disk at offset "
                                         << offset);
}

/**
 * 分配一个新的页面ID
 * @return 新分配的page_id
 *
 * 实现思路：
 * 1. 优先复用已释放的页面（从free_pages_列表中取）
 * 2. 如果没有可复用的页面，分配新的page_id
 * 3. 更新next_page_id和num_pages计数器
 *
 * 这种设计可以有效复用磁盘空间，避免文件无限增长
 */
page_id_t DiskManager::AllocatePage() {
    std::lock_guard<std::mutex> lock(latch_);

    // 页面0和页面1是系统保留页面
    // 页面0: catalog页面
    // 页面1: 索引头部页面
    const page_id_t RESERVED_PAGES = 2;

    if (next_page_id_ < RESERVED_PAGES && num_pages_ <= RESERVED_PAGES) {
        // 初始化时，确保从保留页面之后开始分配
        next_page_id_ = RESERVED_PAGES;
        num_pages_ = std::max(num_pages_, RESERVED_PAGES);
        LOG_DEBUG("AllocatePage: Initialized next_page_id to "
                  << next_page_id_);
    }

    if (!free_pages_.empty()) {
        page_id_t reused_page = free_pages_.back();
        free_pages_.pop_back();

        // 确保重用的页面不是保留页面
        if (reused_page < RESERVED_PAGES) {
            LOG_WARN("AllocatePage: Attempted to reuse reserved page "
                     << reused_page << ", allocating new page instead");
            // 继续分配新页面
        } else {
            LOG_DEBUG("AllocatePage: Reusing deallocated page " << reused_page);
            STATS.RecordPageAllocation();
            return reused_page;
        }
    }

    page_id_t new_page_id = next_page_id_++;

    // 跳过保留页面
    if (new_page_id < RESERVED_PAGES) {
        new_page_id = next_page_id_ = RESERVED_PAGES;
        next_page_id_++;
    }

    num_pages_ = std::max(num_pages_, next_page_id_);

    STATS.RecordPageAllocation();

    LOG_DEBUG("AllocatePage: Allocated new page " << new_page_id);
    return new_page_id;
}

/**
 * 释放一个页面，将其加入空闲列表
 * @param page_id 要释放的页面ID
 *
 * 实现思路：
 * 1. 验证page_id的有效性
 * 2. 将page_id加入free_pages_列表
 * 3. 后续AllocatePage调用时可以复用这些页面
 *
 * 注意：这里只是标记页面为可复用，并不会清除页面内容
 * 实际的页面清理工作由上层模块负责
 */
void DiskManager::DeallocatePage(page_id_t page_id) {
    std::lock_guard<std::mutex> lock(latch_);

    const page_id_t RESERVED_PAGES = 2;

    // 不允许释放保留页面
    if (page_id < RESERVED_PAGES) {
        LOG_WARN("DeallocatePage: Attempted to deallocate reserved page "
                 << page_id << ", ignoring");
        return;
    }

    if (page_id >= 0 && page_id < next_page_id_) {
        free_pages_.push_back(page_id);

        STATS.RecordPageDeallocation();
        
        LOG_DEBUG("DeallocatePage: Deallocated page "
                  << page_id << " (added to free list)");
    }
}

}  // namespace SimpleRDBMS