// src/storage/disk_manager.cpp
#include "storage/disk_manager.h"

#include <sys/stat.h>
#include <unistd.h>

#include <cstring>

#include "common/exception.h"

namespace SimpleRDBMS {

DiskManager::DiskManager(const std::string& db_file)
    : db_file_name_(db_file), num_pages_(0), next_page_id_(0) {  // 改为从0开始
    db_file_.open(db_file_name_,
                  std::ios::binary | std::ios::in | std::ios::out);
    if (!db_file_.is_open()) {
        db_file_.clear();
        db_file_.open(db_file_name_,
                      std::ios::binary | std::ios::trunc | std::ios::out);
        db_file_.close();
        db_file_.open(db_file_name_,
                      std::ios::binary | std::ios::in | std::ios::out);
        if (!db_file_.is_open()) {
            throw StorageException("Cannot open database file: " +
                                   db_file_name_);
        }
    }

    struct stat file_stat;
    if (stat(db_file_name_.c_str(), &file_stat) == 0) {
        num_pages_ = file_stat.st_size / PAGE_SIZE;
        next_page_id_ = std::max(0, static_cast<int>(num_pages_));  // 确保至少从0开始
    }
}

DiskManager::~DiskManager() {
    if (db_file_.is_open()) {
        db_file_.close();
    }
}

void DiskManager::ReadPage(page_id_t page_id, char* page_data) {
    std::lock_guard<std::mutex> lock(latch_);

    if (page_id < 0 || page_id >= num_pages_) {
        throw StorageException("Invalid page id: " + std::to_string(page_id));
    }

    size_t offset = static_cast<size_t>(page_id) * PAGE_SIZE;
    db_file_.seekg(offset);
    db_file_.read(page_data, PAGE_SIZE);

    if (db_file_.bad()) {
        throw StorageException("Failed to read page: " +
                               std::to_string(page_id));
    }

    size_t read_count = db_file_.gcount();
    if (read_count < PAGE_SIZE) {
        std::memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
}

void DiskManager::WritePage(page_id_t page_id, const char* page_data) {
    std::lock_guard<std::mutex> lock(latch_);
    
    if (page_id < 0) {
        throw StorageException("Invalid page id: " + std::to_string(page_id));
    }

    size_t offset = static_cast<size_t>(page_id) * PAGE_SIZE;
    
    // 确保文件足够大
    db_file_.seekp(0, std::ios::end);
    size_t current_size = db_file_.tellp();
    size_t required_size = offset + PAGE_SIZE;
    
    if (current_size < required_size) {
        // 扩展文件大小
        db_file_.seekp(required_size - 1);
        db_file_.write("", 1);
        LOG_DEBUG("Extended file size to accommodate page " << page_id);
    }
    
    // 写入页面数据
    db_file_.seekp(offset);
    db_file_.write(page_data, PAGE_SIZE);
    
    if (db_file_.bad()) {
        throw StorageException("Failed to write page: " + std::to_string(page_id));
    }
    
    // 强制刷新到磁盘
    db_file_.flush();
    
    // 在支持的系统上调用fsync确保数据写入磁盘
    #ifdef __unix__
    int fd = fileno(fopen(db_file_name_.c_str(), "r"));
    if (fd != -1) {
        fsync(fd);
        close(fd);
    }
    #endif
    
    // 更新元数据
    if (page_id >= num_pages_) {
        num_pages_ = page_id + 1;
        if (next_page_id_ <= page_id) {
            next_page_id_ = page_id + 1;
        }
    }
    
    LOG_DEBUG("Successfully wrote page " << page_id << " to disk at offset " << offset);
}

page_id_t DiskManager::AllocatePage() {
    std::lock_guard<std::mutex> lock(latch_);
    
    // 优先使用已释放的页面
    if (!free_pages_.empty()) {
        page_id_t reused_page = free_pages_.back();
        free_pages_.pop_back();
        LOG_DEBUG("Reusing deallocated page " << reused_page);
        return reused_page;
    }
    
    page_id_t new_page_id = next_page_id_++;
    num_pages_ = std::max(num_pages_, next_page_id_);
    
    LOG_DEBUG("Allocated new page " << new_page_id);
    return new_page_id;
}

void DiskManager::DeallocatePage(page_id_t page_id) {
    std::lock_guard<std::mutex> lock(latch_);
    if (page_id >= 0 && page_id < next_page_id_) {
        free_pages_.push_back(page_id);
        LOG_DEBUG("Deallocated page " << page_id << " (added to free list)");
    }
}

}  // namespace SimpleRDBMS