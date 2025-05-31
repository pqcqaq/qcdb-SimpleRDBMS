#pragma once

#include <fstream>
#include <string>
#include <mutex>
#include "common/config.h"
#include <vector>

namespace SimpleRDBMS {

class DiskManager {
public:
    explicit DiskManager(const std::string& db_file);
    ~DiskManager();
    
    // Read a page from disk
    void ReadPage(page_id_t page_id, char* page_data);
    
    // Write a page to disk
    void WritePage(page_id_t page_id, const char* page_data);
    
    // Allocate a new page
    page_id_t AllocatePage();
    
    // Deallocate a page
    void DeallocatePage(page_id_t page_id);
    
    // Get the number of pages
    int GetNumPages() const { return num_pages_; }

private:
    std::string db_file_name_;
    std::fstream db_file_;
    int num_pages_;
    int next_page_id_;
    std::mutex latch_;
    std::vector<page_id_t> free_pages_;  // 已释放的页面列表
};

}  // namespace SimpleRDBMS