#pragma once

#include <memory>
#include <unordered_map>
#include <list>
#include <mutex>
#include "storage/page.h"
#include "storage/disk_manager.h"
#include "buffer/replacer.h"

namespace SimpleRDBMS {

class BufferPoolManager {
public:
    BufferPoolManager(size_t pool_size, 
                      std::unique_ptr<DiskManager> disk_manager,
                      std::unique_ptr<Replacer> replacer);
    ~BufferPoolManager();
    
    // Fetch a page from buffer pool
    Page* FetchPage(page_id_t page_id);
    
    // Create a new page
    Page* NewPage(page_id_t* page_id);
    
    // Delete a page
    bool DeletePage(page_id_t page_id);
    
    // Unpin a page
    bool UnpinPage(page_id_t page_id, bool is_dirty);
    
    // Flush a page to disk
    bool FlushPage(page_id_t page_id);
    
    // Flush all pages to disk
    void FlushAllPages();

     // Get disk manager (for accessing number of pages)
    DiskManager* GetDiskManager() { return disk_manager_.get(); }

private:
    size_t pool_size_;
    Page* pages_;
    std::unique_ptr<DiskManager> disk_manager_;
    std::unique_ptr<Replacer> replacer_;
    
    // Page table: page_id -> frame_id
    std::unordered_map<page_id_t, size_t> page_table_;
    
    // Free list of frame ids
    std::list<size_t> free_list_;
    
    // Synchronization
    std::mutex latch_;
    
    // Helper functions
    size_t FindVictimPage();
    void UpdatePage(Page* page, page_id_t page_id);
};

}  // namespace SimpleRDBMS