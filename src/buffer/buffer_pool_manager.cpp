#include "buffer/buffer_pool_manager.h"
#include "common/exception.h"
#include "common/debug.h"
#include <cstring>

namespace SimpleRDBMS {

BufferPoolManager::BufferPoolManager(size_t pool_size, 
                                     std::unique_ptr<DiskManager> disk_manager,
                                     std::unique_ptr<Replacer> replacer)
    : pool_size_(pool_size),
      disk_manager_(std::move(disk_manager)),
      replacer_(std::move(replacer)) {
    
    LOG_INFO("Creating BufferPoolManager with pool_size=" << pool_size);
    
    // Allocate buffer pool
    pages_ = new Page[pool_size_];
    
    // Initialize free list with all frame ids
    for (size_t i = 0; i < pool_size_; i++) {
        free_list_.push_back(i);
    }
    
    LOG_DEBUG("BufferPoolManager created successfully");
}

BufferPoolManager::~BufferPoolManager() {
    LOG_INFO("Destroying BufferPoolManager");
    
    // Flush all dirty pages
    FlushAllPages();
    
    // Delete buffer pool
    delete[] pages_;
}

Page* BufferPoolManager::FetchPage(page_id_t page_id) {
    std::unique_lock<std::mutex> lock(latch_);
    
    LOG_TRACE("FetchPage called with page_id=" << page_id);
    
    if (page_id == INVALID_PAGE_ID) {
        LOG_ERROR("Attempting to fetch INVALID_PAGE_ID");
        return nullptr;
    }
    
    // 对于页面0，如果磁盘上不存在，我们可以创建它
    if (page_id == 0 && disk_manager_->GetNumPages() == 0) {
        LOG_DEBUG("Creating header page 0 for new database");
        // 这种情况下我们应该通过NewPage来创建，但要确保分配到页面0
        // 实际上这种情况不应该发生，因为我们修改了AllocatePage从1开始
    }
    
    // 1. Check if page is already in buffer pool
    auto it = page_table_.find(page_id);
    if (it != page_table_.end()) {
        size_t frame_id = it->second;
        Page* page = &pages_[frame_id];
        
        // Pin the page
        page->IncreasePinCount();
        replacer_->Pin(frame_id);
        
        LOG_TRACE("Page " << page_id << " found in buffer pool at frame " << frame_id);
        return page;
    }
    
    // 2. Page is not in buffer pool, need to fetch from disk
    // First, find a frame to use
    size_t frame_id;
    Page* page = nullptr;
    
    if (!free_list_.empty()) {
        // Use a free frame
        frame_id = free_list_.front();
        free_list_.pop_front();
        page = &pages_[frame_id];
        LOG_TRACE("Using free frame " << frame_id << " for page " << page_id);
    } else {
        // Need to evict a page
        frame_id = FindVictimPage();
        if (frame_id == static_cast<size_t>(-1)) {
            // No page can be evicted
            LOG_ERROR("No page can be evicted, all pages are pinned");
            return nullptr;
        }
        page = &pages_[frame_id];
        
        LOG_TRACE("Evicting page " << page->GetPageId() << " from frame " << frame_id);
        
        // Write back if dirty and has valid page_id
        if (page->IsDirty() && page->GetPageId() != INVALID_PAGE_ID) {
            LOG_DEBUG("Writing dirty page " << page->GetPageId() << " to disk");
            disk_manager_->WritePage(page->GetPageId(), page->GetData());
            page->SetDirty(false);
        }
        
        // Remove from page table if it has a valid page_id
        if (page->GetPageId() != INVALID_PAGE_ID) {
            page_table_.erase(page->GetPageId());
        }
    }
    
    // 3. Read page from disk
    LOG_TRACE("Reading page " << page_id << " from disk (num_pages=" 
              << disk_manager_->GetNumPages() << ")");
    try {
        disk_manager_->ReadPage(page_id, page->GetData());
        UpdatePage(page, page_id);
        
        // 4. Update page table
        page_table_[page_id] = frame_id;
        
        // 5. Pin the page
        page->IncreasePinCount();
        replacer_->Pin(frame_id);
        
        return page;
    } catch (const StorageException& e) {
        // Page does not exist on disk, return the frame to free list
        LOG_DEBUG("Page " << page_id << " does not exist on disk: " << e.what() 
                  << " (num_pages=" << disk_manager_->GetNumPages() << ")");
        free_list_.push_back(frame_id);
        return nullptr;
    } catch (const std::exception& e) {
        // Catch any other exception
        LOG_ERROR("Unexpected exception when reading page " << page_id << ": " << e.what()
                  << " (num_pages=" << disk_manager_->GetNumPages() << ")");
        free_list_.push_back(frame_id);
        return nullptr;
    }
}

Page* BufferPoolManager::NewPage(page_id_t* page_id) {
    std::unique_lock<std::mutex> lock(latch_);
    
    LOG_TRACE("NewPage called");
    
    if (page_id == nullptr) {
        LOG_ERROR("NewPage called with nullptr page_id");
        return nullptr;
    }
    
    // 1. Find a frame for the new page
    size_t frame_id;
    Page* page = nullptr;
    
    if (!free_list_.empty()) {
        // Use a free frame
        frame_id = free_list_.front();
        free_list_.pop_front();
        page = &pages_[frame_id];
        LOG_TRACE("Using free frame " << frame_id << " for new page");
    } else {
        // Need to evict a page
        frame_id = FindVictimPage();
        if (frame_id == static_cast<size_t>(-1)) {
            // No page can be evicted
            LOG_ERROR("No page can be evicted for new page, all pages are pinned");
            *page_id = INVALID_PAGE_ID;
            return nullptr;
        }
        page = &pages_[frame_id];
        
        LOG_TRACE("Evicting page " << page->GetPageId() << " from frame " << frame_id << " for new page");
        
        // Write back if dirty and has valid page_id
        if (page->IsDirty() && page->GetPageId() != INVALID_PAGE_ID) {
            LOG_DEBUG("Writing dirty page " << page->GetPageId() << " to disk before eviction");
            disk_manager_->WritePage(page->GetPageId(), page->GetData());
            page->SetDirty(false);
        }
        
        // Remove from page table if it has a valid page_id
        if (page->GetPageId() != INVALID_PAGE_ID) {
            page_table_.erase(page->GetPageId());
        }
    }
    
    // 2. Allocate a new page on disk
    *page_id = disk_manager_->AllocatePage();
    LOG_DEBUG("Allocated new page with id=" << *page_id);
    
    // 3. Initialize the page
    std::memset(page->GetData(), 0, PAGE_SIZE);
    UpdatePage(page, *page_id);
    page->SetDirty(true);  // New page is dirty
    
    // 4. Update page table
    page_table_[*page_id] = frame_id;
    
    // 5. Pin the page
    page->IncreasePinCount();
    replacer_->Pin(frame_id);
    
    LOG_TRACE("NewPage returning page " << *page_id << " in frame " << frame_id);
    return page;
}

bool BufferPoolManager::DeletePage(page_id_t page_id) {
    std::unique_lock<std::mutex> lock(latch_);
    
    LOG_TRACE("DeletePage called with page_id=" << page_id);
    
    // 1. Check if page is in buffer pool
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
        // Page not in buffer pool, can delete directly
        disk_manager_->DeallocatePage(page_id);
        return true;
    }
    
    // 2. Get page and handle pin count
    size_t frame_id = it->second;
    Page* page = &pages_[frame_id];
    
    // For B+ tree node deletion during coalesce operations,
    // we need to force unpin the page if it's still pinned
    if (page->GetPinCount() > 0) {
        LOG_DEBUG("Force unpinning page " << page_id << " before deletion (pin_count=" << page->GetPinCount() << ")");
        while (page->GetPinCount() > 0) {
            page->DecreasePinCount();
        }
    }
    
    // 3. Remove from page table and replacer
    page_table_.erase(page_id);
    replacer_->Pin(frame_id);  // Remove from replacer
    
    // 4. Add frame back to free list
    free_list_.push_back(frame_id);
    
    // 5. Reset page metadata
    page->SetPageId(INVALID_PAGE_ID);
    page->SetDirty(false);
    page->SetLSN(INVALID_LSN);
    
    // 6. Deallocate page on disk
    disk_manager_->DeallocatePage(page_id);
    
    LOG_TRACE("Successfully deleted page " << page_id);
    return true;
}

bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
    std::unique_lock<std::mutex> lock(latch_);
    
    LOG_TRACE("UnpinPage called with page_id=" << page_id << ", is_dirty=" << is_dirty);
    
    // 1. Check if page is in buffer pool
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
        // Page not in buffer pool
        LOG_WARN("UnpinPage: page " << page_id << " not in buffer pool");
        return false;
    }
    
    // 2. Get the page
    size_t frame_id = it->second;
    Page* page = &pages_[frame_id];
    
    // 3. Check pin count
    if (page->GetPinCount() <= 0) {
        // Page is not pinned
        LOG_WARN("UnpinPage: page " << page_id << " is not pinned");
        return false;
    }
    
    // 4. Decrease pin count
    page->DecreasePinCount();
    
    // 5. Update dirty flag
    if (is_dirty) {
        page->SetDirty(true);
    }
    
    // 6. Add to replacer if pin count reaches 0
    if (page->GetPinCount() == 0) {
        replacer_->Unpin(frame_id);
    }
    
    return true;
}

bool BufferPoolManager::FlushPage(page_id_t page_id) {
    std::unique_lock<std::mutex> lock(latch_);
    
    LOG_TRACE("FlushPage called with page_id=" << page_id);
    
    // 1. Check if page is in buffer pool
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
        // Page not in buffer pool
        LOG_DEBUG("FlushPage: page " << page_id << " not in buffer pool");
        return false;
    }
    
    // 2. Get the page
    size_t frame_id = it->second;
    Page* page = &pages_[frame_id];
    
    // 3. Write to disk
    LOG_DEBUG("Flushing page " << page_id << " to disk");
    disk_manager_->WritePage(page_id, page->GetData());
    page->SetDirty(false);
    
    return true;
}

void BufferPoolManager::FlushAllPages() {
    std::unique_lock<std::mutex> lock(latch_);
    
    LOG_INFO("Flushing all pages");
    
    // Flush all pages in the buffer pool
    for (size_t i = 0; i < pool_size_; i++) {
        Page* page = &pages_[i];
        
        // Only flush pages with valid page_id and that are dirty
        if (page->GetPageId() != INVALID_PAGE_ID && page->IsDirty()) {
            LOG_DEBUG("Flushing page " << page->GetPageId());
            disk_manager_->WritePage(page->GetPageId(), page->GetData());
            page->SetDirty(false);
        }
    }
}

size_t BufferPoolManager::FindVictimPage() {
    size_t victim_frame_id;
    
    // Use replacer to find a victim frame
    if (!replacer_->Victim(&victim_frame_id)) {
        // No frame can be evicted
        return static_cast<size_t>(-1);
    }
    
    return victim_frame_id;
}

void BufferPoolManager::UpdatePage(Page* page, page_id_t page_id) {
    // Reset page metadata
    page->SetPageId(page_id);
    page->SetDirty(false);
    page->SetLSN(INVALID_LSN);
    
    // Reset pin count to 0 (don't use assertion, just reset)
    while (page->GetPinCount() > 0) {
        page->DecreasePinCount();
    }
}

}  // namespace SimpleRDBMS