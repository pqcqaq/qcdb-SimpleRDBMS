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
    
    LOG_DEBUG("About to allocate buffer pool array...");
    try {
        // Allocate buffer pool
        pages_ = new Page[pool_size_];
        LOG_DEBUG("Buffer pool array allocated successfully");
    } catch (const std::exception& e) {
        LOG_ERROR("Failed to allocate buffer pool: " << e.what());
        throw;
    }
    
    LOG_DEBUG("Initializing free list...");
    // Initialize free list with all frame ids
    for (size_t i = 0; i < pool_size_; i++) {
        free_list_.push_back(i);
        if (i % 10 == 0) {  // 每10个输出一次进度
            LOG_TRACE("Initialized frame " << i << "/" << pool_size_);
        }
    }
    LOG_DEBUG("Free list initialized with " << free_list_.size() << " frames");
    
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
        // 首先设置页面ID，但不清零数据
        page->SetPageId(page_id);
        page->SetDirty(false);
        page->SetLSN(INVALID_LSN);
        // Reset pin count
        while (page->GetPinCount() > 0) {
            page->DecreasePinCount();
        }
        
        // 然后从磁盘读取数据，这会覆盖页面内容
        disk_manager_->ReadPage(page_id, page->GetData());
        
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

    // 3. Initialize the page (对于新页面，我们需要清零)
    std::memset(page->GetData(), 0, PAGE_SIZE);  // 新页面需要清零
    page->SetPageId(*page_id);
    page->SetDirty(true);  // New page is dirty
    page->SetLSN(INVALID_LSN);
    
    // Reset pin count
    while (page->GetPinCount() > 0) {
        page->DecreasePinCount();
    }

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
    
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
        LOG_DEBUG("UnpinPage: page " << page_id << " not in buffer pool");
        return false;
    }
    
    size_t frame_id = it->second;
    Page* page = &pages_[frame_id];
    
    if (page->GetPinCount() <= 0) {
        // 只记录调试信息，不视为错误
        LOG_DEBUG("UnpinPage: page " << page_id << " already unpinned (pin_count=" 
                 << page->GetPinCount() << ")");
        return true;
    }
    
    page->DecreasePinCount();
    if (is_dirty) {
        page->SetDirty(true);
    }
    
    if (page->GetPinCount() == 0) {
        replacer_->Unpin(frame_id);
        LOG_TRACE("UnpinPage: page " << page_id << " unpinned and added to replacer");
    }
    
    LOG_TRACE("UnpinPage: page " << page_id << " pin_count=" << page->GetPinCount());
    return true;
}

bool BufferPoolManager::FlushPage(page_id_t page_id) {
    std::unique_lock<std::mutex> lock(latch_);
    LOG_DEBUG("FlushPage called with page_id=" << page_id);
    
    // 1. Check if page is in buffer pool
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
        // Page not in buffer pool, check if it exists on disk
        LOG_DEBUG("FlushPage: page " << page_id << " not in buffer pool");
        
        // 如果页面不在缓冲池中，但我们仍然想刷新它（可能是新创建的页面）
        // 先尝试从磁盘读取看是否存在
        try {
            // 创建临时缓冲区来测试页面是否存在
            char temp_buffer[PAGE_SIZE];
            disk_manager_->ReadPage(page_id, temp_buffer);
            LOG_DEBUG("FlushPage: page " << page_id << " exists on disk but not in buffer pool");
            return true; // 页面已经在磁盘上了
        } catch (const StorageException&) {
            LOG_DEBUG("FlushPage: page " << page_id << " does not exist on disk or in buffer pool");
            return false;
        }
    }

    // 2. Get the page
    size_t frame_id = it->second;
    Page* page = &pages_[frame_id];

    // 3. Write to disk regardless of dirty flag (force flush)
    LOG_DEBUG("Force flushing page " << page_id << " to disk");
    try {
        disk_manager_->WritePage(page_id, page->GetData());
        page->SetDirty(false);
        
        // 确保数据真正写入磁盘（同步操作）
        // 在实际实现中，这里可能需要调用fsync()等系统调用
        LOG_DEBUG("Page " << page_id << " successfully written to disk");
        return true;
    } catch (const std::exception& e) {
        LOG_ERROR("Failed to flush page " << page_id << " to disk: " << e.what());
        return false;
    }
}

void BufferPoolManager::FlushAllPages() {
    std::unique_lock<std::mutex> lock(latch_);
    LOG_INFO("Flushing all pages");
    
    int flushed_count = 0;
    int error_count = 0;
    
    for (size_t i = 0; i < pool_size_; i++) {
        Page* page = &pages_[i];
        if (page->GetPageId() != INVALID_PAGE_ID && page->IsDirty()) {
            try {
                LOG_DEBUG("Flushing page " << page->GetPageId());
                disk_manager_->WritePage(page->GetPageId(), page->GetData());
                page->SetDirty(false);
                flushed_count++;
            } catch (const std::exception& e) {
                LOG_ERROR("Failed to flush page " << page->GetPageId() << ": " << e.what());
                error_count++;
            }
        }
    }
    
    LOG_INFO("FlushAllPages completed: flushed " << flushed_count 
             << " pages, " << error_count << " errors");
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
    
    // 重要修改：只有在页面是新创建的时候才清零数据
    // 对于从磁盘读取的页面，不应该清零数据
    // 这里我们不清零页面数据，让磁盘读取的数据保持原样
    // std::memset(page->GetData(), 0, PAGE_SIZE);  // 注释掉这行
    
    // Reset pin count to 0 (don't use assertion, just reset)
    while (page->GetPinCount() > 0) {
        page->DecreasePinCount();
    }
}

}  // namespace SimpleRDBMS