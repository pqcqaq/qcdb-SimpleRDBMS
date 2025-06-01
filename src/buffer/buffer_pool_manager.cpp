/*
 * 文件: buffer_pool_manager.cpp
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述: 缓冲池管理器实现，负责管理内存中的页面缓存、LRU替换策略、
 *       页面的读写操作以及脏页管理等核心功能
 */

#include "buffer/buffer_pool_manager.h"

#include <cstring>

#include "common/debug.h"
#include "common/exception.h"

namespace SimpleRDBMS {

/**
 * 构造函数 - 初始化缓冲池管理器
 *
 * @param pool_size 缓冲池大小（页面数量）
 * @param disk_manager 磁盘管理器（智能指针）
 * @param replacer 页面替换器（智能指针，通常是LRU）
 *
 * 实现思路：
 * 1. 先分配固定大小的页面数组作为缓冲池
 * 2. 初始化所有frame都是空闲状态，加入free_list
 * 3. page_table_用来维护page_id到frame_id的映射关系
 */
BufferPoolManager::BufferPoolManager(size_t pool_size,
                                     std::unique_ptr<DiskManager> disk_manager,
                                     std::unique_ptr<Replacer> replacer)
    : pool_size_(pool_size),
      disk_manager_(std::move(disk_manager)),
      replacer_(std::move(replacer)) {
    LOG_INFO("Creating BufferPoolManager with pool_size=" << pool_size);

    LOG_DEBUG("About to allocate buffer pool array...");
    try {
        // 分配缓冲池数组 - 这是整个系统的内存缓存区域
        pages_ = new Page[pool_size_];
        LOG_DEBUG("Buffer pool array allocated successfully");
    } catch (const std::exception& e) {
        LOG_ERROR("Failed to allocate buffer pool: " << e.what());
        throw;
    }

    LOG_DEBUG("Initializing free list...");
    // 把所有frame都标记为空闲，刚开始所有frame都可以用
    for (size_t i = 0; i < pool_size_; i++) {
        free_list_.push_back(i);
        if (i % 10 == 0) {  // 每10个输出一次进度，避免日志太多
            LOG_TRACE("Initialized frame " << i << "/" << pool_size_);
        }
    }
    LOG_DEBUG("Free list initialized with " << free_list_.size() << " frames");

    LOG_DEBUG("BufferPoolManager created successfully");
}

/**
 * 析构函数 - 清理资源并确保所有脏页都写回磁盘
 */
BufferPoolManager::~BufferPoolManager() {
    LOG_INFO("Destroying BufferPoolManager");

    // 把所有脏页都写回磁盘，确保数据不丢失
    FlushAllPages();

    // 释放页面数组内存
    delete[] pages_;
}

/**
 * 获取指定页面 - 这是缓冲池最核心的功能
 *
 * @param page_id 要获取的页面ID
 * @return 页面指针，如果获取失败返回nullptr
 *
 * 实现思路：
 * 1. 先检查页面是否已经在缓冲池里，如果在就直接返回并增加pin_count
 * 2. 如果不在，需要从磁盘读取：
 *    - 优先使用free_list里的空闲frame
 *    - 如果没有空闲frame，就用LRU替换器找个victim evict掉
 *    - 如果victim是脏页，先写回磁盘再复用
 * 3. 从磁盘读取页面数据到选定的frame
 * 4. 更新page_table_映射关系，设置pin_count=1
 */
Page* BufferPoolManager::FetchPage(page_id_t page_id) {
    std::unique_lock<std::mutex> lock(latch_);
    LOG_TRACE("FetchPage called with page_id=" << page_id);

    if (page_id == INVALID_PAGE_ID) {
        LOG_ERROR("Attempting to fetch INVALID_PAGE_ID");
        return nullptr;
    }

    // 先看看这个页面是不是已经在内存里了
    auto it = page_table_.find(page_id);
    if (it != page_table_.end()) {
        // 找到了！直接返回，记得增加引用计数
        size_t frame_id = it->second;
        Page* page = &pages_[frame_id];
        page->IncreasePinCount();  // 增加pin计数，表示有人在用
        replacer_->Pin(frame_id);  // 告诉替换器这个frame正在被使用
        LOG_TRACE("Page " << page_id << " found in buffer pool at frame "
                          << frame_id);
        return page;
    }

    // 页面不在内存里，需要从磁盘加载
    size_t frame_id;
    Page* page = nullptr;

    if (!free_list_.empty()) {
        // 还有空闲的frame，直接用
        frame_id = free_list_.front();
        free_list_.pop_front();
        page = &pages_[frame_id];
        LOG_TRACE("Using free frame " << frame_id << " for page " << page_id);
    } else {
        // 没有空闲frame了，需要踢掉一个页面
        frame_id = FindVictimPage();
        if (frame_id == static_cast<size_t>(-1)) {
            // 所有页面都被pin住了，没法替换
            LOG_ERROR("No page can be evicted, all pages are pinned");
            return nullptr;
        }
        page = &pages_[frame_id];
        LOG_TRACE("Evicting page " << page->GetPageId() << " from frame "
                                   << frame_id);

        // 如果被踢掉的页面是脏的，先写回磁盘
        if (page->IsDirty() && page->GetPageId() != INVALID_PAGE_ID) {
            LOG_DEBUG("Writing dirty page " << page->GetPageId() << " to disk");
            disk_manager_->WritePage(page->GetPageId(), page->GetData());
            page->SetDirty(false);
        }

        // 从映射表里删除旧页面的记录
        if (page->GetPageId() != INVALID_PAGE_ID) {
            page_table_.erase(page->GetPageId());
        }
    }

    // 从磁盘读取页面数据
    LOG_TRACE("Reading page " << page_id << " from disk (num_pages="
                              << disk_manager_->GetNumPages() << ")");

    try {
        // 先设置页面的基本信息
        page->SetPageId(page_id);
        page->SetDirty(false);
        page->SetLSN(INVALID_LSN);
        // 重置pin计数，确保从0开始
        while (page->GetPinCount() > 0) {
            page->DecreasePinCount();
        }

        // 从磁盘读取实际数据，这会覆盖页面内容
        disk_manager_->ReadPage(page_id, page->GetData());

        // 更新映射表，建立page_id -> frame_id的关系
        page_table_[page_id] = frame_id;

        // 设置页面为被使用状态
        page->IncreasePinCount();
        replacer_->Pin(frame_id);

        return page;
    } catch (const StorageException& e) {
        // 页面在磁盘上不存在，把frame放回空闲列表
        LOG_DEBUG("Page " << page_id << " does not exist on disk: " << e.what()
                          << " (num_pages=" << disk_manager_->GetNumPages()
                          << ")");
        free_list_.push_back(frame_id);
        return nullptr;
    } catch (const std::exception& e) {
        // 其他异常，同样回收frame
        LOG_ERROR("Unexpected exception when reading page "
                  << page_id << ": " << e.what()
                  << " (num_pages=" << disk_manager_->GetNumPages() << ")");
        free_list_.push_back(frame_id);
        return nullptr;
    }
}

/**
 * 创建新页面 - 分配一个全新的页面
 *
 * @param page_id 输出参数，返回新分配的页面ID
 * @return 新页面的指针，失败返回nullptr
 *
 * 实现思路：
 * 1. 找一个可用的frame（优先空闲frame，没有就evict一个）
 * 2. 通过disk_manager分配新的page_id
 * 3. 初始化页面数据（清零），设置为脏页
 * 4. 建立映射关系，设置pin_count=1
 */
Page* BufferPoolManager::NewPage(page_id_t* page_id) {
    std::unique_lock<std::mutex> lock(latch_);
    LOG_TRACE("NewPage called");

    if (page_id == nullptr) {
        LOG_ERROR("NewPage called with nullptr page_id");
        return nullptr;
    }

    // 找一个frame来放新页面
    size_t frame_id;
    Page* page = nullptr;

    if (!free_list_.empty()) {
        // 有空闲frame，直接用
        frame_id = free_list_.front();
        free_list_.pop_front();
        page = &pages_[frame_id];
        LOG_TRACE("Using free frame " << frame_id << " for new page");
    } else {
        // 需要evict一个页面
        frame_id = FindVictimPage();
        if (frame_id == static_cast<size_t>(-1)) {
            // 没有可以evict的页面
            LOG_ERROR(
                "No page can be evicted for new page, all pages are pinned");
            *page_id = INVALID_PAGE_ID;
            return nullptr;
        }
        page = &pages_[frame_id];
        LOG_TRACE("Evicting page " << page->GetPageId() << " from frame "
                                   << frame_id << " for new page");

        // 如果被evict的页面是脏的，先写回磁盘
        if (page->IsDirty() && page->GetPageId() != INVALID_PAGE_ID) {
            LOG_DEBUG("Writing dirty page " << page->GetPageId()
                                            << " to disk before eviction");
            disk_manager_->WritePage(page->GetPageId(), page->GetData());
            page->SetDirty(false);
        }

        // 清理旧页面的映射关系
        if (page->GetPageId() != INVALID_PAGE_ID) {
            page_table_.erase(page->GetPageId());
        }
    }

    // 通过磁盘管理器分配一个新的page_id
    *page_id = disk_manager_->AllocatePage();
    LOG_DEBUG("Allocated new page with id=" << *page_id);

    // 初始化新页面 - 新页面需要清零数据
    std::memset(page->GetData(), 0, PAGE_SIZE);
    page->SetPageId(*page_id);
    page->SetDirty(true);  // 新页面标记为脏，因为有了新内容
    page->SetLSN(INVALID_LSN);

    // 重置pin计数
    while (page->GetPinCount() > 0) {
        page->DecreasePinCount();
    }

    // 建立映射关系
    page_table_[*page_id] = frame_id;

    // 设置页面为使用状态
    page->IncreasePinCount();
    replacer_->Pin(frame_id);

    LOG_TRACE("NewPage returning page " << *page_id << " in frame "
                                        << frame_id);
    return page;
}

/**
 * 删除页面 - 从缓冲池和磁盘中删除指定页面
 *
 * @param page_id 要删除的页面ID
 * @return 删除成功返回true
 *
 * 实现思路：
 * 1. 如果页面在缓冲池中，强制unpin（B+树删除时可能需要）
 * 2. 清理page_table_映射关系，frame放回free_list
 * 3. 调用disk_manager删除磁盘上的页面
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
    std::unique_lock<std::mutex> lock(latch_);

    LOG_TRACE("DeletePage called with page_id=" << page_id);

    // 检查页面是否在缓冲池中
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
        // 页面不在缓冲池中，直接删除磁盘上的页面
        disk_manager_->DeallocatePage(page_id);
        return true;
    }

    // 页面在缓冲池中，需要特殊处理
    size_t frame_id = it->second;
    Page* page = &pages_[frame_id];

    // 强制unpin页面 - B+树节点删除时可能页面还被pin着
    if (page->GetPinCount() > 0) {
        LOG_DEBUG("Force unpinning page " << page_id
                                          << " before deletion (pin_count="
                                          << page->GetPinCount() << ")");
        while (page->GetPinCount() > 0) {
            page->DecreasePinCount();
        }
    }

    // 清理缓冲池中的记录
    page_table_.erase(page_id);
    replacer_->Pin(frame_id);  // 从替换器中移除

    // frame重新变成空闲的
    free_list_.push_back(frame_id);

    // 重置页面元数据
    page->SetPageId(INVALID_PAGE_ID);
    page->SetDirty(false);
    page->SetLSN(INVALID_LSN);

    // 删除磁盘上的页面
    disk_manager_->DeallocatePage(page_id);

    LOG_TRACE("Successfully deleted page " << page_id);
    return true;
}

/**
 * 取消页面固定 - 减少页面的引用计数
 *
 * @param page_id 要unpin的页面ID
 * @param is_dirty 是否标记为脏页
 * @return 操作成功返回true
 *
 * 实现思路：
 * 1. 找到页面，减少pin_count
 * 2. 如果is_dirty=true，标记页面为脏
 * 3. 如果pin_count变成0，告诉替换器这个frame可以被替换了
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
    std::unique_lock<std::mutex> lock(latch_);
    LOG_TRACE("UnpinPage called with page_id=" << page_id
                                               << ", is_dirty=" << is_dirty);

    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
        LOG_DEBUG("UnpinPage: page " << page_id << " not in buffer pool");
        return false;
    }

    size_t frame_id = it->second;
    Page* page = &pages_[frame_id];

    if (page->GetPinCount() <= 0) {
        // pin_count已经是0了，记录一下但不报错
        LOG_DEBUG("UnpinPage: page " << page_id
                                     << " already unpinned (pin_count="
                                     << page->GetPinCount() << ")");
        return true;
    }

    // 减少引用计数
    page->DecreasePinCount();
    if (is_dirty) {
        page->SetDirty(true);  // 标记为脏页，之后需要写回磁盘
    }

    // 如果没人用了，可以被替换
    if (page->GetPinCount() == 0) {
        replacer_->Unpin(frame_id);
        LOG_TRACE("UnpinPage: page " << page_id
                                     << " unpinned and added to replacer");
    }

    LOG_TRACE("UnpinPage: page " << page_id
                                 << " pin_count=" << page->GetPinCount());
    return true;
}

/**
 * 强制刷新页面到磁盘 - 不管是否为脏页都写回磁盘
 *
 * @param page_id 要刷新的页面ID
 * @return 刷新成功返回true
 *
 * 实现思路：
 * 1. 如果页面在缓冲池中，直接写回磁盘并清除脏标记
 * 2. 如果不在缓冲池中，检查磁盘上是否存在该页面
 * 3. 这是强制flush，即使页面不脏也要写
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
    std::unique_lock<std::mutex> lock(latch_);
    LOG_DEBUG("FlushPage called with page_id=" << page_id);

    // 检查页面是否在缓冲池中
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
        // 页面不在缓冲池中，检查磁盘上是否存在
        LOG_DEBUG("FlushPage: page " << page_id << " not in buffer pool");

        // 尝试读取看页面是否在磁盘上
        try {
            char temp_buffer[PAGE_SIZE];
            disk_manager_->ReadPage(page_id, temp_buffer);
            LOG_DEBUG("FlushPage: page "
                      << page_id << " exists on disk but not in buffer pool");
            return true;  // 页面已经在磁盘上了
        } catch (const StorageException&) {
            LOG_DEBUG("FlushPage: page "
                      << page_id
                      << " does not exist on disk or in buffer pool");
            return false;
        }
    }

    // 页面在缓冲池中，执行强制写回
    size_t frame_id = it->second;
    Page* page = &pages_[frame_id];

    LOG_DEBUG("Force flushing page " << page_id << " to disk");
    try {
        disk_manager_->WritePage(page_id, page->GetData());
        page->SetDirty(false);  // 清除脏标记

        LOG_DEBUG("Page " << page_id << " successfully written to disk");
        return true;
    } catch (const std::exception& e) {
        LOG_ERROR("Failed to flush page " << page_id
                                          << " to disk: " << e.what());
        return false;
    }
}

/**
 * 刷新所有脏页到磁盘 - 通常在系统关闭时调用
 *
 * 实现思路：遍历所有frame，找到脏页就写回磁盘
 */
void BufferPoolManager::FlushAllPages() {
    std::unique_lock<std::mutex> lock(latch_);
    LOG_INFO("Flushing all pages");

    int flushed_count = 0;
    int error_count = 0;

    // 遍历所有frame，找脏页写回
    for (size_t i = 0; i < pool_size_; i++) {
        Page* page = &pages_[i];
        if (page->GetPageId() != INVALID_PAGE_ID && page->IsDirty()) {
            try {
                LOG_DEBUG("Flushing page " << page->GetPageId());
                disk_manager_->WritePage(page->GetPageId(), page->GetData());
                page->SetDirty(false);
                flushed_count++;
            } catch (const std::exception& e) {
                LOG_ERROR("Failed to flush page " << page->GetPageId() << ": "
                                                  << e.what());
                error_count++;
            }
        }
    }

    LOG_INFO("FlushAllPages completed: flushed " << flushed_count << " pages, "
                                                 << error_count << " errors");
}

/**
 * 寻找可以被替换的页面 - 使用LRU替换策略
 *
 * @return 可以被替换的frame_id，如果没有返回-1
 *
 * 实现思路：直接委托给replacer_，它会根据LRU算法选择victim
 */
size_t BufferPoolManager::FindVictimPage() {
    size_t victim_frame_id;

    // 使用替换器找一个victim frame
    if (!replacer_->Victim(&victim_frame_id)) {
        // 没有frame可以被替换（都被pin住了）
        return static_cast<size_t>(-1);
    }

    return victim_frame_id;
}

/**
 * 更新页面元数据 - 重置页面到初始状态
 *
 * @param page 要更新的页面
 * @param page_id 新的页面ID
 *
 * 注意：这个方法不清零页面数据，因为可能是从磁盘读取的有效数据
 */
void BufferPoolManager::UpdatePage(Page* page, page_id_t page_id) {
    // 重置页面元数据
    page->SetPageId(page_id);
    page->SetDirty(false);
    page->SetLSN(INVALID_LSN);

    // 重置pin计数到0
    while (page->GetPinCount() > 0) {
        page->DecreasePinCount();
    }
}

Page* BufferPoolManager::GetSpecificPage(page_id_t page_id) {
    std::unique_lock<std::mutex> lock(latch_);

    // 首先尝试从缓冲池获取
    auto it = page_table_.find(page_id);
    if (it != page_table_.end()) {
        size_t frame_id = it->second;
        Page* page = &pages_[frame_id];
        page->IncreasePinCount();
        replacer_->Pin(frame_id);
        return page;
    }

    // 获取一个空闲frame
    size_t frame_id;
    Page* page = nullptr;
    if (!free_list_.empty()) {
        frame_id = free_list_.front();
        free_list_.pop_front();
        page = &pages_[frame_id];
    } else {
        frame_id = FindVictimPage();
        if (frame_id == static_cast<size_t>(-1)) {
            return nullptr;
        }
        page = &pages_[frame_id];

        // 如果被驱逐的页面是脏页，先写回磁盘
        if (page->IsDirty() && page->GetPageId() != INVALID_PAGE_ID) {
            disk_manager_->WritePage(page->GetPageId(), page->GetData());
            page->SetDirty(false);
        }
        if (page->GetPageId() != INVALID_PAGE_ID) {
            page_table_.erase(page->GetPageId());
        }
    }

    // 尝试从磁盘读取页面
    try {
        page->SetPageId(page_id);
        page->SetDirty(false);
        page->SetLSN(INVALID_LSN);
        while (page->GetPinCount() > 0) {
            page->DecreasePinCount();
        }

        // 检查页面是否存在，如果不存在就初始化为空页面
        if (page_id < disk_manager_->GetNumPages()) {
            disk_manager_->ReadPage(page_id, page->GetData());
        } else {
            // 页面不存在，初始化为空页面
            std::memset(page->GetData(), 0, PAGE_SIZE);
            page->SetDirty(true);
        }

        page_table_[page_id] = frame_id;
        page->IncreasePinCount();
        replacer_->Pin(frame_id);
        return page;
    } catch (const std::exception& e) {
        LOG_ERROR("Failed to get specific page " << page_id << ": "
                                                 << e.what());
        free_list_.push_back(frame_id);
        return nullptr;
    }
}

}  // namespace SimpleRDBMS