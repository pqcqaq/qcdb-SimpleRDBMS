/*
 * 文件: buffer_pool_manager.h
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述: 缓冲池管理器头文件，定义了数据库系统中最核心的内存管理组件
 *       负责管理磁盘页面在内存中的缓存，实现高效的页面读写和替换策略
 */

#pragma once

#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "buffer/replacer.h"
#include "storage/disk_manager.h"
#include "storage/page.h"

namespace SimpleRDBMS {

/**
 * BufferPoolManager - 缓冲池管理器
 *
 * 这是数据库系统的核心组件之一，相当于操作系统中的内存管理器。
 * 主要功能：
 * 1. 管理固定大小的内存页面池（通常是数百个4KB页面）
 * 2. 实现页面的LRU替换策略，决定哪些页面被踢出内存
 * 3. 处理页面的pin/unpin机制，防止正在使用的页面被替换
 * 4. 管理脏页的写回，确保数据持久化
 * 5. 提供page_id到内存地址的快速映射
 *
 * 设计思路：
 * - 使用fixed-size的页面数组作为内存池
 * - page_table_维护page_id到frame_id的映射关系
 * - free_list_管理空闲的frame
 * - replacer_实现LRU替换算法
 * - 通过pin_count保护正在使用的页面
 */
class BufferPoolManager {
   public:
    /**
     * 构造函数 - 初始化缓冲池管理器
     *
     * @param pool_size 缓冲池大小（页面数量），通常设置为100-1000
     * @param disk_manager 磁盘管理器，负责实际的磁盘I/O操作
     * @param replacer 页面替换器，通常是LRU替换策略的实现
     */
    BufferPoolManager(size_t pool_size,
                      std::unique_ptr<DiskManager> disk_manager,
                      std::unique_ptr<Replacer> replacer);

    /**
     * 析构函数 - 清理资源并确保数据安全
     * 会把所有脏页写回磁盘，防止数据丢失
     */
    ~BufferPoolManager();

    /**
     * 获取页面 - 缓冲池最重要的功能
     *
     * @param page_id 要获取的页面ID
     * @return 页面指针，获取成功时pin_count会增加1；失败返回nullptr
     *
     * 工作流程：
     * 1. 先查page_table_看页面是否已经在内存里
     * 2. 如果在内存里，直接返回并增加pin_count
     * 3. 如果不在，找个空闲frame或者evict一个页面
     * 4. 从磁盘读取页面数据到选定的frame
     * 5. 建立page_id到frame_id的映射关系
     */
    Page* FetchPage(page_id_t page_id);

    /**
     * 创建新页面 - 分配一个全新的页面
     *
     * @param page_id 输出参数，返回新分配的页面ID
     * @return 新页面的指针，pin_count=1；失败返回nullptr
     *
     * 工作流程：
     * 1. 找一个可用的frame（优先使用free_list中的空闲frame）
     * 2. 通过disk_manager分配新的page_id
     * 3. 初始化页面数据（清零），标记为脏页
     * 4. 建立映射关系，设置pin_count=1
     */
    Page* NewPage(page_id_t* page_id);

    /**
     * 删除页面 - 从缓冲池和磁盘中删除页面
     *
     * @param page_id 要删除的页面ID
     * @return 删除成功返回true
     *
     * 注意：这会强制unpin页面，主要用于B+树节点的删除操作
     */
    bool DeletePage(page_id_t page_id);

    /**
     * 取消页面固定 - 减少页面的引用计数
     *
     * @param page_id 要unpin的页面ID
     * @param is_dirty 是否将页面标记为脏页（需要写回磁盘）
     * @return 操作成功返回true
     *
     * 重要：每次FetchPage后都必须调用UnpinPage，否则页面会一直被pin住无法被替换
     */
    bool UnpinPage(page_id_t page_id, bool is_dirty);

    /**
     * 强制刷新页面 - 将页面立即写回磁盘
     *
     * @param page_id 要刷新的页面ID
     * @return 刷新成功返回true
     *
     * 不管页面是否为脏页都会强制写回，通常用于事务提交时的强制持久化
     */
    bool FlushPage(page_id_t page_id);

    /**
     * 刷新所有页面 - 将所有脏页写回磁盘
     * 通常在数据库关闭时调用，确保所有数据都持久化到磁盘
     */
    void FlushAllPages();

    /**
     * 获取磁盘管理器 - 主要用于访问磁盘页面数量等信息
     * @return 磁盘管理器指针
     */
    DiskManager* GetDiskManager() { return disk_manager_.get(); }

    Page* GetSpecificPage(page_id_t page_id);

   private:
    // ===== 核心数据结构 =====

    /** 缓冲池大小 - 表示内存中最多能缓存多少个页面 */
    size_t pool_size_;

    /** 页面数组 - 真正的内存缓冲区，每个元素是一个4KB的页面 */
    Page* pages_;

    /** 磁盘管理器 - 负责实际的磁盘读写操作 */
    std::unique_ptr<DiskManager> disk_manager_;

    /** 页面替换器 - 实现LRU等替换算法，决定哪个页面被踢出内存 */
    std::unique_ptr<Replacer> replacer_;

    // ===== 映射和管理结构 =====

    /** 页面表 - 维护page_id到frame_id的映射关系
     *  这是缓冲池的核心索引，通过page_id快速找到页面在内存中的位置 */
    std::unordered_map<page_id_t, size_t> page_table_;

    /** 空闲列表 - 记录哪些frame当前是空闲的，可以直接使用
     *  优先使用空闲frame，避免不必要的页面替换 */
    std::list<size_t> free_list_;

    // ===== 并发控制 =====

    /** 互斥锁 - 保护缓冲池的并发访问安全
     *  所有public方法都会获取这个锁，确保线程安全 */
    std::mutex latch_;

    // ===== 辅助方法 =====

    /**
     * 寻找victim页面 - 使用LRU算法找到可以被替换的页面
     * @return 可以被替换的frame_id，如果所有页面都被pin住则返回-1
     */
    size_t FindVictimPage();

    /**
     * 更新页面元数据 - 重置页面到初始状态
     * @param page 要更新的页面
     * @param page_id 新的页面ID
     *
     * 主要用于页面复用时的清理工作
     */
    void UpdatePage(Page* page, page_id_t page_id);
};

}  // namespace SimpleRDBMS