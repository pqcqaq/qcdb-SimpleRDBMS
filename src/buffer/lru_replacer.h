/*
 * 文件: lru_replacer.h
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述: LRU页面替换算法头文件，实现基于最近最少使用策略的页面替换器
 */

#pragma once

#include <list>
#include <mutex>
#include <unordered_map>

#include "buffer/replacer.h"

namespace SimpleRDBMS {

/**
 * LRU页面替换器类
 *
 * 核心设计思路：
 * - 使用双向链表维护页面的使用顺序，头部是最久未使用，尾部是最近使用
 * - 使用哈希表存储frame_id到链表节点的映射，实现O(1)查找
 * - 通过Pin/Unpin机制控制页面是否可被替换
 * - 线程安全：所有操作都有mutex保护
 *
 * 数据结构组合：
 * - std::list<size_t>：维护LRU顺序的双向链表
 * - std::unordered_map：快速定位链表中的节点位置
 * - std::mutex：保证线程安全的并发访问
 */
class LRUReplacer : public Replacer {
   public:
    /**
     * 构造函数
     * @param num_pages 缓冲池中页面的总数量
     */
    explicit LRUReplacer(size_t num_pages);

    /**
     * 析构函数 - 清理资源
     */
    ~LRUReplacer() override;

    /**
     * Pin操作 - 将页面标记为正在使用，从可替换列表中移除
     * @param frame_id 要pin的frame ID
     *
     * 当页面被访问时调用，表示该页面正在被使用，不应该被替换
     */
    void Pin(size_t frame_id) override;

    /**
     * Unpin操作 - 将页面标记为可替换，加入LRU列表
     * @param frame_id 要unpin的frame ID
     *
     * 当页面使用完毕时调用，表示该页面可以被替换
     * 会将页面放到LRU列表的尾部（最近使用位置）
     */
    void Unpin(size_t frame_id) override;

    /**
     * Victim操作 - 选择一个页面进行替换
     * @param frame_id 输出参数，返回被选中的frame ID
     * @return 是否成功找到可替换的页面
     *
     * 选择最久未使用的页面（LRU列表头部）进行替换
     */
    bool Victim(size_t* frame_id) override;

    /**
     * 获取当前可替换页面的数量
     * @return 在replacer中的页面数量
     */
    size_t Size() const override;

   private:
    // 缓冲池中页面的总数量，用于容量检查
    size_t num_pages_;

    // LRU双向链表：头部是最久未使用(LRU)，尾部是最近使用(MRU)
    // 存储的是frame_id
    std::list<size_t> lru_list_;

    // 哈希表：frame_id -> 链表中对应节点的迭代器
    // 用于O(1)时间复杂度查找和删除链表中的节点
    std::unordered_map<size_t, std::list<size_t>::iterator> lru_map_;

    // 互斥锁：保护数据结构的并发访问
    // 使用mutable允许在const函数中加锁
    mutable std::mutex latch_;
};

}  // namespace SimpleRDBMS