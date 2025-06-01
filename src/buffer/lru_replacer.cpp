/*
 * 文件: lru_replacer.cpp
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述: LRU页面替换算法实现，用于缓冲池的页面替换策略
 */

#include "buffer/lru_replacer.h"

namespace SimpleRDBMS {

/**
 * 构造函数 - 初始化LRU替换器
 * @param num_pages 缓冲池中页面的最大数量
 *
 * 实现思路：
 * - 这里只需要保存页面数量，list和map会自动初始化为空
 * - 不需要预分配空间，STL容器会自动管理内存
 */
LRUReplacer::LRUReplacer(size_t num_pages) : num_pages_(num_pages) {
    // 构造函数不需要额外操作，成员变量会自动初始化
}

/**
 * 析构函数 - 清理LRU替换器
 *
 * 实现思路：
 * - STL容器会自动释放内存，不需要手动清理
 * - 这里保持默认析构即可
 */
LRUReplacer::~LRUReplacer() {
    // STL容器会自动清理，不需要手动操作
}

/**
 * Pin操作 - 将页面从可替换列表中移除
 * @param frame_id 要pin的frame ID
 *
 * 实现思路：
 * 1. 加锁保护临界区
 * 2. 在map中查找该frame是否存在
 * 3. 如果存在，从list和map中同时移除
 * 4. Pin意味着该页面正在被使用，不能被替换
 */
void LRUReplacer::Pin(size_t frame_id) {
    std::unique_lock<std::mutex> lock(latch_);

    // 在map中查找这个frame
    auto it = lru_map_.find(frame_id);
    if (it == lru_map_.end()) {
        // frame不在replacer中，直接返回
        return;
    }

    // 从LRU链表中移除这个frame
    lru_list_.erase(it->second);

    // 从map中移除这个frame
    lru_map_.erase(frame_id);
}

/**
 * Unpin操作 - 将页面加入可替换列表
 * @param frame_id 要unpin的frame ID
 *
 * 实现思路：
 * 1. 加锁保护临界区
 * 2. 检查frame是否已经在replacer中，避免重复添加
 * 3. 将frame添加到list末尾（最近使用位置）
 * 4. 更新map中的映射关系
 * 5. Unpin意味着页面使用完毕，可以被替换
 */
void LRUReplacer::Unpin(size_t frame_id) {
    std::unique_lock<std::mutex> lock(latch_);

    // 检查frame是否已经在replacer中
    auto it = lru_map_.find(frame_id);
    if (it != lru_map_.end()) {
        // frame已经存在，不需要重复添加
        return;
    }

    // 检查是否超过最大页面数限制
    if (lru_list_.size() >= num_pages_) {
        // 理论上不应该超过，这里做保护性检查
        return;
    }

    // 将frame添加到list末尾（最近使用的位置）
    lru_list_.push_back(frame_id);

    // 获取刚添加元素的迭代器，更新map
    auto list_it = lru_list_.end();
    --list_it;  // 指向刚添加的最后一个元素
    lru_map_[frame_id] = list_it;
}

/**
 * Victim操作 - 选择一个页面进行替换
 * @param frame_id 输出参数，返回被选中替换的frame ID
 * @return 是否成功找到可替换的页面
 *
 * 实现思路：
 * 1. 加锁保护临界区
 * 2. 检查list是否为空
 * 3. 选择list头部的frame（最久未使用）
 * 4. 从list和map中移除该frame
 * 5. 返回被替换的frame ID
 */
bool LRUReplacer::Victim(size_t* frame_id) {
    std::unique_lock<std::mutex> lock(latch_);

    // 检查是否有可替换的frame
    if (lru_list_.empty()) {
        return false;
    }

    // victim选择list头部的frame（最久未使用的）
    *frame_id = lru_list_.front();

    // 从list中移除victim
    lru_list_.pop_front();

    // 从map中移除victim
    lru_map_.erase(*frame_id);

    return true;
}

/**
 * Size操作 - 获取当前可替换页面的数量
 * @return 当前在replacer中的页面数量
 *
 * 实现思路：
 * - 简单返回list的大小
 * - 需要加锁保护并发访问
 */
size_t LRUReplacer::Size() const {
    std::unique_lock<std::mutex> lock(latch_);
    return lru_list_.size();
}

}  // namespace SimpleRDBMS