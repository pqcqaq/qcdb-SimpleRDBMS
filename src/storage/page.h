/*
 * 文件: page.h
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述: Page 是数据库中的基础页结构，封装了页数据、元信息（如
 * page_id、脏页状态、pin_count 等）以及并发访问控制（读写锁）
 */

#pragma once

#include <cstring>
#include <mutex>
#include <shared_mutex>

#include "common/config.h"

namespace SimpleRDBMS {

// Page 类表示数据库中的一个物理页（也可以看作是内存缓存页）
// 每个页都有自己的元数据 + 数据区 + 锁，便于后续做并发控制和缓存管理
class Page {
   public:
    Page();
    ~Page();

    // 禁用拷贝构造和赋值操作，防止意外复制
    Page(const Page&) = delete;
    Page& operator=(const Page&) = delete;

    // 获取页内的数据指针（读写接口）
    char* GetData() { return data_; }
    const char* GetData() const { return data_; }

    // 获取 / 设置当前页的唯一标识 ID
    page_id_t GetPageId() const { return page_id_; }
    void SetPageId(page_id_t page_id) { page_id_ = page_id; }

    // Pin计数相关：记录当前有多少用户在使用这个页
    // 当 pin_count > 0 时，页不能被替换掉
    void IncreasePinCount() { pin_count_++; }
    void DecreasePinCount() { pin_count_--; }
    int GetPinCount() const { return pin_count_; }

    // 设置 / 获取脏页标志：表示这个页有没有被修改过
    // 用于写回磁盘判断
    bool IsDirty() const { return is_dirty_; }
    void SetDirty(bool dirty) { is_dirty_ = dirty; }

    // 设置 / 获取日志序列号（LSN），用于支持 WAL 日志恢复
    lsn_t GetLSN() const { return lsn_; }
    void SetLSN(lsn_t lsn) { lsn_ = lsn; }

    // 读写锁控制，保证并发访问时线程安全
    // 写锁（独占）
    void WLatch() { latch_.lock(); }
    void WUnlatch() { latch_.unlock(); }
    // 读锁（共享）
    void RLatch() { latch_.lock_shared(); }
    void RUnlatch() { latch_.unlock_shared(); }

   protected:
    // 数据区，一页大小固定为 PAGE_SIZE 字节
    char data_[PAGE_SIZE];

    // 页的唯一 ID，用于在磁盘或缓冲池中定位
    page_id_t page_id_;

    // 引用计数，用于缓冲管理中的替换策略
    int pin_count_;

    // 脏页标志，标识该页是否被修改过
    bool is_dirty_;

    // 页对应的日志序列号（用于恢复时重放日志）
    lsn_t lsn_;

    // 用于并发控制的读写锁
    std::shared_mutex latch_;
};

}  // namespace SimpleRDBMS
