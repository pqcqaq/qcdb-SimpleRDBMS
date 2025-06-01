/*
 * 文件: disk_manager.h
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述: 磁盘管理器头文件，定义了database文件的底层I/O接口
 *       负责页面级别的读写操作和存储空间管理
 */

#pragma once

#include <fstream>
#include <mutex>
#include <string>
#include <vector>

#include "common/config.h"

namespace SimpleRDBMS {

/**
 * 磁盘管理器类 - RDBMS存储层的底层组件
 *
 * 职责说明：
 * 1. 管理database文件的打开/关闭
 * 2. 提供页面级别的读写接口
 * 3. 负责页面ID的分配和回收
 * 4. 维护空闲页面列表，支持页面复用
 * 5. 确保磁盘I/O操作的线程安全性
 *
 * 设计思路：
 * - 使用fstream进行文件I/O操作
 * - 通过mutex保证多线程环境下的安全性
 * - 采用页面复用机制减少磁盘空间浪费
 * - 为上层Buffer Pool Manager提供统一的存储接口
 */
class DiskManager {
   public:
    /**
     * 构造函数 - 初始化磁盘管理器
     * @param db_file database文件的路径
     *
     * 功能：打开或创建database文件，初始化页面计数器
     */
    explicit DiskManager(const std::string& db_file);

    /**
     * 析构函数 - 清理资源
     * 确保database文件正确关闭
     */
    ~DiskManager();

    /**
     * 从磁盘读取指定页面的数据
     * @param page_id 要读取的页面ID
     * @param page_data
     * 存储读取数据的buffer（调用者负责分配PAGE_SIZE大小的内存）
     *
     * 注意：如果页面不存在或读取失败会抛出StorageException
     */
    void ReadPage(page_id_t page_id, char* page_data);

    /**
     * 将页面数据写入磁盘
     * @param page_id 要写入的页面ID
     * @param page_data 要写入的数据buffer（必须是PAGE_SIZE大小）
     *
     * 功能：写入数据并强制刷新到磁盘，确保数据持久化
     */
    void WritePage(page_id_t page_id, const char* page_data);

    /**
     * 分配一个新的页面ID
     * @return 新分配的page_id
     *
     * 策略：优先复用已释放的页面，如果没有则分配新页面
     */
    page_id_t AllocatePage();

    /**
     * 释放一个页面，将其标记为可复用
     * @param page_id 要释放的页面ID
     *
     * 注意：只是将页面加入空闲列表，不会清除页面内容
     */
    void DeallocatePage(page_id_t page_id);

    /**
     * 获取当前database文件的总页面数
     * @return 页面数量
     */
    int GetNumPages() const { return num_pages_; }

   private:
    std::string db_file_name_;           // database文件路径
    std::fstream db_file_;               // 文件流对象，用于I/O操作
    int num_pages_;                      // 当前database文件的总页面数
    int next_page_id_;                   // 下一个可分配的页面ID
    std::mutex latch_;                   // 互斥锁，保证线程安全
    std::vector<page_id_t> free_pages_;  // 空闲页面列表，用于页面复用
};

}  // namespace SimpleRDBMS