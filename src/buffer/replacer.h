/*
 * 文件: replacer.h
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述: 页面替换算法的抽象基类，定义了缓冲池页面替换的统一接口
 */

#pragma once

#include "common/config.h"

namespace SimpleRDBMS {

/**
 * 页面替换器抽象基类
 *
 * 设计理念：
 * - 为不同的页面替换算法提供统一的接口（如LRU、LFU、Clock等）
 * - 使用虚函数实现多态，支持运行时算法切换
 * - 定义了缓冲池管理中页面替换的核心操作
 *
 * 主要职责：
 * 1. 管理可替换页面的集合
 * 2. 提供页面pin/unpin机制
 * 3. 根据替换策略选择victim页面
 * 4. 维护替换器状态信息
 */
class Replacer {
   public:
    /**
     * 虚析构函数 - 确保派生类正确析构
     */
    virtual ~Replacer() = default;

    /**
     * Pin操作 - 将页面从可替换集合中移除
     * @param frame_id 要pin的frame ID
     *
     * 当页面正在被使用时调用：
     * - 表示该页面当前不能被替换
     * - 从replacer的管理范围中移除
     * - 通常在页面被访问时调用
     */
    virtual void Pin(size_t frame_id) = 0;

    /**
     * Unpin操作 - 将页面加入可替换集合
     * @param frame_id 要unpin的frame ID
     *
     * 当页面使用完毕时调用：
     * - 表示该页面可以被替换
     * - 加入replacer的管理范围
     * - 根据具体算法更新页面的优先级/位置
     */
    virtual void Unpin(size_t frame_id) = 0;

    /**
     * Victim操作 - 根据替换策略选择要驱逐的页面
     * @param frame_id 输出参数，返回被选中的frame ID
     * @return 是否成功找到可替换的页面
     *
     * 替换策略的核心逻辑：
     * - 根据算法（LRU/LFU/Clock等）选择最佳替换候选
     * - 如果没有可替换页面则返回false
     * - 选中的页面会从replacer中移除
     */
    virtual bool Victim(size_t* frame_id) = 0;

    /**
     * Size操作 - 获取当前可替换页面的数量
     * @return 在replacer中管理的页面数量
     *
     * 用途：
     * - 监控缓冲池状态
     * - 判断是否还有可替换的页面
     * - 调试和性能分析
     */
    virtual size_t Size() const = 0;
};

}  // namespace SimpleRDBMS