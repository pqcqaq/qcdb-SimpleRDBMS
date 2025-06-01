/*
 * 文件: b_plus_tree.h
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述:
 * B+树索引的头文件定义，提供高效的键值对存储和检索功能，支持范围查询和并发访问
 */

#pragma once

#include <memory>
#include <mutex>

#include "buffer/buffer_pool_manager.h"
#include "index/b_plus_tree_page.h"

namespace SimpleRDBMS {

// 前向声明，避免循环包含
template <typename KeyType>
class BPlusTreeInternalPage;

/**
 * B+树索引实现类
 *
 * 设计思路：
 * 1. 这是一个磁盘友好的B+树实现，所有数据最终存储在叶子节点
 * 2. 内部节点只存储键和指向子节点的指针，用于快速导航
 * 3. 叶子节点之间通过链表连接，支持高效的范围查询
 * 4. 使用buffer pool管理页面，支持数据持久化
 * 5. 线程安全设计，支持并发访问
 *
 * @tparam KeyType 键的类型，支持int32_t, int64_t, float, double, string
 * @tparam ValueType 值的类型，通常是RID（Record ID）
 */
template <typename KeyType, typename ValueType>
class BPlusTree {
   public:
    /**
     * B+树构造函数
     * @param name 索引名称，用于在header page中唯一标识该索引
     * @param buffer_pool_manager 缓冲池管理器，负责页面的读写和缓存
     *
     * 功能说明：
     * - 初始化B+树的基本信息
     * - 尝试从磁盘加载已存在的根页面ID
     * - 如果是新索引，根页面ID会被设置为INVALID_PAGE_ID
     */
    BPlusTree(const std::string& name, BufferPoolManager* buffer_pool_manager);

    /**
     * B+树析构函数
     *
     * 功能说明：
     * - 清理资源，但不直接删除磁盘上的页面
     * - 页面的实际清理由buffer pool manager负责
     * - 确保析构过程的线程安全
     */
    ~BPlusTree();

    // ========================================================================
    // 点操作接口 - Point Operations
    // ========================================================================

    /**
     * 插入键值对
     * @param key 要插入的键
     * @param value 要插入的值
     * @param txn_id 事务ID，默认为-1（表示无事务）
     * @return 插入是否成功
     *
     * 功能说明：
     * - 如果键已存在，会更新对应的值
     * - 如果叶子页面满了，会自动触发页面分裂
     * - 分裂可能向上传播，最终可能产生新的根节点
     * - 整个操作是原子的，要么完全成功要么完全失败
     */
    bool Insert(const KeyType& key, const ValueType& value,
                txn_id_t txn_id = -1);

    /**
     * 删除指定键的记录
     * @param key 要删除的键
     * @param txn_id 事务ID，默认为-1（表示无事务）
     * @return 删除是否成功（如果键不存在返回false）
     *
     * 功能说明：
     * - 只有键存在时才返回true
     * - 删除后可能触发节点合并或重分布操作
     * - 如果根节点变空，树的高度可能会减少
     * - 维护叶子节点间的链表结构
     */
    bool Remove(const KeyType& key, txn_id_t txn_id = -1);

    /**
     * 查找指定键对应的值
     * @param key 要查找的键
     * @param value 输出参数，存储找到的值
     * @param txn_id 事务ID，默认为-1（表示无事务）
     * @return 查找是否成功（如果键不存在返回false）
     *
     * 功能说明：
     * - 从根节点开始向下查找，直到叶子节点
     * - 时间复杂度为O(log n)
     * - 只读操作，不会修改树结构
     */
    bool GetValue(const KeyType& key, ValueType* value, txn_id_t txn_id = -1);

    // ========================================================================
    // 迭代器接口 - Iterator for Range Scan
    // ========================================================================

    /**
     * B+树迭代器类
     *
     * 设计思路：
     * 1. 支持顺序遍历所有键值对
     * 2. 利用叶子节点间的链表结构实现高效的范围查询
     * 3. 前向迭代器，只支持++操作
     * 4. 自动处理页面边界的跨越
     *
     * 使用示例：
     * ```cpp
     * for (auto it = tree.Begin(); !it.IsEnd(); ++it) {
     *     auto [key, value] = *it;
     *     // 处理键值对
     * }
     * ```
     */
    class Iterator {
       public:
        /**
         * 迭代器构造函数
         * @param tree 所属的B+树指针
         * @param page_id 当前叶子页面的ID
         * @param index 在当前页面中的索引位置
         */
        Iterator(BPlusTree* tree, page_id_t page_id, int index);

        /**
         * 检查是否到达迭代结束位置
         * @return true表示已到达末尾，false表示还有元素
         */
        bool IsEnd() const;

        /**
         * 前进到下一个元素
         *
         * 实现思路：
         * - 如果当前页面还有元素，直接移动到下一个
         * - 如果当前页面已遍历完，跳转到下一个叶子页面
         * - 如果没有下一个页面，标记为结束
         */
        void operator++();

        /**
         * 解引用操作，获取当前键值对
         * @return 当前位置的键值对
         * @throws std::runtime_error 如果迭代器无效或越界
         */
        std::pair<KeyType, ValueType> operator*();

       private:
        BPlusTree* tree_;            // 所属的B+树
        page_id_t current_page_id_;  // 当前叶子页面ID
        int current_index_;          // 在当前页面中的索引
    };

    /**
     * 获取指向第一个键值对的迭代器
     * @return 指向最小键的迭代器
     *
     * 实现思路：
     * - 从根节点开始，一直向左走到最左边的叶子节点
     * - 返回指向该页面第一个元素的迭代器
     */
    Iterator Begin();

    /**
     * 获取指向第一个大于等于指定键的迭代器
     * @param key 起始键
     * @return 指向第一个大于等于key的键值对的迭代器
     *
     * 功能说明：
     * - 用于范围查询的起始位置
     * - 如果key不存在，返回第一个大于key的位置
     * - 支持半开区间查询：[key, end)
     */
    Iterator Begin(const KeyType& key);

    /**
     * 获取表示结束位置的迭代器
     * @return 结束迭代器
     *
     * 说明：
     * - 用于判断迭代是否结束
     * - 实际上是一个特殊的迭代器，page_id为INVALID_PAGE_ID
     */
    Iterator End();

   private:
    // ========================================================================
    // 私有成员变量 - Private Member Variables
    // ========================================================================

    std::string index_name_;                  // 索引名称，用于持久化标识
    BufferPoolManager* buffer_pool_manager_;  // 缓冲池管理器，不拥有所有权
    page_id_t root_page_id_;  // 根页面ID，INVALID_PAGE_ID表示空树
    std::mutex latch_;        // 互斥锁，保护并发访问

    // ========================================================================
    // 查找和插入的辅助函数 - Helper Functions for Search and Insertion
    // ========================================================================

    /**
     * 从根节点开始查找包含指定键的叶子页面
     * @param key 要查找的键
     * @param is_write_op 是否为写操作（当前未使用，预留用于锁管理）
     * @return 叶子页面指针，调用者负责unpin该页面
     *
     * 实现思路：
     * - 从根节点开始，逐层向下查找
     * - 在内部节点中使用二分查找确定下一个子节点
     * - 包含循环检测和深度限制，防止无限循环
     * - 验证每个页面的有效性
     */
    Page* FindLeafPage(const KeyType& key, bool is_write_op);

    /**
     * 向叶子页面插入键值对，必要时触发分裂
     * @param key 要插入的键
     * @param value 要插入的值
     * @param leaf 目标叶子页面
     * @return 插入是否成功
     *
     * 实现思路：
     * - 检查键是否已存在，存在则更新值
     * - 如果页面有空间，直接插入
     * - 如果页面满了，分裂成两个页面
     * - 更新叶子链表指针，维护顺序结构
     */
    bool InsertIntoLeaf(const KeyType& key, const ValueType& value,
                        BPlusTreeLeafPage<KeyType, ValueType>* leaf);

    /**
     * 将分裂产生的新键插入到父节点
     * @param old_node 原节点
     * @param key 要插入的键（分裂的中间键）
     * @param new_node 新节点
     *
     * 实现思路：
     * - 如果原节点是根节点，创建新的根节点
     * - 否则插入到现有的父节点中
     * - 如果父节点也满了，递归进行分裂
     * - 确保树的高度只在根分裂时增加
     */
    void InsertIntoParent(BPlusTreePage* old_node, const KeyType& key,
                          BPlusTreePage* new_node);

    // ========================================================================
    // 分裂操作 - Split Operation
    // ========================================================================

    /**
     * 分裂节点的通用模板函数
     * @param node 要分裂的节点
     * @return 分裂是否成功
     *
     * 说明：
     * - 这是一个模板函数，可以处理叶子节点和内部节点
     * - 当前主要由InsertIntoLeaf函数处理具体逻辑
     */
    template <typename N>
    bool Split(N* node);

    // ========================================================================
    // 删除操作的辅助函数 - Helper Functions for Deletion
    // ========================================================================

    /**
     * 判断节点删除后是否需要合并或重分布
     * @param node 要检查的节点
     * @return 是否需要进行合并或重分布
     *
     * 判断规则：
     * - 根节点：只有当大小为0时才需要调整
     * - 非根节点：当大小小于最大容量的一半时需要调整
     */
    bool ShouldCoalesceOrRedistribute(BPlusTreePage* node);

    /**
     * 执行节点的合并或重分布操作
     * @param node 需要调整的节点
     * @param txn_id 事务ID
     * @return 节点是否被删除
     *
     * 实现思路：
     * - 首先尝试与兄弟节点重分布元素
     * - 如果重分布后仍不满足要求，则进行合并
     * - 合并后可能需要递归调整父节点
     */
    bool CoalesceOrRedistribute(BPlusTreePage* node, txn_id_t txn_id);

    /**
     * 调整根节点（当根节点变空时）
     * @param old_root_node 原根节点
     * @return 根节点是否被删除
     *
     * 处理情况：
     * - 如果是内部根节点且只有一个子节点，提升子节点为新根
     * - 如果是叶子根节点且为空，整个树变为空
     * - 更新根页面ID并持久化
     */
    bool AdjustRoot(BPlusTreePage* old_root_node);

    // ========================================================================
    // 合并和重分布操作 - Coalesce and Redistribute Operations
    // ========================================================================

    /**
     * 合并节点与其兄弟节点
     * @param neighbor_node 兄弟节点
     * @param node 当前节点
     * @param parent 父节点
     * @param index 在父节点中的索引
     * @param txn_id 事务ID
     * @return 节点是否被删除
     *
     * 实现思路：
     * - 将一个节点的所有元素移动到兄弟节点
     * - 更新父节点，移除指向被合并节点的条目
     * - 对于叶子节点，更新链表指针
     * - 删除空的节点，释放页面
     */
    template <typename N>
    bool Coalesce(N** neighbor_node, N** node,
                  BPlusTreeInternalPage<KeyType>** parent, int index,
                  txn_id_t txn_id);

    /**
     * 在兄弟节点间重分布元素
     * @param neighbor_node 兄弟节点
     * @param node 当前节点
     * @param index 在父节点中的索引
     *
     * 实现思路：
     * - 从兄弟节点借用一些元素
     * - 更新父节点中的分隔键
     * - 确保两个节点的负载相对均衡
     * - 维护B+树的有序性
     */
    template <typename N>
    void Redistribute(N* neighbor_node, N* node, int index);

    // ========================================================================
    // 根页面ID管理 - Root Page ID Management
    // ========================================================================

    /**
     * 更新header page中存储的根页面ID
     * @param root_page_id 新的根页面ID
     *
     * 实现思路：
     * - 使用固定的page 1作为header page
     * - 根据索引名称的hash值分配槽位
     * - 在对应槽位写入hash值和根页面ID
     * - 立即刷新到磁盘，确保持久化
     */
    void UpdateRootPageId(page_id_t root_page_id);

    /**
     * 从header page加载根页面ID
     *
     * 实现思路：
     * - 计算索引名称的hash值
     * - 在header page中查找对应的槽位
     * - 验证hash值匹配，确保找到正确的索引
     * - 验证根页面的有效性
     */
    void LoadRootPageId();

    /**
     * 获取header page的页面ID
     * @return header page的页面ID（固定为1）
     *
     * 设计说明：
     * - 使用固定的page 1作为所有索引的元数据页面
     * - 在该页面中为每个索引分配槽位存储根页面ID
     * - 避免了复杂的元数据管理机制
     */
    page_id_t GetHeaderPageId() const;
};

}  // namespace SimpleRDBMS