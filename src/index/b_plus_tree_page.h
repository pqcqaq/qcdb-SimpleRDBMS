/*
 * 文件: b_plus_tree_page.h
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述: B+树页面类定义，包含基类BPlusTreePage、叶子页面和内部页面的声明
 */

#pragma once

#include <cstring>
#include <stdexcept>
#include <string>

#include "common/config.h"
#include "common/types.h"
#include "storage/page.h"

namespace SimpleRDBMS {

// 前向声明
class BufferPoolManager;

/**
 * 索引页面类型枚举 - 用来区分是叶子页面还是内部页面
 */
enum class IndexPageType {
    INVALID = 0,   // 无效页面
    LEAF_PAGE,     // 叶子页面，存储实际数据
    INTERNAL_PAGE  // 内部页面，存储路由信息
};

/**
 * B+树页面基类 - 所有B+树页面的公共属性和操作
 *
 * 设计思路：
 * - 每个页面都有类型、大小、父页面ID等基本信息
 * - 提供统一的接口供子类使用
 * - 大小管理很重要，防止页面溢出
 */
class BPlusTreePage {
   public:
    // 页面类型判断 - 这些方法经常被用到，所以设计成inline
    bool IsLeafPage() const { return page_type_ == IndexPageType::LEAF_PAGE; }
    bool IsRootPage() const { return parent_page_id_ == INVALID_PAGE_ID; }

    // 页面类型操作
    void SetPageType(IndexPageType type) { page_type_ = type; }
    IndexPageType GetPageType() const { return page_type_; }

    // 页面大小管理 - size_表示当前页面存储了多少个元素
    int GetSize() const { return size_; }
    void SetSize(int size);         // 实现在cpp中，带安全检查
    void IncreaseSize(int amount);  // 实现在cpp中，防止溢出

    // 页面容量管理 - max_size_表示页面最多能存多少元素
    int GetMaxSize() const { return max_size_; }
    void SetMaxSize(int max_size) { max_size_ = max_size; }

    // 父子关系管理 - B+树是一个树结构，需要维护父子关系
    page_id_t GetParentPageId() const { return parent_page_id_; }
    void SetParentPageId(page_id_t parent_id) { parent_page_id_ = parent_id; }

    // 页面ID管理
    page_id_t GetPageId() const { return page_id_; }
    void SetPageId(page_id_t page_id) { page_id_ = page_id; }

   protected:
    IndexPageType page_type_;   // 页面类型（叶子或内部）
    int size_;                  // 当前存储的元素数量
    int max_size_;              // 最大容量
    page_id_t parent_page_id_;  // 父页面ID
    page_id_t page_id_;         // 当前页面ID
};

/**
 * B+树叶子页面 - 存储实际的key-value对
 *
 * 存储布局：
 * [header] [key1,value1] [key2,value2] ... [keyN,valueN] [next_page_id]
 *
 * 设计特点：
 * - 叶子页面之间通过next_page_id形成链表，方便范围查询
 * - 使用flexible array member (data_[0])来存储变长数据
 * - 键值对按key排序存储，支持二分查找
 */
template <typename KeyType, typename ValueType>
class BPlusTreeLeafPage : public BPlusTreePage {
   public:
    /**
     * 页面初始化 - 设置页面基本属性和计算最大容量
     */
    void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID);

    // Key/Value访问操作 - 提供对存储数据的基本访问
    KeyType KeyAt(int index) const;      // 获取第index个key
    ValueType ValueAt(int index) const;  // 获取第index个value

    void SetKeyAt(int index, const KeyType& key);        // 设置第index个key
    void SetValueAt(int index, const ValueType& value);  // 设置第index个value

    /**
     * 查找key的插入位置 - 使用二分查找找到合适的位置
     * 返回第一个>=key的位置，如果所有key都<target_key则返回size
     */
    int KeyIndex(const KeyType& key) const;

    // 插入删除操作 - B+树的核心操作
    bool Insert(const KeyType& key, const ValueType& value);  // 插入键值对
    bool Delete(const KeyType& key);                          // 删除指定key

    // 分裂合并操作 - 当页面满了或太空时需要这些操作
    void MoveHalfTo(BPlusTreeLeafPage* recipient);  // 分裂时移动一半数据
    void MoveAllTo(BPlusTreeLeafPage* recipient);   // 合并时移动所有数据
    void MoveFirstToEndOf(
        BPlusTreeLeafPage* recipient);  // 重分布：移动第一个元素
    void MoveLastToFrontOf(
        BPlusTreeLeafPage* recipient);  // 重分布：移动最后一个元素

    // 叶子页面链表操作 - 叶子页面之间形成有序链表，方便范围扫描
    page_id_t GetNextPageId() const { return next_page_id_; }
    void SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

   private:
    page_id_t next_page_id_;  // 指向下一个叶子页面，形成链表
    // 灵活数组成员 - 实际的key-value对存储在这里
    // 这是C的一个特性，允许结构体最后一个成员是大小为0的数组
    char data_[0];
};

/**
 * B+树内部页面 - 存储路由信息，不存储实际数据
 *
 * 存储布局：
 * [header] [value0] [key1,value1] [key2,value2] ... [keyN,valueN]
 *
 * 设计说明：
 * - 内部页面存储的是"路标"，告诉查找算法应该走哪个子树
 * - key[i]是第i个和第i+1个子节点的分割点
 * - value[i]存储的是子页面的page_id
 * - 比较特殊的是value0，它没有对应的key，是最左边子树的指针
 */
template <typename KeyType>
class BPlusTreeInternalPage : public BPlusTreePage {
   public:
    /**
     * 页面初始化 - 和叶子页面类似，但容量计算不同
     */
    void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID);

    // Key操作 - 内部页面的key是路由信息
    KeyType KeyAt(int index) const;  // 获取第index个key（index从1开始）
    void SetKeyAt(int index, const KeyType& key);  // 设置第index个key
    int KeyIndex(const KeyType& key) const;        // 查找key应该走哪个子树

    // 子页面指针操作 - value存储的是子页面的page_id
    page_id_t ValueAt(int index) const;           // 获取第index个子页面ID
    void SetValueAt(int index, page_id_t value);  // 设置第index个子页面ID
    int ValueIndex(page_id_t value) const;        // 查找子页面ID的位置

    // 插入删除操作 - 通常发生在子页面分裂或合并时
    void InsertNodeAfter(page_id_t old_value, const KeyType& new_key,
                         page_id_t new_value);
    void Remove(int index);  // 移除第index个entry

    // 分裂合并操作 - 比叶子页面复杂，需要处理子页面的父指针更新
    void MoveHalfTo(BPlusTreeInternalPage* recipient,
                    BufferPoolManager* buffer_pool_manager);
    void MoveAllTo(BPlusTreeInternalPage* recipient, const KeyType& middle_key,
                   BufferPoolManager* buffer_pool_manager);
    void MoveFirstToEndOf(BPlusTreeInternalPage* recipient,
                          const KeyType& middle_key,
                          BufferPoolManager* buffer_pool_manager);
    void MoveLastToFrontOf(BPlusTreeInternalPage* recipient,
                           const KeyType& middle_key,
                           BufferPoolManager* buffer_pool_manager);

   private:
    // 灵活数组成员 - 存储key和子页面ID
    // 布局：[value0] [key1] [value1] [key2] [value2] ...
    char data_[0];
};

}  // namespace SimpleRDBMS