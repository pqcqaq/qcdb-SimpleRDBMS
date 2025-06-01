/*
 * 文件: b_plus_tree_page.cpp
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述: B+树页面实现，包含叶子页面和内部页面的核心操作逻辑
 */

#include "index/b_plus_tree_page.h"

#include <algorithm>
#include <cstring>

#include "buffer/buffer_pool_manager.h"
#include "common/debug.h"
#include "common/types.h"

namespace SimpleRDBMS {

/**
 * 叶子页面初始化 - 设置页面的基本属性和计算最大容量
 * 这里需要精确计算能放多少个key-value对，给分裂操作留点空间
 */
template <typename KeyType, typename ValueType>
void BPlusTreeLeafPage<KeyType, ValueType>::Init(page_id_t page_id,
                                                 page_id_t parent_id) {
    SetPageType(IndexPageType::LEAF_PAGE);
    SetPageId(page_id);
    SetParentPageId(parent_id);
    SetSize(0);

    // 计算页面能容纳多少个key-value对
    // header占用的空间 = BPlusTreePage基类大小 + next_page_id_字段
    size_t header_size = sizeof(BPlusTreePage) + sizeof(next_page_id_);
    size_t available_space = PAGE_SIZE - header_size;
    size_t pair_size = sizeof(KeyType) + sizeof(ValueType);

    // 理论上能放多少对数据
    int theoretical_max = static_cast<int>(available_space / pair_size);

    // 实际设置时要保守一点，为分裂操作预留1个元素的空间
    // 这样可以提高空间利用率，同时确保分裂时有足够空间
    SetMaxSize(std::max(16, theoretical_max - 1));

    next_page_id_ = INVALID_PAGE_ID;
}

/**
 * 设置页面大小 - 加个负数检查，防止出现奇怪的bug
 */
void BPlusTreePage::SetSize(int size) {
    if (size < 0) {
        LOG_ERROR("BPlusTreePage::SetSize: Attempting to set negative size "
                  << size);
        size_ = 0;
    } else {
        size_ = size;
    }
}

/**
 * 增加页面大小 - 同样要防止size变成负数
 */
void BPlusTreePage::IncreaseSize(int amount) {
    int new_size = size_ + amount;
    if (new_size < 0) {
        LOG_ERROR("BPlusTreePage::IncreaseSize: Size would become negative: "
                  << size_ << " + " << amount << " = " << new_size);
        size_ = 0;
    } else {
        size_ = new_size;
    }
}

/**
 * 获取指定位置的key - 简单的内存访问，但要检查边界
 */
template <typename KeyType, typename ValueType>
KeyType BPlusTreeLeafPage<KeyType, ValueType>::KeyAt(int index) const {
    if (index < 0 || index >= GetSize()) {
        throw std::out_of_range("Index out of range");
    }

    // 直接计算内存offset，每个entry就是一个key+value的连续存储
    char* data_ptr = const_cast<char*>(data_);
    size_t offset = index * (sizeof(KeyType) + sizeof(ValueType));
    return *reinterpret_cast<KeyType*>(data_ptr + offset);
}

/**
 * 获取指定位置的value - 和KeyAt类似，只是offset要跳过key的部分
 */
template <typename KeyType, typename ValueType>
ValueType BPlusTreeLeafPage<KeyType, ValueType>::ValueAt(int index) const {
    if (index < 0 || index >= GetSize()) {
        throw std::out_of_range("Index out of range");
    }

    // value在key后面，所以offset要加上key的大小
    char* data_ptr = const_cast<char*>(data_);
    size_t offset =
        index * (sizeof(KeyType) + sizeof(ValueType)) + sizeof(KeyType);
    return *reinterpret_cast<ValueType*>(data_ptr + offset);
}

/**
 * 设置指定位置的key - 直接内存写入
 */
template <typename KeyType, typename ValueType>
void BPlusTreeLeafPage<KeyType, ValueType>::SetKeyAt(int index,
                                                     const KeyType& key) {
    if (index < 0 || index >= GetMaxSize()) {
        throw std::out_of_range("Index out of range");
    }

    size_t offset = index * (sizeof(KeyType) + sizeof(ValueType));
    *reinterpret_cast<KeyType*>(data_ + offset) = key;
}

/**
 * 设置指定位置的value - 和SetKeyAt类似
 */
template <typename KeyType, typename ValueType>
void BPlusTreeLeafPage<KeyType, ValueType>::SetValueAt(int index,
                                                       const ValueType& value) {
    if (index < 0 || index >= GetMaxSize()) {
        throw std::out_of_range("Index out of range");
    }

    size_t offset =
        index * (sizeof(KeyType) + sizeof(ValueType)) + sizeof(KeyType);
    *reinterpret_cast<ValueType*>(data_ + offset) = value;
}

/**
 * 查找key应该插入的位置 - 用二分查找找到第一个>=key的位置
 * 这是B+树操作的核心，决定了插入和查找的正确性
 */
template <typename KeyType, typename ValueType>
int BPlusTreeLeafPage<KeyType, ValueType>::KeyIndex(const KeyType& key) const {
    if (GetSize() == 0) {
        return 0;  // 空页面直接返回0
    }

    // 标准的lower_bound二分查找逻辑
    // 找到第一个不小于key的位置
    int left = 0, right = GetSize();

    while (left < right) {
        int mid = left + (right - left) / 2;
        KeyType mid_key = KeyAt(mid);

        if (mid_key < key) {
            left = mid + 1;  // key在右半部分
        } else {
            right = mid;  // key在左半部分（包括mid）
        }
    }

    return left;  // 返回应该插入的位置
}

/**
 * 内部页面的key查找 - 内部页面的逻辑稍微复杂一点
 * 内部页面存储的是路由信息，要找到key应该走哪个子树
 */
template <typename KeyType>
int BPlusTreeInternalPage<KeyType>::KeyIndex(const KeyType& key) const {
    int size = GetSize();
    if (size <= 0) {
        LOG_WARN("BPlusTreeInternalPage::KeyIndex: Invalid size " << size);
        return 0;
    }

    // 做个合理性检查，防止内存被搞坏了
    const int MAX_REASONABLE_SIZE = (PAGE_SIZE - sizeof(BPlusTreePage)) /
                                    (sizeof(KeyType) + sizeof(page_id_t));
    if (size > MAX_REASONABLE_SIZE) {
        LOG_ERROR("BPlusTreeInternalPage::KeyIndex: Size "
                  << size << " exceeds reasonable limit "
                  << MAX_REASONABLE_SIZE);
        return 0;
    }

    try {
        // 在内部页面中，key[i]是第i个和第i+1个子节点的分割点
        // 如果search_key < key[i]，那么应该走第i-1个子节点
        for (int i = 1; i <= size; i++) {
            if (i > GetMaxSize()) {
                LOG_ERROR("BPlusTreeInternalPage::KeyIndex: Index "
                          << i << " exceeds max size " << GetMaxSize());
                return 0;
            }
            if (key < KeyAt(i)) {
                return i - 1;  // 走第i-1个子节点
            }
        }
    } catch (const std::exception& e) {
        LOG_ERROR("BPlusTreeInternalPage::KeyIndex: Exception at size "
                  << size << ": " << e.what());
        return 0;
    }

    return size;  // 如果key比所有分割键都大，走最后一个子节点
}

/**
 * 叶子页面插入操作 - B+树最核心的操作之一
 * 需要处理：1.页面满了的情况 2.key已存在的情况 3.数据移动
 */
template <typename KeyType, typename ValueType>
bool BPlusTreeLeafPage<KeyType, ValueType>::Insert(const KeyType& key,
                                                   const ValueType& value) {
    // 先检查页面是否已满，满了就不能插入了，需要调用者处理分裂
    if (GetSize() >= GetMaxSize()) {
        return false;
    }

    int insert_index = KeyIndex(key);

    // 检查key是否已经存在，存在就直接更新value
    if (insert_index < GetSize() && KeyAt(insert_index) == key) {
        SetValueAt(insert_index, value);
        return true;
    }

    // key不存在，需要插入新的key-value对
    // 先把后面的数据往后移动，给新数据腾地方
    size_t pair_size = sizeof(KeyType) + sizeof(ValueType);
    int move_count = GetSize() - insert_index;
    if (move_count > 0) {
        std::memmove(data_ + (insert_index + 1) * pair_size,
                     data_ + insert_index * pair_size, move_count * pair_size);
    }

    // 插入新的键值对
    SetKeyAt(insert_index, key);
    SetValueAt(insert_index, value);
    IncreaseSize(1);

    return true;
}

/**
 * 叶子页面删除操作 - 找到key然后删除，要处理数据移动
 */
template <typename KeyType, typename ValueType>
bool BPlusTreeLeafPage<KeyType, ValueType>::Delete(const KeyType& key) {
    if (GetSize() <= 0) {
        LOG_WARN(
            "BPlusTreeLeafPage::Delete: Attempting to delete from empty page");
        return false;
    }

    int delete_index = KeyIndex(key);
    if (delete_index >= GetSize() || KeyAt(delete_index) != key) {
        return false;  // key不存在
    }

    // 把后面的数据往前移动，覆盖要删除的数据
    size_t pair_size = sizeof(KeyType) + sizeof(ValueType);
    int move_count = GetSize() - delete_index - 1;
    if (move_count > 0) {
        std::memmove(data_ + delete_index * pair_size,
                     data_ + (delete_index + 1) * pair_size,
                     move_count * pair_size);
    }

    // 更新size，确保不会变成负数
    if (GetSize() > 0) {
        IncreaseSize(-1);
    } else {
        LOG_WARN(
            "BPlusTreeLeafPage::Delete: Page size already 0, cannot decrease");
    }
    return true;
}

/**
 * 页面分裂时移动一半数据 - 把当前页面的后半部分数据移动到新页面
 * 分裂策略：原页面保留较少元素，新页面分到较多元素
 */
template <typename KeyType, typename ValueType>
void BPlusTreeLeafPage<KeyType, ValueType>::MoveHalfTo(
    BPlusTreeLeafPage* recipient) {
    int total_size = GetSize();
    int split_point = (total_size + 1) / 2;  // 向上取整，原页面保留较少元素
    int move_size = total_size - split_point;

    if (move_size <= 0) {
        return;  // 没有数据需要移动
    }

    // 直接内存拷贝，效率比较高
    size_t pair_size = sizeof(KeyType) + sizeof(ValueType);
    size_t move_bytes = move_size * pair_size;

    // 将后半部分数据复制到新页面
    std::memcpy(recipient->data_, data_ + split_point * pair_size, move_bytes);
    recipient->SetSize(move_size);

    // 更新当前页面的大小
    SetSize(split_point);

    // 清除移动的数据区域（可选，但有助于调试）
    std::memset(data_ + split_point * pair_size, 0, move_bytes);
}

/**
 * 页面合并时移动所有数据 - 把当前页面的所有数据移动到接收页面
 */
template <typename KeyType, typename ValueType>
void BPlusTreeLeafPage<KeyType, ValueType>::MoveAllTo(
    BPlusTreeLeafPage* recipient) {
    size_t pair_size = sizeof(KeyType) + sizeof(ValueType);
    size_t move_bytes = GetSize() * pair_size;

    // 复制所有数据到接收页面的末尾
    std::memcpy(recipient->data_ + recipient->GetSize() * pair_size, data_,
                move_bytes);
    recipient->IncreaseSize(GetSize());

    // 清空当前页面
    SetSize(0);
}

/**
 * 重分布操作：移动第一个元素到目标页面末尾
 */
template <typename KeyType, typename ValueType>
void BPlusTreeLeafPage<KeyType, ValueType>::MoveFirstToEndOf(
    BPlusTreeLeafPage* recipient) {
    if (GetSize() == 0) return;

    // 先插入到目标页面，再从当前页面删除
    recipient->Insert(KeyAt(0), ValueAt(0));
    Delete(KeyAt(0));
}

/**
 * 重分布操作：移动最后一个元素到目标页面开头
 * 这个操作稍微复杂一点，需要在目标页面前面插入数据
 */
template <typename KeyType, typename ValueType>
void BPlusTreeLeafPage<KeyType, ValueType>::MoveLastToFrontOf(
    BPlusTreeLeafPage* recipient) {
    if (GetSize() == 0) {
        LOG_WARN("BPlusTreeLeafPage::MoveLastToFrontOf: Source page is empty");
        return;
    }

    int last_index = GetSize() - 1;
    KeyType key = KeyAt(last_index);
    ValueType value = ValueAt(last_index);

    // 先减少当前页面的大小
    IncreaseSize(-1);

    // 在目标页面前面插入数据，需要把原有数据往后移
    size_t pair_size = sizeof(KeyType) + sizeof(ValueType);
    std::memmove(recipient->data_ + pair_size, recipient->data_,
                 recipient->GetSize() * pair_size);
    recipient->SetKeyAt(0, key);
    recipient->SetValueAt(0, value);
    recipient->IncreaseSize(1);
}

/**
 * 内部页面初始化 - 和叶子页面类似，但布局不同
 * 内部页面布局：[value0] [key1] [value1] [key2] [value2] ... [keyN] [valueN]
 */
template <typename KeyType>
void BPlusTreeInternalPage<KeyType>::Init(page_id_t page_id,
                                          page_id_t parent_id) {
    SetPageType(IndexPageType::INTERNAL_PAGE);
    SetPageId(page_id);
    SetParentPageId(parent_id);
    SetSize(0);

    // 内部页面的容量计算
    size_t header_size = sizeof(BPlusTreePage);
    size_t available_space = PAGE_SIZE - header_size;

    // 内部页面需要额外的一个page_id_t用于value0
    size_t entry_size = sizeof(KeyType) + sizeof(page_id_t);

    // 可容纳的键值对数量（不包括value0）
    int theoretical_max =
        static_cast<int>((available_space - sizeof(page_id_t)) / entry_size);
    SetMaxSize(std::max(16, theoretical_max - 1));
}

/**
 * 内部页面获取key - 注意内部页面的key索引从1开始，因为没有key0
 */
template <typename KeyType>
KeyType BPlusTreeInternalPage<KeyType>::KeyAt(int index) const {
    if (index <= 0 || index > GetSize()) {
        throw std::out_of_range("Key index out of range");
    }

    // 内部页面布局：[value0] [key1] [value1] [key2] [value2] ...
    // key1在value0后面
    size_t offset =
        sizeof(page_id_t) + (index - 1) * (sizeof(KeyType) + sizeof(page_id_t));
    return *reinterpret_cast<const KeyType*>(data_ + offset);
}

/**
 * 内部页面设置key
 */
template <typename KeyType>
void BPlusTreeInternalPage<KeyType>::SetKeyAt(int index, const KeyType& key) {
    if (index <= 0 || index > GetMaxSize()) {
        throw std::out_of_range("Key index out of range");
    }

    size_t offset =
        sizeof(page_id_t) + (index - 1) * (sizeof(KeyType) + sizeof(page_id_t));
    *reinterpret_cast<KeyType*>(data_ + offset) = key;
}

/**
 * 内部页面获取value（子页面ID） - value索引从0开始
 */
template <typename KeyType>
page_id_t BPlusTreeInternalPage<KeyType>::ValueAt(int index) const {
    if (index < 0 || index > GetSize()) {
        throw std::out_of_range("Value index out of range");
    }

    size_t offset;
    if (index == 0) {
        // value0在最开头
        offset = 0;
    } else {
        // value[i]在key[i]后面
        offset = sizeof(page_id_t) +
                 (index - 1) * (sizeof(KeyType) + sizeof(page_id_t)) +
                 sizeof(KeyType);
    }

    return *reinterpret_cast<const page_id_t*>(data_ + offset);
}

/**
 * 内部页面设置value
 */
template <typename KeyType>
void BPlusTreeInternalPage<KeyType>::SetValueAt(int index, page_id_t value) {
    if (index < 0 || index > GetMaxSize()) {
        throw std::out_of_range("Value index out of range");
    }

    size_t offset;
    if (index == 0) {
        offset = 0;
    } else {
        offset = sizeof(page_id_t) +
                 (index - 1) * (sizeof(KeyType) + sizeof(page_id_t)) +
                 sizeof(KeyType);
    }

    *reinterpret_cast<page_id_t*>(data_ + offset) = value;
}

/**
 * 查找value在内部页面中的索引位置 - 用于删除和移动操作
 */
template <typename KeyType>
int BPlusTreeInternalPage<KeyType>::ValueIndex(page_id_t value) const {
    for (int i = 0; i <= GetSize(); i++) {
        if (ValueAt(i) == value) {
            return i;
        }
    }
    return -1;  // 没找到
}

/**
 * 在内部页面中插入新的key-value对 - 在指定的old_value后面插入
 * 这通常发生在子页面分裂后，需要在父页面插入新的路由信息
 */
template <typename KeyType>
void BPlusTreeInternalPage<KeyType>::InsertNodeAfter(page_id_t old_value,
                                                     const KeyType& new_key,
                                                     page_id_t new_value) {
    if (GetSize() >= GetMaxSize()) {
        throw std::runtime_error("Cannot insert into full internal page");
    }

    // 找到old_value的位置
    int insert_index = ValueIndex(old_value);

    if (insert_index == -1) {
        throw std::runtime_error("Old value not found");
    }

    // 在old_value后面插入
    insert_index++;

    // 把后面的数据往后移动，腾出插入位置
    for (int i = GetSize(); i >= insert_index; i--) {
        if (i + 1 <= GetMaxSize()) {
            SetValueAt(i + 1, ValueAt(i));
        }
        if (i > 0 && i + 1 <= GetMaxSize()) {
            SetKeyAt(i + 1, KeyAt(i));
        }
    }

    // 插入新的键值对
    if (insert_index > 0 && insert_index <= GetMaxSize()) {
        SetKeyAt(insert_index, new_key);
    }
    if (insert_index <= GetMaxSize()) {
        SetValueAt(insert_index, new_value);
    }

    IncreaseSize(1);
}

/**
 * 从内部页面移除指定位置的entry
 */
template <typename KeyType>
void BPlusTreeInternalPage<KeyType>::Remove(int index) {
    if (index < 0 || index > GetSize()) {
        throw std::out_of_range("Index out of range");
    }

    if (index == 0) {
        // 移除value0的特殊情况，整体往前移动
        for (int i = 0; i < GetSize(); i++) {
            SetValueAt(i, ValueAt(i + 1));
            if (i + 1 <= GetSize()) {
                SetKeyAt(i + 1, KeyAt(i + 1));
            }
        }
    } else {
        // 移除key[index]和value[index]
        for (int i = index; i < GetSize(); i++) {
            if (i + 1 <= GetSize()) {
                SetKeyAt(i, KeyAt(i + 1));
                SetValueAt(i, ValueAt(i + 1));
            }
        }
    }

    IncreaseSize(-1);
}

/**
 * 内部页面分裂时移动一半数据 - 比叶子页面复杂，需要更新子页面的父指针
 */
template <typename KeyType>
void BPlusTreeInternalPage<KeyType>::MoveHalfTo(
    BPlusTreeInternalPage* recipient, BufferPoolManager* buffer_pool_manager) {
    int total_size = GetSize();              // 当前的key数量
    int split_point = (total_size + 1) / 2;  // 分裂点

    // 将分裂点之后的数据移动到新页面
    // 注意：分裂点的key会被提升到父页面，所以新页面从split_point的value开始
    recipient->SetValueAt(0, ValueAt(split_point));

    // 移动剩余的键值对
    int move_count = 0;
    for (int i = split_point + 1; i <= total_size; i++) {
        move_count++;
        recipient->SetKeyAt(move_count, KeyAt(i));
        recipient->SetValueAt(move_count, ValueAt(i));
    }

    recipient->SetSize(move_count);

    // 更新子页面的父指针 - 这很重要，否则子页面找不到正确的父页面
    for (int i = 0; i <= move_count; i++) {
        page_id_t child_page_id = recipient->ValueAt(i);
        if (child_page_id != INVALID_PAGE_ID) {
            Page* child_page = buffer_pool_manager->FetchPage(child_page_id);
            if (child_page) {
                auto* child_tree_page =
                    reinterpret_cast<BPlusTreePage*>(child_page->GetData());
                child_tree_page->SetParentPageId(recipient->GetPageId());
                buffer_pool_manager->UnpinPage(child_page_id, true);
            }
        }
    }

    // 更新当前页面的大小（不包括被提升的键）
    SetSize(split_point - 1);
}

/**
 * 内部页面合并时移动所有数据 - 需要插入中间键
 */
template <typename KeyType>
void BPlusTreeInternalPage<KeyType>::MoveAllTo(
    BPlusTreeInternalPage* recipient, const KeyType& middle_key,
    BufferPoolManager* buffer_pool_manager) {
    int recipient_size = recipient->GetSize();

    // 首先插入中间键（从父页面下来的key）
    recipient->SetKeyAt(recipient_size + 1, middle_key);
    recipient->SetValueAt(recipient_size + 1, ValueAt(0));

    // 移动所有条目
    for (int i = 1; i <= GetSize(); i++) {
        if (recipient_size + i + 1 <= recipient->GetMaxSize()) {
            recipient->SetKeyAt(recipient_size + i + 1, KeyAt(i));
            recipient->SetValueAt(recipient_size + i + 1, ValueAt(i));
        }
    }

    // 更新所有子页面的父指针
    for (int i = 0; i <= GetSize(); i++) {
        page_id_t child_page_id = ValueAt(i);
        if (child_page_id != INVALID_PAGE_ID) {
            Page* child_page = buffer_pool_manager->FetchPage(child_page_id);
            if (child_page) {
                auto* child_tree_page =
                    reinterpret_cast<BPlusTreePage*>(child_page->GetData());
                child_tree_page->SetParentPageId(recipient->GetPageId());
                buffer_pool_manager->UnpinPage(child_page_id, true);
            }
        }
    }

    recipient->IncreaseSize(GetSize() + 1);
    SetSize(0);
}

/**
 * 重分布：移动第一个子节点到目标页面末尾
 */
template <typename KeyType>
void BPlusTreeInternalPage<KeyType>::MoveFirstToEndOf(
    BPlusTreeInternalPage* recipient, const KeyType& middle_key,
    BufferPoolManager* buffer_pool_manager) {
    // 获取第一个子节点
    page_id_t first_child = ValueAt(0);

    int recipient_size = recipient->GetSize();
    if (recipient_size + 1 <= recipient->GetMaxSize()) {
        recipient->SetKeyAt(recipient_size + 1, middle_key);
        recipient->SetValueAt(recipient_size + 1, first_child);
        recipient->IncreaseSize(1);
    }

    // 更新子页面的父指针
    if (first_child != INVALID_PAGE_ID) {
        Page* child_page = buffer_pool_manager->FetchPage(first_child);
        if (child_page) {
            auto* child_tree_page =
                reinterpret_cast<BPlusTreePage*>(child_page->GetData());
            child_tree_page->SetParentPageId(recipient->GetPageId());
            buffer_pool_manager->UnpinPage(first_child, true);
        }
    }

    // 从当前页面移除第一个元素
    Remove(0);
}

/**
 * 重分布：移动最后一个子节点到目标页面开头
 */
template <typename KeyType>
void BPlusTreeInternalPage<KeyType>::MoveLastToFrontOf(
    BPlusTreeInternalPage* recipient, const KeyType& middle_key,
    BufferPoolManager* buffer_pool_manager) {
    // 获取最后一个子节点
    int last_index = GetSize();
    page_id_t last_child = ValueAt(last_index);

    // 在接收页面前端腾出空间
    for (int i = recipient->GetSize(); i >= 0; i--) {
        if (i > 0 && i + 1 <= recipient->GetMaxSize()) {
            recipient->SetKeyAt(i + 1, recipient->KeyAt(i));
        }
        if (i + 1 <= recipient->GetMaxSize()) {
            recipient->SetValueAt(i + 1, recipient->ValueAt(i));
        }
    }

    // 在前端插入新数据
    recipient->SetValueAt(0, last_child);
    if (recipient->GetMaxSize() > 0) {
        recipient->SetKeyAt(1, middle_key);
    }
    recipient->IncreaseSize(1);

    // 更新子页面的父指针
    if (last_child != INVALID_PAGE_ID) {
        Page* child_page = buffer_pool_manager->FetchPage(last_child);
        if (child_page) {
            auto* child_tree_page =
                reinterpret_cast<BPlusTreePage*>(child_page->GetData());
            child_tree_page->SetParentPageId(recipient->GetPageId());
            buffer_pool_manager->UnpinPage(last_child, true);
        }
    }

    // 从当前页面移除最后一个元素
    IncreaseSize(-1);
}

// 显式模板实例化 - 告诉编译器需要为这些类型生成代码
template class BPlusTreeLeafPage<int32_t, RID>;
template class BPlusTreeLeafPage<int64_t, RID>;
template class BPlusTreeLeafPage<float, RID>;
template class BPlusTreeLeafPage<double, RID>;
template class BPlusTreeLeafPage<std::string, RID>;

template class BPlusTreeInternalPage<int32_t>;
template class BPlusTreeInternalPage<int64_t>;
template class BPlusTreeInternalPage<float>;
template class BPlusTreeInternalPage<double>;
template class BPlusTreeInternalPage<std::string>;

}  // namespace SimpleRDBMS