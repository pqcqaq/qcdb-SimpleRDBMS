// ===== 修复后的 src/index/b_plus_tree_page.cpp =====

#include "index/b_plus_tree_page.h"
#include "buffer/buffer_pool_manager.h"
#include "common/types.h"
#include "common/debug.h"
#include <algorithm>
#include <cstring>

namespace SimpleRDBMS {

// BPlusTreeLeafPage implementation
template <typename KeyType, typename ValueType>
void BPlusTreeLeafPage<KeyType, ValueType>::Init(page_id_t page_id, page_id_t parent_id) {
    SetPageType(IndexPageType::LEAF_PAGE);
    SetPageId(page_id);
    SetParentPageId(parent_id);
    SetSize(0);
    
    // 更精确的容量计算
    size_t header_size = sizeof(BPlusTreePage) + sizeof(next_page_id_);
    size_t available_space = PAGE_SIZE - header_size;
    size_t pair_size = sizeof(KeyType) + sizeof(ValueType);
    
    // 计算理论最大容量
    int theoretical_max = static_cast<int>(available_space / pair_size);
    
    // 确保至少能容纳合理数量的记录，但减少保留空间以提高利用率
    // 只为分裂预留1个元素的空间而不是2个
    SetMaxSize(std::max(16, theoretical_max - 1));
    
    next_page_id_ = INVALID_PAGE_ID;
}

void BPlusTreePage::SetSize(int size) { 
    if (size < 0) {
        LOG_ERROR("BPlusTreePage::SetSize: Attempting to set negative size " << size);
        size_ = 0;
    } else {
        size_ = size; 
    }
}

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

template <typename KeyType, typename ValueType>
KeyType BPlusTreeLeafPage<KeyType, ValueType>::KeyAt(int index) const {
    if (index < 0 || index >= GetSize()) {
        throw std::out_of_range("Index out of range");
    }
    
    char* data_ptr = const_cast<char*>(data_);
    size_t offset = index * (sizeof(KeyType) + sizeof(ValueType));
    return *reinterpret_cast<KeyType*>(data_ptr + offset);
}

template <typename KeyType, typename ValueType>
ValueType BPlusTreeLeafPage<KeyType, ValueType>::ValueAt(int index) const {
    if (index < 0 || index >= GetSize()) {
        throw std::out_of_range("Index out of range");
    }
    
    char* data_ptr = const_cast<char*>(data_);
    size_t offset = index * (sizeof(KeyType) + sizeof(ValueType)) + sizeof(KeyType);
    return *reinterpret_cast<ValueType*>(data_ptr + offset);
}

template <typename KeyType, typename ValueType>
void BPlusTreeLeafPage<KeyType, ValueType>::SetKeyAt(int index, const KeyType& key) {
    if (index < 0 || index >= GetMaxSize()) {
        throw std::out_of_range("Index out of range");
    }
    
    size_t offset = index * (sizeof(KeyType) + sizeof(ValueType));
    *reinterpret_cast<KeyType*>(data_ + offset) = key;
}

template <typename KeyType, typename ValueType>
void BPlusTreeLeafPage<KeyType, ValueType>::SetValueAt(int index, const ValueType& value) {
    if (index < 0 || index >= GetMaxSize()) {
        throw std::out_of_range("Index out of range");
    }
    
    size_t offset = index * (sizeof(KeyType) + sizeof(ValueType)) + sizeof(KeyType);
    *reinterpret_cast<ValueType*>(data_ + offset) = value;
}

// 修复后的 KeyIndex 函数
template <typename KeyType, typename ValueType>
int BPlusTreeLeafPage<KeyType, ValueType>::KeyIndex(const KeyType& key) const {
    if (GetSize() == 0) {
        return 0;
    }
    
    // 使用标准库的 lower_bound 逻辑进行二分查找
    int left = 0, right = GetSize();
    
    while (left < right) {
        int mid = left + (right - left) / 2;
        KeyType mid_key = KeyAt(mid);
        
        if (mid_key < key) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    
    return left; // 返回插入位置
}

template <typename KeyType>
int BPlusTreeInternalPage<KeyType>::KeyIndex(const KeyType& key) const {
    int size = GetSize();
    if (size <= 0) {
        LOG_WARN("BPlusTreeInternalPage::KeyIndex: Invalid size " << size);
        return 0;
    }
    
    // 确保不会访问越界
    for (int i = 1; i <= size; i++) {
        try {
            if (i > GetMaxSize()) {
                LOG_ERROR("BPlusTreeInternalPage::KeyIndex: Index " << i 
                          << " exceeds max size " << GetMaxSize());
                return 0;
            }
            if (key < KeyAt(i)) {
                return i - 1;
            }
        } catch (const std::exception& e) {
            LOG_ERROR("BPlusTreeInternalPage::KeyIndex: Exception at index " << i 
                      << " with size " << size << ": " << e.what());
            return 0;
        }
    }
    return size;
}

template <typename KeyType, typename ValueType>
bool BPlusTreeLeafPage<KeyType, ValueType>::Insert(const KeyType& key, const ValueType& value) {
    // 首先检查是否已经满了
    if (GetSize() >= GetMaxSize()) {
        return false; // 页面已满，需要在调用者处理分裂
    }
    
    int insert_index = KeyIndex(key);
    
    // 检查key是否已存在
    if (insert_index < GetSize() && KeyAt(insert_index) == key) {
        // 更新现有值
        SetValueAt(insert_index, value);
        return true;
    }
    
    // 移动后续元素为新元素腾出空间
    size_t pair_size = sizeof(KeyType) + sizeof(ValueType);
    int move_count = GetSize() - insert_index;
    if (move_count > 0) {
        std::memmove(data_ + (insert_index + 1) * pair_size,
                     data_ + insert_index * pair_size,
                     move_count * pair_size);
    }
    
    // 插入新的键值对
    SetKeyAt(insert_index, key);
    SetValueAt(insert_index, value);
    IncreaseSize(1);
    
    return true;
}

template <typename KeyType, typename ValueType>
bool BPlusTreeLeafPage<KeyType, ValueType>::Delete(const KeyType& key) {
    if (GetSize() <= 0) {
        LOG_WARN("BPlusTreeLeafPage::Delete: Attempting to delete from empty page");
        return false;
    }
    
    int delete_index = KeyIndex(key);
    if (delete_index >= GetSize() || KeyAt(delete_index) != key) {
        return false;
    }
    
    size_t pair_size = sizeof(KeyType) + sizeof(ValueType);
    int move_count = GetSize() - delete_index - 1;
    if (move_count > 0) {
        std::memmove(data_ + delete_index * pair_size,
                     data_ + (delete_index + 1) * pair_size,
                     move_count * pair_size);
    }
    
    // 确保 size 不会变成负数
    if (GetSize() > 0) {
        IncreaseSize(-1);
    } else {
        LOG_WARN("BPlusTreeLeafPage::Delete: Page size already 0, cannot decrease");
    }
    return true;
}

template <typename KeyType, typename ValueType>
void BPlusTreeLeafPage<KeyType, ValueType>::MoveHalfTo(BPlusTreeLeafPage* recipient) {
    // 更精确的分裂：将一半数据移动到新页面
    int total_size = GetSize();
    int split_point = (total_size + 1) / 2;  // 向上取整，原页面保留较少元素
    int move_size = total_size - split_point;
    
    if (move_size <= 0) {
        return;
    }
    
    size_t pair_size = sizeof(KeyType) + sizeof(ValueType);
    size_t move_bytes = move_size * pair_size;
    
    // 将后半部分数据复制到新页面
    std::memcpy(recipient->data_, data_ + split_point * pair_size, move_bytes);
    recipient->SetSize(move_size);
    
    // 更新当前页面的大小
    SetSize(split_point);
    
    // 清除移动的数据区域
    std::memset(data_ + split_point * pair_size, 0, move_bytes);
}

template <typename KeyType, typename ValueType>
void BPlusTreeLeafPage<KeyType, ValueType>::MoveAllTo(BPlusTreeLeafPage* recipient) {
    size_t pair_size = sizeof(KeyType) + sizeof(ValueType);
    size_t move_bytes = GetSize() * pair_size;
    
    // 复制所有数据到接收页面
    std::memcpy(recipient->data_ + recipient->GetSize() * pair_size, data_, move_bytes);
    recipient->IncreaseSize(GetSize());
    
    // 清空当前页面
    SetSize(0);
}

template <typename KeyType, typename ValueType>
void BPlusTreeLeafPage<KeyType, ValueType>::MoveFirstToEndOf(BPlusTreeLeafPage* recipient) {
    if (GetSize() == 0) return;
    
    // 将第一个元素移动到接收页面的末尾
    recipient->Insert(KeyAt(0), ValueAt(0));
    
    // 从当前页面删除第一个元素
    Delete(KeyAt(0));
}

template <typename KeyType, typename ValueType>
void BPlusTreeLeafPage<KeyType, ValueType>::MoveLastToFrontOf(BPlusTreeLeafPage* recipient) {
    if (GetSize() == 0) {
        LOG_WARN("BPlusTreeLeafPage::MoveLastToFrontOf: Source page is empty");
        return;
    }
    
    int last_index = GetSize() - 1;
    KeyType key = KeyAt(last_index);
    ValueType value = ValueAt(last_index);
    
    // 先减少当前页面的大小
    IncreaseSize(-1);
    
    // 再移动数据到目标页面
    size_t pair_size = sizeof(KeyType) + sizeof(ValueType);
    std::memmove(recipient->data_ + pair_size, recipient->data_, 
                 recipient->GetSize() * pair_size);
    recipient->SetKeyAt(0, key);
    recipient->SetValueAt(0, value);
    recipient->IncreaseSize(1);
}

// BPlusTreeInternalPage implementation
template <typename KeyType>
void BPlusTreeInternalPage<KeyType>::Init(page_id_t page_id, page_id_t parent_id) {
    SetPageType(IndexPageType::INTERNAL_PAGE);
    SetPageId(page_id);
    SetParentPageId(parent_id);
    SetSize(0);
    
    // 更精确的容量计算
    size_t header_size = sizeof(BPlusTreePage);
    size_t available_space = PAGE_SIZE - header_size;
    
    // 内部页面布局：[value0] [key1] [value1] [key2] [value2] ... [keyN] [valueN]
    // 每个条目大小：sizeof(KeyType) + sizeof(page_id_t)，但需要额外的一个page_id_t用于value0
    size_t entry_size = sizeof(KeyType) + sizeof(page_id_t);
    
    // 可容纳的键值对数量（不包括value0）
    int theoretical_max = static_cast<int>((available_space - sizeof(page_id_t)) / entry_size);
    SetMaxSize(std::max(16, theoretical_max - 1)); // 同样减少保留空间
}

template <typename KeyType>
KeyType BPlusTreeInternalPage<KeyType>::KeyAt(int index) const {
    if (index <= 0 || index > GetSize()) {
        throw std::out_of_range("Key index out of range");
    }
    
    // 内部页面布局：[value0] [key1] [value1] [key2] [value2] ... [keyN] [valueN]
    // key1在offset sizeof(page_id_t)处
    size_t offset = sizeof(page_id_t) + (index - 1) * (sizeof(KeyType) + sizeof(page_id_t));
    return *reinterpret_cast<const KeyType*>(data_ + offset);
}

template <typename KeyType>
void BPlusTreeInternalPage<KeyType>::SetKeyAt(int index, const KeyType& key) {
    if (index <= 0 || index > GetMaxSize()) {
        throw std::out_of_range("Key index out of range");
    }
    
    size_t offset = sizeof(page_id_t) + (index - 1) * (sizeof(KeyType) + sizeof(page_id_t));
    *reinterpret_cast<KeyType*>(data_ + offset) = key;
}

template <typename KeyType>
page_id_t BPlusTreeInternalPage<KeyType>::ValueAt(int index) const {
    if (index < 0 || index > GetSize()) {
        throw std::out_of_range("Value index out of range");
    }
    
    size_t offset;
    if (index == 0) {
        // value0在开头
        offset = 0;
    } else {
        // value[i] = value0位置 + sizeof(page_id_t) + (i-1) * (sizeof(KeyType) + sizeof(page_id_t)) + sizeof(KeyType)
        offset = sizeof(page_id_t) + (index - 1) * (sizeof(KeyType) + sizeof(page_id_t)) + sizeof(KeyType);
    }
    
    return *reinterpret_cast<const page_id_t*>(data_ + offset);
}

template <typename KeyType>
void BPlusTreeInternalPage<KeyType>::SetValueAt(int index, page_id_t value) {
    if (index < 0 || index > GetMaxSize()) {
        throw std::out_of_range("Value index out of range");
    }
    
    size_t offset;
    if (index == 0) {
        offset = 0;
    } else {
        offset = sizeof(page_id_t) + (index - 1) * (sizeof(KeyType) + sizeof(page_id_t)) + sizeof(KeyType);
    }
    
    *reinterpret_cast<page_id_t*>(data_ + offset) = value;
}

template <typename KeyType>
int BPlusTreeInternalPage<KeyType>::ValueIndex(page_id_t value) const {
    for (int i = 0; i <= GetSize(); i++) {
        if (ValueAt(i) == value) {
            return i;
        }
    }
    return -1; // 未找到
}

template <typename KeyType>
void BPlusTreeInternalPage<KeyType>::InsertNodeAfter(page_id_t old_value, const KeyType& new_key, page_id_t new_value) {
    if (GetSize() >= GetMaxSize()) {
        throw std::runtime_error("Cannot insert into full internal page");
    }
    
    // 查找old_value的位置
    int insert_index = ValueIndex(old_value);
    
    if (insert_index == -1) {
        throw std::runtime_error("Old value not found");
    }
    
    // 插入位置在old_value之后
    insert_index++;
    
    // 为新的键值对腾出空间
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

template <typename KeyType>
void BPlusTreeInternalPage<KeyType>::Remove(int index) {
    if (index < 0 || index > GetSize()) {
        throw std::out_of_range("Index out of range");
    }
    
    if (index == 0) {
        // 移除value0的特殊情况
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

template <typename KeyType>
void BPlusTreeInternalPage<KeyType>::MoveHalfTo(BPlusTreeInternalPage* recipient, BufferPoolManager* buffer_pool_manager) {
    int total_size = GetSize();  // 键的数量
    int split_point = (total_size + 1) / 2;  // 分裂点
    
    // 将分裂点之后的数据移动到新页面
    recipient->SetValueAt(0, ValueAt(split_point));
    
    // 移动剩余的键值对
    int move_count = 0;
    for (int i = split_point + 1; i <= total_size; i++) {
        move_count++;
        recipient->SetKeyAt(move_count, KeyAt(i));
        recipient->SetValueAt(move_count, ValueAt(i));
    }
    
    recipient->SetSize(move_count);
    
    // 更新子页面的父指针
    for (int i = 0; i <= move_count; i++) {
        page_id_t child_page_id = recipient->ValueAt(i);
        if (child_page_id != INVALID_PAGE_ID) {
            Page* child_page = buffer_pool_manager->FetchPage(child_page_id);
            if (child_page) {
                auto* child_tree_page = reinterpret_cast<BPlusTreePage*>(child_page->GetData());
                child_tree_page->SetParentPageId(recipient->GetPageId());
                buffer_pool_manager->UnpinPage(child_page_id, true);
            }
        }
    }
    
    // 更新当前页面的大小（不包括被提升的键）
    SetSize(split_point - 1);
}

template <typename KeyType>
void BPlusTreeInternalPage<KeyType>::MoveAllTo(BPlusTreeInternalPage* recipient, const KeyType& middle_key, BufferPoolManager* buffer_pool_manager) {
    int recipient_size = recipient->GetSize();
    
    // 添加中间键
    recipient->SetKeyAt(recipient_size + 1, middle_key);
    recipient->SetValueAt(recipient_size + 1, ValueAt(0));
    
    // 移动所有条目
    for (int i = 1; i <= GetSize(); i++) {
        if (recipient_size + i + 1 <= recipient->GetMaxSize()) {
            recipient->SetKeyAt(recipient_size + i + 1, KeyAt(i));
            recipient->SetValueAt(recipient_size + i + 1, ValueAt(i));
        }
    }
    
    // 更新父指针
    for (int i = 0; i <= GetSize(); i++) {
        page_id_t child_page_id = ValueAt(i);
        if (child_page_id != INVALID_PAGE_ID) {
            Page* child_page = buffer_pool_manager->FetchPage(child_page_id);
            if (child_page) {
                auto* child_tree_page = reinterpret_cast<BPlusTreePage*>(child_page->GetData());
                child_tree_page->SetParentPageId(recipient->GetPageId());
                buffer_pool_manager->UnpinPage(child_page_id, true);
            }
        }
    }
    
    recipient->IncreaseSize(GetSize() + 1);
    SetSize(0);
}

template <typename KeyType>
void BPlusTreeInternalPage<KeyType>::MoveFirstToEndOf(BPlusTreeInternalPage* recipient, const KeyType& middle_key, BufferPoolManager* buffer_pool_manager) {
    // 将第一个子节点移动到接收页面的末尾
    page_id_t first_child = ValueAt(0);
    
    int recipient_size = recipient->GetSize();
    if (recipient_size + 1 <= recipient->GetMaxSize()) {
        recipient->SetKeyAt(recipient_size + 1, middle_key);
        recipient->SetValueAt(recipient_size + 1, first_child);
        recipient->IncreaseSize(1);
    }
    
    // 更新父指针
    if (first_child != INVALID_PAGE_ID) {
        Page* child_page = buffer_pool_manager->FetchPage(first_child);
        if (child_page) {
            auto* child_tree_page = reinterpret_cast<BPlusTreePage*>(child_page->GetData());
            child_tree_page->SetParentPageId(recipient->GetPageId());
            buffer_pool_manager->UnpinPage(first_child, true);
        }
    }
    
    // 从当前页面移除
    Remove(0);
}

template <typename KeyType>
void BPlusTreeInternalPage<KeyType>::MoveLastToFrontOf(BPlusTreeInternalPage* recipient, const KeyType& middle_key, BufferPoolManager* buffer_pool_manager) {
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
    
    // 在前端插入
    recipient->SetValueAt(0, last_child);
    if (recipient->GetMaxSize() > 0) {
        recipient->SetKeyAt(1, middle_key);
    }
    recipient->IncreaseSize(1);
    
    // 更新父指针
    if (last_child != INVALID_PAGE_ID) {
        Page* child_page = buffer_pool_manager->FetchPage(last_child);
        if (child_page) {
            auto* child_tree_page = reinterpret_cast<BPlusTreePage*>(child_page->GetData());
            child_tree_page->SetParentPageId(recipient->GetPageId());
            buffer_pool_manager->UnpinPage(last_child, true);
        }
    }
    
    // 从当前页面移除最后一个元素
    IncreaseSize(-1);
}

// 显式模板实例化
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