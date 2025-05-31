#pragma once

#include <cstring>
#include <stdexcept>
#include <string>
#include "common/config.h"
#include "common/types.h"
#include "storage/page.h"

namespace SimpleRDBMS {

// Forward declarations
class BufferPoolManager;

enum class IndexPageType {
    INVALID = 0,
    LEAF_PAGE,
    INTERNAL_PAGE
};

// Base class for B+ tree pages
class BPlusTreePage {
public:
    bool IsLeafPage() const { return page_type_ == IndexPageType::LEAF_PAGE; }
    bool IsRootPage() const { return parent_page_id_ == INVALID_PAGE_ID; }
    
    void SetPageType(IndexPageType type) { page_type_ = type; }
    IndexPageType GetPageType() const { return page_type_; }
    
    int GetSize() const { return size_; }
    void SetSize(int size) { size_ = size; }
    void IncreaseSize(int amount) { size_ += amount; }
    
    int GetMaxSize() const { return max_size_; }
    void SetMaxSize(int max_size) { max_size_ = max_size; }
    
    page_id_t GetParentPageId() const { return parent_page_id_; }
    void SetParentPageId(page_id_t parent_id) { parent_page_id_ = parent_id; }
    
    page_id_t GetPageId() const { return page_id_; }
    void SetPageId(page_id_t page_id) { page_id_ = page_id; }

protected:
    IndexPageType page_type_;
    int size_;
    int max_size_;
    page_id_t parent_page_id_;
    page_id_t page_id_;
};

// Leaf page for B+ tree
template <typename KeyType, typename ValueType>
class BPlusTreeLeafPage : public BPlusTreePage {
public:
    void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID);
    
    // Key/Value operations
    KeyType KeyAt(int index) const;
    ValueType ValueAt(int index) const;
    
    void SetKeyAt(int index, const KeyType& key);
    void SetValueAt(int index, const ValueType& value);
    
    int KeyIndex(const KeyType& key) const;
    
    // Insert/Delete
    bool Insert(const KeyType& key, const ValueType& value);
    bool Delete(const KeyType& key);
    
    // Split/Merge
    void MoveHalfTo(BPlusTreeLeafPage* recipient);
    void MoveAllTo(BPlusTreeLeafPage* recipient);
    void MoveFirstToEndOf(BPlusTreeLeafPage* recipient);
    void MoveLastToFrontOf(BPlusTreeLeafPage* recipient);
    
    // Next page pointer
    page_id_t GetNextPageId() const { return next_page_id_; }
    void SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

private:
    page_id_t next_page_id_;
    // Flexible array for key-value pairs
    char data_[0];
};

// Internal page for B+ tree
template <typename KeyType>
class BPlusTreeInternalPage : public BPlusTreePage {
public:
    void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID);
    
    // Key operations
    KeyType KeyAt(int index) const;
    void SetKeyAt(int index, const KeyType& key);
    int KeyIndex(const KeyType& key) const;
    
    // Child pointer operations
    page_id_t ValueAt(int index) const;
    void SetValueAt(int index, page_id_t value);
    int ValueIndex(page_id_t value) const;  // New method
    
    // Insert/Delete
    void InsertNodeAfter(page_id_t old_value, const KeyType& new_key, page_id_t new_value);
    void Remove(int index);
    
    // Split/Merge
    void MoveHalfTo(BPlusTreeInternalPage* recipient, BufferPoolManager* buffer_pool_manager);
    void MoveAllTo(BPlusTreeInternalPage* recipient, const KeyType& middle_key, BufferPoolManager* buffer_pool_manager);
    void MoveFirstToEndOf(BPlusTreeInternalPage* recipient, const KeyType& middle_key, BufferPoolManager* buffer_pool_manager);
    void MoveLastToFrontOf(BPlusTreeInternalPage* recipient, const KeyType& middle_key, BufferPoolManager* buffer_pool_manager);

private:
    // Flexible array for keys and child page ids
    char data_[0];
};

}  // namespace SimpleRDBMS