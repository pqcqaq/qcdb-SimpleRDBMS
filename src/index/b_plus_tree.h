// src/index/b_plus_tree.h
#pragma once

#include <memory>
#include <mutex>
#include "buffer/buffer_pool_manager.h"
#include "index/b_plus_tree_page.h"

namespace SimpleRDBMS {

// Forward declaration
template <typename KeyType>
class BPlusTreeInternalPage;

template <typename KeyType, typename ValueType>
class BPlusTree {
public:
    BPlusTree(const std::string& name, BufferPoolManager* buffer_pool_manager);
    
    // Point operations
    bool Insert(const KeyType& key, const ValueType& value, txn_id_t txn_id = -1);
    bool Remove(const KeyType& key, txn_id_t txn_id = -1);
    bool GetValue(const KeyType& key, ValueType* value, txn_id_t txn_id = -1);
    
    // Iterator for range scan
    class Iterator {
    public:
        Iterator(BPlusTree* tree, page_id_t page_id, int index);
        
        bool IsEnd() const;
        void operator++();
        std::pair<KeyType, ValueType> operator*();
        
    private:
        BPlusTree* tree_;
        page_id_t current_page_id_;
        int current_index_;
    };
    
    Iterator Begin();
    Iterator Begin(const KeyType& key);
    Iterator End();

private:
    std::string index_name_;
    BufferPoolManager* buffer_pool_manager_;
    page_id_t root_page_id_;
    std::mutex latch_;
    
    // Helper functions for search and insertion
    Page* FindLeafPage(const KeyType& key, bool is_write_op);
    bool InsertIntoLeaf(const KeyType& key, const ValueType& value, 
                        BPlusTreeLeafPage<KeyType, ValueType>* leaf);
    void InsertIntoParent(BPlusTreePage* old_node, const KeyType& key, 
                          BPlusTreePage* new_node);
    
    // Split operation
    template <typename N>
    bool Split(N* node);
    
    // Helper functions for deletion
    bool ShouldCoalesceOrRedistribute(BPlusTreePage* node);
    bool CoalesceOrRedistribute(BPlusTreePage* node, txn_id_t txn_id);
    bool AdjustRoot(BPlusTreePage* old_root_node);
    
    // Coalesce operation - merges node with its neighbor
    template <typename N>
    bool Coalesce(N** neighbor_node, N** node,
                  BPlusTreeInternalPage<KeyType>** parent, 
                  int index, txn_id_t txn_id);
    
    // Redistribute operation - moves keys between siblings
    template <typename N>
    void Redistribute(N* neighbor_node, N* node, int index);
    
    // Update root page id in header page
    void UpdateRootPageId(page_id_t root_page_id);
    void LoadRootPageId();
};

}  // namespace SimpleRDBMS