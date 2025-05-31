// ===== 修复后的 src/index/b_plus_tree.cpp =====

#include "index/b_plus_tree.h"

#include "common/debug.h"
#include "common/types.h"
#include "index/b_plus_tree_page.h"

namespace SimpleRDBMS {

// 在 BPlusTree 构造函数中添加根页面ID加载
template <typename KeyType, typename ValueType>
BPlusTree<KeyType, ValueType>::BPlusTree(const std::string& name,
                                         BufferPoolManager* buffer_pool_manager)
    : index_name_(name),
      buffer_pool_manager_(buffer_pool_manager),
      root_page_id_(INVALID_PAGE_ID) {
    // 尝试从磁盘加载根页面ID
    LoadRootPageId();
}

template <typename KeyType, typename ValueType>
BPlusTree<KeyType, ValueType>::~BPlusTree() {
    try {
        std::lock_guard<std::mutex> lock(latch_);
        // Don't access buffer_pool_manager_ during destruction
        // Just clear the root page ID
        root_page_id_ = INVALID_PAGE_ID;
        LOG_DEBUG("BPlusTree destroyed: " << index_name_);
    } catch (const std::exception& e) {
        // Log but don't rethrow during destruction
        LOG_ERROR("BPlusTree destructor exception: " << e.what());
    } catch (...) {
        LOG_ERROR("BPlusTree destructor unknown exception");
    }
}

template <typename KeyType, typename ValueType>
void BPlusTree<KeyType, ValueType>::LoadRootPageId() {
    LOG_DEBUG("LoadRootPageId called");

    // 首先检查磁盘是否有页面
    int num_pages = buffer_pool_manager_->GetDiskManager()->GetNumPages();
    LOG_DEBUG("Disk has " << num_pages << " pages");

    if (num_pages == 0) {
        LOG_DEBUG(
            "New database with no pages, setting root_page_id to INVALID");
        root_page_id_ = INVALID_PAGE_ID;
        return;
    }

    // 尝试获取页面0作为header页面
    Page* header_page = buffer_pool_manager_->FetchPage(0);

    if (header_page == nullptr) {
        LOG_DEBUG("Header page 0 does not exist, assuming new database");
        root_page_id_ = INVALID_PAGE_ID;
        return;
    }

    // 读取根页面ID
    root_page_id_ = *reinterpret_cast<page_id_t*>(header_page->GetData());
    buffer_pool_manager_->UnpinPage(0, false);

    LOG_DEBUG("Loaded root page ID: " << root_page_id_ << " from header page");

    // 验证根页面是否有效
    if (root_page_id_ != INVALID_PAGE_ID) {
        // 检查页面ID是否在有效范围内
        if (root_page_id_ >= num_pages) {
            LOG_WARN("Root page " << root_page_id_
                                  << " is out of range (num_pages=" << num_pages
                                  << "), resetting to INVALID");
            root_page_id_ = INVALID_PAGE_ID;
            UpdateRootPageId(root_page_id_);
            return;
        }

        // 验证根页面是否真实存在且有效
        Page* root_page = buffer_pool_manager_->FetchPage(root_page_id_);
        if (root_page == nullptr) {
            LOG_WARN("Root page " << root_page_id_
                                  << " does not exist, resetting to INVALID");
            root_page_id_ = INVALID_PAGE_ID;
            UpdateRootPageId(root_page_id_);
        } else {
            // 验证根页面的内容是否合理
            auto tree_page =
                reinterpret_cast<BPlusTreePage*>(root_page->GetData());
            if (tree_page->GetPageId() != root_page_id_) {
                LOG_WARN("Root page " << root_page_id_
                                      << " has inconsistent page_id (expected "
                                      << root_page_id_ << ", got "
                                      << tree_page->GetPageId()
                                      << "), resetting to INVALID");
                root_page_id_ = INVALID_PAGE_ID;
                UpdateRootPageId(root_page_id_);
            }
            buffer_pool_manager_->UnpinPage(root_page_id_, false);
        }
    }
}

template <typename KeyType, typename ValueType>
bool BPlusTree<KeyType, ValueType>::Insert(const KeyType& key,
                                           const ValueType& value,
                                           txn_id_t txn_id) {
    std::lock_guard<std::mutex> lock(latch_);
    (void)txn_id;

    LOG_TRACE("Inserting key: " << key);

    if (root_page_id_ == INVALID_PAGE_ID) {
        LOG_DEBUG("Creating root page for first insertion");

        // 首先确保页面0（header页面）存在
        Page* header_page = buffer_pool_manager_->FetchPage(0);
        if (header_page == nullptr) {
            // 页面0不存在，先创建它
            page_id_t header_page_id;
            header_page = buffer_pool_manager_->NewPage(&header_page_id);
            if (header_page == nullptr) {
                LOG_ERROR("Failed to create header page");
                return false;
            }
            LOG_DEBUG("Created header page with ID: " << header_page_id);
            // 初始化header页面
            std::memset(header_page->GetData(), 0, PAGE_SIZE);
            *reinterpret_cast<page_id_t*>(header_page->GetData()) =
                INVALID_PAGE_ID;
            header_page->SetDirty(true);
            buffer_pool_manager_->UnpinPage(header_page_id, true);
        } else {
            buffer_pool_manager_->UnpinPage(0, false);
        }

        // 创建根页面（叶子页面）
        page_id_t new_page_id;
        Page* root_page = buffer_pool_manager_->NewPage(&new_page_id);
        if (root_page == nullptr) {
            LOG_ERROR("Failed to create root page");
            return false;
        }

        LOG_DEBUG("Allocated root page with ID: " << new_page_id);

        auto root = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType>*>(
            root_page->GetData());
        LOG_DEBUG("Initializing root page...");
        root->Init(new_page_id);

        LOG_DEBUG("Inserting key " << key << " into root page...");
        if (!root->Insert(key, value)) {
            LOG_ERROR("Failed to insert into empty root");
            buffer_pool_manager_->UnpinPage(new_page_id, false);
            buffer_pool_manager_->DeletePage(new_page_id);
            return false;
        }

        root_page_id_ = new_page_id;
        LOG_DEBUG("Setting root_page_id_ to: " << root_page_id_);

        // 立即更新header页面
        UpdateRootPageId(root_page_id_);

        // 确保页面被标记为dirty
        root_page->SetDirty(true);
        LOG_DEBUG("Unpinning root page...");
        buffer_pool_manager_->UnpinPage(new_page_id, true);

        // 强制刷新根页面和header页面，确保数据写入磁盘
        buffer_pool_manager_->FlushPage(new_page_id);
        buffer_pool_manager_->FlushPage(0);  // 总是刷新header页面

        LOG_DEBUG("Created root page and inserted key: " << key);
        return true;
    }

    // 查找叶子页面
    Page* leaf_page = FindLeafPage(key, true);
    if (leaf_page == nullptr) {
        LOG_ERROR("Failed to find leaf page for key: " << key);
        return false;
    }

    auto leaf = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType>*>(
        leaf_page->GetData());
    page_id_t leaf_page_id = leaf_page->GetPageId();

    bool result = InsertIntoLeaf(key, value, leaf);

    // 确保页面被标记为dirty
    if (result) {
        leaf_page->SetDirty(true);
    }

    buffer_pool_manager_->UnpinPage(leaf_page_id, result);

    if (result) {
        LOG_TRACE("Successfully inserted key: " << key);
    } else {
        LOG_ERROR("Failed to insert key: " << key);
    }

    return result;
}

template <typename KeyType, typename ValueType>
bool BPlusTree<KeyType, ValueType>::Remove(const KeyType& key,
                                           txn_id_t txn_id) {
    std::lock_guard<std::mutex> lock(latch_);
    (void)txn_id;

    if (root_page_id_ == INVALID_PAGE_ID) {
        LOG_DEBUG("Tree is empty, cannot remove key: " << key);
        return false;
    }

    LOG_DEBUG("Removing key: " << key);

    Page* leaf_page = FindLeafPage(key, true);
    if (leaf_page == nullptr) {
        LOG_DEBUG("Failed to find leaf page for key: " << key);
        return false;
    }

    auto leaf = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType>*>(
        leaf_page->GetData());
    page_id_t leaf_page_id = leaf_page->GetPageId();

    bool result = leaf->Delete(key);

    if (result) {
        LOG_DEBUG("Successfully deleted key from leaf: " << key);
        // 标记页面为dirty
        leaf_page->SetDirty(true);

        // 检查是否需要合并或重分布
        bool node_deleted = false;
        if (ShouldCoalesceOrRedistribute(leaf)) {
            LOG_DEBUG("Leaf requires coalesce/redistribute after deletion");
            node_deleted = CoalesceOrRedistribute(leaf, txn_id);
        }

        // 只有当节点没有被删除时才unpin
        if (!node_deleted) {
            buffer_pool_manager_->UnpinPage(leaf_page_id, true);
        }
    } else {
        LOG_DEBUG("Key not found in leaf: " << key);
        // 删除失败，unpin页面
        buffer_pool_manager_->UnpinPage(leaf_page_id, false);
    }

    return result;
}

template <typename KeyType, typename ValueType>
bool BPlusTree<KeyType, ValueType>::GetValue(const KeyType& key,
                                             ValueType* value,
                                             txn_id_t txn_id) {
    std::lock_guard<std::mutex> lock(latch_);
    (void)txn_id;

    if (root_page_id_ == INVALID_PAGE_ID) {
        return false;
    }

    Page* leaf_page = FindLeafPage(key, false);
    if (leaf_page == nullptr) {
        return false;
    }

    auto leaf = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType>*>(
        leaf_page->GetData());

    int index = leaf->KeyIndex(key);
    bool found = (index < leaf->GetSize() && leaf->KeyAt(index) == key);

    if (found) {
        *value = leaf->ValueAt(index);
    }

    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);

    return found;
}

template <typename KeyType, typename ValueType>
typename BPlusTree<KeyType, ValueType>::Iterator
BPlusTree<KeyType, ValueType>::Begin() {
    std::lock_guard<std::mutex> lock(latch_);

    if (root_page_id_ == INVALID_PAGE_ID) {
        return End();
    }

    page_id_t current_page_id = root_page_id_;

    while (true) {
        Page* page = buffer_pool_manager_->FetchPage(current_page_id);
        if (page == nullptr) {
            return End();
        }

        auto tree_page = reinterpret_cast<BPlusTreePage*>(page->GetData());

        if (tree_page->IsLeafPage()) {
            auto leaf =
                reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType>*>(
                    page->GetData());
            // 检查叶子页面是否有数据
            if (leaf->GetSize() == 0) {
                buffer_pool_manager_->UnpinPage(current_page_id, false);
                return End();
            }
            buffer_pool_manager_->UnpinPage(current_page_id, false);
            return Iterator(this, current_page_id, 0);
        }

        auto internal =
            reinterpret_cast<BPlusTreeInternalPage<KeyType>*>(page->GetData());
        page_id_t next_page_id = internal->ValueAt(0);
        buffer_pool_manager_->UnpinPage(current_page_id, false);
        current_page_id = next_page_id;
    }
}

template <typename KeyType, typename ValueType>
typename BPlusTree<KeyType, ValueType>::Iterator
BPlusTree<KeyType, ValueType>::Begin(const KeyType& key) {
    std::lock_guard<std::mutex> lock(latch_);

    if (root_page_id_ == INVALID_PAGE_ID) {
        return End();
    }

    Page* leaf_page = FindLeafPage(key, false);
    if (leaf_page == nullptr) {
        return End();
    }

    auto leaf = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType>*>(
        leaf_page->GetData());

    int index = leaf->KeyIndex(key);
    page_id_t page_id = leaf_page->GetPageId();

    buffer_pool_manager_->UnpinPage(page_id, false);

    return Iterator(this, page_id, index);
}

template <typename KeyType, typename ValueType>
typename BPlusTree<KeyType, ValueType>::Iterator
BPlusTree<KeyType, ValueType>::End() {
    return Iterator(this, INVALID_PAGE_ID, 0);
}

template <typename KeyType, typename ValueType>
Page* BPlusTree<KeyType, ValueType>::FindLeafPage(const KeyType& key,
                                                  bool is_write_op) {
    (void)is_write_op;  // 暂时不使用is_write_op参数
    if (root_page_id_ == INVALID_PAGE_ID) {
        LOG_DEBUG("Tree is empty, root_page_id is invalid");
        return nullptr;
    }

    LOG_DEBUG("FindLeafPage: starting from root page " << root_page_id_);

    page_id_t current_page_id = root_page_id_;
    Page* current_page = nullptr;

    while (true) {
        // 如果之前有页面，先unpin它
        if (current_page != nullptr) {
            buffer_pool_manager_->UnpinPage(current_page->GetPageId(), false);
        }

        LOG_TRACE("FindLeafPage: fetching page " << current_page_id);
        current_page = buffer_pool_manager_->FetchPage(current_page_id);
        if (current_page == nullptr) {
            LOG_ERROR("Failed to fetch page: "
                      << current_page_id << " (num_pages="
                      << buffer_pool_manager_->GetDiskManager()->GetNumPages()
                      << ")");
            return nullptr;
        }

        auto tree_page =
            reinterpret_cast<BPlusTreePage*>(current_page->GetData());

        // 验证页面内容的一致性
        if (tree_page->GetPageId() != current_page_id) {
            LOG_ERROR("Page " << current_page_id
                              << " has inconsistent page_id: "
                              << tree_page->GetPageId());
            buffer_pool_manager_->UnpinPage(current_page_id, false);
            return nullptr;
        }

        if (tree_page->IsLeafPage()) {
            LOG_TRACE("Found leaf page: " << current_page_id);
            return current_page;  // 返回leaf页面，调用者负责unpin
        }

        // 内部页面，继续向下查找
        auto internal = reinterpret_cast<BPlusTreeInternalPage<KeyType>*>(
            current_page->GetData());
        int index = internal->KeyIndex(key);

        // 确保索引在有效范围内
        if (index < 0) {
            LOG_ERROR("Invalid index returned by KeyIndex: " << index);
            index = 0;
        }
        if (index > internal->GetSize()) {
            LOG_ERROR("Index too large: " << index
                                          << ", size: " << internal->GetSize());
            index = internal->GetSize();
        }

        page_id_t next_page_id = internal->ValueAt(index);

        LOG_TRACE("Traversing from page " << current_page_id << " to page "
                                          << next_page_id << " at index "
                                          << index);

        if (next_page_id == INVALID_PAGE_ID) {
            LOG_ERROR("Invalid next page ID in internal page");
            buffer_pool_manager_->UnpinPage(current_page_id, false);
            return nullptr;
        }

        current_page_id = next_page_id;
    }
}

template <typename KeyType, typename ValueType>
bool BPlusTree<KeyType, ValueType>::InsertIntoLeaf(
    const KeyType& key, const ValueType& value,
    BPlusTreeLeafPage<KeyType, ValueType>* leaf) {
    // 先检查键是否已存在
    int index = leaf->KeyIndex(key);
    if (index < leaf->GetSize() && leaf->KeyAt(index) == key) {
        // 键已存在，更新值
        leaf->SetValueAt(index, value);
        return true;
    }

    // 检查页面是否有空间
    if (leaf->GetSize() < leaf->GetMaxSize()) {
        // 有空间，直接插入
        return leaf->Insert(key, value);
    }

    // 页面已满，需要分裂
    LOG_DEBUG("Leaf page is full, need to split. Size: "
              << leaf->GetSize() << ", MaxSize: " << leaf->GetMaxSize());

    // 创建新页面
    page_id_t new_page_id;
    Page* new_page = buffer_pool_manager_->NewPage(&new_page_id);
    if (new_page == nullptr) {
        LOG_ERROR("Failed to allocate new page for split");
        return false;
    }

    auto new_leaf = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType>*>(
        new_page->GetData());
    new_leaf->Init(new_page_id, leaf->GetParentPageId());

    // 创建临时缓冲区来存储所有键值对（包括新的）
    const int total_entries = leaf->GetSize() + 1;
    std::vector<std::pair<KeyType, ValueType>> temp_entries;
    temp_entries.reserve(total_entries);

    // 将原页面的所有键值对和新键值对合并到临时缓冲区中
    // int original_index = 0;
    bool inserted = false;

    for (int i = 0; i < leaf->GetSize(); i++) {
        KeyType current_key = leaf->KeyAt(i);
        ValueType current_value = leaf->ValueAt(i);

        // 如果还没插入新键值对，并且当前键大于要插入的键，则先插入新键值对
        if (!inserted && key < current_key) {
            temp_entries.emplace_back(key, value);
            inserted = true;
        }

        temp_entries.emplace_back(current_key, current_value);
    }

    // 如果新键值对应该在最后，则在这里插入
    if (!inserted) {
        temp_entries.emplace_back(key, value);
    }

    // 计算分裂点
    int split_point = total_entries / 2;

    // 清空原页面并填入前半部分数据
    leaf->SetSize(0);
    for (int i = 0; i < split_point; i++) {
        leaf->SetKeyAt(i, temp_entries[i].first);
        leaf->SetValueAt(i, temp_entries[i].second);
    }
    leaf->SetSize(split_point);

    // 填入新页面后半部分数据
    for (int i = split_point; i < total_entries; i++) {
        new_leaf->SetKeyAt(i - split_point, temp_entries[i].first);
        new_leaf->SetValueAt(i - split_point, temp_entries[i].second);
    }
    new_leaf->SetSize(total_entries - split_point);

    // 更新链表指针
    new_leaf->SetNextPageId(leaf->GetNextPageId());
    leaf->SetNextPageId(new_page_id);

    // 获取中间键（新页面的第一个键）
    KeyType middle_key;
    if (new_leaf->GetSize() > 0) {
        middle_key = new_leaf->KeyAt(0);
    } else {
        LOG_ERROR("New leaf node has no elements after split");
        buffer_pool_manager_->UnpinPage(new_page_id, false);
        buffer_pool_manager_->DeletePage(new_page_id);
        return false;
    }

    LOG_DEBUG("Split completed, middle key: " << middle_key);

    // 确保新页面被标记为dirty
    new_page->SetDirty(true);

    // 插入到父节点
    InsertIntoParent(leaf, middle_key, new_leaf);

    // 立即刷新新创建的页面，确保持久化
    buffer_pool_manager_->UnpinPage(new_page_id, true);
    buffer_pool_manager_->FlushPage(new_page_id);

    return true;
}

template <typename KeyType, typename ValueType>
template <typename N>
bool BPlusTree<KeyType, ValueType>::Split(N* node) {
    // 这个函数现在主要由InsertIntoLeaf处理，保留用于兼容性
    LOG_ERROR(
        "Split function called - this should be handled by InsertIntoLeaf");
    return false;
}

template <typename KeyType, typename ValueType>
void BPlusTree<KeyType, ValueType>::InsertIntoParent(BPlusTreePage* old_node,
                                                     const KeyType& key,
                                                     BPlusTreePage* new_node) {
    if (old_node->IsRootPage()) {
        // 创建新的根页面
        page_id_t new_root_id;
        Page* new_root_page = buffer_pool_manager_->NewPage(&new_root_id);
        if (new_root_page == nullptr) {
            LOG_ERROR("Failed to create new root page");
            return;
        }

        auto new_root = reinterpret_cast<BPlusTreeInternalPage<KeyType>*>(
            new_root_page->GetData());
        new_root->Init(new_root_id);

        // 设置根页面的初始状态
        // 布局：[old_node_id] [key] [new_node_id]
        new_root->SetValueAt(0, old_node->GetPageId());  // 第一个子页面
        new_root->SetKeyAt(1, key);                      // 第一个键
        new_root->SetValueAt(1, new_node->GetPageId());  // 第二个子页面
        new_root->SetSize(1);                            // 有1个键，2个值

        // 更新子页面的父指针
        old_node->SetParentPageId(new_root_id);
        new_node->SetParentPageId(new_root_id);

        // 更新根页面ID
        root_page_id_ = new_root_id;
        UpdateRootPageId(root_page_id_);

        // 确保新根页面被持久化
        new_root_page->SetDirty(true);
        buffer_pool_manager_->UnpinPage(new_root_id, true);
        buffer_pool_manager_->FlushPage(new_root_id);

        LOG_DEBUG("Created new root page: " << new_root_id);
        return;
    }

    // 非根页面的情况 - 其余代码保持不变
    Page* parent_page =
        buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
    if (parent_page == nullptr) {
        LOG_ERROR("Failed to fetch parent page");
        return;
    }

    auto parent = reinterpret_cast<BPlusTreeInternalPage<KeyType>*>(
        parent_page->GetData());
    page_id_t parent_page_id = parent_page->GetPageId();

    // 检查父页面是否有空间
    if (parent->GetSize() < parent->GetMaxSize()) {
        // 有空间，直接插入
        parent->InsertNodeAfter(old_node->GetPageId(), key,
                                new_node->GetPageId());
        new_node->SetParentPageId(parent->GetPageId());
        parent_page->SetDirty(true);
        buffer_pool_manager_->UnpinPage(parent_page_id, true);
        buffer_pool_manager_->FlushPage(parent_page_id);  // 立即刷新父页面
        LOG_DEBUG("Inserted into parent page successfully");
    } else {
        // 父页面满了，需要分裂父页面
        // 这是一个递归过程，但为了简化，我们先尝试插入
        try {
            parent->InsertNodeAfter(old_node->GetPageId(), key,
                                    new_node->GetPageId());
            new_node->SetParentPageId(parent->GetPageId());

            // 现在分裂父页面
            if (parent->GetSize() > parent->GetMaxSize()) {
                LOG_DEBUG("Parent page needs splitting after insertion");
                // 创建新的内部页面
                page_id_t new_parent_page_id;
                Page* new_parent_page =
                    buffer_pool_manager_->NewPage(&new_parent_page_id);
                if (new_parent_page != nullptr) {
                    auto new_parent =
                        reinterpret_cast<BPlusTreeInternalPage<KeyType>*>(
                            new_parent_page->GetData());
                    new_parent->Init(new_parent_page_id,
                                     parent->GetParentPageId());

                    // 分裂内部页面
                    parent->MoveHalfTo(new_parent, buffer_pool_manager_);

                    // 获取中间键（需要提升到上一层）
                    KeyType middle_key_to_promote;
                    if (new_parent->GetSize() > 0) {
                        // 对于内部页面，第一个键被提升
                        middle_key_to_promote = new_parent->KeyAt(1);

                        // 从新页面移除提升的键
                        for (int i = 1; i < new_parent->GetSize(); i++) {
                            new_parent->SetKeyAt(i, new_parent->KeyAt(i + 1));
                        }
                        new_parent->IncreaseSize(-1);

                        // 递归插入到父页面
                        InsertIntoParent(parent, middle_key_to_promote,
                                         new_parent);
                    }

                    new_parent_page->SetDirty(true);
                    buffer_pool_manager_->UnpinPage(new_parent_page_id, true);
                    buffer_pool_manager_->FlushPage(
                        new_parent_page_id);  // 立即刷新新父页面
                }
            }
        } catch (const std::exception& e) {
            LOG_ERROR("Exception during parent insertion: " << e.what());
        }

        parent_page->SetDirty(true);
        buffer_pool_manager_->UnpinPage(parent_page_id, true);
        buffer_pool_manager_->FlushPage(parent_page_id);  // 确保父页面被刷新
    }
}

template <typename KeyType, typename ValueType>
bool BPlusTree<KeyType, ValueType>::ShouldCoalesceOrRedistribute(
    BPlusTreePage* node) {
    if (node->IsRootPage()) {
        return node->GetSize() == 0;
    }
    // 当节点大小小于最大值的一半时，需要合并或重分布
    return node->GetSize() < (node->GetMaxSize() + 1) / 2;
}

template <typename KeyType, typename ValueType>
bool BPlusTree<KeyType, ValueType>::CoalesceOrRedistribute(BPlusTreePage* node,
                                                           txn_id_t txn_id) {
    if (node->IsRootPage()) {
        return AdjustRoot(node);
    }

    Page* parent_page =
        buffer_pool_manager_->FetchPage(node->GetParentPageId());
    if (parent_page == nullptr) {
        return false;
    }

    auto parent = reinterpret_cast<BPlusTreeInternalPage<KeyType>*>(
        parent_page->GetData());
    int node_index = parent->ValueIndex(node->GetPageId());

    if (node_index == -1) {
        buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), false);
        return false;
    }

    // 寻找兄弟节点
    int sibling_index = (node_index == 0) ? 1 : node_index - 1;

    if (sibling_index > parent->GetSize()) {
        buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), false);
        return false;
    }

    page_id_t sibling_page_id = parent->ValueAt(sibling_index);

    Page* sibling_page = buffer_pool_manager_->FetchPage(sibling_page_id);
    if (sibling_page == nullptr) {
        buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), false);
        return false;
    }

    auto sibling = reinterpret_cast<BPlusTreePage*>(sibling_page->GetData());

    bool node_deleted = false;

    // 判断是合并还是重分布
    if (node->GetSize() + sibling->GetSize() < node->GetMaxSize()) {
        // 合并
        bool is_predecessor = sibling_index < node_index;
        node_deleted =
            Coalesce(&sibling, &node, &parent,
                     is_predecessor ? sibling_index + 1 : node_index, txn_id);

        // 标记页面为dirty
        parent_page->SetDirty(true);
        sibling_page->SetDirty(true);

        buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
        buffer_pool_manager_->UnpinPage(sibling_page_id, true);
    } else {
        // 重分布
        Redistribute(sibling, node, node_index);

        // 标记页面为dirty
        parent_page->SetDirty(true);
        sibling_page->SetDirty(true);

        buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
        buffer_pool_manager_->UnpinPage(sibling_page_id, true);
    }

    return node_deleted;
}

template <typename KeyType, typename ValueType>
bool BPlusTree<KeyType, ValueType>::AdjustRoot(BPlusTreePage* old_root_node) {
    if (old_root_node->GetSize() > 0) {
        return false;  // 根节点仍有元素，不需要调整
    }

    if (!old_root_node->IsLeafPage()) {
        // 内部根节点为空，提升唯一的子节点为新根
        auto root =
            reinterpret_cast<BPlusTreeInternalPage<KeyType>*>(old_root_node);
        page_id_t new_root_id = root->ValueAt(0);

        if (new_root_id != INVALID_PAGE_ID) {
            Page* new_root_page = buffer_pool_manager_->FetchPage(new_root_id);
            if (new_root_page != nullptr) {
                auto new_root =
                    reinterpret_cast<BPlusTreePage*>(new_root_page->GetData());
                new_root->SetParentPageId(INVALID_PAGE_ID);
                new_root_page->SetDirty(true);
                buffer_pool_manager_->UnpinPage(new_root_id, true);
            }

            root_page_id_ = new_root_id;
        } else {
            root_page_id_ = INVALID_PAGE_ID;
        }
    } else {
        // 叶子根节点为空，整个树为空
        root_page_id_ = INVALID_PAGE_ID;
    }

    UpdateRootPageId(root_page_id_);
    return true;
}

template <typename KeyType, typename ValueType>
template <typename N>
bool BPlusTree<KeyType, ValueType>::Coalesce(
    N** neighbor_node, N** node, BPlusTreeInternalPage<KeyType>** parent,
    int index, txn_id_t txn_id) {
    auto neighbor = *neighbor_node;
    auto n = *node;
    auto p = *parent;

    // 确保neighbor在左边，node在右边
    if (index == 0) {
        std::swap(neighbor, n);
        index = 1;
    }

    page_id_t node_page_id = n->GetPageId();
    KeyType middle_key = p->KeyAt(index);

    LOG_DEBUG("Coalescing: merging page " << node_page_id
                                          << " into its neighbor");

    if (n->IsLeafPage()) {
        auto leaf_neighbor =
            reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType>*>(neighbor);
        auto leaf_node =
            reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType>*>(n);

        // 将node的所有元素移动到neighbor
        leaf_node->MoveAllTo(leaf_neighbor);

        // 更新链表指针
        leaf_neighbor->SetNextPageId(leaf_node->GetNextPageId());
    } else {
        auto internal_neighbor =
            reinterpret_cast<BPlusTreeInternalPage<KeyType>*>(neighbor);
        auto internal_node =
            reinterpret_cast<BPlusTreeInternalPage<KeyType>*>(n);
        internal_node->MoveAllTo(internal_neighbor, middle_key,
                                 buffer_pool_manager_);
    }

    // 从父页面移除指向合并节点的条目
    p->Remove(index);

    // 检查父页面是否需要合并或重分布
    if (ShouldCoalesceOrRedistribute(p)) {
        CoalesceOrRedistribute(p, txn_id);
    }

    // 删除被合并的页面
    LOG_DEBUG("Deleting merged page " << node_page_id);
    bool delete_success = buffer_pool_manager_->DeletePage(node_page_id);
    if (!delete_success) {
        LOG_ERROR("Failed to delete page " << node_page_id
                                           << " during coalesce operation");
    }

    return true;
}

template <typename KeyType, typename ValueType>
template <typename N>
void BPlusTree<KeyType, ValueType>::Redistribute(N* neighbor_node, N* node,
                                                 int index) {
    auto parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
    if (parent_page == nullptr) {
        return;
    }

    auto parent = reinterpret_cast<BPlusTreeInternalPage<KeyType>*>(
        parent_page->GetData());

    if (node->IsLeafPage()) {
        auto leaf_node =
            reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType>*>(node);
        auto leaf_neighbor =
            reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType>*>(
                neighbor_node);

        if (index == 0) {
            // node是最左的子节点，从右邻居借用
            leaf_neighbor->MoveFirstToEndOf(leaf_node);
            parent->SetKeyAt(1, leaf_neighbor->KeyAt(0));
        } else {
            // node在右边，从左邻居借用
            leaf_neighbor->MoveLastToFrontOf(leaf_node);
            parent->SetKeyAt(index, leaf_node->KeyAt(0));
        }
    } else {
        auto internal_node =
            reinterpret_cast<BPlusTreeInternalPage<KeyType>*>(node);
        auto internal_neighbor =
            reinterpret_cast<BPlusTreeInternalPage<KeyType>*>(neighbor_node);

        if (index == 0) {
            // node是最左的子节点
            KeyType middle_key = parent->KeyAt(1);
            internal_neighbor->MoveFirstToEndOf(internal_node, middle_key,
                                                buffer_pool_manager_);
            parent->SetKeyAt(1, internal_neighbor->KeyAt(1));
        } else {
            // node在右边
            KeyType middle_key = parent->KeyAt(index);
            internal_neighbor->MoveLastToFrontOf(internal_node, middle_key,
                                                 buffer_pool_manager_);
            parent->SetKeyAt(index, internal_node->KeyAt(1));
        }
    }

    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
}

template <typename KeyType, typename ValueType>
void BPlusTree<KeyType, ValueType>::UpdateRootPageId(page_id_t root_page_id) {
    LOG_DEBUG("UpdateRootPageId called with root_page_id: " << root_page_id);

    // 尝试获取页面0作为header页面
    Page* header_page = buffer_pool_manager_->FetchPage(0);

    if (header_page == nullptr) {
        // 页面0不存在，需要创建它
        LOG_DEBUG("Header page 0 does not exist, creating it...");

        page_id_t header_page_id;
        header_page = buffer_pool_manager_->NewPage(&header_page_id);

        if (header_page == nullptr) {
            LOG_ERROR("Failed to create header page");
            return;
        }

        // 如果分配的页面ID不是0，我们需要特殊处理
        if (header_page_id != 0) {
            LOG_WARN("Allocated header page has ID " << header_page_id
                                                     << " instead of 0");
            // 继续使用分配的页面，但这不是理想情况
        }

        // 初始化header页面数据
        std::memset(header_page->GetData(), 0, PAGE_SIZE);
    }

    // 写入根页面ID到header页面的开头
    *reinterpret_cast<page_id_t*>(header_page->GetData()) = root_page_id;

    // 标记页面为dirty，确保会被写入磁盘
    header_page->SetDirty(true);

    // Unpin页面，标记为dirty
    buffer_pool_manager_->UnpinPage(header_page->GetPageId(), true);

    LOG_DEBUG("Updated header page " << header_page->GetPageId()
                                     << " with root page ID: " << root_page_id);
}

// Iterator implementation
template <typename KeyType, typename ValueType>
BPlusTree<KeyType, ValueType>::Iterator::Iterator(BPlusTree* tree,
                                                  page_id_t page_id, int index)
    : tree_(tree), current_page_id_(page_id), current_index_(index) {}

template <typename KeyType, typename ValueType>
bool BPlusTree<KeyType, ValueType>::Iterator::IsEnd() const {
    return current_page_id_ == INVALID_PAGE_ID;
}

template <typename KeyType, typename ValueType>
std::pair<KeyType, ValueType>
BPlusTree<KeyType, ValueType>::Iterator::operator*() {
    if (IsEnd()) {
        throw std::runtime_error("Iterator is at end");
    }

    Page* page = tree_->buffer_pool_manager_->FetchPage(current_page_id_);
    if (page == nullptr) {
        throw std::runtime_error("Failed to fetch page");
    }

    auto leaf = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType>*>(
        page->GetData());

    // 添加边界检查
    if (current_index_ >= leaf->GetSize() || current_index_ < 0) {
        tree_->buffer_pool_manager_->UnpinPage(current_page_id_, false);
        throw std::runtime_error("Iterator index out of range");
    }

    KeyType key = leaf->KeyAt(current_index_);
    ValueType value = leaf->ValueAt(current_index_);

    tree_->buffer_pool_manager_->UnpinPage(current_page_id_, false);

    return std::make_pair(key, value);
}

template <typename KeyType, typename ValueType>
void BPlusTree<KeyType, ValueType>::Iterator::operator++() {
    if (IsEnd()) {
        return;
    }

    Page* page = tree_->buffer_pool_manager_->FetchPage(current_page_id_);
    if (page == nullptr) {
        current_page_id_ = INVALID_PAGE_ID;
        return;
    }

    auto leaf = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType>*>(
        page->GetData());

    current_index_++;

    if (current_index_ >= leaf->GetSize()) {
        // 移动到下一个页面
        page_id_t next_page_id = leaf->GetNextPageId();
        tree_->buffer_pool_manager_->UnpinPage(current_page_id_, false);

        if (next_page_id == INVALID_PAGE_ID) {
            // 没有下一个页面，迭代结束
            current_page_id_ = INVALID_PAGE_ID;
            current_index_ = 0;
        } else {
            // 移动到下一个页面的第一个元素
            current_page_id_ = next_page_id;
            current_index_ = 0;

            // 验证下一个页面是否有效且有数据
            Page* next_page =
                tree_->buffer_pool_manager_->FetchPage(next_page_id);
            if (next_page == nullptr) {
                current_page_id_ = INVALID_PAGE_ID;
            } else {
                auto next_leaf =
                    reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType>*>(
                        next_page->GetData());
                if (next_leaf->GetSize() == 0) {
                    // 下一个页面为空，迭代结束
                    current_page_id_ = INVALID_PAGE_ID;
                }
                tree_->buffer_pool_manager_->UnpinPage(next_page_id, false);
            }
        }
    } else {
        tree_->buffer_pool_manager_->UnpinPage(current_page_id_, false);
    }
}

// 显式模板实例化
template class BPlusTree<int32_t, RID>;
template class BPlusTree<int64_t, RID>;
template class BPlusTree<float, RID>;
template class BPlusTree<double, RID>;
template class BPlusTree<std::string, RID>;

}  // namespace SimpleRDBMS