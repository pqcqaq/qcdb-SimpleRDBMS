/*
 * 文件: b_plus_tree.cpp
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述:
 * B+树索引的完整实现，支持插入、删除、查找操作，包含节点分裂、合并等高级功能
 */

#include "index/b_plus_tree.h"

#include <unordered_set>

#include "common/debug.h"
#include "common/types.h"
#include "index/b_plus_tree_page.h"

namespace SimpleRDBMS {

/**
 * B+树构造函数
 * @param name 索引名称，用于在header page中定位该索引的根页面ID
 * @param buffer_pool_manager 缓冲池管理器，用于页面读写操作
 *
 * 实现思路：
 * 1. 初始化基本成员变量
 * 2. 验证参数有效性（buffer_pool_manager不能为空，索引名不能为空）
 * 3. 尝试从磁盘加载已存在的根页面ID
 * 4. 如果加载失败，则设置为INVALID_PAGE_ID，表示这是一个新的空树
 */
template <typename KeyType, typename ValueType>
BPlusTree<KeyType, ValueType>::BPlusTree(const std::string& name,
                                         BufferPoolManager* buffer_pool_manager)
    : index_name_(name),
      buffer_pool_manager_(buffer_pool_manager),
      root_page_id_(INVALID_PAGE_ID) {
    if (!buffer_pool_manager_) {
        throw std::invalid_argument("BufferPoolManager cannot be null");
    }

    if (index_name_.empty()) {
        throw std::invalid_argument("Index name cannot be empty");
    }

    LOG_DEBUG("Creating BPlusTree with name: " << index_name_);

    try {
        // 尝试从磁盘加载已存在的根页面ID
        LoadRootPageId();
        LOG_DEBUG(
            "BPlusTree constructor completed, root_page_id: " << root_page_id_);
    } catch (const std::exception& e) {
        LOG_ERROR(
            "BPlusTree constructor failed to load root page ID: " << e.what());
        // 如果加载失败，保持根页面ID为INVALID，表示空树
        root_page_id_ = INVALID_PAGE_ID;
    }
}

/**
 * B+树析构函数
 *
 * 实现思路：
 * 1. 使用锁保护，防止析构过程中的并发访问
 * 2. 不在析构函数中访问buffer_pool_manager_，避免悬挂指针问题
 * 3. 简单清理root_page_id_，实际的页面清理由buffer pool负责
 * 4. 捕获所有异常，避免析构函数抛出异常
 */
template <typename KeyType, typename ValueType>
BPlusTree<KeyType, ValueType>::~BPlusTree() {
    try {
        std::lock_guard<std::mutex> lock(latch_);
        // 不在析构中访问buffer_pool_manager_，避免悬挂指针
        // 只是简单清理根页面ID
        root_page_id_ = INVALID_PAGE_ID;
        LOG_DEBUG("BPlusTree destroyed: " << index_name_);
    } catch (const std::exception& e) {
        // 析构函数中不抛出异常，只记录日志
        LOG_ERROR("BPlusTree destructor exception: " << e.what());
    } catch (...) {
        LOG_ERROR("BPlusTree destructor unknown exception");
    }
}

/**
 * 从磁盘加载根页面ID
 *
 * 实现思路：
 * 1. 检查数据库文件大小，太小的话说明是新数据库
 * 2. 获取header page（固定为page 1），这里存储所有索引的根页面ID
 * 3. 使用索引名称的hash值计算在header page中的槽位位置
 * 4. 检查槽位中存储的hash是否匹配，确保找到正确的索引
 * 5. 验证根页面是否真实存在且有效
 * 6. 设置root_page_id_
 */
template <typename KeyType, typename ValueType>
void BPlusTree<KeyType, ValueType>::LoadRootPageId() {
    LOG_DEBUG("LoadRootPageId called for index: " << index_name_);

    int num_pages = buffer_pool_manager_->GetDiskManager()->GetNumPages();
    LOG_DEBUG("Disk has " << num_pages << " pages");

    // 数据库太小，说明是新创建的
    if (num_pages <= 1) {
        LOG_DEBUG("Database too small, setting root_page_id to INVALID");
        root_page_id_ = INVALID_PAGE_ID;
        return;
    }

    // 获取header page，这里存储所有索引的元数据
    page_id_t header_page_id = GetHeaderPageId();
    Page* header_page = buffer_pool_manager_->FetchPage(header_page_id);
    if (header_page == nullptr) {
        LOG_DEBUG("Header page " << header_page_id
                                 << " does not exist for index "
                                 << index_name_);
        root_page_id_ = INVALID_PAGE_ID;
        return;
    }

    // 计算这个索引在header page中的槽位位置
    // 使用简单的字符串hash算法
    uint32_t hash = 0;
    for (char c : index_name_) {
        hash = hash * 31 + static_cast<uint32_t>(c);
    }

    // 每个槽位存储：hash(4字节) + root_page_id(4字节)
    size_t slot_size = sizeof(page_id_t) + sizeof(uint32_t);
    size_t max_slots = (PAGE_SIZE - sizeof(uint32_t)) / slot_size;
    size_t slot_index = hash % max_slots;
    size_t offset = sizeof(uint32_t) + slot_index * slot_size;

    char* data = header_page->GetData();

    // 读取槽位中的数据
    uint32_t stored_hash = *reinterpret_cast<uint32_t*>(data + offset);
    page_id_t stored_root_page_id =
        *reinterpret_cast<page_id_t*>(data + offset + sizeof(uint32_t));

    buffer_pool_manager_->UnpinPage(header_page_id, false);

    // 检查hash是否匹配，不匹配说明这个槽位是空的或者是其他索引的
    if (stored_hash != hash) {
        LOG_DEBUG("Hash mismatch for index " << index_name_
                                             << ", tree is empty");
        root_page_id_ = INVALID_PAGE_ID;
        return;
    }

    // 检查root page ID的有效性
    if (stored_root_page_id == INVALID_PAGE_ID ||
        stored_root_page_id >= num_pages) {
        LOG_DEBUG("Invalid stored root page ID "
                  << stored_root_page_id << " for index " << index_name_);
        root_page_id_ = INVALID_PAGE_ID;
        return;
    }

    // 验证root page确实存在且是有效的B+树页面
    Page* root_page = buffer_pool_manager_->FetchPage(stored_root_page_id);
    if (root_page == nullptr) {
        LOG_WARN("Root page " << stored_root_page_id
                              << " does not exist, resetting to INVALID");
        root_page_id_ = INVALID_PAGE_ID;
        return;
    }

    // 检查页面类型是否正确
    auto tree_page = reinterpret_cast<BPlusTreePage*>(root_page->GetData());
    IndexPageType page_type = tree_page->GetPageType();

    if (page_type != IndexPageType::LEAF_PAGE &&
        page_type != IndexPageType::INTERNAL_PAGE) {
        LOG_ERROR("Root page "
                  << stored_root_page_id
                  << " has invalid page type: " << static_cast<int>(page_type));
        buffer_pool_manager_->UnpinPage(stored_root_page_id, false);
        root_page_id_ = INVALID_PAGE_ID;
        return;
    }

    buffer_pool_manager_->UnpinPage(stored_root_page_id, false);

    // 一切检查通过，设置根页面ID
    root_page_id_ = stored_root_page_id;
    LOG_DEBUG("Successfully loaded root page ID: "
              << root_page_id_ << " for index: " << index_name_);
}

/**
 * 获取header page的页面ID
 * @return header page的页面ID（固定为1）
 *
 * 实现思路：
 * 使用固定的page 1作为所有B+Tree的header metadata页面
 * 在page 1中，为每个索引分配一个槽位来存储其root page ID
 */
template <typename KeyType, typename ValueType>
page_id_t BPlusTree<KeyType, ValueType>::GetHeaderPageId() const {
    return 1;
}

/**
 * 插入键值对
 * @param key 要插入的键
 * @param value 要插入的值
 * @param txn_id 事务ID（当前未使用）
 * @return 插入是否成功
 *
 * 实现思路：
 * 1. 使用锁保护整个插入过程，确保线程安全
 * 2. 如果树为空，创建根页面并直接插入
 * 3. 如果树不为空，找到对应的叶子页面
 * 4. 尝试插入到叶子页面，如果页面满了会触发分裂
 * 5. 更新相关的dirty标记，确保数据持久化
 */
template <typename KeyType, typename ValueType>
bool BPlusTree<KeyType, ValueType>::Insert(const KeyType& key,
                                           const ValueType& value,
                                           txn_id_t txn_id) {
    std::lock_guard<std::mutex> lock(latch_);
    (void)txn_id;  // 当前未使用事务ID
    LOG_TRACE("Inserting key: " << key << " into index: " << index_name_);

    // 情况1：树为空，需要创建根页面
    if (root_page_id_ == INVALID_PAGE_ID) {
        LOG_DEBUG(
            "Creating root page for first insertion in index: " << index_name_);

        // 创建或获取header page
        page_id_t header_page_id = GetHeaderPageId();
        Page* header_page = buffer_pool_manager_->FetchPage(header_page_id);
        if (header_page == nullptr) {
            // 如果header page不存在，通过UpdateRootPageId来创建
            // 先创建根页面，然后调用UpdateRootPageId
            page_id_t new_page_id;
            Page* root_page = buffer_pool_manager_->NewPage(&new_page_id);
            if (root_page == nullptr) {
                LOG_ERROR("Failed to create root page");
                return false;
            }
            LOG_DEBUG("Allocated root page with ID: " << new_page_id);
            auto root =
                reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType>*>(
                    root_page->GetData());
            LOG_DEBUG("Initializing root page...");
            root->Init(new_page_id);
            if (root->GetPageId() != new_page_id) {
                LOG_WARN("Root page ID not set correctly, fixing...");
                root->SetPageId(new_page_id);
            }
            LOG_DEBUG("Inserting key " << key << " into root page...");
            if (!root->Insert(key, value)) {
                LOG_ERROR("Failed to insert into empty root");
                buffer_pool_manager_->UnpinPage(new_page_id, false);
                buffer_pool_manager_->DeletePage(new_page_id);
                return false;
            }
            root_page_id_ = new_page_id;
            LOG_DEBUG("Setting root_page_id_ to: " << root_page_id_);

            // 现在创建并更新header page
            UpdateRootPageId(root_page_id_);
            root_page->SetDirty(true);
            LOG_DEBUG("Unpinning root page...");
            buffer_pool_manager_->UnpinPage(new_page_id, true);
            // buffer_pool_manager_->FlushPage(new_page_id);

            LOG_DEBUG("Created root page and inserted key: "
                      << key << ", root_page_id: " << root_page_id_);
            return true;
        } else {
            buffer_pool_manager_->UnpinPage(header_page_id, false);
        }

        // 创建根页面（叶子页面）
        page_id_t new_page_id;
        Page* root_page = buffer_pool_manager_->NewPage(&new_page_id);
        if (root_page == nullptr) {
            LOG_ERROR("Failed to create root page");
            return false;
        }
        LOG_DEBUG("Allocated root page with ID: " << new_page_id);

        // 初始化为叶子页面
        auto root = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType>*>(
            root_page->GetData());
        LOG_DEBUG("Initializing root page...");
        root->Init(new_page_id);

        // 确保页面ID被正确设置
        if (root->GetPageId() != new_page_id) {
            LOG_WARN("Root page ID not set correctly, fixing...");
            root->SetPageId(new_page_id);
        }

        // 插入第一个键值对
        LOG_DEBUG("Inserting key " << key << " into root page...");
        if (!root->Insert(key, value)) {
            LOG_ERROR("Failed to insert into empty root");
            buffer_pool_manager_->UnpinPage(new_page_id, false);
            buffer_pool_manager_->DeletePage(new_page_id);
            return false;
        }

        // 更新根页面ID
        root_page_id_ = new_page_id;
        LOG_DEBUG("Setting root_page_id_ to: " << root_page_id_);

        // 立即更新并持久化header page中的根页面ID
        UpdateRootPageId(root_page_id_);

        // 标记页面为dirty并释放
        root_page->SetDirty(true);
        LOG_DEBUG("Unpinning root page...");
        buffer_pool_manager_->UnpinPage(new_page_id, true);

        // 强制刷新到磁盘，确保持久化
        // buffer_pool_manager_->FlushPage(new_page_id);
        // buffer_pool_manager_->FlushPage(header_page_id);

        LOG_DEBUG("Created root page and inserted key: "
                  << key << ", root_page_id: " << root_page_id_);
        return true;
    }

    // 情况2：树不为空，查找对应的叶子页面
    Page* leaf_page = FindLeafPage(key, true);
    if (leaf_page == nullptr) {
        LOG_ERROR("Failed to find leaf page for key: " << key);
        return false;
    }

    auto leaf = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType>*>(
        leaf_page->GetData());
    page_id_t leaf_page_id = leaf_page->GetPageId();

    // 尝试插入到叶子页面（可能触发分裂）
    bool result = InsertIntoLeaf(key, value, leaf);

    // 如果插入成功，标记页面为dirty
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

/**
 * 删除指定键的记录
 * @param key 要删除的键
 * @param txn_id 事务ID（当前未使用）
 * @return 删除是否成功
 *
 * 实现思路：
 * 1. 检查树是否为空
 * 2. 找到包含该键的叶子页面
 * 3. 从叶子页面中删除键值对
 * 4. 检查删除后是否需要进行合并或重分布操作
 * 5. 如果需要，执行相应的合并或重分布操作
 */
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

    // 找到包含该键的叶子页面
    Page* leaf_page = FindLeafPage(key, true);
    if (leaf_page == nullptr) {
        LOG_DEBUG("Failed to find leaf page for key: " << key);
        return false;
    }

    auto leaf = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType>*>(
        leaf_page->GetData());
    page_id_t leaf_page_id = leaf_page->GetPageId();

    // 尝试删除键值对
    bool result = leaf->Delete(key);

    if (result) {
        LOG_DEBUG("Successfully deleted key from leaf: " << key);
        leaf_page->SetDirty(true);

        // 检查删除后是否需要合并或重分布
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
        buffer_pool_manager_->UnpinPage(leaf_page_id, false);
    }

    return result;
}

/**
 * 查找指定键对应的值
 * @param key 要查找的键
 * @param value 输出参数，存储找到的值
 * @param txn_id 事务ID（当前未使用）
 * @return 查找是否成功
 *
 * 实现思路：
 * 1. 检查树是否为空
 * 2. 找到可能包含该键的叶子页面
 * 3. 在叶子页面中搜索键值对
 * 4. 如果找到，将值复制到输出参数
 */
template <typename KeyType, typename ValueType>
bool BPlusTree<KeyType, ValueType>::GetValue(const KeyType& key,
                                             ValueType* value,
                                             txn_id_t txn_id) {
    std::lock_guard<std::mutex> lock(latch_);
    (void)txn_id;

    if (root_page_id_ == INVALID_PAGE_ID) {
        return false;
    }

    // 找到可能包含该键的叶子页面
    Page* leaf_page = FindLeafPage(key, false);
    if (leaf_page == nullptr) {
        return false;
    }

    auto leaf = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType>*>(
        leaf_page->GetData());

    // 在叶子页面中查找键
    int index = leaf->KeyIndex(key);
    bool found = (index < leaf->GetSize() && leaf->KeyAt(index) == key);

    if (found) {
        *value = leaf->ValueAt(index);
    }

    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);

    return found;
}

/**
 * 返回指向第一个键值对的迭代器
 * @return 指向第一个键值对的迭代器
 *
 * 实现思路：
 * 1. 如果树为空，返回End()
 * 2. 从根页面开始，一直向左走到最左边的叶子页面
 * 3. 检查叶子页面是否有数据
 * 4. 返回指向第一个元素的迭代器
 */
template <typename KeyType, typename ValueType>
typename BPlusTree<KeyType, ValueType>::Iterator
BPlusTree<KeyType, ValueType>::Begin() {
    std::lock_guard<std::mutex> lock(latch_);

    if (root_page_id_ == INVALID_PAGE_ID) {
        return End();
    }

    page_id_t current_page_id = root_page_id_;

    // 一直向左走，找到最左边的叶子页面
    while (true) {
        Page* page = buffer_pool_manager_->FetchPage(current_page_id);
        if (page == nullptr) {
            return End();
        }

        auto tree_page = reinterpret_cast<BPlusTreePage*>(page->GetData());

        if (tree_page->IsLeafPage()) {
            // 到达叶子页面，检查是否有数据
            auto leaf =
                reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType>*>(
                    page->GetData());
            if (leaf->GetSize() == 0) {
                buffer_pool_manager_->UnpinPage(current_page_id, false);
                return End();
            }
            buffer_pool_manager_->UnpinPage(current_page_id, false);
            return Iterator(this, current_page_id, 0);
        }

        // 内部页面，继续向左走
        auto internal =
            reinterpret_cast<BPlusTreeInternalPage<KeyType>*>(page->GetData());
        page_id_t next_page_id = internal->ValueAt(0);  // 最左边的子页面
        buffer_pool_manager_->UnpinPage(current_page_id, false);
        current_page_id = next_page_id;
    }
}

/**
 * 返回指向第一个大于等于key的键值对的迭代器
 * @param key 搜索的键
 * @return 指向第一个大于等于key的键值对的迭代器
 *
 * 实现思路：
 * 1. 如果树为空，返回End()
 * 2. 找到可能包含该键的叶子页面
 * 3. 在叶子页面中找到第一个大于等于key的位置
 * 4. 返回指向该位置的迭代器
 */
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

    // 找到第一个大于等于key的位置
    int index = leaf->KeyIndex(key);
    page_id_t page_id = leaf_page->GetPageId();

    buffer_pool_manager_->UnpinPage(page_id, false);

    return Iterator(this, page_id, index);
}

/**
 * 返回表示结束的迭代器
 * @return 结束迭代器
 */
template <typename KeyType, typename ValueType>
typename BPlusTree<KeyType, ValueType>::Iterator
BPlusTree<KeyType, ValueType>::End() {
    return Iterator(this, INVALID_PAGE_ID, 0);
}

/**
 * 从根页面开始查找能包含指定键的叶子页面
 * @param key 要查找的键
 * @param is_write_op 是否为写操作（当前未使用）
 * @return 叶子页面指针，调用者负责unpin
 *
 * 实现思路：
 * 1. 从根页面开始遍历
 * 2. 在每个内部页面中找到下一层应该访问的子页面
 * 3. 重复直到到达叶子页面
 * 4. 添加循环检测和深度限制，防止无限循环
 * 5. 验证页面的有效性，确保数据一致性
 */
template <typename KeyType, typename ValueType>
Page* BPlusTree<KeyType, ValueType>::FindLeafPage(const KeyType& key,
                                                  bool is_write_op) {
    (void)is_write_op;
    if (root_page_id_ == INVALID_PAGE_ID) {
        LOG_DEBUG("Tree is empty, root_page_id is invalid");
        return nullptr;
    }

    LOG_DEBUG("FindLeafPage: starting from root page " << root_page_id_);
    page_id_t current_page_id = root_page_id_;
    Page* current_page = nullptr;

    // 循环检测和深度限制，防止无限循环
    std::unordered_set<page_id_t> visited_pages;
    const int MAX_TREE_DEPTH = 20;  // 合理的最大树深度
    int depth = 0;

    while (true) {
        // 检查是否已访问过此页面（循环检测）
        if (visited_pages.count(current_page_id) > 0) {
            LOG_ERROR(
                "FindLeafPage: Detected cycle in B+tree traversal at page "
                << current_page_id);
            if (current_page != nullptr) {
                buffer_pool_manager_->UnpinPage(current_page->GetPageId(),
                                                false);
            }
            return nullptr;
        }

        // 检查树深度
        if (depth >= MAX_TREE_DEPTH) {
            LOG_ERROR("FindLeafPage: Tree depth exceeded maximum limit "
                      << MAX_TREE_DEPTH);
            if (current_page != nullptr) {
                buffer_pool_manager_->UnpinPage(current_page->GetPageId(),
                                                false);
            }
            return nullptr;
        }

        visited_pages.insert(current_page_id);
        depth++;

        // 释放上一个页面
        if (current_page != nullptr) {
            buffer_pool_manager_->UnpinPage(current_page->GetPageId(), false);
        }

        // 获取当前页面
        LOG_TRACE("FindLeafPage: fetching page " << current_page_id
                                                 << " at depth " << depth);
        current_page = buffer_pool_manager_->FetchPage(current_page_id);
        if (current_page == nullptr) {
            LOG_ERROR("Failed to fetch page: "
                      << current_page_id << " (num_pages="
                      << buffer_pool_manager_->GetDiskManager()->GetNumPages()
                      << ")");
            return nullptr;
        }

        // 验证页面的一致性
        auto tree_page =
            reinterpret_cast<BPlusTreePage*>(current_page->GetData());
        if (tree_page->GetPageId() != current_page_id) {
            LOG_ERROR("Page " << current_page_id
                              << " has inconsistent page_id: "
                              << tree_page->GetPageId());
            buffer_pool_manager_->UnpinPage(current_page_id, false);
            return nullptr;
        }

        // 如果是叶子页面，找到了目标
        if (tree_page->IsLeafPage()) {
            LOG_TRACE("Found leaf page: " << current_page_id << " at depth "
                                          << depth);
            return current_page;
        }

        // 内部页面，继续向下查找
        auto internal = reinterpret_cast<BPlusTreeInternalPage<KeyType>*>(
            current_page->GetData());

        // 验证内部页面的有效性
        if (internal->GetSize() <= 0 ||
            internal->GetSize() > internal->GetMaxSize()) {
            LOG_ERROR("Invalid internal page size: "
                      << internal->GetSize() << " on page " << current_page_id);
            buffer_pool_manager_->UnpinPage(current_page_id, false);
            return nullptr;
        }

        // 在内部页面中查找下一个应该访问的子页面
        int index = internal->KeyIndex(key);
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

        // 验证下一个页面ID的有效性
        if (next_page_id >=
            buffer_pool_manager_->GetDiskManager()->GetNumPages()) {
            LOG_ERROR("Next page ID " << next_page_id
                                      << " exceeds database size");
            buffer_pool_manager_->UnpinPage(current_page_id, false);
            return nullptr;
        }

        current_page_id = next_page_id;
    }
}

/**
 * 向叶子页面插入键值对，如果页面满了会触发分裂
 * @param key 要插入的键
 * @param value 要插入的值
 * @param leaf 目标叶子页面
 * @return 插入是否成功
 *
 * 实现思路：
 * 1. 检查键是否已存在，如果存在则更新值
 * 2. 如果页面有空间，直接插入
 * 3. 如果页面已满，需要分裂：
 *    a. 创建新的叶子页面
 *    b. 将所有键值对（包括新的）排序后分成两部分
 *    c. 原页面保留前半部分，新页面保存后半部分
 *    d. 更新叶子页面间的链表指针
 *    e. 将分裂的中间键提升到父页面
 */
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

    // 创建新的叶子页面
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

    // 将原页面的所有键值对和新键值对合并到临时缓冲区中，保持排序
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

    // 计算分裂点（通常是中间位置）
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

    // 更新叶子页面间的链表指针
    new_leaf->SetNextPageId(leaf->GetNextPageId());
    leaf->SetNextPageId(new_page_id);

    // 获取中间键（新页面的第一个键）用于提升到父页面
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

    // 将分裂的中间键插入到父页面
    InsertIntoParent(leaf, middle_key, new_leaf);

    // 立即刷新新创建的页面，确保持久化
    buffer_pool_manager_->UnpinPage(new_page_id, true);
    // buffer_pool_manager_->FlushPage(new_page_id);

    return true;
}

/**
 * 分裂节点的通用函数（主要由InsertIntoLeaf处理，保留用于兼容性）
 * @param node 要分裂的节点
 * @return 分裂是否成功
 */
template <typename KeyType, typename ValueType>
template <typename N>
bool BPlusTree<KeyType, ValueType>::Split(N* node) {
    // 这个函数现在主要由InsertIntoLeaf处理，保留用于兼容性
    LOG_ERROR(
        "Split function called - this should be handled by InsertIntoLeaf");
    return false;
}

/**
 * 将分裂产生的新键插入到父页面中
 * @param old_node 原节点
 * @param key 要插入的键（分裂的中间键）
 * @param new_node 新节点
 *
 * 实现思路：
 * 1. 如果原节点是根节点，创建新的根节点
 * 2. 如果不是根节点，获取父页面
 * 3. 如果父页面有空间，直接插入
 * 4. 如果父页面没有空间，先插入再分裂父页面（递归过程）
 * 5. 确保所有相关页面被正确标记为dirty并持久化
 */
template <typename KeyType, typename ValueType>
void BPlusTree<KeyType, ValueType>::InsertIntoParent(BPlusTreePage* old_node,
                                                     const KeyType& key,
                                                     BPlusTreePage* new_node) {
    // 情况1：原节点是根节点，需要创建新的根节点
    if (old_node->IsRootPage()) {
        page_id_t new_root_id;
        Page* new_root_page = buffer_pool_manager_->NewPage(&new_root_id);
        if (new_root_page == nullptr) {
            LOG_ERROR("Failed to create new root page");
            return;
        }

        auto new_root = reinterpret_cast<BPlusTreeInternalPage<KeyType>*>(
            new_root_page->GetData());
        new_root->Init(new_root_id);

        // 设置新根页面的初始状态
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
        // buffer_pool_manager_->FlushPage(new_root_id);

        LOG_DEBUG("Created new root page: " << new_root_id);
        return;
    }

    // 情况2：不是根节点，获取父页面
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
        // buffer_pool_manager_->FlushPage(parent_page_id);
        LOG_DEBUG("Inserted into parent page successfully");
    } else {
        // 父页面满了，需要分裂父页面
        try {
            // 先插入到父页面
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
                    // buffer_pool_manager_->FlushPage(new_parent_page_id);
                }
            }
        } catch (const std::exception& e) {
            LOG_ERROR("Exception during parent insertion: " << e.what());
        }

        parent_page->SetDirty(true);
        buffer_pool_manager_->UnpinPage(parent_page_id, true);
        // buffer_pool_manager_->FlushPage(parent_page_id);
    }
}

/**
 * 检查节点是否需要合并或重分布
 * @param node 要检查的节点
 * @return 是否需要合并或重分布
 *
 * 实现思路：
 * 1. 如果是根节点，只有当大小为0时才需要调整
 * 2. 对于非根节点，当大小小于最大值的一半时需要合并或重分布
 */
template <typename KeyType, typename ValueType>
bool BPlusTree<KeyType, ValueType>::ShouldCoalesceOrRedistribute(
    BPlusTreePage* node) {
    if (node->IsRootPage()) {
        return node->GetSize() == 0;
    }
    // 当节点大小小于最大值的一半时，需要合并或重分布
    return node->GetSize() < (node->GetMaxSize() + 1) / 2;
}

/**
 * 执行节点的合并或重分布操作
 * @param node 要处理的节点
 * @param txn_id 事务ID
 * @return 节点是否被删除
 *
 * 实现思路：
 * 1. 如果是根节点，调用AdjustRoot
 * 2. 获取父页面和兄弟节点
 * 3. 判断是合并还是重分布：
 *    - 如果当前节点和兄弟节点的总大小小于最大值，进行合并
 *    - 否则进行重分布
 */
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

    // 寻找兄弟节点（优先选择右兄弟，否则选择左兄弟）
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

/**
 * 调整根节点（当根节点为空时）
 * @param old_root_node 原根节点
 * @return 根节点是否被删除
 *
 * 实现思路：
 * 1. 如果根节点仍有元素，不需要调整
 * 2. 如果是内部根节点且为空，提升唯一的子节点为新根
 * 3. 如果是叶子根节点且为空，整个树变为空
 * 4. 更新根页面ID
 */
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

/**
 * 合并两个节点
 * @param neighbor_node 兄弟节点
 * @param node 当前节点
 * @param parent 父节点
 * @param index 在父节点中的索引
 * @param txn_id 事务ID
 * @return 节点是否被删除
 *
 * 实现思路：
 * 1. 确保neighbor在左边，node在右边
 * 2. 将node的所有元素移动到neighbor
 * 3. 更新链表指针（对于叶子页面）
 * 4. 从父页面移除指向被合并节点的条目
 * 5. 递归检查父页面是否需要合并
 * 6. 删除被合并的页面
 */
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
        // 叶子页面合并
        auto leaf_neighbor =
            reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType>*>(neighbor);
        auto leaf_node =
            reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType>*>(n);

        // 将node的所有元素移动到neighbor
        leaf_node->MoveAllTo(leaf_neighbor);

        // 更新链表指针
        leaf_neighbor->SetNextPageId(leaf_node->GetNextPageId());
    } else {
        // 内部页面合并
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

/**
 * 在两个节点之间重分布元素
 * @param neighbor_node 兄弟节点
 * @param node 当前节点
 * @param index 在父节点中的索引
 *
 * 实现思路：
 * 1. 获取父页面
 * 2. 根据节点位置（左右）从兄弟节点借用元素
 * 3. 更新父页面中的分隔键
 * 4. 确保借用后两个节点的元素数量相对均衡
 */
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

/**
 * 更新header page中的根页面ID
 * @param root_page_id 新的根页面ID
 *
 * 实现思路：
 * 1. 获取或创建header page（固定为page 1）
 * 2. 使用索引名称的hash值计算槽位位置
 * 3. 在对应槽位写入hash值和根页面ID
 * 4. 标记页面为dirty并立即刷新到磁盘
 */
template <typename KeyType, typename ValueType>
void BPlusTree<KeyType, ValueType>::UpdateRootPageId(page_id_t root_page_id) {
    LOG_DEBUG("UpdateRootPageId called with root_page_id: "
              << root_page_id << " for index: " << index_name_);

    page_id_t header_page_id = GetHeaderPageId();
    Page* header_page = buffer_pool_manager_->FetchPage(header_page_id);

    if (header_page == nullptr) {
        LOG_DEBUG("Header page " << header_page_id
                                 << " does not exist, creating it...");

        page_id_t allocated_page_id;
        header_page = buffer_pool_manager_->NewPage(&allocated_page_id);
        if (header_page == nullptr) {
            LOG_ERROR("Failed to create header page");
            return;
        }

        // 确保分配的页面ID是我们期望的header page ID
        if (allocated_page_id != header_page_id) {
            LOG_WARN("UpdateRootPageId: Expected header page "
                     << header_page_id << " but got " << allocated_page_id);
            // 继续使用分配的页面，但需要更新逻辑
        }

        LOG_DEBUG("Created header page with ID: " << allocated_page_id);
        // 初始化header page
        std::memset(header_page->GetData(), 0, PAGE_SIZE);
    }

    // 计算在header page中的偏移量
    // 使用简单的hash来分配槽位，避免冲突
    uint32_t hash = 0;
    for (char c : index_name_) {
        hash = hash * 31 + static_cast<uint32_t>(c);
    }

    // 每个索引在header page中占用8字节（page_id_t + 4字节的名称hash）
    size_t slot_size = sizeof(page_id_t) + sizeof(uint32_t);
    size_t max_slots =
        (PAGE_SIZE - sizeof(uint32_t)) / slot_size;  // 保留4字节用于槽位计数
    size_t slot_index = hash % max_slots;
    size_t offset = sizeof(uint32_t) + slot_index * slot_size;

    char* data = header_page->GetData();

    // 写入hash和root page ID
    *reinterpret_cast<uint32_t*>(data + offset) = hash;
    *reinterpret_cast<page_id_t*>(data + offset + sizeof(uint32_t)) =
        root_page_id;

    header_page->SetDirty(true);
    buffer_pool_manager_->UnpinPage(header_page->GetPageId(), true);
    // buffer_pool_manager_->FlushPage(header_page->GetPageId());

    LOG_DEBUG("Updated header page with root page ID: "
              << root_page_id << " for index: " << index_name_ << " at slot "
              << slot_index);
}

// ============================================================================
// Iterator implementation - B+树迭代器实现
// ============================================================================

/**
 * Iterator构造函数
 * @param tree 所属的B+树指针
 * @param page_id 当前页面ID
 * @param index 在页面中的索引位置
 */
template <typename KeyType, typename ValueType>
BPlusTree<KeyType, ValueType>::Iterator::Iterator(BPlusTree* tree,
                                                  page_id_t page_id, int index)
    : tree_(tree), current_page_id_(page_id), current_index_(index) {}

/**
 * 检查迭代器是否到达末尾
 * @return 是否为结束迭代器
 */
template <typename KeyType, typename ValueType>
bool BPlusTree<KeyType, ValueType>::Iterator::IsEnd() const {
    return current_page_id_ == INVALID_PAGE_ID;
}

/**
 * 解引用操作符，返回当前键值对
 * @return 当前位置的键值对
 *
 * 实现思路：
 * 1. 检查迭代器是否有效
 * 2. 获取当前页面
 * 3. 检查索引边界
 * 4. 读取键值对并返回
 */
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

/**
 * 前进操作符，移动到下一个键值对
 *
 * 实现思路：
 * 1. 如果已经到达末尾，直接返回
 * 2. 获取当前页面
 * 3. 增加索引
 * 4. 如果索引超出当前页面范围，移动到下一个叶子页面
 * 5. 验证下一个页面的有效性
 */
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

// 显式模板实例化 - 支持的键值类型组合
template class BPlusTree<int32_t, RID>;
template class BPlusTree<int64_t, RID>;
template class BPlusTree<float, RID>;
template class BPlusTree<double, RID>;
template class BPlusTree<std::string, RID>;

}  // namespace SimpleRDBMS