#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include "buffer/buffer_pool_manager.h"
#include "catalog/schema.h"
#include "index/b_plus_tree.h"

namespace SimpleRDBMS {

class IndexManager {
public:
    explicit IndexManager(BufferPoolManager* buffer_pool_manager);
    
    // Create an index
    bool CreateIndex(const std::string& index_name, 
                    const std::string& table_name,
                    const std::vector<std::string>& key_columns);
    
    // Drop an index
    bool DropIndex(const std::string& index_name);
    
    // Get index
    template <typename KeyType>
    BPlusTree<KeyType, RID>* GetIndex(const std::string& index_name);

private:
    BufferPoolManager* buffer_pool_manager_;
    std::unordered_map<std::string, std::unique_ptr<void, std::function<void(void*)>>> indexes_;
};

}  // namespace SimpleRDBMS