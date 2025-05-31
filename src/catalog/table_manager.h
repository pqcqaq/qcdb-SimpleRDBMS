#pragma once

#include <memory>
#include <string>
#include <vector>
#include "buffer/buffer_pool_manager.h"
#include "parser/ast.h"  // 需要包含这个头文件以使用 CreateTableStatement
// #include "catalog/catalog.h"  // 移除直接包含，改用前向声明

namespace SimpleRDBMS {

// Forward declarations
class Catalog;

class TableManager {
public:
    TableManager(BufferPoolManager* buffer_pool_manager, Catalog* catalog);
    
    // DDL operations
    bool CreateTable(const CreateTableStatement* stmt);
    bool DropTable(const std::string& table_name);
    bool CreateIndex(const std::string& index_name,
                    const std::string& table_name,
                    const std::vector<std::string>& key_columns);
    bool DropIndex(const std::string& index_name);
    
    // Get catalog
    Catalog* GetCatalog() { return catalog_; }

private:
    BufferPoolManager* buffer_pool_manager_;
    Catalog* catalog_;
};

}  // namespace SimpleRDBMS