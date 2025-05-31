#include <iostream>
#include <cassert>
#include <memory>
#include <vector>
#include "buffer/buffer_pool_manager.h"
#include "buffer/lru_replacer.h"
#include "storage/disk_manager.h"
#include "storage/page.h"
#include "common/config.h"

using namespace SimpleRDBMS;

// Test LRU Replacer
void TestLRUReplacer() {
    std::cout << "Testing LRU Replacer..." << std::endl;
    
    auto replacer = std::make_unique<LRUReplacer>(3);
    
    // Test basic operations
    replacer->Unpin(0);  // Use frame_id instead of page_id
    replacer->Unpin(1);
    replacer->Unpin(2);
    
    size_t victim;
    assert(replacer->Size() == 3);
    
    // Should evict frame 0 (least recently used)
    assert(replacer->Victim(&victim) == true);
    assert(victim == 0);
    assert(replacer->Size() == 2);
    
    // Pin frame 1
    replacer->Pin(1);
    assert(replacer->Size() == 1);
    
    // Should evict frame 2
    assert(replacer->Victim(&victim) == true);
    assert(victim == 2);
    assert(replacer->Size() == 0);
    
    std::cout << "LRU Replacer tests passed!" << std::endl;
}

// Test Buffer Pool Manager
void TestBufferPoolManager() {
    std::cout << "Testing Buffer Pool Manager..." << std::endl;
    
    const std::string db_name = "test.db";
    const size_t buffer_pool_size = 10;
    
    auto disk_manager = std::make_unique<DiskManager>(db_name);
    auto replacer = std::make_unique<LRUReplacer>(buffer_pool_size);
    auto bpm = std::make_unique<BufferPoolManager>(
        buffer_pool_size, 
        std::move(disk_manager), 
        std::move(replacer)
    );
    
    // Test new page
    page_id_t page_id;
    auto* page = bpm->NewPage(&page_id);
    assert(page != nullptr);
    assert(page->GetPageId() == page_id);
    
    // Write some data
    std::string data = "Hello, SimpleRDBMS!";
    std::memcpy(page->GetData(), data.c_str(), data.size());
    
    // Unpin the page
    assert(bpm->UnpinPage(page_id, true) == true);
    
    // Fetch the page again
    auto* fetched_page = bpm->FetchPage(page_id);
    assert(fetched_page != nullptr);
    assert(std::memcmp(fetched_page->GetData(), data.c_str(), data.size()) == 0);
    
    // Clean up
    bpm->UnpinPage(page_id, false);
    bpm->DeletePage(page_id);
    
    std::cout << "Buffer Pool Manager tests passed!" << std::endl;
    
    // Remove test file
    std::remove(db_name.c_str());
}

// Test Page operations
void TestPage() {
    std::cout << "Testing Page..." << std::endl;
    
    Page page;
    page.SetPageId(1);
    assert(page.GetPageId() == 1);
    
    // Test pin count
    assert(page.GetPinCount() == 0);
    page.IncreasePinCount();
    assert(page.GetPinCount() == 1);
    page.DecreasePinCount();
    assert(page.GetPinCount() == 0);
    
    // Test dirty flag
    assert(page.IsDirty() == false);
    page.SetDirty(true);
    assert(page.IsDirty() == true);
    
    // Test LSN
    page.SetLSN(100);
    assert(page.GetLSN() == 100);
    
    std::cout << "Page tests passed!" << std::endl;
}

// Main test runner
int main() {
    std::cout << "Running SimpleRDBMS Tests..." << std::endl;
    std::cout << "=============================" << std::endl;
    
    try {
        TestPage();
        TestLRUReplacer();
        TestBufferPoolManager();
        
        // TODO: Add more tests for other components
        // TestBPlusTree();
        // TestTableHeap();
        // TestTransactionManager();
        // TestRecoveryManager();
        // TestParser();
        // TestExecutionEngine();
        
        std::cout << "=============================" << std::endl;
        std::cout << "All tests passed!" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Test failed with exception: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}