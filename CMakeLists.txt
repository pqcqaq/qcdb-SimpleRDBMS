cmake_minimum_required(VERSION 3.10)
project(SimpleRDBMS)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 包含目录
include_directories(src)

# 核心库源文件
set(CORE_SOURCES
    src/buffer/buffer_pool_manager.cpp
    src/buffer/lru_replacer.cpp
    src/storage/disk_manager.cpp
    src/storage/page.cpp
    src/catalog/catalog.cpp
    src/catalog/schema.cpp
    src/catalog/table_manager.cpp
    src/record/table_heap.cpp
    src/record/tuple.cpp
    src/index/b_plus_tree.cpp
    src/index/b_plus_tree_page.cpp
    src/parser/parser.cpp
    src/execution/execution_engine.cpp
    src/execution/executor.cpp
    src/execution/expression_cloner.cpp
    src/execution/expression_evaluator.cpp
    src/transaction/transaction.cpp
    src/transaction/transaction_manager.cpp
    src/transaction/lock_manager.cpp
    src/recovery/log_manager.cpp
    src/recovery/log_record.cpp
    src/recovery/recovery_manager.cpp
)

# 创建核心静态库
add_library(simple_rdbms_core STATIC ${CORE_SOURCES})

# 主程序
add_executable(simple_rdbms src/main.cpp)
target_link_libraries(simple_rdbms simple_rdbms_core)

# 测试程序
add_executable(test_main test/unit/test_main.cpp)
target_link_libraries(test_main simple_rdbms_core)

add_executable(bplus_tree_test test/unit/bplus_tree_performance_test.cpp)
target_link_libraries(bplus_tree_test simple_rdbms_core)

add_executable(simple_bplus_test test/unit/simple_bplus_test.cpp)
target_link_libraries(simple_bplus_test simple_rdbms_core)

add_executable(update_delete_test test/unit/update_delete_test.cpp)
target_link_libraries(update_delete_test simple_rdbms_core)

add_executable(comprehensive_test test/comprehensive_test.cpp)
target_link_libraries(comprehensive_test simple_rdbms_core)

# 编译标志
target_compile_options(simple_rdbms_core PRIVATE -Wall -Wextra)

# 设置输出目录
set_target_properties(simple_rdbms simple_rdbms_core test_main bplus_tree_test 
                      simple_bplus_test update_delete_test comprehensive_test
                      PROPERTIES
                      RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}
                      LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}
                      ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})