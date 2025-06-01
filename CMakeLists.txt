cmake_minimum_required(VERSION 3.10)
project(SimpleRDBMS)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 设置编译选项
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -pedantic -O3")

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
    src/index/index_manager.cpp
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
    src/stat/stat.cpp
)

set(SERVER_SOURCES
    src/server/connection/connection.cpp
    src/server/connection/connection_manager.cpp
    src/server/connection/session.cpp
    src/server/protocol/protocol_handler.cpp
    src/server/protocol/simple_protocol.cpp
    src/server/thread/thread_pool.cpp
    src/server/thread/worker_thread.cpp
    src/server/query/query_processor.cpp
    src/server/query/query_context.cpp
    src/server/config/server_config.cpp
    src/server/config/config_manager.cpp
    src/server/database_server.cpp
)

# 创建核心静态库
add_library(simple_rdbms_core STATIC ${CORE_SOURCES})

# 添加服务器源文件到核心库
add_library(simple_rdbms_server_core STATIC ${SERVER_SOURCES})

# 主程序
add_executable(simple_rdbms src/main.cpp)
target_link_libraries(simple_rdbms simple_rdbms_core)

# Server
add_executable(simple_rdbms_server 
    src/server/server_main.cpp
)
target_link_libraries(simple_rdbms_server 
    simple_rdbms_server_core
    simple_rdbms_core
    pthread
)
# 测试程序
add_executable(test_main test/unit/test_main.cpp)
target_link_libraries(test_main simple_rdbms_core)

add_executable(bplus_tree_test test/unit/bplus_tree_performance_test.cpp)
target_link_libraries(bplus_tree_test simple_rdbms_core)

add_executable(simple_bplus_test test/unit/simple_bplus_test.cpp)
target_link_libraries(simple_bplus_test simple_rdbms_core)

add_executable(update_delete_test test/unit/update_delete_test.cpp)
target_link_libraries(update_delete_test simple_rdbms_core)

add_executable(persistence_recovery_test test/unit/persistence_recovery_test.cpp)
target_link_libraries(persistence_recovery_test simple_rdbms_core)

add_executable(comprehensive_test test/comprehensive_test.cpp)
target_link_libraries(comprehensive_test simple_rdbms_core)

# simple_index_manager_test
add_executable(simple_index_manager_test test/unit/simple_index_manager_test.cpp)
target_link_libraries(simple_index_manager_test simple_rdbms_core)

# index_manager_test
add_executable(index_manager_test test/unit/index_manager_test.cpp)
target_link_libraries(index_manager_test simple_rdbms_core gtest)

# test_log_recovery.cpp
add_executable(test_log_recovery test/unit/test_log_recovery.cpp)
target_link_libraries(test_log_recovery simple_rdbms_core gtest)

# index_performance_test
add_executable(index_performance_test test/unit/index_performance_test.cpp)
target_link_libraries(index_performance_test simple_rdbms_core gtest)

# 编译标志
target_compile_options(simple_rdbms_core PRIVATE -Wall -Wextra)

target_compile_options(simple_rdbms_server_core PRIVATE -Wall -Wextra)

# 设置输出目录
set_target_properties(simple_rdbms simple_rdbms_core test_main bplus_tree_test 
                      simple_bplus_test update_delete_test comprehensive_test 
                        persistence_recovery_test simple_rdbms_server_core
                      PROPERTIES
                      RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}
                      LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}
                      ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})