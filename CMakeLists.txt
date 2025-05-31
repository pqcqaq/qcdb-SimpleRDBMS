cmake_minimum_required(VERSION 3.16)
project(SimpleRDBMS)

# Set C++ standard
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Debug build by default
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()

# Compiler flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror")

# Debug flags - 添加更多调试信息
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g3 -O0 -fno-omit-frame-pointer -rdynamic")

# Release flags
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -DNDEBUG")

# Add include directories
include_directories(${CMAKE_SOURCE_DIR}/src)

# Find all source files
file(GLOB_RECURSE SOURCES 
    "${CMAKE_SOURCE_DIR}/src/*.cpp"
)

# Exclude test files from main executable
list(REMOVE_ITEM SOURCES 
    "${CMAKE_SOURCE_DIR}/src/test/test_main.cpp"
)

# Create main executable
add_executable(simple_rdbms ${SOURCES})

# Link libraries - 添加调试库
target_link_libraries(simple_rdbms pthread dl)

# Create test executable
file(GLOB_RECURSE TEST_SOURCES 
    "${CMAKE_SOURCE_DIR}/test/unit/*.cpp"
    "${CMAKE_SOURCE_DIR}/src/*.cpp"
)
list(REMOVE_ITEM TEST_SOURCES 
    "${CMAKE_SOURCE_DIR}/src/main.cpp"
)

set(TEST_SOURCES
    src/storage/disk_manager.cpp
    src/storage/page.cpp
    src/buffer/buffer_pool_manager.cpp
    src/buffer/lru_replacer.cpp
    src/index/b_plus_tree.cpp
    src/index/b_plus_tree_page.cpp
)

# B+ 树性能测试程序
add_executable(bplus_tree_test
    test/unit/bplus_tree_performance_test.cpp
    ${TEST_SOURCES}
)

target_include_directories(bplus_tree_test PRIVATE
    ${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(bplus_tree_test PRIVATE pthread)

set_target_properties(bplus_tree_test PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
)

add_executable(simple_bplus_test
    test/unit/simple_bplus_test.cpp
    ${TEST_SOURCES}
)

target_include_directories(simple_bplus_test PRIVATE
    ${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(simple_bplus_test PRIVATE pthread)

set_target_properties(simple_bplus_test PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
)

add_executable(comprehensive_test 
    test/comprehensive_test.cpp
    src/buffer/buffer_pool_manager.cpp
    src/buffer/lru_replacer.cpp
    src/catalog/catalog.cpp
    src/catalog/schema.cpp
    src/catalog/table_manager.cpp
    src/execution/execution_engine.cpp
    src/execution/executor.cpp
    src/index/b_plus_tree.cpp
    src/index/b_plus_tree_page.cpp
    src/parser/parser.cpp
    src/record/table_heap.cpp
    src/record/tuple.cpp
    src/recovery/log_manager.cpp
    src/recovery/log_record.cpp
    src/recovery/recovery_manager.cpp
    src/storage/disk_manager.cpp
    src/storage/page.cpp
    src/transaction/lock_manager.cpp
    src/transaction/transaction.cpp
    src/transaction/transaction_manager.cpp
)

target_include_directories(comprehensive_test PRIVATE
    ${CMAKE_SOURCE_DIR}/src
)
target_link_libraries(comprehensive_test PRIVATE pthread)
set_target_properties(comprehensive_test PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
)
# Print build type
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "C++ flags: ${CMAKE_CXX_FLAGS}")
message(STATUS "C++ debug flags: ${CMAKE_CXX_FLAGS_DEBUG}")