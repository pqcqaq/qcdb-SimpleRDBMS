#pragma once

#include <cstdint>
#include <cstddef>

namespace SimpleRDBMS {

// Page constants
static constexpr size_t PAGE_SIZE = 4096;  // 4KB
static constexpr size_t BUFFER_POOL_SIZE = 100;  // Number of pages in buffer pool

// B+ Tree constants
static constexpr size_t MAX_TUPLE_SIZE = 512;
static constexpr size_t B_PLUS_TREE_ORDER = 64;

// Transaction constants
static constexpr int INVALID_TXN_ID = -1;
static constexpr int INVALID_LSN = -1;

// Type definitions
using page_id_t = int32_t;
using slot_offset_t = int32_t;
using txn_id_t = uint32_t;
using lsn_t = int32_t;
using oid_t = uint32_t;

static constexpr page_id_t INVALID_PAGE_ID = -1;

}  // namespace SimpleRDBMS