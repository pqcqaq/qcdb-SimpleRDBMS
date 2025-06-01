/*
 * 文件: config.h
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述: 系统配置文件，定义了数据库系统的核心常量和类型别名
 */

#pragma once

#include <cstddef>
#include <cstdint>

namespace SimpleRDBMS {

// ==================== 页面管理相关常量 ====================
// 页面大小设置为4KB，这是大多数现代数据库系统的标准选择
// 4KB既能保证良好的磁盘I/O效率，又不会造成过多的内存浪费
static constexpr size_t PAGE_SIZE = 4096;

// 缓冲池默认大小为100个页面，约400KB内存
// 这个大小适合教学和小型测试，生产环境中通常会设置得更大
static constexpr size_t BUFFER_POOL_SIZE = 100;

// ==================== B+树索引相关常量 ====================
// 单个tuple的最大大小限制为512字节
// 这个限制确保一个页面能容纳足够多的记录，避免页面利用率过低
static constexpr size_t MAX_TUPLE_SIZE = 512;

// B+树的阶数设置为64，意味着每个内部节点最多有64个子节点
// 这个值在磁盘I/O效率和树高度之间取得了平衡
// 阶数越大，树越"胖"，查找时需要的磁盘访问次数越少
static constexpr size_t B_PLUS_TREE_ORDER = 64;

// ==================== 事务管理相关常量 ====================
// 无效事务ID，用于标识未开始或已结束的事务
static constexpr int INVALID_TXN_ID = -1;

// 无效日志序列号，用于标识无效的日志记录
static constexpr int INVALID_LSN = -1;

// ==================== 核心类型定义 ====================
// 这些类型别名让代码更清晰，也方便以后修改底层类型

// 页面ID类型，用于唯一标识磁盘上的每一页
// 使用32位整数，支持约21亿个页面（约8TB数据）
using page_id_t = int32_t;

// 槽位偏移类型，用于定位页面内的记录位置
using slot_offset_t = int32_t;

// 事务ID类型，用于唯一标识每个事务
// 使用无符号32位整数，支持约42亿个并发事务
using txn_id_t = uint32_t;

// 日志序列号类型，用于WAL（Write-Ahead Logging）系统
// LSN确保日志记录的顺序性，是恢复机制的核心
using lsn_t = int32_t;

// 对象ID类型，用于标识数据库中的各种对象（表、索引等）
using oid_t = uint32_t;

// 无效页面ID常量，用于表示不存在或未分配的页面
static constexpr page_id_t INVALID_PAGE_ID = -1;

}  // namespace SimpleRDBMS