/*
 * 文件: types.h
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述: 数据库系统核心类型定义，包括SQL数据类型、值存储、列定义和记录标识符
 */

#pragma once

#include <functional>
#include <memory>
#include <string>
#include <variant>
#include <vector>

#include "common/config.h"

namespace SimpleRDBMS {

// ==================== SQL数据类型枚举 ====================
// 定义数据库支持的所有SQL数据类型
// 这个枚举用于类型检查、存储大小计算、序列化等操作
enum class TypeId {
    INVALID = 0,  // 无效类型，用于错误检测
    BOOLEAN,      // 布尔型：true/false，占1字节
    TINYINT,      // 微整型：-128到127，占1字节
    SMALLINT,     // 短整型：-32768到32767，占2字节
    INTEGER,      // 整型：-2^31到2^31-1，占4字节
    BIGINT,       // 长整型：-2^63到2^63-1，占8字节
    DECIMAL,      // 定点数：高精度数值类型（暂未完全实现）
    FLOAT,        // 单精度浮点：32位IEEE 754标准
    DOUBLE,       // 双精度浮点：64位IEEE 754标准
    VARCHAR,      // 变长字符串：可指定最大长度
    TIMESTAMP     // 时间戳：日期时间类型（暂未完全实现）
};

// ==================== 通用值存储类型 ====================
// 使用std::variant实现类型安全的联合体
// 这样可以在一个变量中存储不同类型的值，同时保持类型安全
// variant会自动管理内存，并提供类型检查功能
using Value = std::variant<bool,        // 对应BOOLEAN类型
                           int8_t,      // 对应TINYINT类型
                           int16_t,     // 对应SMALLINT类型
                           int32_t,     // 对应INTEGER类型
                           int64_t,     // 对应BIGINT类型
                           float,       // 对应FLOAT类型
                           double,      // 对应DOUBLE类型
                           std::string  // 对应VARCHAR类型
                           >;

// ==================== 列定义结构体 ====================
// 描述表中每一列的完整信息
// 这个结构体在创建表、验证数据、生成执行计划时都会用到
struct Column {
    std::string name;     // 列名，用于SQL中的引用
    TypeId type;          // 数据类型，决定了存储方式和操作规则
    size_t size;          // 列大小，主要用于VARCHAR类型指定最大长度
    bool nullable;        // 是否允许NULL值，影响插入时的验证
    bool is_primary_key;  // 是否为主键，影响唯一性约束和索引创建
};

// ==================== 记录标识符结构体 ====================
// RID (Record Identifier) 用于唯一标识数据库中的每一条记录
// 这是一个两级定位系统：先定位到页面，再定位到页面内的槽位
struct RID {
    page_id_t page_id;       // 页面ID，标识记录所在的磁盘页面
    slot_offset_t slot_num;  // 槽位号，标识记录在页面内的位置

    // 重载相等运算符，用于RID比较
    // 两个RID相等当且仅当它们指向同一个页面的同一个槽位
    bool operator==(const RID& other) const {
        return page_id == other.page_id && slot_num == other.slot_num;
    }
};

}  // namespace SimpleRDBMS

// ==================== 标准库扩展 ====================
// 为RID提供hash函数特化，这样RID就可以用在std::unordered_map等hash容器中
// 这对于实现锁表、缓存等功能非常重要
namespace std {
template <>
struct hash<SimpleRDBMS::RID> {
    size_t operator()(const SimpleRDBMS::RID& rid) const {
        // 使用异或和位移的组合来生成hash值
        // 这种方法能够较好地分散hash值，减少冲突
        return hash<SimpleRDBMS::page_id_t>()(rid.page_id) ^
               (hash<SimpleRDBMS::slot_offset_t>()(rid.slot_num) << 1);
    }
};
}  // namespace std