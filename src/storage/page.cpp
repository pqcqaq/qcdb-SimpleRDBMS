/*
 * 文件: page.cpp
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述: 实现存储层的 Page 类，Page 是 RDBMS
 * 中最基本的数据页单位，用于缓冲页面内容，记录页ID、脏页状态、Pin计数等状态。
 */

#include "storage/page.h"

namespace SimpleRDBMS {

// 构造函数：初始化页面对象的基本状态
// 默认是 INVALID_PAGE_ID，表示这个 Page 还没分配；pin_count 为 0
// 表示当前没人引用它 is_dirty 表示这个页有没有被修改过；lsn
// 是日志序列号，用于恢复
Page::Page()
    : page_id_(INVALID_PAGE_ID),
      pin_count_(0),
      is_dirty_(false),
      lsn_(INVALID_LSN) {
    // 注意：这里我们没有立刻清空 data_，是为了性能考虑
    // 真正使用 Page 数据时，再去手动 memset(data_, 0, PAGE_SIZE) 即可
    // 延迟初始化是为了避免一次性构造大量 Page 时影响性能
}

// 析构函数：当前没做资源释放，默认行为就够用了
Page::~Page() = default;

}  // namespace SimpleRDBMS
