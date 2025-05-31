// src/storage/page.cpp
#include "storage/page.h"

namespace SimpleRDBMS {

Page::Page() : page_id_(INVALID_PAGE_ID), pin_count_(0), is_dirty_(false), lsn_(INVALID_LSN) {
    // 延迟初始化：只有在实际使用时才清零数据
    // 这样可以避免在创建大量Page对象时的性能问题
    // std::memset(data_, 0, PAGE_SIZE);
}

Page::~Page() = default;

}  // namespace SimpleRDBMS