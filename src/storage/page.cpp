// src/storage/page.cpp
#include "storage/page.h"

namespace SimpleRDBMS {

Page::Page() : page_id_(INVALID_PAGE_ID), pin_count_(0), is_dirty_(false), lsn_(INVALID_LSN) {
    std::memset(data_, 0, PAGE_SIZE);
}

Page::~Page() = default;

}  // namespace SimpleRDBMS