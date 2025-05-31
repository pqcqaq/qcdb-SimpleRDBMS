#pragma once

#include <cstring>
#include <mutex>
#include <shared_mutex>

#include "common/config.h"

namespace SimpleRDBMS {

class Page {
   public:
    Page();
    ~Page();

    // Disable copy
    Page(const Page&) = delete;
    Page& operator=(const Page&) = delete;

    // Get page data
    char* GetData() { return data_; }
    const char* GetData() const { return data_; }

    // Page metadata
    page_id_t GetPageId() const { return page_id_; }
    void SetPageId(page_id_t page_id) { page_id_ = page_id; }

    // Pin count management
    void IncreasePinCount() { pin_count_++; }
    void DecreasePinCount() { pin_count_--; }
    int GetPinCount() const { return pin_count_; }

    // Dirty flag
    bool IsDirty() const { return is_dirty_; }
    void SetDirty(bool dirty) { is_dirty_ = dirty; }

    // LSN for recovery
    lsn_t GetLSN() const { return lsn_; }
    void SetLSN(lsn_t lsn) { lsn_ = lsn; }

    // Latch for thread safety
    void WLatch() { latch_.lock(); }
    void WUnlatch() { latch_.unlock(); }
    void RLatch() { latch_.lock_shared(); }
    void RUnlatch() { latch_.unlock_shared(); }

   protected:
    char data_[PAGE_SIZE];
    page_id_t page_id_;
    int pin_count_;
    bool is_dirty_;
    lsn_t lsn_;
    std::shared_mutex latch_;
};

}  // namespace SimpleRDBMS
