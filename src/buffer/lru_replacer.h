#pragma once

#include <list>
#include <unordered_map>
#include <mutex>
#include "buffer/replacer.h"

namespace SimpleRDBMS {

class LRUReplacer : public Replacer {
public:
    explicit LRUReplacer(size_t num_pages);
    ~LRUReplacer() override;
    
    void Pin(size_t frame_id) override;
    void Unpin(size_t frame_id) override;
    bool Victim(size_t* frame_id) override;
    size_t Size() const override;

private:
    size_t num_pages_;
    std::list<size_t> lru_list_;
    std::unordered_map<size_t, std::list<size_t>::iterator> lru_map_;
    mutable std::mutex latch_;
};

}  // namespace SimpleRDBMS