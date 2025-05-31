#include "buffer/lru_replacer.h"

namespace SimpleRDBMS {

LRUReplacer::LRUReplacer(size_t num_pages) : num_pages_(num_pages) {
    // Constructor doesn't need to do anything else
    // The list and map will be empty initially
}

LRUReplacer::~LRUReplacer() {
    // Destructor doesn't need to do anything special
    // The STL containers will clean up automatically
}

void LRUReplacer::Pin(size_t frame_id) {
    std::unique_lock<std::mutex> lock(latch_);
    
    // Check if frame exists in the replacer
    auto it = lru_map_.find(frame_id);
    if (it == lru_map_.end()) {
        // Frame is not in the replacer, nothing to do
        return;
    }
    
    // Remove the frame from the LRU list
    lru_list_.erase(it->second);
    
    // Remove the frame from the map
    lru_map_.erase(frame_id);
}

void LRUReplacer::Unpin(size_t frame_id) {
    std::unique_lock<std::mutex> lock(latch_);
    
    // Check if frame already exists in the replacer
    auto it = lru_map_.find(frame_id);
    if (it != lru_map_.end()) {
        // Frame already exists, nothing to do
        // This is important: we don't want to add duplicates
        return;
    }
    
    // Check if we've reached the maximum number of frames
    // This check is optional - some implementations don't enforce this
    if (lru_list_.size() >= num_pages_) {
        // In a real implementation, this might be an error
        // For now, we'll just ignore it
        return;
    }
    
    // Add the frame to the end of the LRU list (most recently used position)
    lru_list_.push_back(frame_id);
    
    // Update the map with the iterator to the new element
    auto list_it = lru_list_.end();
    --list_it;  // Point to the last element we just added
    lru_map_[frame_id] = list_it;
}

bool LRUReplacer::Victim(size_t* frame_id) {
    std::unique_lock<std::mutex> lock(latch_);
    
    // Check if there are any frames to evict
    if (lru_list_.empty()) {
        return false;
    }
    
    // The victim is at the front of the list (least recently used)
    *frame_id = lru_list_.front();
    
    // Remove the victim from the list
    lru_list_.pop_front();
    
    // Remove the victim from the map
    lru_map_.erase(*frame_id);
    
    return true;
}

size_t LRUReplacer::Size() const {
    std::unique_lock<std::mutex> lock(latch_);
    return lru_list_.size();
}

}  // namespace SimpleRDBMS