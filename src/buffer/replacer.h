#pragma once

#include "common/config.h"

namespace SimpleRDBMS {

// Abstract base class for page replacement algorithms
class Replacer {
public:
    virtual ~Replacer() = default;
    
    // Remove a frame from replacer
    virtual void Pin(size_t frame_id) = 0;
    
    // Add a frame to replacer
    virtual void Unpin(size_t frame_id) = 0;
    
    // Pick a victim frame to evict
    virtual bool Victim(size_t* frame_id) = 0;
    
    // Get the number of frames that can be evicted
    virtual size_t Size() const = 0;
};

}  // namespace SimpleRDBMS