// src/recovery/log_record.cpp
#include "recovery/log_record.h"
#include <memory>

namespace SimpleRDBMS {

std::unique_ptr<LogRecord> LogRecord::DeserializeFrom(const char* buffer) {
    // Skip size field
    buffer += sizeof(size_t);
    
    // Read type
    LogRecordType type = *reinterpret_cast<const LogRecordType*>(buffer);
    buffer += sizeof(LogRecordType);
    
    // Read transaction id
    txn_id_t txn_id = *reinterpret_cast<const txn_id_t*>(buffer);
    buffer += sizeof(txn_id_t);
    
    // Read previous LSN
    lsn_t prev_lsn = *reinterpret_cast<const lsn_t*>(buffer);
    buffer += sizeof(lsn_t);
    
    // Create appropriate log record based on type
    switch (type) {
        case LogRecordType::BEGIN:
            return std::make_unique<BeginLogRecord>(txn_id);
            
        case LogRecordType::COMMIT:
            return std::make_unique<CommitLogRecord>(txn_id, prev_lsn);
            
        case LogRecordType::ABORT:
            return std::make_unique<AbortLogRecord>(txn_id, prev_lsn);
            
        case LogRecordType::INSERT:
        case LogRecordType::UPDATE:
        case LogRecordType::DELETE:
            // TODO: Implement deserialization for DML records
            // This would involve reading RID and tuple data
            return nullptr;
            
        default:
            return nullptr;
    }
}

void InsertLogRecord::SerializeTo(char* buffer) const {
    // Write page_id
    *reinterpret_cast<page_id_t*>(buffer) = rid_.page_id;
    buffer += sizeof(page_id_t);
    
    // Write slot_num
    *reinterpret_cast<slot_offset_t*>(buffer) = rid_.slot_num;
    buffer += sizeof(slot_offset_t);
    
    // Write tuple data
    tuple_.SerializeTo(buffer);
}

void UpdateLogRecord::SerializeTo(char* buffer) const {
    // Write page_id
    *reinterpret_cast<page_id_t*>(buffer) = rid_.page_id;
    buffer += sizeof(page_id_t);
    
    // Write slot_num
    *reinterpret_cast<slot_offset_t*>(buffer) = rid_.slot_num;
    buffer += sizeof(slot_offset_t);
    
    // Write old tuple
    old_tuple_.SerializeTo(buffer);
    buffer += old_tuple_.GetSerializedSize();
    
    // Write new tuple
    new_tuple_.SerializeTo(buffer);
}

void BeginLogRecord::SerializeTo(char* buffer) const {
    (void) buffer;  // Unused parameter
    // BEGIN record has no additional data
}

void CommitLogRecord::SerializeTo(char* buffer) const {
    (void) buffer;  // Unused parameter
    // COMMIT record has no additional data
}

void AbortLogRecord::SerializeTo(char* buffer) const {
    (void) buffer;  // Unused parameter
    // ABORT record has no additional data
}

}  // namespace SimpleRDBMS