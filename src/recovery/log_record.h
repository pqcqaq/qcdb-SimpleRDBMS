#pragma once

#include <cstring>
#include <vector>
#include "common/config.h"
#include "common/types.h"
#include "record/tuple.h"

namespace SimpleRDBMS {

enum class LogRecordType {
    INVALID = 0,
    INSERT,
    UPDATE,
    DELETE,
    BEGIN,
    COMMIT,
    ABORT,
    CHECKPOINT
};

// Base log record
class LogRecord {
public:
    LogRecord(LogRecordType type, txn_id_t txn_id, lsn_t prev_lsn)
        : type_(type), txn_id_(txn_id), prev_lsn_(prev_lsn), size_(0) {}
    virtual ~LogRecord() = default;
    virtual void SerializeTo(char* buffer) const = 0;
    virtual size_t GetLogRecordSize() const = 0;  // 新增
    static std::unique_ptr<LogRecord> DeserializeFrom(const char* buffer);
    LogRecordType GetType() const { return type_; }
    txn_id_t GetTxnId() const { return txn_id_; }
    lsn_t GetPrevLSN() const { return prev_lsn_; }
    size_t GetSize() const { return size_; }
protected:
    LogRecordType type_;
    txn_id_t txn_id_;
    lsn_t prev_lsn_;
    size_t size_;
};

class InsertLogRecord : public LogRecord {
public:
    InsertLogRecord(txn_id_t txn_id, lsn_t prev_lsn, const RID& rid, const Tuple& tuple)
        : LogRecord(LogRecordType::INSERT, txn_id, prev_lsn),
          rid_(rid), tuple_(tuple) {}
    void SerializeTo(char* buffer) const override;
    size_t GetLogRecordSize() const override {
        return sizeof(page_id_t) + sizeof(slot_offset_t) + tuple_.GetSerializedSize();
    }
    const RID& GetRID() const { return rid_; }
    const Tuple& GetTuple() const { return tuple_; }
private:
    RID rid_;
    Tuple tuple_;
};

class UpdateLogRecord : public LogRecord {
public:
    UpdateLogRecord(txn_id_t txn_id, lsn_t prev_lsn, const RID& rid,
                    const Tuple& old_tuple, const Tuple& new_tuple)
        : LogRecord(LogRecordType::UPDATE, txn_id, prev_lsn),
          rid_(rid), old_tuple_(old_tuple), new_tuple_(new_tuple) {}
    void SerializeTo(char* buffer) const override;
    size_t GetLogRecordSize() const override {
        return sizeof(page_id_t) + sizeof(slot_offset_t) + 
               old_tuple_.GetSerializedSize() + new_tuple_.GetSerializedSize();
    }
    const RID& GetRID() const { return rid_; }
    const Tuple& GetOldTuple() const { return old_tuple_; }
    const Tuple& GetNewTuple() const { return new_tuple_; }
private:
    RID rid_;
    Tuple old_tuple_;
    Tuple new_tuple_;
};

class BeginLogRecord : public LogRecord {
public:
    BeginLogRecord(txn_id_t txn_id)
        : LogRecord(LogRecordType::BEGIN, txn_id, INVALID_LSN) {}
    ~BeginLogRecord() override = default;
    void SerializeTo(char* buffer) const override;
    size_t GetLogRecordSize() const override { return 0; }
};

class CommitLogRecord : public LogRecord {
public:
    CommitLogRecord(txn_id_t txn_id, lsn_t prev_lsn)
        : LogRecord(LogRecordType::COMMIT, txn_id, prev_lsn) {}
    ~CommitLogRecord() override = default;
    void SerializeTo(char* buffer) const override;
    size_t GetLogRecordSize() const override { return 0; }
};

class AbortLogRecord : public LogRecord {
public:
    AbortLogRecord(txn_id_t txn_id, lsn_t prev_lsn)
        : LogRecord(LogRecordType::ABORT, txn_id, prev_lsn) {}
    ~AbortLogRecord() override = default;
    void SerializeTo(char* buffer) const override;
    size_t GetLogRecordSize() const override { return 0; }
};

}  // namespace SimpleRDBMS