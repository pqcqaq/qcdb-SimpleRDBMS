/*
 * 文件: log_record.h
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述:
 * WAL日志记录的头文件定义，包含各种类型的日志记录类，用于事务恢复和undo/redo操作
 */

#pragma once

#include <cstring>
#include <vector>

#include "common/config.h"
#include "common/types.h"
#include "record/tuple.h"

namespace SimpleRDBMS {

/**
 * 日志记录的类型枚举
 * 用于区分不同种类的操作，recovery时需要根据类型做不同处理
 */
enum class LogRecordType {
    INVALID = 0,  // 无效类型，用作默认值
    INSERT,       // 插入操作的日志
    UPDATE,       // 更新操作的日志
    DELETE,       // 删除操作的日志
    BEGIN,        // 事务开始的日志
    COMMIT,       // 事务提交的日志
    ABORT,        // 事务中止的日志
    CHECKPOINT    // 检查点日志，用于优化recovery过程
};

/**
 * 日志记录的基类
 * 所有具体的log record都继承自这个类，提供统一的接口
 * 采用了策略模式，不同类型的log有不同的序列化方式
 */
class LogRecord {
   public:
    /**
     * 构造函数
     * @param type 日志记录类型
     * @param txn_id 事务ID，用于标识哪个事务产生的日志
     * @param prev_lsn 前一个LSN，构建事务的undo chain
     */
    LogRecord(LogRecordType type, txn_id_t txn_id, lsn_t prev_lsn)
        : type_(type), txn_id_(txn_id), prev_lsn_(prev_lsn), size_(0) {}

    virtual ~LogRecord() = default;

    /**
     * 纯虚函数：将log record序列化到buffer中
     * 每种具体的log record都有自己的序列化格式
     * @param buffer 目标缓冲区，调用方保证空间足够
     */
    virtual void SerializeTo(char* buffer) const = 0;

    /**
     * 纯虚函数：获取当前log record序列化后的大小
     * LogManager需要知道每个record占多少空间
     * @return 序列化后的字节数
     */
    virtual size_t GetLogRecordSize() const = 0;

    /**
     * 静态工厂方法：从buffer中反序列化出LogRecord对象
     * 根据buffer中的type字段创建对应的具体log record
     * @param buffer 包含序列化数据的缓冲区
     * @return 反序列化得到的LogRecord智能指针
     */
    static std::unique_ptr<LogRecord> DeserializeFrom(const char* buffer);

    // getter方法，用于访问log record的基本信息
    LogRecordType GetType() const { return type_; }
    txn_id_t GetTxnId() const { return txn_id_; }
    lsn_t GetPrevLSN() const { return prev_lsn_; }
    size_t GetSize() const { return size_; }

   protected:
    LogRecordType type_;  // 日志类型
    txn_id_t txn_id_;     // 事务ID
    lsn_t prev_lsn_;      // 前一个LSN，用于链接同一事务的日志
    size_t size_;         // 日志记录的大小
};

/**
 * INSERT操作的日志记录
 * 记录插入的位置(RID)和数据内容(Tuple)，用于undo时删除该记录
 */
class InsertLogRecord : public LogRecord {
   public:
    /**
     * 构造函数
     * @param txn_id 事务ID
     * @param prev_lsn 前一个LSN
     * @param rid 插入记录的位置标识
     * @param tuple 插入的数据内容
     */
    InsertLogRecord(txn_id_t txn_id, lsn_t prev_lsn, const RID& rid,
                    const Tuple& tuple)
        : LogRecord(LogRecordType::INSERT, txn_id, prev_lsn),
          rid_(rid),
          tuple_(tuple) {}

    void SerializeTo(char* buffer) const override;

    /**
     * 计算INSERT log的序列化大小
     * 包括page_id + slot_offset + tuple的序列化大小
     */
    size_t GetLogRecordSize() const override {
        return sizeof(page_id_t) + sizeof(slot_offset_t) +
               tuple_.GetSerializedSize();
    }

    // getter方法
    const RID& GetRID() const { return rid_; }
    const Tuple& GetTuple() const { return tuple_; }

   private:
    RID rid_;      // 插入位置
    Tuple tuple_;  // 插入的数据
};

/**
 * UPDATE操作的日志记录
 * 需要记录old tuple和new tuple，支持undo和redo操作
 */
class UpdateLogRecord : public LogRecord {
   public:
    /**
     * 构造函数
     * @param txn_id 事务ID
     * @param prev_lsn 前一个LSN
     * @param rid 更新记录的位置
     * @param old_tuple 更新前的数据，undo时使用
     * @param new_tuple 更新后的数据，redo时使用
     */
    UpdateLogRecord(txn_id_t txn_id, lsn_t prev_lsn, const RID& rid,
                    const Tuple& old_tuple, const Tuple& new_tuple)
        : LogRecord(LogRecordType::UPDATE, txn_id, prev_lsn),
          rid_(rid),
          old_tuple_(old_tuple),
          new_tuple_(new_tuple) {}

    void SerializeTo(char* buffer) const override;

    /**
     * 计算UPDATE log的序列化大小
     * 包括RID + old tuple + new tuple的总大小
     */
    size_t GetLogRecordSize() const override {
        return sizeof(page_id_t) + sizeof(slot_offset_t) +
               old_tuple_.GetSerializedSize() + new_tuple_.GetSerializedSize();
    }

    // getter方法
    const RID& GetRID() const { return rid_; }
    const Tuple& GetOldTuple() const { return old_tuple_; }
    const Tuple& GetNewTuple() const { return new_tuple_; }

   private:
    RID rid_;          // 更新位置
    Tuple old_tuple_;  // 旧数据
    Tuple new_tuple_;  // 新数据
};

class DeleteLogRecord : public LogRecord {
   public:
    DeleteLogRecord(txn_id_t txn_id, lsn_t prev_lsn, const RID& rid,
                    const Tuple& deleted_tuple)
        : LogRecord(LogRecordType::DELETE, txn_id, prev_lsn),
          rid_(rid),
          deleted_tuple_(deleted_tuple) {}
    
    ~DeleteLogRecord() override = default;
    
    void SerializeTo(char* buffer) const override;
    
    size_t GetLogRecordSize() const override {
        return sizeof(page_id_t) + sizeof(slot_offset_t) +
               deleted_tuple_.GetSerializedSize();
    }
    
    const RID& GetRID() const { return rid_; }
    const Tuple& GetDeletedTuple() const { return deleted_tuple_; }
    
   private:
    RID rid_;
    Tuple deleted_tuple_;
};

/**
 * BEGIN操作的日志记录
 * 标记事务开始，比较简单，只有基本的事务信息
 */
class BeginLogRecord : public LogRecord {
   public:
    /**
     * 构造函数
     * BEGIN log没有prev_lsn，因为它是事务的第一个log
     * @param txn_id 事务ID
     */
    BeginLogRecord(txn_id_t txn_id)
        : LogRecord(LogRecordType::BEGIN, txn_id, INVALID_LSN) {}

    ~BeginLogRecord() override = default;

    void SerializeTo(char* buffer) const override;

    /**
     * BEGIN log没有额外数据，所以size是0
     */
    size_t GetLogRecordSize() const override { return 0; }
};

/**
 * COMMIT操作的日志记录
 * 标记事务成功提交，需要prev_lsn来链接之前的操作
 */
class CommitLogRecord : public LogRecord {
   public:
    /**
     * 构造函数
     * @param txn_id 事务ID
     * @param prev_lsn 前一个LSN，链接事务的操作历史
     */
    CommitLogRecord(txn_id_t txn_id, lsn_t prev_lsn)
        : LogRecord(LogRecordType::COMMIT, txn_id, prev_lsn) {}

    ~CommitLogRecord() override = default;

    void SerializeTo(char* buffer) const override;

    /**
     * COMMIT log也没有额外数据
     */
    size_t GetLogRecordSize() const override { return 0; }
};

/**
 * ABORT操作的日志记录
 * 标记事务被中止，recovery时需要undo该事务的所有操作
 */
class AbortLogRecord : public LogRecord {
   public:
    /**
     * 构造函数
     * @param txn_id 事务ID
     * @param prev_lsn 前一个LSN，用于找到需要undo的操作
     */
    AbortLogRecord(txn_id_t txn_id, lsn_t prev_lsn)
        : LogRecord(LogRecordType::ABORT, txn_id, prev_lsn) {}

    ~AbortLogRecord() override = default;

    void SerializeTo(char* buffer) const override;

    /**
     * ABORT log同样没有额外数据
     */
    size_t GetLogRecordSize() const override { return 0; }
};

}  // namespace SimpleRDBMS