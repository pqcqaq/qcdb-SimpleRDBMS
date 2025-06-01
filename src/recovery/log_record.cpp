/*
 * 文件: log_record.cpp
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述: WAL日志记录的实现，负责各种类型日志记录的序列化和反序列化操作
 */

#include "recovery/log_record.h"

#include <memory>

namespace SimpleRDBMS {

/**
 * 从buffer中反序列化出LogRecord对象
 * 这是个工厂方法，根据log type创建对应的具体log record
 *
 * @param buffer 包含序列化数据的缓冲区
 * @return 反序列化得到的LogRecord智能指针，失败时返回nullptr
 */
std::unique_ptr<LogRecord> LogRecord::DeserializeFrom(const char* buffer) {
    // 跳过size字段，这个在LogManager那边已经用过了
    buffer += sizeof(size_t);

    // 读取log record的类型，用来决定创建哪种具体的record
    LogRecordType type = *reinterpret_cast<const LogRecordType*>(buffer);
    buffer += sizeof(LogRecordType);

    // 读取事务ID，所有log record都有这个字段
    txn_id_t txn_id = *reinterpret_cast<const txn_id_t*>(buffer);
    buffer += sizeof(txn_id_t);

    // 读取前一个LSN，用于构建undo chain
    lsn_t prev_lsn = *reinterpret_cast<const lsn_t*>(buffer);
    buffer += sizeof(lsn_t);

    // 根据log type创建对应的log record对象
    switch (type) {
        case LogRecordType::BEGIN:
            // BEGIN log比较简单，只需要txn_id
            return std::make_unique<BeginLogRecord>(txn_id);

        case LogRecordType::COMMIT:
            // COMMIT log需要prev_lsn来链接之前的操作
            return std::make_unique<CommitLogRecord>(txn_id, prev_lsn);

        case LogRecordType::ABORT:
            // ABORT log类似COMMIT，也需要链接信息
            return std::make_unique<AbortLogRecord>(txn_id, prev_lsn);

        case LogRecordType::INSERT:
        case LogRecordType::UPDATE:
        case LogRecordType::DELETE:
            // DML操作的log record比较复杂，涉及到具体的数据
            // 目前还没实现，等record layer完善后再补充
            return nullptr;

        default:
            // 未知的log type，直接返回空
            return nullptr;
    }
}

/**
 * INSERT log record的序列化实现
 * 需要保存RID和插入的tuple数据，用于undo时删除这条记录
 *
 * @param buffer 目标缓冲区，调用方保证空间足够
 */
void InsertLogRecord::SerializeTo(char* buffer) const {
    // 先写page_id，定位到哪个page
    *reinterpret_cast<page_id_t*>(buffer) = rid_.page_id;
    buffer += sizeof(page_id_t);

    // 再写slot_num，定位到page内的具体位置
    *reinterpret_cast<slot_offset_t*>(buffer) = rid_.slot_num;
    buffer += sizeof(slot_offset_t);

    // 最后写入完整的tuple数据，undo时需要知道插入了什么
    tuple_.SerializeTo(buffer);
}

/**
 * UPDATE log record的序列化实现
 * 需要保存RID、old tuple和new tuple，支持undo和redo操作
 *
 * @param buffer 目标缓冲区，调用方保证空间足够
 */
void UpdateLogRecord::SerializeTo(char* buffer) const {
    // 写入RID信息，定位更新的记录位置
    *reinterpret_cast<page_id_t*>(buffer) = rid_.page_id;
    buffer += sizeof(page_id_t);

    *reinterpret_cast<slot_offset_t*>(buffer) = rid_.slot_num;
    buffer += sizeof(slot_offset_t);

    // 先写old tuple，undo时恢复用
    old_tuple_.SerializeTo(buffer);
    buffer += old_tuple_.GetSerializedSize();

    // 再写new tuple，redo时应用用
    new_tuple_.SerializeTo(buffer);
}

void DeleteLogRecord::SerializeTo(char* buffer) const {
    *reinterpret_cast<page_id_t*>(buffer) = rid_.page_id;
    buffer += sizeof(page_id_t);
    *reinterpret_cast<slot_offset_t*>(buffer) = rid_.slot_num;
    buffer += sizeof(slot_offset_t);
    deleted_tuple_.SerializeTo(buffer);
}

/**
 * BEGIN log record的序列化实现
 * BEGIN record很简单，base class已经处理了基本字段，这里没有额外数据
 *
 * @param buffer 目标缓冲区（本方法中未使用）
 */
void BeginLogRecord::SerializeTo(char* buffer) const {
    (void)buffer;  // 避免编译器unused parameter警告
    // BEGIN record没有额外的数据需要序列化
}

/**
 * COMMIT log record的序列化实现
 * COMMIT record也很简单，标记事务成功提交即可
 *
 * @param buffer 目标缓冲区（本方法中未使用）
 */
void CommitLogRecord::SerializeTo(char* buffer) const {
    (void)buffer;  // 避免编译器unused parameter警告
    // COMMIT record没有额外的数据需要序列化
}

/**
 * ABORT log record的序列化实现
 * ABORT record标记事务被中止，也没有额外数据
 *
 * @param buffer 目标缓冲区（本方法中未使用）
 */
void AbortLogRecord::SerializeTo(char* buffer) const {
    (void)buffer;  // 避免编译器unused parameter警告
    // ABORT record没有额外的数据需要序列化
}

}  // namespace SimpleRDBMS