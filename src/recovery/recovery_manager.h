/*
 * 文件: recovery_manager.h
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述: WAL恢复管理器头文件，定义了基于ARIES算法的崩溃恢复机制
 *       包含Analysis、Redo、Undo三个阶段的实现声明
 */

#pragma once

#include <unordered_map>
#include <unordered_set>

#include "buffer/buffer_pool_manager.h"
#include "catalog/catalog.h"
#include "recovery/log_manager.h"
#include "transaction/lock_manager.h"

namespace SimpleRDBMS {

/**
 * @brief WAL恢复管理器类
 *
 * 负责系统崩溃后的数据库恢复工作，实现了ARIES (Algorithm for Recovery and
 * Isolation Exploiting Semantics) 算法
 *
 * 主要功能：
 * - 系统重启后分析WAL日志，确定需要恢复的数据
 * - 重做(Redo)已提交但未写入磁盘的事务
 * - 撤销(Undo)未提交事务的所有修改
 * - 创建检查点以减少恢复时间
 *
 * ARIES算法三个阶段：
 * 1. Analysis: 分析日志确定事务状态和脏页信息
 * 2. Redo: 重做所有需要的操作，确保已提交事务的持久性
 * 3. Undo: 撤销未提交事务的操作，确保原子性
 */
class RecoveryManager {
   public:
    /**
     * @brief 构造函数，初始化恢复管理器
     * @param buffer_pool_manager 缓冲池管理器，用于页面操作
     * @param catalog 元数据管理器，用于获取表信息
     * @param log_manager 日志管理器，用于读取WAL日志
     * @param lock_manager 锁管理器，用于并发控制
     */
    RecoveryManager(BufferPoolManager* buffer_pool_manager, Catalog* catalog,
                    LogManager* log_manager, LockManager* lock_manager);

    /**
     * @brief 执行崩溃恢复过程
     *
     * 系统重启后调用此函数来恢复数据库到一致状态
     * 按照ARIES算法顺序执行Analysis、Redo、Undo三个阶段
     */
    void Recover();

    /**
     * @brief 创建检查点
     *
     * 将内存中的所有脏页flush到磁盘，并记录检查点信息
     * 检查点可以减少恢复时需要处理的日志量，提高恢复效率
     */
    void Checkpoint();

   private:
    // ====== 核心组件指针 ======
    BufferPoolManager* buffer_pool_manager_;  ///< 缓冲池管理器
    Catalog* catalog_;                        ///< 元数据管理器
    LogManager* log_manager_;                 ///< 日志管理器
    LockManager* lock_manager_;               ///< 锁管理器

    // ====== ARIES算法的数据结构 ======

    /**
     * @brief 活跃事务表(Active Transaction Table, ATT)
     *
     * 记录在崩溃时刻仍然活跃(未提交)的事务
     * Key: 事务ID (txn_id_t)
     * Value: 该事务最后一条日志记录的LSN (lsn_t)
     *
     * 用途：
     * - Analysis阶段构建，记录需要回滚的事务
     * - Undo阶段使用，确定哪些事务需要撤销
     */
    std::unordered_map<txn_id_t, lsn_t> active_txn_table_;

    /**
     * @brief 脏页表(Dirty Page Table, DPT)
     *
     * 记录在崩溃时刻缓冲池中的脏页信息
     * Key: 页面ID (page_id_t)
     * Value: 该页面第一次变脏时的LSN (lsn_t)
     *
     * 用途：
     * - Analysis阶段构建，记录可能需要redo的页面
     * - Redo阶段使用，确定从哪个LSN开始redo
     *
     * 注意：当前实现中暂未完全使用，但保留了接口
     */
    std::unordered_map<page_id_t, lsn_t> dirty_page_table_;

    // ====== ARIES算法三个阶段的实现 ======

    /**
     * @brief Analysis阶段：分析日志记录确定恢复所需信息
     * @param log_records 从日志文件读取的所有日志记录
     *
     * 工作内容：
     * 1. 扫描所有日志记录
     * 2. 构建活跃事务表(ATT)，识别未提交的事务
     * 3. 构建脏页表(DPT)，识别可能需要redo的页面
     * 4. 为后续的Redo和Undo阶段准备数据结构
     */
    void AnalysisPhase(
        const std::vector<std::unique_ptr<LogRecord>>& log_records);

    /**
     * @brief Redo阶段：重做所有必要的操作
     * @param log_records 从日志文件读取的所有日志记录
     *
     * 工作内容：
     * 1. 对于每个数据修改类型的日志记录(INSERT/UPDATE/DELETE)
     * 2. 检查是否需要redo(通过比较页面LSN和日志LSN)
     * 3. 重新执行操作，确保已提交事务的修改都被应用
     * 4. 更新页面LSN
     */
    void RedoPhase(const std::vector<std::unique_ptr<LogRecord>>& log_records);

    /**
     * @brief Undo阶段：撤销未提交事务的所有操作
     * @param log_records 从日志文件读取的所有日志记录
     *
     * 工作内容：
     * 1. 对于ATT中的每个活跃事务
     * 2. 按照日志的逆序撤销该事务的所有操作
     * 3. 为每个撤销操作生成CLR(Compensation Log Record)
     * 4. 最后为事务生成ABORT日志记录
     */
    void UndoPhase(const std::vector<std::unique_ptr<LogRecord>>& log_records);

    // ====== 具体操作的Redo实现 ======

    /**
     * @brief 重做插入操作
     * @param log_record 插入操作的日志记录
     *
     * 实现逻辑：
     * 1. 检查页面LSN < 日志LSN，确认需要redo
     * 2. 重新执行插入操作
     * 3. 更新页面LSN为当前日志LSN
     */
    void RedoInsert(const InsertLogRecord* log_record);

    /**
     * @brief 重做更新操作
     * @param log_record 更新操作的日志记录
     *
     * 实现逻辑：
     * 1. 检查页面LSN < 日志LSN，确认需要redo
     * 2. 将tuple更新为new_value
     * 3. 更新页面LSN为当前日志LSN
     */
    void RedoUpdate(const UpdateLogRecord* log_record);

    // void RedoDelete(const DeleteLogRecord* log_record);  // 暂未实现

    // ====== 具体操作的Undo实现 ======

    /**
     * @brief 撤销插入操作
     * @param log_record 插入操作的日志记录
     *
     * 实现逻辑：
     * 1. 找到之前插入的tuple位置(通过RID)
     * 2. 删除该tuple
     * 3. 生成对应的CLR日志记录
     */
    void UndoInsert(const InsertLogRecord* log_record);

    /**
     * @brief 撤销更新操作
     * @param log_record 更新操作的日志记录
     *
     * 实现逻辑：
     * 1. 找到被更新的tuple位置(通过RID)
     * 2. 将tuple恢复为old_value
     * 3. 生成对应的CLR日志记录
     */
    void UndoUpdate(const UpdateLogRecord* log_record);

    /**
     * @brief 撤销删除操作
     * @param log_record 删除操作的日志记录
     *
     * 实现逻辑：
     * 1. 重新插入被删除的tuple
     * 2. 新的RID可能与原来不同，这是正常的
     * 3. 生成对应的CLR日志记录
     */
    void UndoDelete(const UpdateLogRecord* log_record);

    /**
     * @brief 重做删除操作
     * @param log_record 删除操作的日志记录
     *
     * 实现逻辑：
     * 1. 找到被删除的tuple位置(通过RID)
     * 2. 删除该tuple
     * 3. 更新页面LSN为当前日志LSN
     */
    void RedoDelete(const DeleteLogRecord* log_record);

    /**
     * @brief 撤销删除操作：重新插入被删除的tuple
     * @param log_record 删除操作的日志记录
     *
     * 实现逻辑：
     * 1. 从日志记录中获取被删除的tuple
     * 2. 将该tuple重新插入到表中
     * 3. 注意：新插入的RID可能和原来不同，这是正常的
     */
    void UndoDelete(const DeleteLogRecord* log_record);
};

}  // namespace SimpleRDBMS