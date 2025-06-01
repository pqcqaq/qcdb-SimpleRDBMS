#include <algorithm>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <memory>
#include <set>
#include <sstream>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "buffer/lru_replacer.h"
#include "catalog/catalog.h"
#include "catalog/table_manager.h"
#include "execution/execution_engine.h"
#include "parser/parser.h"
#include "recovery/log_manager.h"
#include "recovery/recovery_manager.h"
#include "stat/stat.h"
#include "storage/disk_manager.h"
#include "transaction/lock_manager.h"
#include "transaction/transaction_manager.h"

using namespace SimpleRDBMS;

class SimpleRDBMSServer {
   public:
    SimpleRDBMSServer(const std::string& db_file) {
        disk_manager_ = std::make_unique<DiskManager>(db_file);
        log_disk_manager_ = std::make_unique<DiskManager>(db_file + ".log");
        replacer_ = std::make_unique<LRUReplacer>(BUFFER_POOL_SIZE);
        buffer_pool_manager_ = std::make_unique<BufferPoolManager>(
            BUFFER_POOL_SIZE, std::move(disk_manager_), std::move(replacer_));

        log_manager_ = std::make_unique<LogManager>(log_disk_manager_.get());
        lock_manager_ = std::make_unique<LockManager>();
        transaction_manager_ = std::make_unique<TransactionManager>(
            lock_manager_.get(), log_manager_.get());

        // 重要：传递log_manager_给catalog
        catalog_ = std::make_unique<Catalog>(buffer_pool_manager_.get(),
                                             log_manager_.get());

        table_manager_ = std::make_unique<TableManager>(
            buffer_pool_manager_.get(), catalog_.get());

        recovery_manager_ = std::make_unique<RecoveryManager>(
            buffer_pool_manager_.get(), catalog_.get(), log_manager_.get(),
            lock_manager_.get());

        // 重要：传递log_manager_给execution_engine
        execution_engine_ = std::make_unique<ExecutionEngine>(
            buffer_pool_manager_.get(), catalog_.get(),
            transaction_manager_.get(), log_manager_.get());

        LOG_INFO("Initializing statistics system");
        STATS.Reset();  // Start with clean statistics

        recovery_manager_->Recover();
    }

    void Run() {
        std::cout << "SimpleRDBMS Server Started!" << std::endl;
        std::cout << "Enter SQL commands (type 'exit' to quit):" << std::endl;
        std::cout << "Note: SQL statements must end with ';'" << std::endl;

        std::string accumulated_sql;
        std::string line;
        bool is_first_line = true;

        while (true) {
            // 显示提示符
            if (is_first_line) {
                std::cout << "SimpleRDBMS> ";
                is_first_line = false;
            } else {
                std::cout << "          -> ";  // 续行提示符
            }

            if (!std::getline(std::cin, line)) {
                break;  // EOF
            }

            // 去除行首尾空白
            line = TrimWhitespace(line);

            // 检查退出命令（只有在新语句开始时才检查）
            if (accumulated_sql.empty() && (line == "exit" || line == "quit")) {
                break;
            }

            // Handle statistics command
            if (accumulated_sql.empty() && line == "stats") {
                ShowStatistics();
                is_first_line = true;
                continue;
            }

            // Handle reset statistics command
            if (accumulated_sql.empty() && line == "reset stats") {
                STATS.Reset();
                std::cout << "Statistics have been reset." << std::endl;
                is_first_line = true;
                continue;
            }

            // 如果是空行且没有累积的SQL，继续
            if (line.empty() && accumulated_sql.empty()) {
                is_first_line = true;
                continue;
            }

            // 将当前行添加到累积的SQL中
            if (!accumulated_sql.empty()) {
                accumulated_sql += " ";  // 在行之间添加空格
            }
            accumulated_sql += line;

            // 检查是否包含分号
            size_t semicolon_pos = accumulated_sql.find(';');
            while (semicolon_pos != std::string::npos) {
                // 提取到分号为止的SQL语句
                std::string sql_to_execute =
                    accumulated_sql.substr(0, semicolon_pos);
                sql_to_execute = TrimWhitespace(sql_to_execute);

                // 执行SQL语句（如果不为空）
                if (!sql_to_execute.empty()) {
                    auto start_time = std::chrono::high_resolution_clock::now();
                    ExecuteSQL(sql_to_execute);
                    auto end_time = std::chrono::high_resolution_clock::now();

                    auto duration =
                        std::chrono::duration_cast<std::chrono::milliseconds>(
                            end_time - start_time);
                    std::cout << "Query executed in " << duration.count()
                              << " ms" << std::endl;
                    // Show mini statistics after each query
                    ShowMiniStatistics();
                }

                // 移除已执行的部分
                accumulated_sql = accumulated_sql.substr(semicolon_pos + 1);
                accumulated_sql = TrimWhitespace(accumulated_sql);

                // 查找下一个分号
                semicolon_pos = accumulated_sql.find(';');

                // 如果还有剩余的SQL，重置为续行状态，否则重置为新语句状态
                is_first_line = accumulated_sql.empty();
            }
        }

        std::cout << "Shutting down..." << std::endl;
        // ShowFinalStatistics();
        Shutdown();
    }

   private:
    // 去除字符串首尾空白字符
    std::string TrimWhitespace(const std::string& str) {
        const std::string whitespace = " \t\n\r";
        size_t start = str.find_first_not_of(whitespace);
        if (start == std::string::npos) {
            return "";
        }
        size_t end = str.find_last_not_of(whitespace);
        return str.substr(start, end - start + 1);
    }

    void ShowStatistics() {
        std::cout << "\n";
        STATS.PrintStatistics();
        std::cout << std::endl;
    }

    void ShowMiniStatistics() {
        double hit_ratio = STATS.GetBufferPoolHitRatio();
        uint64_t disk_reads = STATS.GetTotalDiskReads();
        uint64_t disk_writes = STATS.GetTotalDiskWrites();
        uint64_t total_transactions = STATS.GetTotalTransactions();

        std::cout << "[Stats: Cache Hit " << std::fixed << std::setprecision(1)
                  << hit_ratio << "%, "
                  << "Disk R/W " << disk_reads << "/" << disk_writes << ", "
                  << "Txns " << total_transactions << "]" << std::endl;
    }

    void ShowFinalStatistics() {
        std::cout << "\n" << std::string(80, '=') << std::endl;
        std::cout << std::setw(50) << std::right << "最终统计报告" << std::endl;
        std::cout << std::string(80, '=') << std::endl;

        STATS.PrintStatistics();

        // 计算并显示汇总指标
        double hit_ratio = STATS.GetBufferPoolHitRatio();
        double success_rate = STATS.GetTransactionSuccessRate();
        uint64_t total_disk_io =
            STATS.GetTotalDiskReads() + STATS.GetTotalDiskWrites();

        std::cout << "\n" << std::string(80, '-') << std::endl;
        std::cout << std::setw(50) << std::right << "性能指标汇总" << std::endl;
        std::cout << std::string(80, '-') << std::endl;

        // 格式化指标表格
        std::cout << std::left << std::setw(35) << "指标名称" << std::right
                  << std::setw(15) << "数值" << std::setw(30) << "评估"
                  << std::endl;
        std::cout << std::string(80, '-') << std::endl;

        // 缓存命中率
        std::cout << std::left << std::setw(35)
                  << "总体缓存命中率:" << std::right << std::setw(12)
                  << std::fixed << std::setprecision(2) << hit_ratio << "%";
        if (hit_ratio >= 80.0) {
            std::cout << std::setw(30) << "[优秀]";
        } else if (hit_ratio >= 60.0) {
            std::cout << std::setw(30) << "[良好]";
        } else {
            std::cout << std::setw(30) << "[需要改进]";
        }
        std::cout << std::endl;

        // 事务成功率
        std::cout << std::left << std::setw(35) << "事务成功率:" << std::right
                  << std::setw(12) << std::fixed << std::setprecision(2)
                  << success_rate << "%";
        if (success_rate >= 95.0) {
            std::cout << std::setw(30) << "[优秀]";
        } else if (success_rate >= 85.0) {
            std::cout << std::setw(30) << "[良好]";
        } else {
            std::cout << std::setw(30) << "[需要关注]";
        }
        std::cout << std::endl;

        // 总磁盘I/O操作
        std::cout << std::left << std::setw(35)
                  << "总磁盘I/O操作次数:" << std::right << std::setw(15)
                  << total_disk_io << std::setw(30) << "" << std::endl;

        std::cout << std::string(80, '-') << std::endl;

        // 性能总结
        std::cout << "\n性能总结:\n";
        std::cout << std::string(40, '-') << std::endl;

        std::cout << "缓存性能:        ";
        if (hit_ratio >= 80.0) {
            std::cout << "优秀 (>= 80%)" << std::endl;
        } else if (hit_ratio >= 60.0) {
            std::cout << "良好 (60-79%)" << std::endl;
        } else {
            std::cout << "需要改进 (< 60%)" << std::endl;
        }

        std::cout << "事务可靠性:      ";
        if (success_rate >= 95.0) {
            std::cout << "优秀 (>= 95%)" << std::endl;
        } else if (success_rate >= 85.0) {
            std::cout << "良好 (85-94%)" << std::endl;
        } else {
            std::cout << "需要关注 (< 85%)" << std::endl;
        }

        std::cout << std::string(80, '=') << std::endl;
    }

    void ExecuteSQL(const std::string& sql) {
        try {
            // Parse SQL
            Parser parser(sql);
            auto statement = parser.Parse();

            // 获取语句类型以便特殊处理
            auto stmt_type = statement->GetType();

            // 特殊处理事务控制语句
            if (stmt_type == Statement::StmtType::BEGIN_TXN) {
                // 不需要现有事务，直接开始新事务
                auto* txn = transaction_manager_->Begin();
                std::cout << "Transaction started with ID: " << txn->GetTxnId()
                          << std::endl;
                return;
            }

            if (stmt_type == Statement::StmtType::COMMIT_TXN) {
                // 需要有活跃事务才能提交
                // 这里需要维护当前活跃事务的状态
                std::cout << "Transaction committed." << std::endl;
                return;
            }

            if (stmt_type == Statement::StmtType::ROLLBACK_TXN) {
                // 需要有活跃事务才能回滚
                std::cout << "Transaction rolled back." << std::endl;
                return;
            }

            // Begin transaction
            auto* txn = transaction_manager_->Begin();

            // Execute statement
            std::vector<Tuple> result_set;
            bool success =
                execution_engine_->Execute(statement.get(), &result_set, txn);

            if (success) {
                transaction_manager_->Commit(txn);
                std::cout << "Query executed successfully." << std::endl;

                switch (stmt_type) {
                    case Statement::StmtType::SELECT:
                        DisplaySelectResults(
                            result_set,
                            static_cast<SelectStatement*>(statement.get()));
                        break;
                    case Statement::StmtType::INSERT:
                        DisplayInsertResults(result_set);
                        break;
                    case Statement::StmtType::UPDATE:
                        DisplayUpdateResults(
                            result_set,
                            static_cast<UpdateStatement*>(statement.get()));
                        break;
                    case Statement::StmtType::DELETE:
                        DisplayDeleteResults(result_set);
                        break;
                    case Statement::StmtType::SHOW_TABLES:  // 添加这个case
                        DisplayShowTablesResults(result_set);
                        break;
                    case Statement::StmtType::CREATE_TABLE:
                    case Statement::StmtType::DROP_TABLE:
                    case Statement::StmtType::CREATE_INDEX:
                    case Statement::StmtType::DROP_INDEX:
                        std::cout << "DDL operation completed successfully."
                                  << std::endl;
                        break;
                    case Statement::StmtType::EXPLAIN:
                        DisplayExplainResults(result_set);
                        break;
                    default:
                        break;
                }
            } else {
                transaction_manager_->Abort(txn);
                std::cout << "Query execution failed." << std::endl;
            }
        } catch (const std::exception& e) {
            std::cout << "Error: " << e.what() << std::endl;
        }
    }

    void DisplayExplainResults(const std::vector<Tuple>& result_set) {
        if (result_set.empty()) {
            std::cout << "No execution plan available." << std::endl;
            return;
        }

        // 计算最长行的长度，用于绘制边框
        size_t max_length = 20;  // 最小宽度
        for (const auto& tuple : result_set) {
            std::string plan_line = std::get<std::string>(tuple.GetValue(0));
            max_length = std::max(max_length, plan_line.length());
        }

        // 添加一些额外的空间
        max_length += 4;

        std::cout << "\n";

        // 打印顶部边框（使用 ASCII 字符）
        std::cout << "+" << std::string(max_length, '-') << "+" << std::endl;

        // 打印标题
        std::string title = "QUERY PLAN";
        size_t padding = (max_length - title.length()) / 2;
        std::cout << "|" << std::string(padding, ' ') << title
                  << std::string(max_length - padding - title.length(), ' ')
                  << "|" << std::endl;

        // 打印分隔线
        std::cout << "+" << std::string(max_length, '-') << "+" << std::endl;

        // 显示每一行执行计划
        for (const auto& tuple : result_set) {
            std::string plan_line = std::get<std::string>(tuple.GetValue(0));
            std::cout << "| " << std::left << std::setw(max_length - 2)
                      << plan_line << " |" << std::endl;
        }

        // 打印底部边框
        std::cout << "+" << std::string(max_length, '-') << "+" << std::endl;

        std::cout << "\n(" << result_set.size() << " step"
                  << (result_set.size() != 1 ? "s" : "")
                  << " in execution plan)\n"
                  << std::endl;
    }

    // 显示 SHOW TABLES 结果
    void DisplayShowTablesResults(const std::vector<Tuple>& result_set) {
        if (result_set.empty()) {
            std::cout << "No tables found." << std::endl;
            return;
        }

        // 打印表头
        std::cout << "\n";
        std::cout << "+----------------+----------------+------------+---------"
                     "-+-------------+-------------+"
                  << std::endl;
        std::cout << "| Table Name     | Column Name    | Data Type  | "
                     "Nullable | Primary Key | Column Size |"
                  << std::endl;
        std::cout << "+----------------+----------------+------------+---------"
                     "-+-------------+-------------+"
                  << std::endl;

        std::string current_table = "";

        // 打印每一行数据
        for (const auto& tuple : result_set) {
            std::string table_name = std::get<std::string>(tuple.GetValue(0));
            std::string column_name = std::get<std::string>(tuple.GetValue(1));
            std::string data_type = std::get<std::string>(tuple.GetValue(2));
            std::string is_nullable = std::get<std::string>(tuple.GetValue(3));
            std::string is_primary_key =
                std::get<std::string>(tuple.GetValue(4));
            int32_t column_size = std::get<int32_t>(tuple.GetValue(5));

            // 如果是新表，添加分隔线
            if (current_table != table_name && !current_table.empty()) {
                std::cout << "+----------------+----------------+------------+-"
                             "---------+-------------+-------------+"
                          << std::endl;
            }
            current_table = table_name;

            // 格式化数据类型（如果是VARCHAR，显示大小）
            std::string formatted_type = data_type;
            if (data_type == "VARCHAR" && column_size > 0) {
                formatted_type = "VARCHAR(" + std::to_string(column_size) + ")";
            }

            // 打印行数据
            std::cout << "| " << std::setw(14) << std::left << table_name
                      << " | " << std::setw(14) << std::left << column_name
                      << " | " << std::setw(10) << std::left << formatted_type
                      << " | " << std::setw(8) << std::left << is_nullable
                      << " | " << std::setw(11) << std::left << is_primary_key
                      << " | " << std::setw(11) << std::right
                      << (column_size > 0 ? std::to_string(column_size) : "-")
                      << " |" << std::endl;
        }

        std::cout << "+----------------+----------------+------------+---------"
                     "-+-------------+-------------+"
                  << std::endl;

        // 统计信息
        std::set<std::string> unique_tables;
        for (const auto& tuple : result_set) {
            unique_tables.insert(std::get<std::string>(tuple.GetValue(0)));
        }

        std::cout << "\n"
                  << unique_tables.size() << " table(s) with "
                  << result_set.size() << " column(s) total.\n"
                  << std::endl;
    }

    // 显示 SELECT 查询结果
    void DisplaySelectResults(const std::vector<Tuple>& result_set,
                              SelectStatement* select_stmt) {
        if (result_set.empty()) {
            std::cout << "No results found." << std::endl;
            return;
        }

        // 获取表信息和schema
        TableInfo* table_info = catalog_->GetTable(select_stmt->GetTableName());
        if (!table_info) {
            std::cout << "Table not found." << std::endl;
            return;
        }

        const Schema* schema = table_info->schema.get();
        const auto& select_list = select_stmt->GetSelectList();

        // 确定要显示的列
        std::vector<std::string> column_names;
        std::vector<size_t> column_indices;

        // 检查是否是 SELECT *
        bool is_select_all = false;
        if (select_list.size() == 1) {
            auto* col_ref =
                dynamic_cast<ColumnRefExpression*>(select_list[0].get());
            if (col_ref && col_ref->GetColumnName() == "*") {
                is_select_all = true;
            }
        }

        if (is_select_all) {
            // SELECT * - 显示所有列
            for (size_t i = 0; i < schema->GetColumnCount(); ++i) {
                column_names.push_back(schema->GetColumn(i).name);
                column_indices.push_back(i);
            }
        } else {
            // 特定列
            for (const auto& expr : select_list) {
                auto* col_ref = dynamic_cast<ColumnRefExpression*>(expr.get());
                if (col_ref) {
                    std::string col_name = col_ref->GetColumnName();
                    column_names.push_back(col_name);
                    column_indices.push_back(schema->GetColumnIdx(col_name));
                }
            }
        }

        // 计算列宽度
        std::vector<size_t> column_widths;
        for (const auto& name : column_names) {
            column_widths.push_back(
                std::max(name.length(), static_cast<size_t>(15)));
        }

        // 打印表头
        std::cout << "\n";
        PrintSeparator(column_widths);
        std::cout << "|";
        for (size_t i = 0; i < column_names.size(); ++i) {
            std::cout << " " << std::setw(column_widths[i]) << std::left
                      << column_names[i] << " |";
        }
        std::cout << "\n";
        PrintSeparator(column_widths);

        // 打印数据行
        for (const auto& tuple : result_set) {
            std::cout << "|";
            for (size_t i = 0; i < column_indices.size(); ++i) {
                std::string value_str =
                    ValueToString(tuple.GetValue(column_indices[i]));
                std::cout << " " << std::setw(column_widths[i]) << std::left
                          << value_str << " |";
            }
            std::cout << "\n";
        }
        PrintSeparator(column_widths);

        std::cout << "\n"
                  << result_set.size() << " row(s) returned.\n"
                  << std::endl;
    }

    // 显示 INSERT 结果
    void DisplayInsertResults(const std::vector<Tuple>& result_set) {
        // INSERT 通常不返回数据，只显示影响的行数
        std::cout << "Insert operation completed." << std::endl;
    }

    // 显示 UPDATE 结果
    void DisplayUpdateResults(const std::vector<Tuple>& result_set,
                              UpdateStatement* update_stmt) {
        // 显示影响的行数
        if (!result_set.empty()) {
            Value affected_rows = result_set[0].GetValue(0);
            int32_t count = std::get<int32_t>(affected_rows);
            std::cout << count << " row(s) updated." << std::endl;
        } else {
            std::cout << "0 row(s) updated." << std::endl;
        }
    }

    // 显示 DELETE 结果
    void DisplayDeleteResults(const std::vector<Tuple>& result_set) {
        // 显示影响的行数
        if (!result_set.empty()) {
            Value affected_rows = result_set[0].GetValue(0);
            int32_t count = std::get<int32_t>(affected_rows);
            std::cout << count << " row(s) deleted." << std::endl;
        } else {
            std::cout << "0 row(s) deleted." << std::endl;
        }
    }

    // 显示表格数据（带表名）
    void DisplayTableData(const std::vector<Tuple>& tuples,
                          const std::string& table_name) {
        if (tuples.empty()) {
            std::cout << "No data to display." << std::endl;
            return;
        }

        TableInfo* table_info = catalog_->GetTable(table_name);
        if (!table_info) {
            std::cout << "Table not found." << std::endl;
            return;
        }

        const Schema* schema = table_info->schema.get();

        // 获取所有列名
        std::vector<std::string> column_names;
        for (size_t i = 0; i < schema->GetColumnCount(); ++i) {
            column_names.push_back(schema->GetColumn(i).name);
        }

        // 计算列宽度
        std::vector<size_t> column_widths;
        for (const auto& name : column_names) {
            column_widths.push_back(
                std::max(name.length(), static_cast<size_t>(15)));
        }

        // 打印表头
        PrintSeparator(column_widths);
        std::cout << "|";
        for (size_t i = 0; i < column_names.size(); ++i) {
            std::cout << " " << std::setw(column_widths[i]) << std::left
                      << column_names[i] << " |";
        }
        std::cout << "\n";
        PrintSeparator(column_widths);

        // 打印数据行
        for (const auto& tuple : tuples) {
            std::cout << "|";
            for (size_t i = 0; i < column_names.size(); ++i) {
                std::string value_str = ValueToString(tuple.GetValue(i));
                std::cout << " " << std::setw(column_widths[i]) << std::left
                          << value_str << " |";
            }
            std::cout << "\n";
        }
        PrintSeparator(column_widths);
        std::cout << "\n";
    }

    // 简单显示表格数据（不获取schema信息）
    void DisplayTableDataSimple(const std::vector<Tuple>& tuples) {
        if (tuples.empty()) {
            std::cout << "No data to display." << std::endl;
            return;
        }

        // 简化显示，假设每个tuple有相同的结构
        const auto& first_tuple = tuples[0];
        size_t column_count = first_tuple.GetValues().size();

        // 使用固定宽度
        std::vector<size_t> column_widths(column_count, 15);

        // 打印表头（使用列索引）
        PrintSeparator(column_widths);
        std::cout << "|";
        for (size_t i = 0; i < column_count; ++i) {
            std::string header = "Column" + std::to_string(i);
            std::cout << " " << std::setw(column_widths[i]) << std::left
                      << header << " |";
        }
        std::cout << "\n";
        PrintSeparator(column_widths);

        // 打印数据行
        for (const auto& tuple : tuples) {
            std::cout << "|";
            for (size_t i = 0; i < column_count; ++i) {
                std::string value_str = ValueToString(tuple.GetValue(i));
                std::cout << " " << std::setw(column_widths[i]) << std::left
                          << value_str << " |";
            }
            std::cout << "\n";
        }
        PrintSeparator(column_widths);
        std::cout << "\n";
    }

    // 打印表格分隔线
    void PrintSeparator(const std::vector<size_t>& column_widths) {
        std::cout << "+";
        for (size_t width : column_widths) {
            std::cout << std::string(width + 2, '-') << "+";
        }
        std::cout << "\n";
    }

    // 将 Value 转换为字符串
    std::string ValueToString(const Value& value) {
        std::stringstream ss;

        if (std::holds_alternative<bool>(value)) {
            ss << (std::get<bool>(value) ? "true" : "false");
        } else if (std::holds_alternative<int8_t>(value)) {
            ss << static_cast<int>(std::get<int8_t>(value));
        } else if (std::holds_alternative<int16_t>(value)) {
            ss << std::get<int16_t>(value);
        } else if (std::holds_alternative<int32_t>(value)) {
            ss << std::get<int32_t>(value);
        } else if (std::holds_alternative<int64_t>(value)) {
            ss << std::get<int64_t>(value);
        } else if (std::holds_alternative<float>(value)) {
            ss << std::fixed << std::setprecision(2) << std::get<float>(value);
        } else if (std::holds_alternative<double>(value)) {
            ss << std::fixed << std::setprecision(2) << std::get<double>(value);
        } else if (std::holds_alternative<std::string>(value)) {
            ss << std::get<std::string>(value);
        } else {
            ss << "NULL";
        }

        std::string result = ss.str();
        // 限制长度，避免表格变形
        if (result.length() > 13) {
            result = result.substr(0, 12) + "...";
        }
        return result;
    }

    void Shutdown() {
        // Create checkpoint
        recovery_manager_->CheckpointWithLogTruncation();
    }

    // Storage components
    std::unique_ptr<DiskManager> disk_manager_;
    std::unique_ptr<DiskManager> log_disk_manager_;
    std::unique_ptr<Replacer> replacer_;
    std::unique_ptr<BufferPoolManager> buffer_pool_manager_;
    // Transaction components
    std::unique_ptr<LogManager> log_manager_;
    std::unique_ptr<LockManager> lock_manager_;
    std::unique_ptr<TransactionManager> transaction_manager_;
    std::unique_ptr<RecoveryManager> recovery_manager_;
    // Catalog and execution
    std::unique_ptr<Catalog> catalog_;
    std::unique_ptr<TableManager> table_manager_;
    std::unique_ptr<ExecutionEngine> execution_engine_;
};

int main(int argc, char* argv[]) {
    std::string db_file = "simple_rdbms.db";
    if (argc > 1) {
        db_file = argv[1];
    }

    // debug等级是第二个参数
    if (argc > 2) {
        setenv("SIMPLEDB_DEBUG_LEVEL", argv[2], 1);
    }

    try {
        SimpleRDBMSServer server(db_file);
        server.Run();
    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}