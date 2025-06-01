# SimpleRDBMS

一个用C++实现的教学型关系数据库管理系统，实现了现代数据库系统的核心组件，包括存储管理、B+树索引、事务处理、日志恢复和SQL查询处理。

## 📋 目录

- [项目概述](#项目概述)
- [系统架构](#系统架构)
- [已实现功能](#已实现功能)
- [未实现功能](#未实现功能)
- [快速开始](#快速开始)
- [编译和运行](#编译和运行)
- [使用示例](#使用示例)
- [技术特性](#技术特性)
- [性能特性](#性能特性)
- [项目结构](#项目结构)
- [调试支持](#调试支持)
- [贡献](#贡献)
- [许可证](#许可证)

## 🎯 项目概述

SimpleRDBMS 是一个功能相对完整的教学型关系数据库管理系统，实现了现代DBMS的核心组件：

- **存储引擎**：页面式存储、LRU缓冲池管理
- **索引系统**：完整的B+树索引（支持分裂、合并、持久化）
- **事务处理**：基本ACID特性、两阶段锁协议
- **恢复机制**：基于WAL的ARIES恢复算法框架
- **SQL处理**：基础DDL/DML解析和执行引擎
- **表达式系统**：WHERE子句和表达式求值

## 🏗️ 系统架构

```
┌─────────────────────────────────────────────────────────┐
│                    SQL Interface                         │
├─────────────────────────────────────────────────────────┤
│  Parser & AST  │  Execution Engine  │  Catalog Manager │
│                │  Expression Eval   │  Table Manager   │
├─────────────────────────────────────────────────────────┤
│              Transaction Manager                         │
│  Lock Manager  │  Log Manager  │  Recovery Manager     │
├─────────────────────────────────────────────────────────┤
│  Record Manager  │  Index Manager (B+ Tree)            │
│  Table Heap     │  Split/Merge/Redistribute            │
├─────────────────────────────────────────────────────────┤
│              Buffer Pool Manager                         │
│  LRU Replacer  │  Page Management │ Concurrent Access  │
├─────────────────────────────────────────────────────────┤
│              Storage Manager                             │
│  Disk Manager  │  Page I/O        │ File Management    │
└─────────────────────────────────────────────────────────┘
```

## ✅ 已实现功能

### 🗄️ 存储层

#### 磁盘管理（完整实现）
- [x] **页面读写**：高效的4KB页面磁盘I/O操作
- [x] **页面分配与回收**：智能页面分配和重用机制  
- [x] **文件管理**：自动数据库文件创建和管理
- [x] **空闲页面管理**：已删除页面的重用系统
- [x] **文件扩展**：动态文件大小扩展
- [x] **同步写入**：强制数据持久化到磁盘

#### 缓冲池管理（完整实现）
- [x] **LRU页面替换**：最近最少使用算法
- [x] **页面固定机制**：Pin/Unpin引用计数
- [x] **并发访问控制**：线程安全的缓冲池操作
- [x] **脏页管理**：自动脏页检测和刷新
- [x] **页面驱逐**：智能页面替换和写回机制

### 📊 记录管理（完整实现）

#### 元组处理
- [x] **数据类型支持**：`BOOLEAN`, `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT`, `FLOAT`, `DOUBLE`, `VARCHAR`
- [x] **高效序列化**：记录序列化和反序列化
- [x] **变长记录**：VARCHAR字段的支持
- [x] **类型转换**：基本数据类型转换
- [x] **RID管理**：记录标识符系统

#### 表堆管理
- [x] **基础CRUD操作**：插入、删除、更新、查询
- [x] **多页面管理**：表跨页面的链表结构
- [x] **空间优化**：页面内空闲空间管理
- [x] **页面分裂**：表页面满时的自动扩展
- [x] **记录定位**：基于RID的记录定位
- [x] **迭代器支持**：全表顺序扫描

### 🌲 索引系统（完整实现）

#### B+树核心功能
- [x] **多类型键支持**：支持int32_t, int64_t, float, double, string作为索引键
- [x] **高效点查询**：O(log n)时间复杂度的查找
- [x] **插入操作**：支持重复键值的插入
- [x] **删除操作**：键值删除操作

#### B+树高级特性
- [x] **自动分裂**：节点满时的分裂机制
- [x] **节点合并**：删除时的节点合并（基础实现）
- [x] **重分布**：兄弟节点间的负载均衡（基础实现）
- [x] **持久化**：索引结构的磁盘持久化
- [x] **并发控制**：基础的并发访问控制
- [x] **根节点管理**：动态根节点创建和管理

### 📝 SQL处理（基础实现）

#### 词法和语法分析
- [x] **SQL词法分析**：基本SQL Token识别
- [x] **语法解析**：生成抽象语法树(AST)
- [x] **错误处理**：基本语法错误报告
- [x] **表达式解析**：WHERE子句表达式解析

#### 支持的SQL语句
- [x] **CREATE TABLE**：建表语句（包含基本约束）
```sql
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    age INT,
    active BOOLEAN
);
```

- [x] **INSERT**：插入语句（支持多行插入）
```sql
INSERT INTO users VALUES 
    (1, 'Alice', 25, TRUE), 
    (2, 'Bob', 30, FALSE);
```

- [x] **SELECT**：查询语句（支持WHERE子句）
```sql
SELECT id, name FROM users WHERE age > 25;
SELECT * FROM users;
```

- [x] **UPDATE**：更新语句（支持WHERE子句）
```sql
UPDATE users SET age = 26 WHERE name = 'Alice';
```

- [x] **DELETE**：删除语句（支持WHERE子句）
```sql
DELETE FROM users WHERE age < 18;
```

- [x] **CREATE INDEX**：索引创建
```sql
CREATE INDEX idx_name ON users(name);
```

- [x] **DROP TABLE/INDEX**：删除操作
- [x] **SHOW TABLES**：显示表信息
- [x] **事务控制**：BEGIN、COMMIT、ROLLBACK
- [x] **EXPLAIN**：显示执行计划

#### 表达式系统（基础实现）
- [x] **比较操作**：`=`, `!=`, `<`, `>`, `<=`, `>=`
- [x] **逻辑操作**：`AND`, `OR`, `NOT`
- [x] **算术操作**：`+`, `-`, `*`, `/`
- [x] **WHERE子句**：基本WHERE条件
- [x] **类型转换**：数值类型间的转换

### 🔄 事务处理（基础实现）

#### 事务管理
- [x] **基础ACID属性**：基本的事务特性
- [x] **事务状态管理**：GROWING、SHRINKING、COMMITTED、ABORTED
- [x] **事务ID管理**：唯一事务标识符分配
- [x] **事务生命周期**：开始到提交/回滚的管理

#### 锁管理（基础实现）
- [x] **两阶段锁**：基础2PL协议
- [x] **共享锁/排他锁**：读写锁机制
- [x] **锁升级**：共享锁到排他锁的升级
- [x] **死锁预防**：基于超时的死锁预防
- [x] **锁队列管理**：基本锁请求队列

### 📄 日志和恢复（基础实现）

#### WAL日志系统
- [x] **基础日志记录**：BEGIN、COMMIT、ABORT、INSERT、UPDATE、DELETE
- [x] **LSN管理**：日志序列号
- [x] **日志持久化**：Write-Ahead Logging
- [x] **日志缓冲**：基本日志缓冲

#### ARIES恢复算法（框架实现）
- [x] **分析阶段**：活跃事务表重建
- [x] **重做阶段**：基础操作重放
- [x] **撤销阶段**：未提交事务回滚
- [x] **检查点机制**：基本检查点

### 📚 元数据管理（完整实现）

#### Schema管理
- [x] **列定义**：数据类型、约束、大小
- [x] **表结构验证**：创建表时的检查
- [x] **主键约束**：主键唯一性保证
- [x] **NOT NULL约束**：非空约束
- [x] **VARCHAR大小限制**：变长字段大小验证

#### 目录管理
- [x] **表管理**：表的创建、删除、查询、持久化
- [x] **索引管理**：索引的创建、删除、查询
- [x] **OID分配**：对象标识符分配
- [x] **元数据持久化**：catalog信息的磁盘持久化

### 🎯 执行引擎（基础实现）

#### 查询执行
- [x] **Volcano模型**：基于迭代器的查询执行
- [x] **执行计划生成**：SQL到执行计划的转换
- [x] **多种执行器**：SeqScan、IndexScan、Insert、Update、Delete、Projection
- [x] **表达式求值**：WHERE子句求值
- [x] **简单查询优化**：基于等值条件的索引选择

## ❌ 未实现功能

### 🔴 高级查询功能（高优先级缺失）

#### 连接操作
- [ ] **内连接**：INNER JOIN
- [ ] **外连接**：LEFT/RIGHT/FULL OUTER JOIN  
- [ ] **交叉连接**：CROSS JOIN
- [ ] **自连接**：表与自身的连接
- [ ] **多表连接**：三表及以上的连接

#### 聚合和分组
- [ ] **聚合函数**：SUM, COUNT, AVG, MIN, MAX
- [ ] **GROUP BY**：分组查询
- [ ] **HAVING**：分组后的条件过滤
- [ ] **COUNT(DISTINCT)**：去重计数

#### 排序和分页
- [ ] **ORDER BY**：排序查询（单列和多列）
- [ ] **DISTINCT**：去重查询
- [ ] **LIMIT/OFFSET**：结果分页

### 🟡 复杂查询特性（中优先级缺失）

#### 子查询
- [ ] **标量子查询**：返回单个值的子查询
- [ ] **EXISTS子查询**：存在性检查  
- [ ] **IN/NOT IN**：集合成员检查
- [ ] **相关子查询**：与外查询相关的子查询

#### 高级SQL功能
- [ ] **集合操作**：UNION、INTERSECT、EXCEPT
- [ ] **公共表表达式**：WITH子句(CTE)
- [ ] **窗口函数**：ROW_NUMBER、RANK等
- [ ] **CASE表达式**：条件表达式

### 🟢 约束和完整性（中优先级缺失）

#### 参照完整性
- [ ] **外键约束**：引用完整性
- [ ] **级联操作**：CASCADE DELETE/UPDATE
- [ ] **约束检查**：运行时约束验证

#### 高级约束  
- [ ] **UNIQUE约束**：唯一性约束（除主键外）
- [ ] **CHECK约束**：自定义约束条件
- [ ] **DEFAULT值**：列默认值

### 🔵 索引和优化（中优先级缺失）

#### 高级索引
- [ ] **复合索引**：多列索引
- [ ] **部分索引**：带条件的索引
- [ ] **函数索引**：基于表达式的索引

#### 查询优化
- [ ] **基于成本的优化器**：CBO查询优化
- [ ] **统计信息**：表和索引统计
- [ ] **执行计划缓存**：查询计划缓存
- [ ] **连接顺序优化**：多表连接优化

### 🟣 数据类型扩展（低优先级缺失）

#### 扩展数据类型
- [ ] **日期时间类型**：DATE、TIME、TIMESTAMP
- [ ] **高精度数值**：DECIMAL、NUMERIC
- [ ] **二进制类型**：BLOB、BYTEA
- [ ] **JSON类型**：JSON数据类型和操作

#### 内置函数
- [ ] **字符串函数**：LENGTH、SUBSTRING、CONCAT等
- [ ] **数学函数**：ABS、ROUND、CEIL、FLOOR等
- [ ] **日期函数**：NOW、DATE_ADD、DATE_DIFF等
- [ ] **类型转换函数**：CAST、CONVERT

### 🟤 高级特性（低优先级缺失）

#### 数据库对象
- [ ] **视图**：虚拟表
- [ ] **存储过程**：服务器端编程
- [ ] **触发器**：事件驱动逻辑
- [ ] **序列**：自增序列生成器

#### 系统管理
- [ ] **用户权限管理**：认证和授权
- [ ] **数据库级操作**：CREATE/DROP DATABASE
- [ ] **备份恢复**：数据备份和恢复工具
- [ ] **性能监控**：查询性能统计

## 🚀 快速开始

### 系统要求

- **编译器**：支持C++17的编译器（GCC 7+, Clang 5+, MSVC 2017+）
- **构建系统**：CMake 3.10+
- **操作系统**：Linux, macOS, Windows
- **内存**：建议2GB以上
- **磁盘空间**：50MB以上

### 编译和安装

```bash
# 克隆仓库（如果从GitHub获取）
git clone <repository-url>
cd SimpleRDBMS

# 创建构建目录
mkdir build && cd build

# 配置和编译
cmake ..
make -j4

# 运行数据库
./simple_rdbms [database_file]
```

### 构建选项

```bash
# Debug版本（详细日志）
cmake -DCMAKE_BUILD_TYPE=Debug ..

# Release版本（优化性能）
cmake -DCMAKE_BUILD_TYPE=Release ..
```

## 📝 使用示例

### 基本操作

```sql
-- 启动数据库
SimpleRDBMS> CREATE TABLE employees (
    id INTEGER PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    age INTEGER,
    salary FLOAT,
    active BOOLEAN
);
Query executed successfully.

-- 插入数据
SimpleRDBMS> INSERT INTO employees VALUES 
    (1, 'Alice Johnson', 28, 75000.50, TRUE),
    (2, 'Bob Smith', 35, 82000.00, TRUE),
    (3, 'Charlie Brown', 42, 95000.75, FALSE);
Query executed successfully.

-- 查询数据
SimpleRDBMS> SELECT * FROM employees WHERE age > 30;
Query executed successfully.

+----+---------------+-----+----------+--------+
| id | name          | age | salary   | active |
+----+---------------+-----+----------+--------+
| 2  | Bob Smith     | 35  | 82000.00 | TRUE   |
| 3  | Charlie Brown | 42  | 95000.75 | FALSE  |
+----+---------------+-----+----------+--------+

2 row(s) returned.

-- 创建索引
SimpleRDBMS> CREATE INDEX idx_age ON employees(age);
Query executed successfully.

-- 更新数据
SimpleRDBMS> UPDATE employees SET salary = 85000.00 WHERE name = 'Bob Smith';
Query executed successfully.
1 row(s) updated.

-- 条件删除
SimpleRDBMS> DELETE FROM employees WHERE active = FALSE;
Query executed successfully.
1 row(s) deleted.

-- 显示表信息
SimpleRDBMS> SHOW TABLES;
Query executed successfully.

+----------------+----------------+------------+----------+-------------+-------------+
| Table Name     | Column Name    | Data Type  | Nullable | Primary Key | Column Size |
+----------------+----------------+------------+----------+-------------+-------------+
| employees      | id             | INTEGER    | NO       | YES         | -           |
| employees      | name           | VARCHAR    | NO       | NO          | 50          |
| employees      | age            | INTEGER    | YES      | NO          | -           |
| employees      | salary         | FLOAT      | YES      | NO          | -           |
| employees      | active         | BOOLEAN    | YES      | NO          | -           |
+----------------+----------------+------------+----------+-------------+-------------+

-- 查看执行计划
SimpleRDBMS> EXPLAIN SELECT * FROM employees WHERE age = 35;
Query executed successfully.

+--------------------------------------------+
|                QUERY PLAN                  |
+--------------------------------------------+
| -> Index Scan using idx_age on employees  |
|    (Index Cond: WHERE clause)             |
+--------------------------------------------+

-- 事务操作
SimpleRDBMS> BEGIN;
Query executed successfully.

SimpleRDBMS> INSERT INTO employees VALUES (4, 'David Wilson', 29, 78000.00, TRUE);
Query executed successfully.

SimpleRDBMS> COMMIT;
Query executed successfully.

-- 退出
SimpleRDBMS> exit
```

## 🔧 技术特性

### 存储特性
- **页面大小**：4KB标准页面
- **缓冲池**：100页默认大小（可配置）
- **记录格式**：变长记录，支持NULL值
- **索引结构**：B+树，支持多种数据类型
- **文件格式**：自定义二进制格式

### 并发特性
- **隔离级别**：支持多种标准隔离级别定义
- **锁粒度**：行级锁
- **死锁处理**：超时检测机制
- **事务恢复**：WAL日志系统

### 性能特性
- **索引优化**：等值条件自动选择索引扫描
- **缓冲管理**：LRU替换策略
- **并发控制**：多线程安全设计
- **页面压缩**：高效的页面空间利用

## 📈 性能特性

### 估算性能指标

#### B+树索引性能（理论值）
- **插入性能**：~10,000-50,000 ops/秒
- **查询性能**：~20,000-100,000 ops/秒  
- **范围扫描**：~10,000-80,000 records/秒
- **删除性能**：~10,000-40,000 ops/秒

#### 事务处理性能
- **简单事务**：~1,000-10,000 事务/秒
- **锁获取延迟**：~100微秒-1毫秒
- **日志写入**：~5,000-20,000 记录/秒

#### SQL执行性能
- **简单SELECT**：~5,000-50,000 查询/秒
- **带WHERE的SELECT**：~1,000-30,000 查询/秒
- **UPDATE/DELETE**：~1,000-25,000 操作/秒

*注：实际性能取决于硬件配置、数据大小和查询复杂度*

## 📁 项目结构

```
SimpleRDBMS/
├── src/
│   ├── buffer/           # 缓冲池管理
│   │   ├── buffer_pool_manager.cpp/.h
│   │   ├── lru_replacer.cpp/.h
│   │   └── replacer.h
│   ├── catalog/          # 元数据管理
│   │   ├── catalog.cpp/.h
│   │   ├── schema.cpp/.h
│   │   └── table_manager.cpp/.h
│   ├── common/           # 通用工具和定义
│   │   ├── config.h
│   │   ├── debug.h
│   │   ├── exception.h
│   │   └── types.h
│   ├── execution/        # 查询执行
│   │   ├── execution_engine.cpp/.h
│   │   ├── executor.cpp/.h
│   │   ├── expression_cloner.cpp/.h
│   │   ├── expression_evaluator.cpp/.h
│   │   └── plan_node.h
│   ├── index/            # 索引实现
│   │   ├── b_plus_tree.cpp/.h
│   │   ├── b_plus_tree_page.cpp/.h
│   │   └── index_manager.cpp/.h
│   ├── parser/           # SQL解析
│   │   ├── ast.h
│   │   ├── lexer.h
│   │   └── parser.cpp/.h
│   ├── record/           # 记录管理
│   │   ├── table_heap.cpp/.h
│   │   └── tuple.cpp/.h
│   ├── recovery/         # 日志和恢复
│   │   ├── log_manager.cpp/.h
│   │   ├── log_record.cpp/.h
│   │   └── recovery_manager.cpp/.h
│   ├── storage/          # 存储管理
│   │   ├── disk_manager.cpp/.h
│   │   └── page.cpp/.h
│   ├── transaction/      # 事务管理
│   │   ├── lock_manager.cpp/.h
│   │   ├── transaction.cpp/.h
│   │   └── transaction_manager.cpp/.h
│   └── main.cpp          # 主程序
├── CMakeLists.txt
└── README.md
```

## 🐛 调试支持

系统支持多级调试输出，通过环境变量控制：

```bash
# 设置调试级别
# 0=NONE, 1=ERROR, 2=WARN, 3=INFO, 4=DEBUG, 5=TRACE
export SIMPLEDB_DEBUG_LEVEL=4

# 运行数据库（会显示详细的内部操作日志）
./simple_rdbms test.db
```

调试输出包括：
- 页面I/O操作
- 缓冲池管理活动
- B+树节点操作
- 事务和锁操作
- SQL解析过程
- 查询执行步骤

## 🤝 贡献

### 优先贡献领域

1. **JOIN操作实现** - 内连接、外连接的完整实现
2. **聚合函数** - SUM、COUNT、AVG等聚合操作
3. **ORDER BY/GROUP BY** - 排序和分组查询
4. **子查询支持** - 各种形式的子查询
5. **查询优化器改进** - 更智能的查询优化
6. **复合索引** - 多列索引支持
7. **更多数据类型** - DATE、TIME、DECIMAL等类型
8. **性能优化** - 并行查询、缓存优化
9. **完善恢复机制** - 更完整的ARIES算法实现
10. **外键约束** - 参照完整性支持

### 贡献指南

1. **Fork项目** 并创建功能分支
2. **编写测试** 确保新功能的正确性
3. **遵循代码规范** 保持代码风格一致
4. **更新文档** 包括README和代码注释
5. **提交Pull Request** 详细描述变更内容

### 代码规范
- 使用Google C++代码风格
- 添加详细的注释说明
- 为新功能编写单元测试
- 更新相关文档

## 📜 许可证

本项目仅用于教学目的。

## 🙏 致谢

- 参考了CMU 15-445数据库系统课程的设计理念
- 借鉴了SQLite、PostgreSQL的部分实现思想
- 感谢开源社区的支持和贡献

## 📞 联系方式

- **项目维护者**：[PQCQAQ]
- **邮箱**：1220204124@zust.edu.cn
- **项目主页**：https://github.com/pqcqaq/qcdb-SimpleRDBMS

---

**注意**：这是一个教学型数据库系统，已实现了数据库系统的核心功能，但仍有许多高级特性需要完善。虽然功能相对完整，但不建议在生产环境中使用。该项目非常适合学习数据库内部原理和进行功能扩展开发。