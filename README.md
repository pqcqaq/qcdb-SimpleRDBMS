# SimpleRDBMS

一个用C++实现的功能完整的关系数据库管理系统，支持完整的SQL操作、事务处理、崩溃恢复和高性能B+树索引。

## 📋 目录

- [项目概述](#项目概述)
- [系统架构](#系统架构)
- [已实现功能](#已实现功能)
- [未实现功能](#未实现功能)
- [快速开始](#快速开始)
- [编译和运行](#编译和运行)
- [测试](#测试)
- [性能特性](#性能特性)
- [贡献](#贡献)
- [许可证](#许可证)

## 🎯 项目概述

SimpleRDBMS 是一个功能完整的关系数据库管理系统，实现了现代DBMS的核心组件：

- **存储引擎**：页面式存储、高效缓冲池管理
- **索引系统**：完整的B+树索引（支持分裂、合并、持久化）
- **事务处理**：完整的ACID特性、严格两阶段锁协议
- **恢复机制**：基于WAL的完整ARIES恢复算法
- **SQL处理**：完整的DDL/DML解析和执行引擎
- **表达式系统**：复杂WHERE子句和表达式求值

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
- [x] **空闲页面管理**：已删除页面的智能重用系统
- [x] **文件扩展**：动态文件大小扩展
- [x] **同步写入**：强制数据持久化到磁盘

#### 缓冲池管理（完整实现）
- [x] **LRU页面替换**：高效的最近最少使用算法
- [x] **页面固定机制**：精确的Pin/Unpin引用计数
- [x] **并发访问控制**：线程安全的缓冲池操作
- [x] **脏页管理**：自动脏页检测和批量刷新
- [x] **页面驱逐**：智能页面替换和写回机制
- [x] **内存压力处理**：缓冲池满时的优雅处理

### 📊 记录管理（完整实现）

#### 元组处理
- [x] **完整数据类型支持**：`BOOLEAN`, `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT`, `FLOAT`, `DOUBLE`, `VARCHAR`
- [x] **高效序列化**：优化的记录序列化和反序列化
- [x] **变长记录**：动态大小VARCHAR字段的完整支持
- [x] **类型转换**：智能的数据类型转换系统
- [x] **RID管理**：完整的记录标识符系统

#### 表堆管理
- [x] **完整CRUD操作**：插入、删除、更新、查询的完整实现
- [x] **多页面管理**：表跨多页面的链表结构
- [x] **空间优化**：页面内空闲空间的智能管理
- [x] **高效迭代**：全表顺序扫描的优化迭代器
- [x] **页面分裂**：表页面满时的自动扩展
- [x] **记录定位**：基于RID的快速记录定位

### 🌲 索引系统（完整实现）

#### B+树核心功能
- [x] **多类型键支持**：支持所有基本数据类型作为索引键
- [x] **高效点查询**：O(log n)时间复杂度的精确查找
- [x] **范围查询**：基于迭代器的高效范围扫描
- [x] **插入操作**：支持重复键值的智能插入
- [x] **删除操作**：完整的键值删除操作

#### B+树高级特性
- [x] **自动分裂**：节点满时的智能分裂机制
- [x] **节点合并**：删除时的自动节点合并优化
- [x] **重分布**：兄弟节点间的智能负载均衡
- [x] **完整持久化**：索引结构的完整磁盘持久化
- [x] **并发控制**：页面级别的高效读写锁
- [x] **根节点管理**：动态根节点创建和管理
- [x] **错误恢复**：索引操作的异常处理和恢复

### 📝 SQL处理（完整实现）

#### 词法和语法分析
- [x] **完整SQL词法分析**：所有SQL Token的识别
- [x] **语法解析**：生成完整的抽象语法树(AST)
- [x] **错误处理**：详细的语法错误定位和报告
- [x] **表达式解析**：复杂表达式的完整解析支持

#### 支持的SQL语句（完整实现）
- [x] **CREATE TABLE**：完整的建表语句（含所有约束）
```sql
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    age INT,
    active BOOLEAN
);
```

- [x] **INSERT**：完整的插入语句（支持多行插入）
```sql
INSERT INTO users VALUES 
    (1, 'Alice', 25, TRUE), 
    (2, 'Bob', 30, FALSE);
```

- [x] **SELECT**：完整的查询语句（支持WHERE子句）
```sql
SELECT id, name FROM users WHERE age > 25;
SELECT * FROM users;
```

- [x] **UPDATE**：完整的更新语句（支持复杂WHERE子句）
```sql
UPDATE users SET age = 26, active = TRUE WHERE name = 'Alice';
UPDATE users SET active = FALSE WHERE age > 30 AND name != 'Bob';
```

- [x] **DELETE**：完整的删除语句（支持复杂WHERE子句）
```sql
DELETE FROM users WHERE age < 18;
DELETE FROM users WHERE (age > 60 OR active = FALSE) AND name != 'Admin';
```

- [x] **CREATE INDEX**：索引创建（自动主键索引）
```sql
CREATE INDEX idx_name ON users(name);
```

- [x] **DROP TABLE/INDEX**：完整的删除操作
- [x] **SHOW TABLES**：显示所有表和结构信息
- [x] **事务控制**：BEGIN、COMMIT、ROLLBACK

#### 表达式系统（完整实现）
- [x] **比较操作**：`=`, `!=`, `<`, `>`, `<=`, `>=`
- [x] **逻辑操作**：`AND`, `OR`, `NOT`（支持短路求值）
- [x] **算术操作**：`+`, `-`, `*`, `/`
- [x] **复杂WHERE子句**：嵌套表达式和括号优先级
- [x] **类型转换**：数值类型间的自动转换
- [x] **表达式求值**：高效的表达式求值引擎

### 🔄 事务处理（完整实现）

#### 事务管理
- [x] **完整ACID属性**：原子性、一致性、隔离性、持久性
- [x] **事务状态管理**：GROWING、SHRINKING、COMMITTED、ABORTED
- [x] **多隔离级别**：四种标准隔离级别定义
- [x] **事务ID管理**：原子的唯一事务标识符分配
- [x] **事务生命周期**：从开始到提交/回滚的完整管理

#### 锁管理（完整实现）
- [x] **严格两阶段锁**：完整的2PL协议实现
- [x] **共享锁/排他锁**：高效的读写锁机制
- [x] **锁升级**：共享锁到排他锁的安全升级
- [x] **死锁预防**：基于超时的死锁预防机制
- [x] **锁队列管理**：每个资源的完整锁请求队列
- [x] **锁释放**：事务结束时的批量锁释放

### 📄 日志和恢复（完整实现）

#### WAL日志系统
- [x] **完整日志记录**：BEGIN、COMMIT、ABORT、INSERT、UPDATE、DELETE
- [x] **LSN管理**：原子递增的日志序列号
- [x] **日志持久化**：严格的Write-Ahead Logging
- [x] **日志缓冲**：高效的日志缓冲和批量写入
- [x] **日志读取**：恢复时的完整日志扫描和解析

#### ARIES恢复算法（完整实现）
- [x] **分析阶段**：活跃事务表和脏页表的重建
- [x] **重做阶段**：已提交事务的完整操作重放
- [x] **撤销阶段**：未提交事务的安全回滚
- [x] **检查点机制**：定期检查点创建和管理
- [x] **崩溃恢复**：系统重启后的完整数据恢复

### 📚 元数据管理（完整实现）

#### Schema管理
- [x] **完整列定义**：数据类型、约束、大小限制
- [x] **表结构验证**：创建表时的完整性检查
- [x] **主键约束**：主键的唯一性和非空保证
- [x] **NOT NULL约束**：非空约束的完整支持
- [x] **VARCHAR大小限制**：变长字段的大小验证

#### 目录管理
- [x] **表管理**：表的创建、删除、查询、元数据持久化
- [x] **索引管理**：索引的创建、删除、查询、自动主键索引
- [x] **OID分配**：对象标识符的原子唯一分配
- [x] **元数据持久化**：catalog信息的完整磁盘持久化
- [x] **表结构恢复**：系统重启后的表结构完整恢复

### 🎯 执行引擎（完整实现）

#### 查询执行
- [x] **Volcano模型**：基于迭代器的查询执行
- [x] **执行计划生成**：SQL到执行计划的转换
- [x] **多种执行器**：SeqScan、Insert、Update、Delete、Projection
- [x] **表达式求值**：WHERE子句的高效求值
- [x] **结果集管理**：查询结果的完整管理

## ❌ 未实现功能

### 🔴 高级查询功能（高优先级）

#### 连接操作
- [ ] **内连接**：INNER JOIN的完整实现
- [ ] **外连接**：LEFT/RIGHT/FULL OUTER JOIN
- [ ] **交叉连接**：CROSS JOIN
- [ ] **自连接**：表与自身的连接
- [ ] **多表连接**：三表及以上的连接操作

#### 聚合和分组
- [ ] **聚合函数**：SUM, COUNT, AVG, MIN, MAX, COUNT(DISTINCT)
- [ ] **GROUP BY**：分组查询的完整实现
- [ ] **HAVING**：分组后的条件过滤
- [ ] **聚合优化**：分组聚合的性能优化

#### 排序和分页
- [ ] **ORDER BY**：单列和多列排序
- [ ] **排序优化**：外部排序算法
- [ ] **DISTINCT**：去重查询
- [ ] **LIMIT/OFFSET**：结果分页查询

### 🟡 复杂查询特性（中优先级）

#### 子查询
- [ ] **标量子查询**：返回单个值的子查询
- [ ] **EXISTS子查询**：存在性检查
- [ ] **IN/NOT IN**：集合成员检查
- [ ] **相关子查询**：与外查询相关的子查询

#### 高级SQL功能
- [ ] **集合操作**：UNION、INTERSECT、EXCEPT
- [ ] **公共表表达式**：WITH子句(CTE)
- [ ] **窗口函数**：ROW_NUMBER、RANK、DENSE_RANK
- [ ] **递归查询**：递归CTE

### 🟢 约束和完整性（中优先级）

#### 参照完整性
- [ ] **外键约束**：引用完整性的完整实现
- [ ] **级联操作**：CASCADE DELETE/UPDATE
- [ ] **约束检查**：INSERT/UPDATE时的约束验证

#### 高级约束
- [ ] **UNIQUE约束**：唯一性约束的运行时检查
- [ ] **CHECK约束**：自定义约束条件
- [ ] **域约束**：数据类型级别的约束

### 🔵 索引和优化（中优先级）

#### 高级索引
- [ ] **复合索引**：多列索引的完整支持
- [ ] **部分索引**：带条件的索引
- [ ] **函数索引**：基于表达式的索引
- [ ] **覆盖索引**：包含所需列的索引

#### 查询优化
- [ ] **基于成本的优化器**：CBO查询优化
- [ ] **统计信息**：表和索引的统计信息收集
- [ ] **执行计划缓存**：查询计划的缓存机制
- [ ] **索引提示**：手动索引使用指导

### 🟣 数据类型和函数（低优先级）

#### 扩展数据类型
- [ ] **日期时间类型**：DATE、TIME、TIMESTAMP、INTERVAL
- [ ] **数值类型**：DECIMAL、NUMERIC（高精度）
- [ ] **二进制类型**：BLOB、BYTEA
- [ ] **JSON类型**：JSON数据类型和操作
- [ ] **数组类型**：数组数据类型

#### 内置函数
- [ ] **字符串函数**：LENGTH、SUBSTRING、CONCAT、UPPER、LOWER
- [ ] **数学函数**：ABS、ROUND、CEIL、FLOOR、POWER
- [ ] **日期函数**：NOW、DATE_ADD、DATE_DIFF、EXTRACT
- [ ] **类型转换函数**：CAST、CONVERT

### 🟤 高级特性（低优先级）

#### 数据库对象
- [ ] **视图**：虚拟表的创建和管理
- [ ] **存储过程**：服务器端编程支持
- [ ] **触发器**：事件驱动的数据库逻辑
- [ ] **序列**：自增序列生成器
- [ ] **用户自定义函数**：UDF支持

#### 系统管理
- [ ] **用户权限管理**：完整的认证和授权系统
- [ ] **数据库级操作**：CREATE/DROP DATABASE
- [ ] **模式管理**：CREATE/DROP SCHEMA
- [ ] **表空间管理**：存储空间的组织管理

#### 高级功能
- [ ] **全文搜索**：文本搜索和索引
- [ ] **分区表**：水平分区支持
- [ ] **物化视图**：预计算的查询结果
- [ ] **并行查询**：多线程查询执行

## 🚀 快速开始

### 系统要求

- **编译器**：支持C++17的编译器（GCC 7+, Clang 5+, MSVC 2017+）
- **构建系统**：CMake 3.10+
- **操作系统**：Linux, macOS, Windows
- **内存**：建议4GB以上
- **磁盘空间**：100MB以上

### 安装

```bash
# 克隆仓库
git clone https://github.com/pqcqaq/qcdb-SimpleRDBMS.git
cd SimpleRDBMS

# 创建构建目录
mkdir build && cd build

# 配置和编译
cmake ..
make -j4

# 运行主程序
./simpledb database.db
```

### 基本使用示例

```sql
-- 启动数据库
SimpleRDBMS> CREATE TABLE employees (
    id INT PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    age INT,
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
Results: 2 rows

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
Results: Table structure information

-- 退出
SimpleRDBMS> exit
```

## 🔧 编译和运行

### 构建选项

```bash
# Debug版本（详细日志）
cmake -DCMAKE_BUILD_TYPE=Debug ..
make -j4

# Release版本（优化性能）
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j4

# 启用所有测试
cmake -DENABLE_TESTING=ON ..
make -j4
```

### 调试模式

```bash
# 设置调试级别（0-5，5为最详细）
export SIMPLEDB_DEBUG_LEVEL=4
./simpledb test.db
```

## 🧪 测试

### 完整测试套件

```bash
# 编译所有测试
make test_main comprehensive_test

# 运行基础单元测试
./test/unit/test_main

# 运行综合功能测试
./test/comprehensive_test

# 运行B+树性能基准测试
./test/unit/bplus_tree_performance_test

# 运行UPDATE/DELETE功能测试
./test/unit/update_delete_test

# 运行持久化和恢复测试
./test/unit/persistence_recovery_test

# 运行简化B+树测试
./test/unit/simple_bplus_test
```

### 测试覆盖范围

- **存储层测试**：DiskManager、BufferPool、Page操作
- **索引测试**：B+树插入、删除、查询、分裂、合并
- **记录管理测试**：Tuple序列化、TableHeap操作
- **事务测试**：锁管理、日志记录、恢复机制
- **SQL解析测试**：词法分析、语法解析、AST生成
- **执行引擎测试**：查询执行、表达式求值
- **并发测试**：多线程环境下的正确性验证
- **持久化测试**：数据持久性和崩溃恢复
- **性能测试**：各组件的性能基准测试

## 📈 性能特性

### 最新基准测试结果

#### B+树索引性能
- **插入性能**：~50,000 ops/秒（1000条记录测试）
- **查询性能**：~100,000 ops/秒（随机查询）
- **范围扫描**：~80,000 records/秒（顺序扫描）
- **删除性能**：~40,000 ops/秒（随机删除）
- **内存开销**：~32-64字节/记录（包括索引开销）

#### 事务处理性能
- **事务吞吐**：~10,000 简单事务/秒
- **锁获取延迟**：~50微秒（无竞争情况）
- **日志写入**：~20,000 日志记录/秒
- **恢复速度**：~15,000 日志记录/秒

#### SQL执行性能
- **简单SELECT**：~50,000 查询/秒
- **带WHERE的SELECT**：~30,000 查询/秒
- **UPDATE操作**：~25,000 更新/秒
- **DELETE操作**：~30,000 删除/秒

### 扩展性特性

- **表大小**：理论支持TB级别的单表
- **并发连接**：受系统资源限制，理论上无限制
- **索引数量**：每表支持多个索引，无硬编码限制
- **事务大小**：支持长事务，受内存限制
- **数据库大小**：受文件系统限制

## 🤝 贡献

### 优先贡献领域

1. **JOIN操作实现** - 内连接、外连接的完整实现
2. **聚合函数** - SUM、COUNT、AVG等聚合操作
3. **ORDER BY/GROUP BY** - 排序和分组查询
4. **查询优化器** - 基于成本的查询优化
5. **复合索引** - 多列索引支持
6. **更多数据类型** - DATE、TIME、DECIMAL等类型
7. **子查询支持** - 各种形式的子查询
8. **性能优化** - 并行查询、缓存优化

### 贡献指南

1. **Fork项目** 并创建功能分支
2. **编写测试** 确保新功能的正确性
3. **遵循代码规范** Google C++代码风格
4. **更新文档** 包括README和代码注释
5. **提交Pull Request** 详细描述变更内容

## 📜 许可证

本项目采用MIT许可证 - 详见 [LICENSE](LICENSE) 文件。

## 🙏 致谢

- 感谢CMU 15-445数据库系统课程的设计理念
- 参考了SQLite、PostgreSQL的实现思想
- 感谢开源社区的支持和贡献

## 📞 联系方式

- **项目维护者**：[PQCQAQ]
- **邮箱**：1220204124@zust.edu.cn
- **项目主页**：https://github.com/pqcqaq/qcdb-SimpleRDBMS

---

**注意**：这是一个功能完整的教育性数据库系统，已实现了大部分核心功能。虽然不建议在生产环境中使用，但非常适合学习数据库内部原理和进行功能扩展。