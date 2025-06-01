/*
 * 文件: lexer.h
 * 作者: QCQCQC
 * 日期: 2025-6-1
 * 描述: SQL词法分析器，将SQL文本分解为token序列，为语法分析提供输入
 */

#pragma once

#include <string>
#include <vector>

namespace SimpleRDBMS {

/**
 * Token类型枚举
 *
 * 定义了SQL语言中所有可能的token类型，词法分析器会将输入文本
 * 分解成这些基本的语法单元。
 *
 * 设计思路：
 * - 按功能分组：关键字、操作符、字面量、标识符、标点符号等
 * - 覆盖了基本的SQL DDL/DML语句
 * - 支持常用的数据类型和操作符
 * - 包含事务控制和管理命令
 */
enum class TokenType {
    // SQL关键字 - 查询相关
    SELECT,  // SELECT关键字，用于查询语句
    FROM,    // FROM关键字，指定查询的表
    WHERE,   // WHERE关键字，指定过滤条件

    // SQL关键字 - 数据操作
    INSERT,  // INSERT关键字，插入数据
    INTO,    // INTO关键字，配合INSERT使用
    VALUES,  // VALUES关键字，指定插入的值
    UPDATE,  // UPDATE关键字，更新数据
    SET,     // SET关键字，配合UPDATE使用
    DELETE,  // DELETE关键字，删除数据

    // SQL关键字 - 表结构操作
    CREATE,  // CREATE关键字，创建数据库对象
    TABLE,   // TABLE关键字，表对象
    DROP,    // DROP关键字，删除数据库对象
    INDEX,   // INDEX关键字，索引对象
    ON,      // ON关键字，指定索引的列

    // SQL关键字 - 约束和属性
    PRIMARY,  // PRIMARY关键字，主键约束
    KEY,      // KEY关键字，配合PRIMARY使用
    NOT,      // NOT关键字，否定操作符
    _NULL,    // NULL关键字，空值（加下划线避免与C++关键字冲突）

    // 数据类型关键字
    INT,      // INT数据类型，32位整数
    BIGINT,   // BIGINT数据类型，64位整数
    VARCHAR,  // VARCHAR数据类型，变长字符串
    FLOAT,    // FLOAT数据类型，单精度浮点数
    DOUBLE,   // DOUBLE数据类型，双精度浮点数
    BOOLEAN,  // BOOLEAN数据类型，布尔值

    // 逻辑操作符
    AND,  // AND逻辑操作符，逻辑与
    OR,   // OR逻辑操作符，逻辑或

    // 比较操作符
    EQUALS,          // = 等于操作符
    NOT_EQUALS,      // != 或 <> 不等于操作符
    LESS_THAN,       // < 小于操作符
    GREATER_THAN,    // > 大于操作符
    LESS_EQUALS,     // <= 小于等于操作符
    GREATER_EQUALS,  // >= 大于等于操作符

    // 算术操作符
    PLUS,      // + 加法操作符
    MINUS,     // - 减法操作符（也可用作负号）
    MULTIPLY,  // * 乘法操作符（也用于SELECT *）
    DIVIDE,    // / 除法操作符

    // 字面量token - 具体的值
    INTEGER_LITERAL,  // 整数字面量，如123, -456
    FLOAT_LITERAL,    // 浮点数字面量，如3.14, -2.5
    STRING_LITERAL,   // 字符串字面量，如'hello', "world"
    BOOLEAN_LITERAL,  // 布尔字面量，如TRUE, FALSE

    // 标识符 - 用户定义的名称
    IDENTIFIER,  // 标识符，如表名、列名、索引名等

    // 标点符号和分隔符
    LPAREN,     // ( 左圆括号
    RPAREN,     // ) 右圆括号
    COMMA,      // , 逗号分隔符
    SEMICOLON,  // ; 语句结束符
    STAR,       // * 星号（SELECT *中使用）

    // 特殊token
    EOF_TOKEN,  // 文件结束标记
    INVALID,    // 无效token，词法错误时使用

    // 数据库管理命令
    SHOW,    // SHOW关键字，显示信息
    TABLES,  // TABLES关键字，配合SHOW使用

    // 事务控制命令
    BEGIN,     // BEGIN关键字，开始事务
    COMMIT,    // COMMIT关键字，提交事务
    ROLLBACK,  // ROLLBACK关键字，回滚事务

    // 查询计划相关
    EXPLAIN,  // EXPLAIN关键字，显示执行计划
};

/**
 * Token结构体
 *
 * 表示词法分析产生的一个语法单元，包含了token的所有必要信息
 *
 * 设计要点：
 * - type：token的类型，用于语法分析时的匹配
 * - value：token的原始文本值，用于获取具体内容（如标识符名称、字面量值）
 * - line/column：位置信息，用于错误报告和调试
 *
 * 使用场景：
 * - 语法分析器根据type进行语法规则匹配
 * - 语义分析时使用value获取具体的名称或值
 * - 错误报告时使用line/column定位错误位置
 */
struct Token {
    TokenType type;     // token类型
    std::string value;  // token的文本值
    size_t line;        // 所在行号（从1开始）
    size_t column;      // 所在列号（从1开始）
};

/**
 * 词法分析器类
 *
 * 负责将SQL文本输入分解成token序列，这是编译前端的第一个阶段
 *
 * 工作原理：
 * 1. 逐字符扫描输入文本
 * 2. 识别关键字、标识符、字面量、操作符等
 * 3. 跳过空白字符和注释
 * 4. 记录位置信息用于错误报告
 * 5. 生成token序列供语法分析器使用
 *
 * 设计特点：
 * - 单向扫描，不回退（简化实现）
 * - 贪心匹配（如>>=会识别为>=而不是>>和=）
 * - 大小写不敏感的关键字识别
 * - 支持单引号和双引号字符串
 * - 详细的位置跟踪用于错误报告
 */
class Lexer {
   public:
    /**
     * 构造函数
     * @param input 要分析的SQL文本
     *
     * 初始化词法分析器的状态：
     * - 保存输入文本
     * - 设置扫描位置为开始
     * - 初始化行号和列号
     */
    explicit Lexer(const std::string& input);

    /**
     * 获取下一个token
     * @return 下一个token，如果到达文件末尾则返回EOF_TOKEN
     *
     * 这是词法分析器的核心方法，每次调用返回一个token
     * 内部实现：
     * 1. 跳过空白字符
     * 2. 根据当前字符判断token类型
     * 3. 调用相应的扫描方法
     * 4. 更新位置信息
     * 5. 返回构造好的token
     */
    Token NextToken();

   private:
    std::string input_;  // 输入的SQL文本
    size_t position_;    // 当前扫描位置
    size_t line_;        // 当前行号
    size_t column_;      // 当前列号

    /**
     * 查看当前位置的字符但不前进
     * @return 当前字符，如果到达末尾返回'\0'
     *
     * 用于向前看一个字符，决定如何处理当前token
     */
    char Peek();

    /**
     * 获取当前字符并前进到下一个位置
     * @return 当前字符，同时更新position、line、column
     *
     * 这是扫描的基本操作，每次读取一个字符并更新状态
     */
    char Advance();

    /**
     * 跳过空白字符（空格、制表符、换行符等）
     *
     * 空白字符在SQL中只起分隔作用，不产生token
     * 同时负责正确更新行号和列号
     */
    void SkipWhitespace();

    /**
     * 扫描数字字面量（整数或浮点数）
     * @return 数字token（INTEGER_LITERAL或FLOAT_LITERAL）
     *
     * 处理逻辑：
     * - 扫描连续的数字字符
     * - 如果遇到小数点，继续扫描为浮点数
     * - 支持科学计数法（可选扩展）
     * - 处理负号的情况
     */
    Token ScanNumber();

    /**
     * 扫描字符串字面量
     * @return 字符串token（STRING_LITERAL）
     *
     * 处理逻辑：
     * - 支持单引号和双引号
     * - 处理转义字符（如\'、\"、\\等）
     * - 检测未闭合的字符串错误
     * - 支持多行字符串（可选）
     */
    Token ScanString();

    /**
     * 扫描标识符或关键字
     * @return 标识符token或关键字token
     *
     * 处理逻辑：
     * - 扫描字母、数字、下划线组成的标识符
     * - 检查是否为SQL关键字
     * - 关键字匹配不区分大小写
     * - 如果是关键字返回对应的关键字token
     * - 否则返回IDENTIFIER token
     */
    Token ScanIdentifier();
};

}  // namespace SimpleRDBMS