# Apache Phoenix 使用指南

## 📖 概述

Apache Phoenix 是构建在 HBase 之上的 SQL 层,提供了 JDBC 驱动和完整的 SQL 支持。

## 🚀 快速开始

### 1. 安装 Phoenix

```bash
# 下载 Phoenix
wget https://archive.apache.org/dist/phoenix/phoenix-5.1.2/phoenix-hbase-2.4-5.1.2-bin.tar.gz

# 解压
tar -xzf phoenix-hbase-2.4-5.1.2-bin.tar.gz
mv phoenix-5.1.2-HBase-2.4 /usr/local/phoenix

# 配置环境变量
export PHOENIX_HOME=/usr/local/phoenix
export PATH=$PATH:$PHOENIX_HOME/bin

# 将 Phoenix 服务端 JAR 拷贝到 HBase
cp $PHOENIX_HOME/phoenix-server-hbase-2.4-5.1.2.jar $HBASE_HOME/lib/

# 重启 HBase
stop-hbase.sh
start-hbase.sh
```

### 2. 连接 Phoenix

```bash
# 使用 sqlline 客户端
sqlline.py localhost:2181

# 看到提示符
jdbc:phoenix:localhost:2181>
```

### 3. 运行示例代码

```bash
# 运行 Java 示例
mvn exec:java -Dexec.mainClass="com.bigdata.phoenix.PhoenixJavaApp"

# 运行 Scala 示例
mvn exec:java -Dexec.mainClass="com.bigdata.phoenix.PhoenixScalaApp"
```

## 💻 SQL 基础操作

### 创建表

```sql
-- 基本表
CREATE TABLE users (
    user_id VARCHAR PRIMARY KEY,
    name VARCHAR,
    email VARCHAR,
    age INTEGER,
    city VARCHAR
);

-- 带压缩的表
CREATE TABLE users (
    user_id VARCHAR PRIMARY KEY,
    name VARCHAR,
    age INTEGER
) COMPRESSION='SNAPPY';

-- 带 TTL 的表
CREATE TABLE logs (
    log_id VARCHAR PRIMARY KEY,
    message VARCHAR,
    timestamp TIMESTAMP
) TTL=86400;

-- 带预分区的表
CREATE TABLE users (
    user_id VARCHAR PRIMARY KEY,
    name VARCHAR
) SPLIT ON ('A', 'M', 'Z');

-- Salting 表(避免热点)
CREATE TABLE users (
    user_id VARCHAR PRIMARY KEY,
    name VARCHAR
) SALT_BUCKETS=10;
```

### 插入数据

```sql
-- UPSERT (INSERT + UPDATE)
UPSERT INTO users VALUES ('user001', 'John', 'john@example.com', 30, 'Beijing');

-- 批量插入
UPSERT INTO users VALUES 
    ('user001', 'John', 'john@example.com', 30, 'Beijing'),
    ('user002', 'Jane', 'jane@example.com', 28, 'Shanghai'),
    ('user003', 'Bob', 'bob@example.com', 35, 'Shenzhen');

-- 从查询结果插入
UPSERT INTO users_backup SELECT * FROM users WHERE city = 'Beijing';
```

### 查询数据

```sql
-- 基本查询
SELECT * FROM users;

-- 条件查询
SELECT * FROM users WHERE city = 'Beijing';

-- 聚合查询
SELECT city, COUNT(*), AVG(age) FROM users GROUP BY city;

-- 排序和限制
SELECT * FROM users ORDER BY age DESC LIMIT 10;

-- JOIN 查询
SELECT u.name, o.product_name, o.amount
FROM users u
INNER JOIN orders o ON u.user_id = o.user_id;

-- 窗口函数
SELECT 
    name, 
    city, 
    age,
    ROW_NUMBER() OVER (PARTITION BY city ORDER BY age DESC) as rank
FROM users;

-- 子查询
SELECT * FROM users 
WHERE age > (SELECT AVG(age) FROM users);
```

### 更新数据

```sql
-- 更新
UPSERT INTO users (user_id, age) VALUES ('user001', 31);

-- 批量更新
UPSERT INTO users (user_id, city) 
SELECT user_id, 'New City' FROM users WHERE city = 'Old City';
```

### 删除数据

```sql
-- 删除单行
DELETE FROM users WHERE user_id = 'user001';

-- 条件删除
DELETE FROM users WHERE age < 18;

-- 删除表
DROP TABLE users;
```

## 🔍 索引管理

### 创建索引

```sql
-- 全局索引(读多写少)
CREATE INDEX idx_email ON users(email);

-- 覆盖索引(包含查询需要的列)
CREATE INDEX idx_city_covered ON users(city) INCLUDE(name, age);

-- 本地索引(写多读少)
CREATE LOCAL INDEX idx_local_email ON users(email);

-- 异步创建索引(大表)
CREATE INDEX idx_email ON users(email) ASYNC;
```

### 使用索引

```sql
-- 自动使用索引
SELECT * FROM users WHERE email = 'john@example.com';

-- 强制使用索引
SELECT /*+ INDEX(users idx_city_covered) */ name, age 
FROM users WHERE city = 'Beijing';

-- 强制不使用索引
SELECT /*+ NO_INDEX */ * FROM users WHERE city = 'Beijing';
```

### 管理索引

```sql
-- 查看所有索引
SELECT * FROM SYSTEM.CATALOG 
WHERE TABLE_NAME = 'USERS' AND COLUMN_FAMILY IS NOT NULL;

-- 删除索引
DROP INDEX idx_email ON users;

-- 重建索引
ALTER INDEX idx_email ON users REBUILD;

-- 禁用索引
ALTER INDEX idx_email ON users DISABLE;

-- 启用索引
ALTER INDEX idx_email ON users ACTIVE;
```

## 📊 视图和序列

### 视图

```sql
-- 创建视图
CREATE VIEW high_salary_users AS
SELECT user_id, name, salary
FROM users
WHERE salary > 100000;

-- 查询视图
SELECT * FROM high_salary_users;

-- 可更新视图
CREATE VIEW beijing_users AS
SELECT * FROM users WHERE city = 'Beijing';

UPSERT INTO beijing_users VALUES ('user999', 'Tom', 'tom@example.com', 25, 'Beijing');

-- 删除视图
DROP VIEW high_salary_users;
```

### 序列

```sql
-- 创建序列
CREATE SEQUENCE user_seq START WITH 1000 INCREMENT BY 1;

-- 获取下一个值
SELECT NEXT VALUE FOR user_seq;

-- 获取当前值
SELECT CURRENT VALUE FOR user_seq;

-- 在插入中使用
UPSERT INTO users VALUES (
    TO_CHAR(NEXT VALUE FOR user_seq),
    'John',
    'john@example.com',
    30,
    'Beijing'
);

-- 删除序列
DROP SEQUENCE user_seq;
```

## 🔐 事务支持

```sql
-- 创建事务表
CREATE TABLE account (
    account_id VARCHAR PRIMARY KEY,
    balance DOUBLE
) TRANSACTIONAL=true;

-- 事务操作(JDBC)
Connection conn = DriverManager.getConnection("jdbc:phoenix:localhost:2181");
conn.setAutoCommit(false);

try {
    PreparedStatement ps1 = conn.prepareStatement(
        "UPSERT INTO account VALUES (?, ?)");
    ps1.setString(1, "A001");
    ps1.setDouble(2, 1000.0);
    ps1.executeUpdate();
    
    PreparedStatement ps2 = conn.prepareStatement(
        "UPSERT INTO account VALUES (?, ?)");
    ps2.setString(1, "A002");
    ps2.setDouble(2, 2000.0);
    ps2.executeUpdate();
    
    conn.commit();
} catch (SQLException e) {
    conn.rollback();
}
```

## 📈 性能优化

### 1. 表设计优化

```sql
-- Salting(避免热点)
CREATE TABLE users (
    user_id VARCHAR PRIMARY KEY,
    data VARCHAR
) SALT_BUCKETS=20;

-- 预分区
CREATE TABLE users (
    user_id VARCHAR PRIMARY KEY,
    data VARCHAR
) SPLIT ON ('1000', '2000', '3000', '4000', '5000');

-- 压缩
CREATE TABLE users (
    user_id VARCHAR PRIMARY KEY,
    data VARCHAR
) COMPRESSION='SNAPPY';

-- 列族配置
CREATE TABLE users (
    user_id VARCHAR PRIMARY KEY,
    info.name VARCHAR,
    info.age INTEGER,
    address.city VARCHAR
) info.VERSIONS=3;
```

### 2. 查询优化

```sql
-- 更新统计信息
UPDATE STATISTICS users;

-- 查看执行计划
EXPLAIN SELECT * FROM users WHERE city = 'Beijing';

-- 使用查询提示
SELECT /*+ USE_SORT_MERGE_JOIN */ 
    u.name, o.amount
FROM users u
JOIN orders o ON u.user_id = o.user_id;

-- 避免全表扫描
-- ❌ Bad
SELECT * FROM users WHERE UPPER(name) = 'JOHN';

-- ✅ Good
CREATE INDEX idx_name ON users(name);
SELECT * FROM users WHERE name = 'John';
```

### 3. 批量操作

```java
// Java 批量插入
Connection conn = DriverManager.getConnection("jdbc:phoenix:localhost:2181");
conn.setAutoCommit(false);

PreparedStatement stmt = conn.prepareStatement(
    "UPSERT INTO users VALUES (?, ?, ?, ?, ?)");

for (int i = 0; i < 10000; i++) {
    stmt.setString(1, "user" + i);
    stmt.setString(2, "Name" + i);
    stmt.setString(3, "email" + i + "@example.com");
    stmt.setInt(4, 20 + i % 50);
    stmt.setString(5, "City" + (i % 10));
    stmt.addBatch();
    
    if (i % 1000 == 0) {
        stmt.executeBatch();
        conn.commit();
    }
}

stmt.executeBatch();
conn.commit();
```

## 🐍 与其他工具集成

### 1. Spark 集成

```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("Phoenix Integration")
  .getOrCreate()

// 读取
val df = spark.read
  .format("org.apache.phoenix.spark")
  .option("table", "users")
  .option("zkUrl", "localhost:2181")
  .load()

// 写入
df.write
  .format("org.apache.phoenix.spark")
  .option("table", "users_backup")
  .option("zkUrl", "localhost:2181")
  .mode("overwrite")
  .save()
```

### 2. Python 集成

```python
import phoenixdb

# 连接
conn = phoenixdb.connect('http://localhost:8765/', autocommit=True)
cursor = conn.cursor()

# 查询
cursor.execute("SELECT * FROM users WHERE city = ?", ['Beijing'])
for row in cursor:
    print(row)

# 插入
cursor.execute("UPSERT INTO users VALUES (?, ?, ?)", 
               ['user001', 'John', 'john@example.com'])

cursor.close()
conn.close()
```

### 3. JDBC 工具连接

```properties
# DBeaver / DataGrip 配置
Driver: org.apache.phoenix.jdbc.PhoenixDriver
URL: jdbc:phoenix:localhost:2181
```

## 🔧 常用命令

### SQLLine 命令

```bash
# 查看所有表
!tables

# 查看表结构
!describe users

# 执行 SQL 文件
!run /path/to/script.sql

# 设置输出格式
!outputformat table

# 退出
!quit
```

### 管理命令

```sql
-- 查看所有表
SELECT * FROM SYSTEM.CATALOG WHERE TABLE_TYPE = 'u';

-- 查看表统计信息
SELECT * FROM SYSTEM.STATS WHERE PHYSICAL_NAME = 'USERS';

-- 查看索引
SELECT * FROM SYSTEM.CATALOG 
WHERE TABLE_NAME = 'USERS' AND COLUMN_FAMILY IS NOT NULL;

-- 查看序列
SELECT * FROM SYSTEM.SEQUENCE;
```

## ⚠️ 注意事项

1. **主键设计**
   - Phoenix 主键对应 HBase RowKey
   - 避免热点(使用 Salting 或 Hash 前缀)
   - 考虑查询模式

2. **数据类型**
   - Phoenix 类型会影响排序
   - VARCHAR vs CHAR 的区别
   - 时间戳精度问题

3. **性能考虑**
   - 更新统计信息
   - 合理使用索引
   - 批量操作而非单条操作

4. **版本兼容**
   - Phoenix 版本必须与 HBase 版本匹配
   - 客户端和服务端版本一致

## 📚 参考资料

- [Phoenix 官方文档](https://phoenix.apache.org/index.html)
- [Phoenix 语法参考](https://phoenix.apache.org/language/index.html)
- [Phoenix 性能优化](https://phoenix.apache.org/performance.html)
- [Phoenix FAQ](https://phoenix.apache.org/faq.html)
