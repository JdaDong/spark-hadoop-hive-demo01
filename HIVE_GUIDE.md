# Hive 高级特性使用指南

本文档详细介绍 Hive 高级特性的使用方法和最佳实践。

## 📑 目录

1. [分区表管理](#分区表管理)
2. [分桶表](#分桶表)
3. [复杂数据类型](#复杂数据类型)
4. [窗口函数](#窗口函数)
5. [事务表(ACID)](#事务表acid)
6. [物化视图](#物化视图)
7. [性能优化](#性能优化)

---

## 分区表管理

### 概述

分区表将数据按照某个列的值分割到不同的子目录,大幅提升查询效率。

### 静态分区

#### 创建分区表

```sql
CREATE TABLE sales (
    order_id INT,
    product STRING,
    amount DOUBLE
) PARTITIONED BY (year INT, month INT)
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');
```

#### 插入静态分区

```sql
-- 插入到指定分区
INSERT INTO sales PARTITION (year=2024, month=1)
VALUES (1, 'ProductA', 100.0), (2, 'ProductB', 200.0);

-- 从其他表插入
INSERT INTO sales PARTITION (year=2024, month=1)
SELECT order_id, product, amount FROM source_table WHERE year=2024 AND month=1;
```

#### 管理分区

```sql
-- 查看所有分区
SHOW PARTITIONS sales;

-- 查看特定分区
SHOW PARTITIONS sales PARTITION (year=2024);

-- 添加分区(空分区)
ALTER TABLE sales ADD PARTITION (year=2024, month=3);

-- 删除分区
ALTER TABLE sales DROP PARTITION (year=2024, month=3);

-- 重命名分区
ALTER TABLE sales PARTITION (year=2024, month=1) 
  RENAME TO PARTITION (year=2024, month=13);

-- 修复分区(从 HDFS 扫描并添加分区)
MSCK REPAIR TABLE sales;
```

### 动态分区

动态分区可以自动根据数据的值创建分区,无需手动指定。

#### 启用动态分区

```sql
-- 启用动态分区
SET hive.exec.dynamic.partition = true;

-- 设置为非严格模式(允许全部分区列都是动态的)
SET hive.exec.dynamic.partition.mode = nonstrict;

-- 设置最大动态分区数
SET hive.exec.max.dynamic.partitions = 10000;
SET hive.exec.max.dynamic.partitions.pernode = 1000;
```

#### 动态分区插入

```sql
-- 全动态分区
INSERT INTO sales PARTITION (year, month)
SELECT order_id, product, amount, year, month FROM source_table;

-- 混合分区(year 静态, month 动态)
INSERT INTO sales PARTITION (year=2024, month)
SELECT order_id, product, amount, month FROM source_table WHERE year=2024;
```

### 分区查询优化

```sql
-- 好: 使用分区列过滤(分区裁剪)
SELECT * FROM sales WHERE year=2024 AND month=1;

-- 差: 不使用分区列
SELECT * FROM sales WHERE amount > 1000;

-- 好: 分区列在 WHERE 中
SELECT * FROM sales WHERE year=2024 AND product='ProductA';

-- 使用 IN 子句
SELECT * FROM sales WHERE year=2024 AND month IN (1,2,3);
```

### 分区最佳实践

1. **合理选择分区列**:
   - 常用于过滤的列
   - 基数适中(不要太高或太低)
   - 通常选择日期、地区等

2. **分区粒度**:
   - 按天分区: 数据量大,查询通常按天
   - 按月分区: 数据量中等
   - 多级分区: year/month/day

3. **避免小分区**:
   - 每个分区至少几百MB
   - 太多小分区影响 NameNode 性能

4. **定期清理**:
   ```sql
   -- 删除90天前的分区
   ALTER TABLE sales DROP PARTITION (year=2023, month=10);
   ```

---

## 分桶表

### 概述

分桶表对数据进行哈希分桶,优化 JOIN 和采样查询。

### 创建分桶表

```sql
CREATE TABLE users (
    user_id INT,
    username STRING,
    email STRING,
    age INT
) CLUSTERED BY (user_id) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='false');
```

### 分桶 + 排序

```sql
CREATE TABLE users (
    user_id INT,
    username STRING,
    age INT
) CLUSTERED BY (user_id) SORTED BY (age DESC) INTO 32 BUCKETS
STORED AS ORC;
```

### 插入分桶表

```sql
-- 启用分桶
SET hive.enforce.bucketing = true;
SET hive.enforce.sorting = true;

-- 插入数据
INSERT INTO users 
SELECT user_id, username, email, age FROM source_users;
```

### 分桶优势

#### 1. 高效采样

```sql
-- 采样 1/4 的数据
SELECT * FROM users TABLESAMPLE(BUCKET 1 OUT OF 4 ON user_id);

-- 采样 10% 的数据
SELECT * FROM users TABLESAMPLE(10 PERCENT);
```

#### 2. Bucket Map Join

```sql
-- 启用 Bucket Map Join
SET hive.optimize.bucketmapjoin = true;
SET hive.optimize.bucketmapjoin.sortedmerge = true;

-- 两个分桶表 JOIN
SELECT u.username, o.order_id
FROM users_bucketed u
JOIN orders_bucketed o
ON u.user_id = o.user_id;
```

### 分桶数量选择

- 选择 2 的幂次: 2, 4, 8, 16, 32, 64, 128
- 每个桶 128MB - 1GB 之间
- 计算公式: `桶数 = 总数据量 / 目标桶大小`

---

## 复杂数据类型

### Array 数组

#### 创建表

```sql
CREATE TABLE products (
    product_id INT,
    name STRING,
    tags ARRAY<STRING>,
    prices ARRAY<DOUBLE>
) STORED AS ORC;
```

#### 插入数据

```sql
INSERT INTO products VALUES 
    (1, 'Laptop', array('Electronics', 'Computer'), array(1000.0, 1200.0, 900.0)),
    (2, 'Phone', array('Electronics', 'Mobile'), array(500.0, 600.0));
```

#### 查询数组

```sql
-- 访问元素
SELECT name, tags[0] as first_tag, prices[0] as first_price FROM products;

-- 数组长度
SELECT name, size(tags) as tag_count, size(prices) as price_count FROM products;

-- 检查元素存在
SELECT name FROM products WHERE array_contains(tags, 'Electronics');

-- 数组排序
SELECT name, sort_array(prices) as sorted_prices FROM products;

-- 数组去重
SELECT name, array_distinct(tags) as unique_tags FROM products;
```

#### 展开数组

```sql
-- LATERAL VIEW explode
SELECT product_id, name, tag
FROM products
LATERAL VIEW explode(tags) tagTable AS tag;

-- 带序号展开
SELECT product_id, name, pos, tag
FROM products
LATERAL VIEW posexplode(tags) tagTable AS pos, tag;
```

### Map 映射

#### 创建表

```sql
CREATE TABLE users (
    user_id INT,
    name STRING,
    properties MAP<STRING, STRING>
) STORED AS ORC;
```

#### 插入数据

```sql
INSERT INTO users VALUES 
    (1, 'Alice', map('age', '28', 'city', 'Beijing', 'country', 'China')),
    (2, 'Bob', map('age', '32', 'city', 'Shanghai'));
```

#### 查询Map

```sql
-- 访问键值
SELECT name, properties['age'] as age, properties['city'] as city FROM users;

-- Map 大小
SELECT name, size(properties) as prop_count FROM users;

-- 获取所有键
SELECT name, map_keys(properties) as keys FROM users;

-- 获取所有值
SELECT name, map_values(properties) as values FROM users;
```

#### 展开Map

```sql
SELECT user_id, name, key, value
FROM users
LATERAL VIEW explode(properties) propTable AS key, value;
```

### Struct 结构体

#### 创建表

```sql
CREATE TABLE employees (
    emp_id INT,
    name STRING,
    address STRUCT<street:STRING, city:STRING, zipcode:STRING>,
    contact STRUCT<phone:STRING, email:STRING>
) STORED AS ORC;
```

#### 插入数据

```sql
INSERT INTO employees VALUES 
    (1, 'Alice', 
     named_struct('street', '123 Main St', 'city', 'Beijing', 'zipcode', '100000'),
     named_struct('phone', '1234567890', 'email', 'alice@example.com'));
```

#### 查询Struct

```sql
-- 访问字段
SELECT name, address.city, address.zipcode, contact.email FROM employees;

-- Struct 作为整体
SELECT name, address FROM employees WHERE address.city = 'Beijing';
```

### 复杂类型嵌套

```sql
-- Array<Struct>
CREATE TABLE orders (
    order_id INT,
    items ARRAY<STRUCT<product:STRING, quantity:INT, price:DOUBLE>>
);

INSERT INTO orders VALUES (
    1,
    array(
        named_struct('product', 'Laptop', 'quantity', 2, 'price', 1000.0),
        named_struct('product', 'Mouse', 'quantity', 5, 'price', 25.0)
    )
);

-- 查询嵌套结构
SELECT order_id, item.product, item.quantity
FROM orders
LATERAL VIEW explode(items) itemTable AS item;

-- Map<String, Array<String>>
CREATE TABLE user_tags (
    user_id INT,
    tag_map MAP<STRING, ARRAY<STRING>>
);
```

---

## 窗口函数

### 排名函数

```sql
-- ROW_NUMBER: 连续排名,相同值不同排名
SELECT 
    name, department, salary,
    ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as row_num
FROM employees;

-- RANK: 跳跃排名,相同值相同排名
SELECT 
    name, salary,
    RANK() OVER (ORDER BY salary DESC) as rank
FROM employees;
-- 结果: 1, 2, 2, 4, 5 (有两个第2名,跳过第3名)

-- DENSE_RANK: 密集排名,相同值相同排名,不跳跃
SELECT 
    name, salary,
    DENSE_RANK() OVER (ORDER BY salary DESC) as dense_rank
FROM employees;
-- 结果: 1, 2, 2, 3, 4 (有两个第2名,不跳过第3名)

-- NTILE: 分组排名
SELECT 
    name, salary,
    NTILE(4) OVER (ORDER BY salary DESC) as quartile
FROM employees;
-- 将数据分成4组
```

### 聚合窗口函数

```sql
SELECT 
    name, department, salary,
    -- 部门平均值
    AVG(salary) OVER (PARTITION BY department) as dept_avg,
    -- 部门总和
    SUM(salary) OVER (PARTITION BY department) as dept_total,
    -- 部门最大值
    MAX(salary) OVER (PARTITION BY department) as dept_max,
    -- 部门最小值
    MIN(salary) OVER (PARTITION BY department) as dept_min,
    -- 部门人数
    COUNT(*) OVER (PARTITION BY department) as dept_count,
    -- 与部门平均的差值
    salary - AVG(salary) OVER (PARTITION BY department) as diff_from_avg
FROM employees;
```

### 偏移函数

```sql
SELECT 
    date, revenue,
    -- 前一天的收入
    LAG(revenue, 1) OVER (ORDER BY date) as prev_revenue,
    -- 后一天的收入
    LEAD(revenue, 1) OVER (ORDER BY date) as next_revenue,
    -- 前7天的收入
    LAG(revenue, 7) OVER (ORDER BY date) as revenue_week_ago,
    -- 第一天的收入
    FIRST_VALUE(revenue) OVER (ORDER BY date) as first_revenue,
    -- 最后一天的收入
    LAST_VALUE(revenue) OVER (ORDER BY date 
        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) as last_revenue
FROM daily_sales;
```

### 框架子句 (Frame)

```sql
-- 累计汇总
SELECT 
    date, amount,
    SUM(amount) OVER (
        ORDER BY date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as cumulative_sum
FROM sales;

-- 移动平均(3天)
SELECT 
    date, amount,
    AVG(amount) OVER (
        ORDER BY date 
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) as moving_avg_3
FROM sales;

-- 范围框架
SELECT 
    date, amount,
    SUM(amount) OVER (
        ORDER BY date 
        RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW
    ) as sum_last_week
FROM sales;
```

### 窗口函数应用场景

**1. Top-N 查询**
```sql
-- 每个部门薪资前3名
SELECT * FROM (
    SELECT 
        name, department, salary,
        ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as rn
    FROM employees
) t WHERE rn <= 3;
```

**2. 同比环比分析**
```sql
SELECT 
    month, revenue,
    revenue - LAG(revenue, 1) OVER (ORDER BY month) as mom_diff,
    (revenue - LAG(revenue, 1) OVER (ORDER BY month)) / 
        LAG(revenue, 1) OVER (ORDER BY month) * 100 as mom_pct,
    revenue - LAG(revenue, 12) OVER (ORDER BY month) as yoy_diff
FROM monthly_sales;
```

**3. 去重**
```sql
-- 保留每组中最新的一条
SELECT * FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY timestamp DESC) as rn
    FROM user_actions
) t WHERE rn = 1;
```

---

## 事务表(ACID)

### 概述

Hive 3.0+ 支持完整的 ACID 事务,允许 UPDATE、DELETE、MERGE 操作。

### 启用事务

```xml
<!-- hive-site.xml -->
<property>
  <name>hive.support.concurrency</name>
  <value>true</value>
</property>
<property>
  <name>hive.txn.manager</name>
  <value>org.apache.hadoop.hive.ql.lockmgr.DbTxnManager</value>
</property>
<property>
  <name>hive.compactor.initiator.on</name>
  <value>true</value>
</property>
<property>
  <name>hive.compactor.worker.threads</name>
  <value>1</value>
</property>
```

### 创建事务表

```sql
CREATE TABLE accounts (
    account_id INT,
    account_name STRING,
    balance DOUBLE,
    last_update TIMESTAMP
) STORED AS ORC
TBLPROPERTIES (
    'transactional' = 'true',
    'orc.compress' = 'SNAPPY'
);
```

**要求:**
- 必须使用 ORC 文件格式
- 必须设置 `transactional=true`
- 建议设置分桶

### INSERT 操作

```sql
-- 单条插入
INSERT INTO accounts VALUES (1, 'Alice', 1000.0, current_timestamp());

-- 批量插入
INSERT INTO accounts VALUES 
    (2, 'Bob', 1500.0, current_timestamp()),
    (3, 'Charlie', 2000.0, current_timestamp());

-- 从查询插入
INSERT INTO accounts SELECT * FROM temp_accounts;
```

### UPDATE 操作

```sql
-- 更新单个字段
UPDATE accounts SET balance = 1100.0 WHERE account_id = 1;

-- 更新多个字段
UPDATE accounts 
SET balance = balance + 100, last_update = current_timestamp()
WHERE account_id = 1;

-- 基于条件更新
UPDATE accounts 
SET balance = balance * 1.05 
WHERE balance > 1000;
```

### DELETE 操作

```sql
-- 删除单条
DELETE FROM accounts WHERE account_id = 3;

-- 删除多条
DELETE FROM accounts WHERE balance < 100;

-- 删除全部(不推荐,使用 TRUNCATE)
DELETE FROM accounts;
```

### MERGE 操作 (UPSERT)

```sql
-- 基本 MERGE
MERGE INTO accounts AS target
USING updates AS source
ON target.account_id = source.account_id
WHEN MATCHED THEN UPDATE SET balance = source.balance
WHEN NOT MATCHED THEN INSERT VALUES (source.account_id, source.name, source.balance, current_timestamp());

-- 复杂 MERGE
MERGE INTO accounts AS target
USING (
    SELECT account_id, SUM(amount) as total_amount
    FROM transactions
    WHERE date = '2024-01-01'
    GROUP BY account_id
) AS source
ON target.account_id = source.account_id
WHEN MATCHED AND source.total_amount > 0 THEN 
    UPDATE SET balance = balance + source.total_amount, last_update = current_timestamp()
WHEN MATCHED AND source.total_amount < 0 THEN 
    DELETE
WHEN NOT MATCHED THEN 
    INSERT VALUES (source.account_id, 'New Account', source.total_amount, current_timestamp());
```

### Compaction (压缩)

事务表会产生 Delta 文件,需要定期压缩。

```sql
-- Minor Compaction (合并小 Delta 文件)
ALTER TABLE accounts COMPACT 'minor';

-- Major Compaction (合并所有 Delta 到 Base)
ALTER TABLE accounts COMPACT 'major';

-- 指定分区 Compaction
ALTER TABLE sales PARTITION (year=2024, month=1) COMPACT 'major';

-- 查看 Compaction 状态
SHOW COMPACTIONS;

-- 清理旧的 Compaction
ALTER TABLE accounts PARTITION (year=2024) COMPACT 'clean';
```

### 事务管理

```sql
-- 查看锁
SHOW LOCKS;

-- 查看事务
SHOW TRANSACTIONS;

-- 终止事务
ABORT TRANSACTIONS <transaction_id>;
```

---

## 物化视图

### 概述

物化视图预计算并存储查询结果,加速复杂查询。

### 创建物化视图

```sql
-- 基本物化视图
CREATE MATERIALIZED VIEW sales_summary AS
SELECT 
    region, 
    product_category,
    SUM(amount) as total_sales,
    COUNT(*) as order_count,
    AVG(amount) as avg_sales
FROM sales
GROUP BY region, product_category;

-- 带分区的物化视图
CREATE MATERIALIZED VIEW daily_sales_mv
PARTITIONED ON (sale_date)
AS
SELECT 
    sale_date,
    region,
    SUM(amount) as total
FROM sales
GROUP BY sale_date, region;
```

### 刷新物化视图

```sql
-- 全量刷新
ALTER MATERIALIZED VIEW sales_summary REBUILD;

-- 增量刷新(仅支持部分场景)
ALTER MATERIALIZED VIEW sales_summary REBUILD INCREMENTAL;
```

### 查询重写

Hive 自动使用物化视图加速查询。

```sql
-- 启用查询重写
SET hive.materializedview.rewriting = true;

-- 原始查询
SELECT region, SUM(amount) 
FROM sales 
GROUP BY region;

-- Hive 自动改写为
SELECT region, SUM(total_sales)
FROM sales_summary
GROUP BY region;
```

### 管理物化视图

```sql
-- 查看所有物化视图
SHOW MATERIALIZED VIEWS;

-- 查看物化视图详情
DESCRIBE FORMATTED sales_summary;

-- 禁用物化视图
ALTER MATERIALIZED VIEW sales_summary DISABLE REWRITE;

-- 启用物化视图
ALTER MATERIALIZED VIEW sales_summary ENABLE REWRITE;

-- 删除物化视图
DROP MATERIALIZED VIEW sales_summary;
```

---

## 性能优化

### 文件格式优化

```sql
-- ORC 格式(推荐)
CREATE TABLE data_orc (
    id INT,
    name STRING,
    value DOUBLE
) STORED AS ORC
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'orc.compress.size' = '262144',
    'orc.stripe.size' = '268435456',
    'orc.row.index.stride' = '10000'
);

-- Parquet 格式
CREATE TABLE data_parquet (
    id INT,
    name STRING,
    value DOUBLE
) STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY',
    'parquet.block.size' = '134217728'
);
```

### 查询优化配置

```sql
-- ========== CBO 优化器 ==========
SET hive.cbo.enable = true;
SET hive.compute.query.using.stats = true;
SET hive.stats.autogather = true;
SET hive.stats.fetch.column.stats = true;

-- ========== 向量化执行 ==========
SET hive.vectorized.execution.enabled = true;
SET hive.vectorized.execution.reduce.enabled = true;
SET hive.vectorized.execution.reduce.groupby.enabled = true;

-- ========== Map-side JOIN ==========
SET hive.auto.convert.join = true;
SET hive.auto.convert.join.noconditionaltask = true;
SET hive.auto.convert.join.noconditionaltask.size = 25000000;
SET hive.mapjoin.smalltable.filesize = 25000000;

-- ========== 并行执行 ==========
SET hive.exec.parallel = true;
SET hive.exec.parallel.thread.number = 16;

-- ========== 谓词下推 ==========
SET hive.optimize.ppd = true;
SET hive.optimize.ppd.storage = true;

-- ========== 小文件合并 ==========
SET hive.merge.mapfiles = true;
SET hive.merge.mapredfiles = true;
SET hive.merge.size.per.task = 268435456;
SET hive.merge.smallfiles.avgsize = 16777216;

-- ========== 动态分区优化 ==========
SET hive.optimize.sort.dynamic.partition = true;

-- ========== 数据倾斜处理 ==========
SET hive.groupby.skewindata = true;
SET hive.optimize.skewjoin = true;
SET hive.skewjoin.key = 100000;

-- ========== 压缩 ==========
SET hive.exec.compress.intermediate = true;
SET hive.exec.compress.output = true;
SET mapreduce.output.fileoutputformat.compress.codec = org.apache.hadoop.io.compress.SnappyCodec;
```

### 查询优化技巧

**1. 分区裁剪**
```sql
-- 好
SELECT * FROM sales WHERE year=2024 AND month=1;

-- 差
SELECT * FROM sales WHERE concat(year, month) = '202401';
```

**2. 列裁剪**
```sql
-- 好
SELECT id, name FROM users;

-- 差
SELECT * FROM users;
```

**3. JOIN 优化**
```sql
-- 使用 MAPJOIN hint
SELECT /*+ MAPJOIN(small_table) */ *
FROM small_table s JOIN large_table l ON s.id = l.id;

-- 使用分桶表 JOIN
SELECT * FROM users_bucketed u JOIN orders_bucketed o
ON u.user_id = o.user_id;
```

**4. GROUP BY 优化**
```sql
-- 使用 DISTRIBUTE BY
SELECT department, COUNT(*)
FROM employees
DISTRIBUTE BY department;
```

**5. EXPLAIN 分析**
```sql
EXPLAIN SELECT * FROM sales WHERE year=2024;
EXPLAIN EXTENDED SELECT * FROM users JOIN orders;
EXPLAIN DEPENDENCY SELECT * FROM sales;
```

### 表统计信息

```sql
-- 收集表统计信息
ANALYZE TABLE sales COMPUTE STATISTICS;

-- 收集列统计信息
ANALYZE TABLE sales COMPUTE STATISTICS FOR COLUMNS;

-- 查看统计信息
DESCRIBE FORMATTED sales;
```

---

## 总结

Hive 高级特性大大增强了数据仓库能力:

✅ **分区表**: 提升查询效率
✅ **分桶表**: 优化 JOIN 和采样
✅ **复杂类型**: 处理半结构化数据
✅ **窗口函数**: 强大的分析能力
✅ **ACID事务**: 支持 UPDATE/DELETE
✅ **物化视图**: 加速复杂查询
✅ **性能优化**: 多种优化手段

合理使用这些特性可以构建高效、灵活的数据仓库解决方案。
