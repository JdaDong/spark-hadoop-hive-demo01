# 🚀 现代大数据技术栈完整指南

本文档详细介绍项目中新增的现代大数据技术栈,包括 Flink、Kafka、Flink CDC、Iceberg、ClickHouse 等。

---

## 📋 目录

- [Apache Flink 流处理](#apache-flink-流处理)
- [Apache Kafka 消息队列](#apache-kafka-消息队列)
- [Flink CDC 实时数据同步](#flink-cdc-实时数据同步)
- [Apache Iceberg 数据湖](#apache-iceberg-数据湖)
- [ClickHouse 实时OLAP](#clickhouse-实时olap)
- [技术栈对比](#技术栈对比)
- [架构最佳实践](#架构最佳实践)

---

## 🌊 Apache Flink 流处理

### 核心概念

Flink 是一个**真正的流处理引擎**,提供低延迟、高吞吐的数据处理能力。

#### Flink vs Spark Streaming

| 特性 | Flink | Spark Streaming |
|------|-------|-----------------|
| 处理模型 | 真正的流处理 | 微批处理 |
| 延迟 | 毫秒级 | 秒级 |
| 状态管理 | 原生支持,强大 | 有限支持 |
| 精确一次 | ✅ 原生支持 | ✅ 支持 |
| 窗口操作 | 丰富(事件时间/处理时间) | 基础支持 |
| 生态 | 快速发展 | 成熟 |

### 核心功能

#### 1. DataStream API

```java
// Java示例
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

DataStream<String> stream = env.fromElements("a", "b", "c");

stream
    .map(s -> s.toUpperCase())
    .filter(s -> !s.equals("A"))
    .print();

env.execute("DataStream Example");
```

```scala
// Scala示例 - 函数式风格
val env = StreamExecutionEnvironment.getExecutionEnvironment

val stream = env.fromElements("a", "b", "c")

stream
  .map(_.toUpperCase)
  .filter(_ != "A")
  .print()

env.execute("DataStream Example")
```

#### 2. 窗口计算

**窗口类型:**

1. **滚动窗口** (Tumbling Window)
   - 固定大小,无重叠
   - 适用场景: 每5分钟统计一次

```java
stream
    .keyBy(event -> event.userId)
    .window(TumblingEventTimeWindows.of(Time.minutes(5)))
    .sum("amount");
```

2. **滑动窗口** (Sliding Window)
   - 固定大小,有重叠
   - 适用场景: 最近1小时,每5分钟更新

```java
stream
    .keyBy(event -> event.userId)
    .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
    .sum("amount");
```

3. **会话窗口** (Session Window)
   - 动态大小,基于活动间隔
   - 适用场景: 用户会话分析

```java
stream
    .keyBy(event -> event.userId)
    .window(EventTimeSessionWindows.withGap(Time.minutes(10)))
    .apply(new SessionAggregateFunction());
```

#### 3. 状态管理

Flink 提供强大的状态管理能力:

**状态类型:**
- **ValueState**: 单个值
- **ListState**: 列表
- **MapState**: 键值对
- **ReducingState**: 聚合状态
- **AggregatingState**: 自定义聚合

```java
// ValueState 示例
public class StatefulMapFunction extends RichMapFunction<Event, Result> {
    private transient ValueState<Integer> countState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Integer> descriptor = 
            new ValueStateDescriptor<>("count", Integer.class, 0);
        countState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public Result map(Event event) throws Exception {
        Integer currentCount = countState.value();
        currentCount += 1;
        countState.update(currentCount);
        return new Result(event.userId, currentCount);
    }
}
```

#### 4. 水位线 (Watermark)

水位线用于处理乱序事件:

```java
WatermarkStrategy<Event> watermarkStrategy = 
    WatermarkStrategy
        .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
        .withTimestampAssigner((event, timestamp) -> event.timestamp);

stream.assignTimestampsAndWatermarks(watermarkStrategy);
```

### Flink 部署模式

1. **Standalone**: 独立集群
2. **YARN**: Hadoop 资源管理
3. **Kubernetes**: 云原生部署
4. **Flink Session Cluster**: 共享集群
5. **Flink Application Cluster**: 独立应用

### 性能优化

1. **并行度设置**: 根据数据量和资源调整
2. **检查点间隔**: 1-5分钟
3. **状态后端**: RocksDB (大状态), Heap (小状态)
4. **资源配置**: TaskManager 内存、CPU

---

## 📨 Apache Kafka 消息队列

### 核心概念

Kafka 是**分布式流平台**,提供高吞吐、低延迟的消息传递。

#### 核心组件

```
┌─────────────────────────────────────────┐
│           Kafka Cluster                 │
│                                         │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐ │
│  │ Broker1 │  │ Broker2 │  │ Broker3 │ │
│  └─────────┘  └─────────┘  └─────────┘ │
│       │            │            │       │
│  ┌────┴────────────┴────────────┴────┐  │
│  │         ZooKeeper/KRaft          │  │
│  └──────────────────────────────────┘  │
└─────────────────────────────────────────┘
         ↑                    ↓
    Producer              Consumer
```

### Topic 与分区

**Topic**: 消息类别
**Partition**: 并行单元,提高吞吐量

```bash
# 创建 Topic
kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic events \
  --partitions 3 \
  --replication-factor 2
```

### 生产者

```java
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9092");
props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
props.put("acks", "all"); // 可靠性
props.put("retries", 3);
props.put("batch.size", 16384); // 批量大小
props.put("linger.ms", 10); // 延迟时间
props.put("compression.type", "snappy"); // 压缩

KafkaProducer<String, String> producer = new KafkaProducer<>(props);

ProducerRecord<String, String> record = 
    new ProducerRecord<>("events", "key", "value");
    
producer.send(record, (metadata, exception) -> {
    if (exception == null) {
        System.out.println("Offset: " + metadata.offset());
    }
});
```

### 消费者

```java
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9092");
props.put("group.id", "my-group");
props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
props.put("enable.auto.commit", "false"); // 手动提交
props.put("auto.offset.reset", "earliest");

KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
consumer.subscribe(Arrays.asList("events"));

while (true) {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
    for (ConsumerRecord<String, String> record : records) {
        System.out.println("Offset: " + record.offset() + ", Value: " + record.value());
    }
    consumer.commitSync(); // 手动提交
}
```

### 事务消息

精确一次语义 (Exactly-Once):

```java
Properties props = new Properties();
props.put("transactional.id", "my-transactional-id");
props.put("enable.idempotence", "true");

KafkaProducer<String, String> producer = new KafkaProducer<>(props);
producer.initTransactions();

try {
    producer.beginTransaction();
    producer.send(new ProducerRecord<>("topic", "key", "value"));
    producer.commitTransaction();
} catch (Exception e) {
    producer.abortTransaction();
}
```

### Kafka 性能优化

1. **批量大小**: batch.size=16384
2. **压缩**: compression.type=snappy/lz4/zstd
3. **副本因子**: replication-factor=2-3
4. **分区数**: 根据吞吐量调整
5. **消费者并行度**: 分区数 = 消费者数

---

## 🔄 Flink CDC 实时数据同步

### 什么是 CDC?

**CDC** (Change Data Capture) 是一种捕获数据库变更的技术。

#### Flink CDC vs 传统同步

| 特性 | Flink CDC | Sqoop/DataX |
|------|-----------|-------------|
| 类型 | 实时增量 | 离线全量/增量 |
| 延迟 | 毫秒-秒级 | 分钟-小时级 |
| 资源 | 低 | 高 |
| 复杂度 | 简单 | 复杂 |

### 支持的数据源

- **MySQL** (最常用)
- **PostgreSQL**
- **Oracle**
- **MongoDB**
- **SQL Server**
- **Db2**

### MySQL CDC 示例

```scala
import com.ververica.cdc.connectors.mysql.source.MySqlSource
import com.ververica.cdc.connectors.mysql.table.StartupOptions

val mySqlSource = MySqlSource.builder[String]()
  .hostname("localhost")
  .port(3306)
  .databaseList("mydb")
  .tableList("mydb.users", "mydb.orders")
  .username("root")
  .password("password")
  .serverId("5400-5404")
  .startupOptions(StartupOptions.initial()) // 启动选项
  .deserializer(new JsonDebeziumDeserializationSchema())
  .build()

val env = StreamExecutionEnvironment.getExecutionEnvironment
env.enableCheckpointing(60000) // 必须开启检查点

val cdcStream = env.fromSource(
  mySqlSource,
  WatermarkStrategy.noWatermarks(),
  "MySQL CDC Source"
)
```

### 启动选项

1. **initial()**: 初始化快照 + 增量
2. **earliest()**: 从最早 Binlog 开始
3. **latest()**: 只读取新增数据
4. **timestamp(ts)**: 从指定时间开始
5. **specificOffset(offset)**: 从指定位点开始

### CDC 事件格式

```json
{
  "before": null,
  "after": {
    "id": 1,
    "name": "张三",
    "age": 25
  },
  "source": {
    "version": "1.5.0",
    "connector": "mysql",
    "name": "mysql_binlog_source",
    "ts_ms": 1609459200000,
    "db": "test_db",
    "table": "users"
  },
  "op": "c",  // c=insert, u=update, d=delete, r=read
  "ts_ms": 1609459200123
}
```

### 实时数仓架构

```
MySQL → Flink CDC → Kafka → Flink → Iceberg/Hudi → Hive/Trino
                       ↓
                   Redis/HBase (实时查询)
```

---

## 🗄️ Apache Iceberg 数据湖

### 什么是数据湖?

数据湖是一个集中式存储库,可以**存储任意规模的结构化和非结构化数据**。

#### Iceberg vs 传统 Hive

| 特性 | Iceberg | Hive |
|------|---------|------|
| ACID | ✅ 完整支持 | ❌ 不支持 |
| Schema Evolution | ✅ 灵活 | ❌ 复杂 |
| 时间旅行 | ✅ 原生支持 | ❌ 不支持 |
| 隐藏分区 | ✅ 自动裁剪 | ❌ 手动指定 |
| 小文件问题 | ✅ 自动合并 | ❌ 需要手动 |
| 性能 | 更快 | 基准 |

### 核心特性

#### 1. ACID 事务

```sql
-- 支持并发写入
INSERT INTO iceberg_table VALUES (1, 'Alice');
-- 自动处理冲突

-- 支持 UPDATE/DELETE
UPDATE iceberg_table SET name = 'Bob' WHERE id = 1;
DELETE FROM iceberg_table WHERE id = 2;
```

#### 2. Schema Evolution

```sql
-- 添加列
ALTER TABLE users ADD COLUMN address STRING;

-- 删除列
ALTER TABLE users DROP COLUMN age;

-- 重命名列
ALTER TABLE users RENAME COLUMN email TO email_address;
```

**特点**: 旧数据自动适配新 Schema,无需重写数据。

#### 3. 时间旅行

```sql
-- 查询历史快照
SELECT * FROM users FOR SYSTEM_VERSION AS OF 123456789;

-- 查询指定时间的数据
SELECT * FROM users FOR SYSTEM_TIME AS OF '2024-01-01 00:00:00';

-- 增量读取
SELECT * FROM users FOR INCREMENTAL FROM 100 TO 200;
```

#### 4. 隐藏分区

```sql
CREATE TABLE events (
    event_id BIGINT,
    event_time TIMESTAMP
) PARTITIONED BY (year(event_time), month(event_time));

-- 查询时无需指定分区
SELECT * FROM events WHERE event_time >= '2024-01-01';
-- Iceberg 自动进行分区裁剪
```

#### 5. 表维护

```sql
-- 合并小文件
CALL spark.procedures.rewrite_data_files(
    table => 'db.users',
    strategy => 'binpack'
);

-- 过期快照
CALL spark.procedures.expire_snapshots(
    table => 'db.users',
    older_than => TIMESTAMP '2024-01-01 00:00:00',
    retain_last => 5
);

-- 删除孤立文件
CALL spark.procedures.remove_orphan_files(
    table => 'db.users'
);
```

### Iceberg 与其他湖仓对比

| 特性 | Iceberg | Hudi | Delta Lake |
|------|---------|------|------------|
| 开源 | Apache | Apache | Linux Foundation |
| 生态 | 最广泛 | 中等 | Databricks |
| Upsert | Merge | 原生 | Merge |
| 流式写入 | 支持 | 更强 | 支持 |
| 查询引擎 | Spark, Flink, Trino | Spark, Flink | Spark |
| 适用场景 | 通用数据湖 | CDC, 实时写入 | Databricks 生态 |

---

## ⚡ ClickHouse 实时OLAP

### 什么是 OLAP?

**OLAP** (Online Analytical Processing) 是在线分析处理,用于复杂的聚合查询。

#### ClickHouse vs 传统数仓

| 特性 | ClickHouse | Hive | Presto |
|------|------------|------|--------|
| 查询速度 | 极快 (秒级) | 慢 (分钟级) | 快 (秒级) |
| 实时写入 | ✅ 支持 | ❌ 批量 | ❌ 只读 |
| 压缩比 | 10:1 | 3:1 | - |
| 并发 | 高 | 低 | 高 |
| 学习曲线 | 中 | 低 | 中 |

### 核心特性

#### 1. 表引擎

**MergeTree 系列** (最常用):

```sql
-- 基础 MergeTree
CREATE TABLE events (
    event_id UInt64,
    user_id UInt64,
    event_time DateTime
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, user_id);

-- ReplacingMergeTree (去重)
CREATE TABLE users (
    user_id UInt64,
    name String,
    updated_at DateTime
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY user_id;

-- SummingMergeTree (预聚合)
CREATE TABLE metrics (
    date Date,
    metric_name String,
    value UInt64
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (date, metric_name);
```

#### 2. 物化视图

预计算常用查询:

```sql
-- 创建物化视图
CREATE MATERIALIZED VIEW events_daily_mv
ENGINE = SummingMergeTree()
ORDER BY (date, event_type)
AS SELECT
    toDate(event_time) as date,
    event_type,
    count() as event_count
FROM events
GROUP BY date, event_type;

-- 查询自动使用物化视图
SELECT date, sum(event_count) 
FROM events_daily_mv 
GROUP BY date;
```

#### 3. 分布式表

```sql
-- 创建本地表
CREATE TABLE events_local ON CLUSTER my_cluster (
    event_id UInt64,
    event_time DateTime
) ENGINE = MergeTree()
ORDER BY event_time;

-- 创建分布式表
CREATE TABLE events_distributed ON CLUSTER my_cluster
AS events_local
ENGINE = Distributed(my_cluster, default, events_local, rand());

-- 查询分布式表(自动路由到所有节点)
SELECT count() FROM events_distributed;
```

#### 4. 高级查询

```sql
-- 窗口函数
SELECT 
    user_id,
    amount,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY amount DESC) as rank
FROM orders;

-- 数组操作
SELECT 
    groupArray(name) as names,
    arrayJoin([1,2,3]) as num
FROM users;

-- 漏斗分析
SELECT
    windowFunnel(3600)(event_time, 
        event_type = 'page_view',
        event_type = 'add_cart',
        event_type = 'purchase'
    ) as level
FROM events
GROUP BY user_id;
```

### ClickHouse 性能优化

1. **分区**: 按时间分区,避免跨分区查询
2. **主键**: ORDER BY 选择过滤性强的列
3. **压缩**: LZ4 (默认), ZSTD (高压缩比)
4. **物化视图**: 预聚合常用查询
5. **采样**: SAMPLE 0.1 查询10%数据

### 实时 OLAP 架构

```
Kafka → Flink → ClickHouse → Grafana/Superset
                     ↓
                 物化视图 (预聚合)
```

---

## 📊 技术栈对比

### 流处理引擎

| 引擎 | 优势 | 劣势 | 适用场景 |
|------|------|------|----------|
| **Flink** | 真流处理,低延迟,状态管理强 | 学习曲线陡 | 实时计算,状态管理 |
| **Spark Streaming** | 生态成熟,易学 | 微批,延迟高 | 批流统一 |
| **Storm** | 低延迟 | 生态弱,维护少 | 实时告警 |

### 数据湖

| 技术 | 优势 | 劣势 | 适用场景 |
|------|------|------|----------|
| **Iceberg** | 生态广,标准化 | 相对年轻 | 通用数据湖 |
| **Hudi** | Upsert 强,流式好 | 复杂度高 | CDC,实时写入 |
| **Delta Lake** | Spark 深度集成 | 绑定 Databricks | Databricks 用户 |

### OLAP 引擎

| 引擎 | 优势 | 劣势 | 适用场景 |
|------|------|------|----------|
| **ClickHouse** | 极快,压缩好 | 不支持 JOIN | 实时分析 |
| **Doris** | 国产,生态好 | 社区小 | 实时数仓 |
| **Presto/Trino** | 联邦查询 | 只读 | 跨源查询 |
| **Hive** | 成熟,生态广 | 慢 | 离线数仓 |

---

## 🏗️ 架构最佳实践

### 1. Lambda 架构 (批流分离)

```
数据源
  ├── 批处理层: HDFS → Spark → Hive → Presto
  └── 流处理层: Kafka → Flink → Redis/HBase
             ↓
         合并查询层
```

**优点**: 批流解耦,稳定
**缺点**: 两套代码,维护成本高

### 2. Kappa 架构 (纯流处理)

```
数据源 → Kafka → Flink → Iceberg/Hudi → Trino
                   ↓
               ClickHouse (实时)
```

**优点**: 一套代码,实时性好
**缺点**: 对流处理引擎要求高

### 3. 湖仓一体架构 (推荐)

```
数据源 → Kafka → Flink CDC → Iceberg → Spark/Trino → BI
                        ↓
                    ClickHouse (实时指标)
```

**优点**: 统一存储,ACID 支持,批流一体
**技术栈**: Flink + Kafka + Iceberg + ClickHouse

### 技术选型建议

| 场景 | 推荐技术栈 |
|------|------------|
| **实时数仓** | Flink + Kafka + Iceberg + Trino |
| **实时大屏** | Flink + Kafka + ClickHouse + Grafana |
| **数据湖** | Iceberg + Spark + Hive |
| **日志分析** | Filebeat + Kafka + Flink + Elasticsearch |
| **CDC 同步** | Flink CDC + Kafka + Iceberg |

---

## 🎓 学习路径

### 初学者 (0-3个月)
1. Hadoop 生态基础
2. Spark 批处理
3. Hive SQL

### 进阶 (3-6个月)
4. Flink 流处理
5. Kafka 消息队列
6. HBase NoSQL

### 高级 (6-12个月)
7. Flink CDC 实时同步
8. Iceberg 数据湖
9. ClickHouse 实时 OLAP
10. 数据治理与优化

---

## 📚 参考资源

### 官方文档
- [Flink 官方文档](https://flink.apache.org)
- [Kafka 官方文档](https://kafka.apache.org)
- [Iceberg 官方文档](https://iceberg.apache.org)
- [ClickHouse 官方文档](https://clickhouse.com)

### 推荐书籍
- 《Flink 实战》
- 《Kafka 权威指南》
- 《数据密集型应用系统设计》(DDIA)
- 《ClickHouse 原理解析与应用实践》

### 在线课程
- 尚硅谷大数据系列
- 黑马程序员大数据课程
- Coursera Big Data Specialization

---

## 💡 常见问题

### Q1: Flink 和 Spark Streaming 如何选择?

**选 Flink**:
- 需要毫秒级延迟
- 复杂状态管理
- 事件时间处理

**选 Spark Streaming**:
- 批流统一
- 团队熟悉 Spark
- 生态成熟度要求高

### Q2: 数据湖选 Iceberg 还是 Hudi?

**选 Iceberg**:
- 通用数据湖场景
- 需要广泛的生态支持
- Schema Evolution 需求

**选 Hudi**:
- 大量 CDC 数据
- 频繁 Upsert 操作
- 流式写入为主

### Q3: 实时 OLAP 为什么选 ClickHouse?

- **查询速度**: 比 Hive 快 10-100 倍
- **实时写入**: 支持秒级数据写入
- **成本低**: 压缩比高,硬件成本低
- **易用性**: SQL 接口,学习成本低

---

## 🚀 总结

现代大数据技术栈的核心是:

1. **Flink**: 实时流处理引擎
2. **Kafka**: 数据总线
3. **Iceberg**: 数据湖存储
4. **ClickHouse**: 实时 OLAP

掌握这些技术,你就能构建**高性能、低延迟、易维护**的现代数据平台! 🎉
