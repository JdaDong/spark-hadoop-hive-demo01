# 🚀 现代大数据技术栈完整项目

这是一个涵盖**传统大数据**和**现代大数据**技术栈的综合项目,包含 Java 和 Scala 两种语言实现。

## 📋 项目概述

本项目演示了大数据生态系统中**核心组件**和**前沿技术**的集成使用:

### 传统大数据技术栈
- **Apache Spark**: 分布式计算引擎,批处理
- **Apache Hadoop HDFS**: 分布式文件系统
- **Apache Hive**: 数据仓库,SQL 查询
- **Apache HBase**: NoSQL 列式数据库
- **Apache Phoenix**: HBase SQL 层

### 现代大数据技术栈 ⭐ NEW!
- **Apache Flink**: 真正的流处理引擎
- **Apache Kafka**: 分布式消息队列
- **Flink CDC**: 实时数据变更捕获
- **Apache Iceberg**: 数据湖表格式
- **ClickHouse**: 实时 OLAP 分析引擎

## 🏗️ 项目结构

```
spark-hadoop-hive-demo01/
├── pom.xml                          # Maven 构建配置(已更新新依赖)
├── README.md                        # 项目说明
├── TECH_STACK_GUIDE.md             # 🆕 现代技术栈完整指南
├── HDFS_GUIDE.md                   # HDFS 高级特性指南
├── HIVE_GUIDE.md                   # Hive 高级特性指南
├── PHOENIX_GUIDE.md                # Phoenix 使用指南
├── COPROCESSOR_GUIDE.md            # HBase 协处理器指南
├── src/
│   ├── main/
│   │   ├── java/com/bigdata/
│   │   │   ├── spark/
│   │   │   │   └── SparkHiveJavaApp.java     # Spark+Hive Java实现
│   │   │   ├── hbase/
│   │   │   │   ├── HBaseJavaApp.java         # HBase Java基础
│   │   │   │   ├── HBaseAdvancedJavaApp.java # HBase Java高级
│   │   │   │   └── coprocessor/              # HBase 协处理器
│   │   │   ├── phoenix/
│   │   │   │   └── PhoenixJavaApp.java       # Phoenix Java实现
│   │   │   ├── hive/
│   │   │   │   └── HiveAdvancedJavaApp.java  # Hive 高级特性
│   │   │   ├── hdfs/
│   │   │   │   ├── HDFSJavaApp.java          # HDFS Java实现
│   │   │   │   └── HDFSAdvancedJavaApp.java  # HDFS 高级特性
│   │   │   ├── flink/                        # 🆕 Flink 流处理
│   │   │   │   └── FlinkStreamingJavaApp.java
│   │   │   ├── kafka/                        # 🆕 Kafka 消息队列
│   │   │   │   └── KafkaJavaApp.java
│   │   │   ├── iceberg/                      # 🆕 Iceberg 数据湖
│   │   │   │   └── IcebergJavaApp.java
│   │   │   └── clickhouse/                   # 🆕 ClickHouse OLAP
│   │   │       └── ClickHouseJavaApp.java
│   │   ├── scala/com/bigdata/
│   │   │   ├── spark/
│   │   │   │   └── SparkHiveApplication.scala # Spark+Hive Scala实现
│   │   │   ├── hbase/
│   │   │   │   ├── HBaseScalaApp.scala       # HBase Scala基础
│   │   │   │   └── HBaseAdvancedScalaApp.scala
│   │   │   ├── phoenix/
│   │   │   │   └── PhoenixScalaApp.scala     # Phoenix Scala实现
│   │   │   ├── hive/
│   │   │   │   └── HiveAdvancedScalaApp.scala # Hive 高级特性
│   │   │   ├── hdfs/
│   │   │   │   ├── HDFSScalaApp.scala        # HDFS Scala实现
│   │   │   │   └── HDFSAdvancedScalaApp.scala
│   │   │   └── flink/                        # 🆕 Flink Scala
│   │   │       ├── FlinkStreamingScalaApp.scala
│   │   │       └── FlinkCDCApp.scala         # Flink CDC
│   │   └── resources/
│   │       ├── application.properties         # 应用配置
│   │       ├── log4j.properties              # 日志配置
│   │       ├── core-site.xml                 # Hadoop核心配置
│   │       ├── hdfs-site.xml                 # HDFS配置
│   │       └── hbase-site.xml                # HBase配置
│   └── test/
└── scripts/                                   # 启动脚本
```

## 🚀 功能特性

### Spark + Hive 模块
- ✅ 创建和管理 Hive 数据库
- ✅ 表的 CRUD 操作
- ✅ 复杂 SQL 查询和聚合
- ✅ 窗口函数和排名
- ✅ 分区表操作
- ✅ DataFrame API 操作
- ✅ 多表 JOIN 操作

### HBase 模块
- ✅ 表的创建和管理
- ✅ 数据的增删改查
- ✅ 批量操作
- ✅ 过滤器查询
- ✅ 范围扫描
- ✅ 多版本数据处理
- ✅ 条件更新

### HBase 高级特性
- ✅ 表预分区(Pre-splitting)
- ✅ 复杂过滤器组合
- ✅ 计数器(原子增减)
- ✅ CAS 操作(Compare And Set)
- ✅ 批量操作优化(BufferedMutator)
- ✅ 前缀和模糊行过滤
- ✅ 列分页查询
- ✅ Bloom Filter 配置
- ✅ 压缩算法优化
- ✅ TTL(Time To Live)设置
- ✅ 扫描优化技巧
- ✅ 多版本数据查询
- ✅ 行锁和并发控制
- ✅ 协处理器(Coprocessor)概念
- ✅ 二级索引实现思路
- ✅ 热点问题优化
- ✅ 性能监控和调优

### HBase 协处理器(Coprocessor)
- ✅ Observer 协处理器 - 审计日志
- ✅ Endpoint 协处理器 - 服务端聚合
- ✅ 静态和动态部署
- ✅ 从 HDFS 加载协处理器
- ✅ 协处理器管理工具

### Apache Phoenix(HBase SQL层)
- ✅ 标准 SQL 查询(SELECT、JOIN、GROUP BY)
- ✅ 二级索引支持
- ✅ 视图和序列
- ✅ 事务支持
- ✅ JDBC 驱动集成
- ✅ 窗口函数和聚合
- ✅ 数组类型操作
- ✅ 动态列支持
- ✅ 与 Spark 集成
- ✅ 性能优化(Salting、预分区、统计信息)

### HDFS 高级特性
- ✅ 快照管理(Snapshot)
- ✅ 配额管理(Quota)
- ✅ 文件压缩(Gzip, Snappy, LZ4)
- ✅ 小文件合并
- ✅ 副本因子动态调整
- ✅ 块信息查询与分析
- ✅ DataNode 管理
- ✅ 存储策略管理(HOT, WARM, COLD)
- ✅ 垃圾回收机制(Trash)
- ✅ ACL 权限管理
- ✅ 缓存管理(Cache Pool)
- ✅ 文件校验和验证
- ✅ 异构存储支持

### Hive 高级特性
- ✅ 分区表管理(静态/动态分区)
- ✅ 分桶表(Bucketing)
- ✅ 复杂数据类型(Array, Map, Struct)
- ✅ 窗口函数(ROW_NUMBER, RANK, LEAD, LAG)
- ✅ UDF/UDAF/UDTF
- ✅ 事务表(ACID - INSERT/UPDATE/DELETE)
- ✅ 表优化(Compaction)
- ✅ 视图和物化视图
- ✅ CTE(公共表表达式)
- ✅ GROUPING SETS
- ✅ 查询优化(CBO, 谓词下推, 向量化)
- ✅ Map-side JOIN
- ✅ 并行执行
- ✅ 小文件合并
- ✅ 数据倾斜处理

---

## ⭐ 现代大数据技术栈 (NEW!)

### Apache Flink 流处理
- ✅ DataStream API (Java + Scala 双实现)
- ✅ 窗口计算 (滚动、滑动、会话窗口)
- ✅ 状态管理 (ValueState, ListState, MapState)
- ✅ 水位线与事件时间处理
- ✅ Kafka 集成
- ✅ 侧输出流 (Side Output)
- ✅ 复杂事件处理 (CEP)
- ✅ 容错与检查点
- ✅ 精确一次语义 (Exactly-Once)
- ✅ 函数式编程风格 (Scala)

### Apache Kafka 消息队列
- ✅ Topic 管理 (创建、删除、查询、修改配置)
- ✅ 生产者 (同步/异步发送、批量发送)
- ✅ 消费者 (手动/自动提交偏移量)
- ✅ 消费者组管理
- ✅ 分区与副本管理
- ✅ 事务消息 (Exactly-Once 语义)
- ✅ 拦截器 (ProducerInterceptor)
- ✅ 自定义分区器
- ✅ 性能优化配置

### Flink CDC 实时数据同步
- ✅ MySQL Binlog 实时订阅
- ✅ 增量数据捕获 (CDC)
- ✅ 数据转换与过滤
- ✅ 多表同步
- ✅ Schema Evolution
- ✅ 实时写入 Kafka/Iceberg
- ✅ 数据脱敏与路由
- ✅ 断点续传

### Apache Iceberg 数据湖
- ✅ 创建 Iceberg 表 (Schema + Partition)
- ✅ ACID 事务 (插入、更新、删除)
- ✅ Schema Evolution (添加/删除/重命名列)
- ✅ 分区管理 (隐藏分区、分区演化)
- ✅ 时间旅行 (查询历史快照)
- ✅ 快照管理 (创建、回滚、过期)
- ✅ 增量读取 (Incremental Read)
- ✅ 表维护 (Compact, Expire Snapshots)
- ✅ 并发控制 (乐观锁)

### ClickHouse 实时 OLAP
- ✅ 表引擎 (MergeTree, ReplicatedMergeTree, SummingMergeTree)
- ✅ 分布式表 (Distributed)
- ✅ 物化视图 (Materialized View)
- ✅ 批量写入优化
- ✅ OLAP 查询 (聚合、窗口函数、漏斗分析)
- ✅ 数据压缩 (LZ4, ZSTD)
- ✅ 分区管理 (按时间分区)
- ✅ 副本与分片
- ✅ 性能优化

---

### HDFS 模块
- ✅ 文件上传和下载
- ✅ 目录操作
- ✅ 文件读写和追加
- ✅ 文件重命名和删除
- ✅ 获取文件状态和块信息
- ✅ 文件合并
- ✅ 并行文件处理

## 🔧 环境要求

### 基础环境
- **Java**: JDK 1.8+
- **Scala**: 2.12.15
- **Maven**: 3.6+

### 传统大数据组件
- **Hadoop**: 3.3.1
- **Spark**: 3.2.1
- **HBase**: 2.4.9
- **Hive**: 3.1.2
- **Phoenix**: 5.1.2

### 现代大数据组件 ⭐ NEW!
- **Flink**: 1.17.1
- **Kafka**: 3.4.0
- **Flink CDC**: 2.4.1
- **Iceberg**: 1.4.2
- **ClickHouse**: 0.5.0 (JDBC)

## 📦 安装步骤

### 1. 克隆项目

```bash
git clone <repository-url>
cd spark-hadoop-hive-demo01
```

### 2. 配置 Hadoop 环境

```bash
# 设置 HADOOP_HOME 环境变量
export HADOOP_HOME=/usr/local/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# 格式化 NameNode (首次运行)
hdfs namenode -format

# 启动 HDFS
start-dfs.sh
```

### 3. 配置 HBase 环境

```bash
# 设置 HBASE_HOME 环境变量
export HBASE_HOME=/usr/local/hbase
export PATH=$PATH:$HBASE_HOME/bin

# 启动 HBase
start-hbase.sh
```

### 4. 配置 Hive 环境

```bash
# 设置 HIVE_HOME 环境变量
export HIVE_HOME=/usr/local/hive
export PATH=$PATH:$HIVE_HOME/bin

# 初始化 Hive 元数据库 (首次运行)
schematool -dbType derby -initSchema

# 启动 Hive Metastore
hive --service metastore &
```

### 5. 配置 Phoenix 环境

```bash
# 设置 PHOENIX_HOME 环境变量
export PHOENIX_HOME=/usr/local/phoenix
export PATH=$PATH:$PHOENIX_HOME/bin

# 将 Phoenix 服务端 JAR 拷贝到 HBase lib 目录
cp $PHOENIX_HOME/phoenix-server-hbase-2.4-*.jar $HBASE_HOME/lib/

# 重启 HBase
stop-hbase.sh
start-hbase.sh

# 测试 Phoenix 连接
sqlline.py localhost:2181
```

### 6. 编译项目

```bash
mvn clean package
```

## 🎯 运行示例

### 运行 Spark + Hive 应用

**Java 版本:**
```bash
spark-submit \
  --class com.bigdata.spark.SparkHiveJavaApp \
  --master local[*] \
  target/spark-hadoop-hive-demo-1.0.0-jar-with-dependencies.jar
```

**Scala 版本:**
```bash
spark-submit \
  --class com.bigdata.spark.SparkHiveApplication \
  --master local[*] \
  target/spark-hadoop-hive-demo-1.0.0-jar-with-dependencies.jar
```

### 运行 HBase 应用

**Java 版本:**
```bash
java -cp target/spark-hadoop-hive-demo-1.0.0-jar-with-dependencies.jar \
  com.bigdata.hbase.HBaseJavaApp
```

**Scala 版本:**
```bash
java -cp target/spark-hadoop-hive-demo-1.0.0-jar-with-dependencies.jar \
  com.bigdata.hbase.HBaseScalaApp
```

### 运行 HDFS 应用

**Java 版本:**
```bash
java -cp target/spark-hadoop-hive-demo-1.0.0-jar-with-dependencies.jar \
  com.bigdata.hdfs.HDFSJavaApp
```

**Scala 版本:**
```bash
java -cp target/spark-hadoop-hive-demo-1.0.0-jar-with-dependencies.jar \
  com.bigdata.hdfs.HDFSScalaApp
```

## 🧪 在本地模式运行

如果你想在本地开发环境测试,可以直接使用 Maven 运行:

```bash
# 运行 Spark Scala 应用
mvn exec:java -Dexec.mainClass="com.bigdata.spark.SparkHiveApplication"

# 运行 HBase Scala 应用
mvn exec:java -Dexec.mainClass="com.bigdata.hbase.HBaseScalaApp"

# 运行 HBase 高级特性应用
mvn exec:java -Dexec.mainClass="com.bigdata.hbase.HBaseAdvancedScalaApp"

# 运行 Phoenix 应用
mvn exec:java -Dexec.mainClass="com.bigdata.phoenix.PhoenixJavaApp"
mvn exec:java -Dexec.mainClass="com.bigdata.phoenix.PhoenixScalaApp"

# 运行协处理器部署工具
mvn exec:java -Dexec.mainClass="com.bigdata.hbase.coprocessor.CoprocessorDeployment"

# 运行 HDFS 应用
mvn exec:java -Dexec.mainClass="com.bigdata.hdfs.HDFSScalaApp"
mvn exec:java -Dexec.mainClass="com.bigdata.hdfs.HDFSJavaApp"

# 运行 HDFS 高级特性应用
mvn exec:java -Dexec.mainClass="com.bigdata.hdfs.HDFSAdvancedJavaApp"
mvn exec:java -Dexec.mainClass="com.bigdata.hdfs.HDFSAdvancedScalaApp"

# 运行 Hive 高级特性应用
mvn exec:java -Dexec.mainClass="com.bigdata.hive.HiveAdvancedJavaApp"
mvn exec:java -Dexec.mainClass="com.bigdata.hive.HiveAdvancedScalaApp"

# ========== 现代技术栈 ⭐ NEW! ==========

# 运行 Flink 流处理应用
mvn exec:java -Dexec.mainClass="com.bigdata.flink.FlinkStreamingJavaApp"
mvn exec:java -Dexec.mainClass="com.bigdata.flink.FlinkStreamingScalaApp"

# 运行 Kafka 应用
mvn exec:java -Dexec.mainClass="com.bigdata.kafka.KafkaJavaApp"

# 运行 Flink CDC 实时同步应用
mvn exec:java -Dexec.mainClass="com.bigdata.flink.FlinkCDCApp"

# 运行 Iceberg 数据湖应用
mvn exec:java -Dexec.mainClass="com.bigdata.iceberg.IcebergJavaApp"

# 运行 ClickHouse 应用
mvn exec:java -Dexec.mainClass="com.bigdata.clickhouse.ClickHouseJavaApp"
```

## 📊 数据流程示例

1. **数据摄入**: 将原始数据文件上传到 HDFS
2. **数据处理**: 使用 Spark 读取 HDFS 数据并进行 ETL 处理
3. **数据存储**: 将处理后的数据写入 Hive 表或 HBase 表
4. **数据分析**: 使用 Spark SQL 查询 Hive 表进行分析
5. **实时查询**: 使用 HBase 进行实时数据读写

## 🛠️ 配置说明

### application.properties
主要应用配置文件,包含:
- Hadoop/HDFS 连接信息
- Spark 运行参数
- Hive Metastore 配置
- HBase 连接信息
- Zookeeper 配置

### XML 配置文件
- `core-site.xml`: Hadoop 核心配置
- `hdfs-site.xml`: HDFS 特定配置
- `hbase-site.xml`: HBase 特定配置

## 📝 开发建议

1. **本地开发**: 使用 `local[*]` 模式进行本地测试
2. **日志级别**: 开发时设置为 DEBUG,生产环境使用 INFO 或 WARN
3. **资源管理**: 注意关闭所有的数据库连接和文件流
4. **错误处理**: 使用 try-catch-finally 确保资源正确释放
5. **性能优化**: 
   - 使用批量操作减少网络开销
   - 合理设置 Spark 并行度
   - 使用分区表提高查询效率

## 🐛 常见问题

### 1. 连接超时
确保 Hadoop、HBase 和 Zookeeper 服务都已启动。

### 2. 权限问题
检查 HDFS 目录权限: `hdfs dfs -chmod -R 777 /user`

### 3. 内存不足
调整 Spark executor 和 driver 内存设置。

### 4. Hive Metastore 连接失败
确保 Metastore 服务已启动,检查端口 9083 是否可访问。

## 📚 参考文档

- [Apache Spark 官方文档](https://spark.apache.org/docs/latest/)
- [Apache Hadoop 官方文档](https://hadoop.apache.org/docs/current/)
- [Apache Hive 官方文档](https://hive.apache.org/)
- [Apache HBase 官方文档](https://hbase.apache.org/)

---

## 🔥 HBase 高级特性详解

### 1. 表预分区(Pre-splitting)

**为什么需要预分区?**
- 默认创建的表只有一个 Region,所有写入会集中在一个 RegionServer
- 预分区可以在创建表时就分散数据,避免热点问题

**示例:**
```java
// 定义分区键
byte[][] splitKeys = new byte[][] {
    Bytes.toBytes("row100"),
    Bytes.toBytes("row200"),
    Bytes.toBytes("row300")
};

admin.createTable(tableDescriptor, splitKeys);
```

### 2. 复杂过滤器(Filters)

**常用过滤器类型:**
- `PrefixFilter`: 前缀匹配
- `ColumnPrefixFilter`: 列前缀匹配
- `SingleColumnValueFilter`: 单列值过滤
- `FuzzyRowFilter`: 模糊行过滤
- `PageFilter`: 分页过滤
- `FirstKeyOnlyFilter`: 只返回每行第一列
- `KeyOnlyFilter`: 只返回键不返回值
- `RandomRowFilter`: 随机采样

**过滤器组合:**
```java
FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
filterList.addFilter(new PrefixFilter(Bytes.toBytes("user")));
filterList.addFilter(new SingleColumnValueFilter(...));
```

### 3. 原子操作

**计数器(Counter):**
```java
// 原子增加
long newValue = table.incrementColumnValue(
    rowKey, family, qualifier, amount);

// 使用 Increment API
Increment increment = new Increment(rowKey);
increment.addColumn(family, qualifier, 10L);
table.increment(increment);
```

**CAS 操作(Compare And Set):**
```java
// 只有当值等于预期值时才更新
boolean success = table.checkAndMutate(rowKey, family)
    .qualifier(qualifier)
    .ifEquals(expectedValue)
    .thenPut(newPut);
```

### 4. 批量操作优化

**BufferedMutator:**
```java
BufferedMutatorParams params = new BufferedMutatorParams(tableName)
    .writeBufferSize(5 * 1024 * 1024);  // 5MB buffer

BufferedMutator mutator = connection.getBufferedMutator(params);
// 批量插入...
mutator.flush();
```

**批量读取:**
```java
List<Get> gets = Arrays.asList(...);
Result[] results = table.get(gets);
```

### 5. 扫描优化

**设置缓存和批次:**
```java
Scan scan = new Scan();
scan.setCaching(1000);        // 每次RPC获取的行数
scan.setBatch(10);            // 每行返回的列数
scan.setMaxResultSize(1024 * 1024);  // 最大结果大小
```

**限制扫描范围:**
```java
scan.withStartRow(startRow);
scan.withStopRow(stopRow);
scan.addColumn(family, qualifier);  // 只获取特定列
```

### 6. 列族配置优化

**压缩算法:**
- `NONE`: 无压缩
- `SNAPPY`: 快速压缩,适合大多数场景(推荐)
- `GZ`: 高压缩率,CPU 消耗高
- `LZ4`: 非常快,压缩率一般
- `ZSTD`: 最佳压缩率,速度适中

**Bloom Filter:**
- `NONE`: 不使用
- `ROW`: 行级别(默认,推荐)
- `ROWCOL`: 行+列级别(查询特定列时更高效)

**配置示例:**
```java
ColumnFamilyDescriptor cf = ColumnFamilyDescriptorBuilder
    .newBuilder(Bytes.toBytes("cf"))
    .setMaxVersions(5)                    // 保留版本数
    .setTimeToLive(86400)                 // TTL: 1天
    .setCompressionType(Compression.Algorithm.SNAPPY)
    .setBloomFilterType(BloomType.ROW)
    .setBlocksize(64 * 1024)              // Block 大小
    .setInMemory(false)                   // 是否常驻内存
    .setBlockCacheEnabled(true)           // 启用 Block Cache
    .build();
```

### 7. RowKey 设计原则

**避免热点的设计:**

**❌ 错误示例:**
```
timestamp_userId        // 顺序写入,导致热点
userId_timestamp        // userId 分布不均
```

**✅ 正确示例:**
```java
// 1. Salting (加盐)
String saltedKey = (userId.hashCode() % 10) + "_" + userId + "_" + timestamp;

// 2. Reversing (反转)
String reversedKey = reverse(timestamp) + "_" + userId;

// 3. Hashing (哈希前缀)
String hashedKey = md5(userId).substring(0, 4) + "_" + userId + "_" + timestamp;
```

### 8. 二级索引实现

**方案1: 索引表**
```
主表: userId -> userData
索引表: email -> userId

查询流程:
1. 索引表查询: email -> userId
2. 主表查询: userId -> userData
```

**方案2: 使用 Apache Phoenix**
```sql
CREATE INDEX idx_email ON users(email);
```

### 9. 协处理器(Coprocessor)

**Observer Coprocessor:**
- 类似数据库触发器
- 在操作前后执行自定义逻辑
- 用途: 审计日志、权限控制、数据验证

**Endpoint Coprocessor:**
- 类似存储过程
- 服务端聚合计算
- 用途: 减少数据传输,服务端计算

### 10. 性能调优清单

**客户端优化:**
- ✅ 使用 BufferedMutator 批量写入
- ✅ 合理设置 Scan 的 caching 和 batch
- ✅ 指定需要的列,避免全表扫描
- ✅ 使用过滤器减少数据传输

**表设计优化:**
- ✅ 合理设计 RowKey 避免热点
- ✅ 列族数量 1-3 个为宜
- ✅ 启用压缩(SNAPPY 推荐)
- ✅ 配置合适的 TTL
- ✅ 启用 Bloom Filter

**Region 管理:**
- ✅ 预分区大表
- ✅ Region 大小控制在 10-50GB
- ✅ 监控 Region 分布
- ✅ 避免 Region 过多或过少

**内存配置:**
- ✅ MemStore: 每个 Region 的写缓存
- ✅ BlockCache: 读缓存(默认 40% heap)
- ✅ 考虑使用 Off-heap BlockCache

**监控指标:**
- ✅ 请求延迟(P50, P99)
- ✅ Region 数量和分布
- ✅ Compaction 队列长度
- ✅ BlockCache 命中率
- ✅ MemStore 大小

---

## 📄 许可证

MIT License

## 👥 贡献

欢迎提交 Issue 和 Pull Request!

## 📧 联系方式

如有问题,请联系项目维护者。

---

## 🔧 HBase 协处理器详解

### 什么是协处理器?

HBase 协处理器(Coprocessor)类似于关系型数据库中的触发器和存储过程,允许在 HBase 服务端执行自定义代码。

### 协处理器类型

#### 1. **Observer Coprocessor(观察者)**
- 类似数据库触发器
- 在特定事件发生时执行代码
- 用途:
  - 审计日志
  - 数据验证
  - 权限控制
  - 自动备份

**示例: 审计日志**
```java
public class AuditLogObserver implements RegionObserver {
    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> c, 
                       Put put, WALEdit edit, Durability durability) {
        // 记录操作日志
        LOG.info("User inserting data: " + Bytes.toString(put.getRow()));
        
        // 数据验证
        if (invalidData(put)) {
            throw new IOException("Invalid data");
        }
    }
}
```

#### 2. **Endpoint Coprocessor(终端)**
- 类似数据库存储过程
- 在服务端进行计算,减少网络传输
- 用途:
  - 行数统计
  - 求和、平均值
  - 复杂聚合计算
  - 自定义查询逻辑

**示例: 服务端聚合**
```java
public class RowCountEndpoint {
    public long getRowCount(Scan scan) throws IOException {
        long count = 0;
        InternalScanner scanner = env.getRegion().getScanner(scan);
        // 在服务端统计,避免传输所有数据
        while (scanner.next(results)) {
            count++;
        }
        return count;
    }
}
```

### 协处理器部署方式

#### 1. **静态部署(全局)**
在 `hbase-site.xml` 配置:
```xml
<property>
  <name>hbase.coprocessor.region.classes</name>
  <value>com.bigdata.hbase.coprocessor.AuditLogObserver</value>
</property>
```

#### 2. **动态部署(表级别)**
通过 Java API:
```java
TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
builder.setCoprocessor("com.bigdata.hbase.coprocessor.AuditLogObserver");
admin.modifyTable(builder.build());
```

#### 3. **从 HDFS 加载**
```bash
# 上传 JAR 到 HDFS
hdfs dfs -put coprocessor.jar /hbase/coprocessors/

# HBase Shell 配置
disable 'user_table'
alter 'user_table', METHOD => 'table_att', 
  'coprocessor' => 'hdfs://localhost:9000/hbase/coprocessors/coprocessor.jar|
                    com.bigdata.hbase.coprocessor.AuditLogObserver|1001'
enable 'user_table'
```

### 协处理器应用场景

| 场景 | 类型 | 说明 |
|------|------|------|
| 审计日志 | Observer | 记录所有数据变更 |
| 数据验证 | Observer | 插入前验证数据格式 |
| 权限控制 | Observer | 检查用户访问权限 |
| 自动备份 | Observer | 数据变更时自动备份 |
| 行数统计 | Endpoint | 服务端统计避免全表扫描 |
| 求和聚合 | Endpoint | 服务端计算减少数据传输 |
| 二级索引 | Observer | 自动维护索引表 |

### 协处理器性能考虑

✅ **最佳实践:**
- 避免在协处理器中执行耗时操作
- 不要在协处理器中访问外部系统
- 合理使用批量操作
- 考虑使用异步处理

❌ **避免:**
- 阻塞主线程
- 频繁的网络 I/O
- 大量的日志输出
- 内存泄漏

---

## 🐦 Apache Phoenix 详解

### 什么是 Phoenix?

Apache Phoenix 是构建在 HBase 之上的 SQL 层,将 HBase 的 NoSQL 能力与 SQL 的易用性结合在一起。

### Phoenix 的优势

#### 1. **标准 SQL 支持**
```sql
-- 完整的 SQL 语法
SELECT u.name, COUNT(o.order_id) as order_count, SUM(o.amount) as total
FROM users u
LEFT JOIN orders o ON u.user_id = o.user_id
GROUP BY u.name
HAVING total > 1000
ORDER BY total DESC;
```

#### 2. **性能优异**
- 查询性能优于直接使用 HBase API
- 编译 SQL 为原生 HBase 扫描
- 自动查询优化
- 支持查询并行化

#### 3. **二级索引**
```sql
-- 创建索引
CREATE INDEX idx_email ON users(email);

-- 自动使用索引
SELECT * FROM users WHERE email = 'test@example.com';
```

#### 4. **JDBC 驱动**
```java
Connection conn = DriverManager.getConnection("jdbc:phoenix:localhost:2181");
PreparedStatement stmt = conn.prepareStatement("SELECT * FROM users WHERE age > ?");
stmt.setInt(1, 25);
ResultSet rs = stmt.executeQuery();
```

### Phoenix 核心特性

#### 1. **数据类型支持**
- 基本类型: VARCHAR, INTEGER, BIGINT, DOUBLE, BOOLEAN
- 日期时间: DATE, TIME, TIMESTAMP
- 二进制: BINARY, VARBINARY
- 数组: INTEGER ARRAY, VARCHAR ARRAY
- JSON(Phoenix 5.1+)

#### 2. **索引类型**

**全局索引(Global Index)**
```sql
CREATE INDEX idx_global ON users(city);
```
- 适合读多写少场景
- 写入时同步更新索引
- 查询快,写入慢

**覆盖索引(Covered Index)**
```sql
CREATE INDEX idx_covered ON users(city) INCLUDE(name, age);
```
- 索引包含查询所需的所有列
- 避免回表查询
- 最佳查询性能

**本地索引(Local Index)**
```sql
CREATE LOCAL INDEX idx_local ON users(email);
```
- 索引与数据在同一 Region
- 适合写多读少场景
- 写入快,查询相对慢

#### 3. **视图(View)**
```sql
-- 创建视图
CREATE VIEW high_value_users AS
SELECT * FROM users WHERE salary > 50000;

-- 查询视图
SELECT * FROM high_value_users;
```

#### 4. **序列(Sequence)**
```sql
-- 创建序列
CREATE SEQUENCE user_seq START WITH 1000 INCREMENT BY 1;

-- 使用序列
UPSERT INTO users VALUES (NEXT VALUE FOR user_seq, 'John', 'john@example.com');
```

#### 5. **事务支持**
```sql
-- 创建事务表
CREATE TABLE account (
    account_id VARCHAR PRIMARY KEY,
    balance DOUBLE
) TRANSACTIONAL=true;

-- 事务操作
BEGIN;
UPSERT INTO account VALUES ('A001', 1000);
UPSERT INTO account VALUES ('A002', 2000);
COMMIT;
```

#### 6. **窗口函数**
```sql
SELECT 
    name,
    city,
    salary,
    ROW_NUMBER() OVER (PARTITION BY city ORDER BY salary DESC) as rank,
    AVG(salary) OVER (PARTITION BY city) as avg_salary
FROM users;
```

### Phoenix 性能优化

#### 1. **表预分区(Salting)**
```sql
CREATE TABLE users (
    user_id VARCHAR PRIMARY KEY,
    name VARCHAR,
    age INTEGER
) SALT_BUCKETS=10;
```

#### 2. **手动预分区**
```sql
CREATE TABLE users (
    user_id VARCHAR PRIMARY KEY,
    name VARCHAR
) SPLIT ON ('A', 'M', 'Z');
```

#### 3. **压缩**
```sql
CREATE TABLE users (
    user_id VARCHAR PRIMARY KEY,
    name VARCHAR
) COMPRESSION='SNAPPY';
```

#### 4. **TTL(Time To Live)**
```sql
CREATE TABLE logs (
    log_id VARCHAR PRIMARY KEY,
    message VARCHAR
) TTL=86400;  -- 1天后过期
```

#### 5. **统计信息**
```sql
-- 更新统计信息以优化查询
UPDATE STATISTICS users;

-- 查看执行计划
EXPLAIN SELECT * FROM users WHERE city = 'Beijing';
```

#### 6. **查询 Hint**
```sql
-- 强制使用索引
SELECT /*+ INDEX(users idx_city) */ * FROM users WHERE city = 'Beijing';

-- 强制不使用索引
SELECT /*+ NO_INDEX */ * FROM users WHERE city = 'Beijing';

-- 使用 Sort-Merge Join
SELECT /*+ USE_SORT_MERGE_JOIN */ * FROM users u JOIN orders o ON u.user_id = o.user_id;
```

### Phoenix vs 直接使用 HBase

| 特性 | Phoenix | HBase API |
|------|---------|-----------|
| 易用性 | SQL,易学易用 | Java API,学习曲线陡 |
| 查询性能 | 优化后更快 | 需手动优化 |
| 二级索引 | 原生支持 | 需手动实现 |
| 事务支持 | 支持 | 行级事务 |
| 聚合查询 | 高效 | 需自己实现 |
| 灵活性 | 受 SQL 限制 | 完全控制 |

### Phoenix 与 Spark 集成

```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("Phoenix Integration")
  .getOrCreate()

// 读取 Phoenix 表
val df = spark.read
  .format("org.apache.phoenix.spark")
  .option("table", "users")
  .option("zkUrl", "localhost:2181")
  .load()

df.show()

// 使用 Spark SQL 查询
df.createOrReplaceTempView("users")
spark.sql("SELECT city, COUNT(*) FROM users GROUP BY city").show()

// 写入 Phoenix 表
df.write
  .format("org.apache.phoenix.spark")
  .option("table", "users_backup")
  .option("zkUrl", "localhost:2181")
  .mode("overwrite")
  .save()
```

### Phoenix 使用场景

✅ **适合使用 Phoenix:**
- 需要 SQL 查询 HBase 数据
- 复杂的多表 JOIN 和聚合
- 需要二级索引
- BI 工具集成(通过 JDBC)
- 快速原型开发

❌ **不适合使用 Phoenix:**
- 极致性能要求(直接用 HBase API)
- 非结构化数据
- 需要完全控制底层实现
- 超大规模写入(Phoenix 写入比 HBase 慢)

### Phoenix 监控和调优

```sql
-- 查看表统计信息
SELECT * FROM SYSTEM.STATS WHERE PHYSICAL_NAME = 'USERS';

-- 查看索引信息
SELECT * FROM SYSTEM.CATALOG WHERE TABLE_NAME = 'USERS';

-- 查看查询缓存
SELECT * FROM SYSTEM.SEQUENCE;
```

---

## 🗄️ HDFS 高级特性详解

### 快照管理 (Snapshot)

快照是文件系统在某个时刻的只读副本,用于数据备份和恢复。

**应用场景:**
- 数据备份
- 数据恢复
- 测试环境
- 容灾演练

**使用示例:**
```bash
# 启用快照
hdfs dfsadmin -allowSnapshot /user/data

# 创建快照
hdfs dfs -createSnapshot /user/data snap1

# 查看快照
hdfs lsSnapshottableDir
hdfs dfs -ls /user/data/.snapshot

# 恢复数据
hdfs dfs -cp /user/data/.snapshot/snap1/file.txt /user/data/

# 删除快照
hdfs dfs -deleteSnapshot /user/data snap1
```

### 配额管理 (Quota)

限制目录的文件数量和存储空间,防止资源滥用。

**配额类型:**
- **名称配额**: 限制文件/目录数量
- **空间配额**: 限制存储空间

```bash
# 设置名称配额 (最多100个文件)
hdfs dfsadmin -setQuota 100 /user/test

# 设置空间配额 (最多1GB)
hdfs dfsadmin -setSpaceQuota 1g /user/test

# 查看配额
hdfs dfs -count -q /user/test

# 清除配额
hdfs dfsadmin -clrQuota /user/test
hdfs dfsadmin -clrSpaceQuota /user/test
```

### 存储策略 (Storage Policy)

根据数据访问频率选择不同的存储介质。

**策略类型:**
- **HOT**: 热数据 - 全部存储在磁盘
- **WARM**: 温数据 - 一份磁盘 + 一份归档
- **COLD**: 冷数据 - 全部存储在归档存储
- **ALL_SSD**: 全 SSD 存储
- **ONE_SSD**: 一份 SSD + 其他磁盘

```bash
# 查看存储策略
hdfs storagepolicies -listPolicies

# 设置存储策略
hdfs storagepolicies -setStoragePolicy -path /user/archive -policy COLD

# 查看路径的存储策略
hdfs storagepolicies -getStoragePolicy -path /user/archive

# 取消存储策略
hdfs storagepolicies -unsetStoragePolicy -path /user/archive
```

### 文件压缩

支持多种压缩算法,减少存储空间和网络传输。

**压缩算法对比:**

| 算法 | 压缩率 | 速度 | 可分割 | 适用场景 |
|------|--------|------|--------|----------|
| Gzip | 高 | 中 | 否 | 归档存储 |
| Snappy | 中 | 快 | 是 | 实时计算 |
| LZ4 | 中 | 最快 | 是 | 低延迟 |
| Bzip2 | 最高 | 慢 | 是 | 长期归档 |
| Zstd | 高 | 快 | 是 | 通用场景 |

### ACL 权限管理

扩展的访问控制列表,提供更细粒度的权限控制。

```bash
# 启用 ACL
hdfs dfsadmin -setconf dfs.namenode.acls.enabled=true

# 设置 ACL
hdfs dfs -setfacl -m user:alice:rwx /user/data

# 查看 ACL
hdfs dfs -getfacl /user/data

# 删除 ACL
hdfs dfs -setfacl -x user:alice /user/data
```

### HDFS 性能优化建议

1. **合理设置块大小**: 大文件使用大块(256MB+)
2. **启用短路读取**: 客户端与 DataNode 在同一节点时直接读取
3. **使用 EC(Erasure Coding)**: 降低存储开销(1.4x vs 3x)
4. **合并小文件**: 避免大量小文件影响 NameNode 性能
5. **使用压缩**: 减少存储空间和网络传输
6. **配置副本因子**: 根据重要性调整副本数
7. **启用缓存**: 热数据缓存到内存

---

## 📊 Hive 高级特性详解

### 分区表 (Partitioned Table)

按照某个列的值将数据分割到不同的子目录,提高查询效率。

**分区类型:**

**1. 静态分区**
```sql
-- 创建分区表
CREATE TABLE sales (
    order_id INT,
    amount DOUBLE
) PARTITIONED BY (year INT, month INT)
STORED AS ORC;

-- 插入静态分区
INSERT INTO sales PARTITION (year=2024, month=1)
VALUES (1, 100.0), (2, 200.0);
```

**2. 动态分区**
```sql
-- 启用动态分区
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

-- 动态分区插入
INSERT INTO sales PARTITION (year, month)
SELECT order_id, amount, year, month FROM source_table;
```

**分区优势:**
- 查询时只扫描相关分区,减少数据读取
- 支持分区裁剪(Partition Pruning)
- 方便数据管理和删除

### 分桶表 (Bucketed Table)

对数据进行哈希分桶,优化 JOIN 和采样查询。

```sql
-- 创建分桶表
CREATE TABLE users (
    user_id INT,
    name STRING
) CLUSTERED BY (user_id) INTO 32 BUCKETS
STORED AS ORC;

-- 分桶优势
-- 1. 高效 JOIN (Bucket Map Join)
-- 2. 高效采样
SELECT * FROM users TABLESAMPLE(BUCKET 1 OUT OF 32);
```

**分桶 vs 分区:**
- 分区: 按值分割,适合时间范围查询
- 分桶: 按哈希分割,适合 JOIN 优化

### 复杂数据类型

**Array 数组**
```sql
CREATE TABLE products (
    product_id INT,
    tags ARRAY<STRING>
);

-- 查询
SELECT tags[0], size(tags), array_contains(tags, 'hot') FROM products;

-- 展开
SELECT product_id, tag FROM products LATERAL VIEW explode(tags) t AS tag;
```

**Map 映射**
```sql
CREATE TABLE users (
    user_id INT,
    properties MAP<STRING, STRING>
);

-- 查询
SELECT properties['age'], map_keys(properties), size(properties) FROM users;
```

**Struct 结构体**
```sql
CREATE TABLE employees (
    emp_id INT,
    address STRUCT<street:STRING, city:STRING, zip:STRING>
);

-- 查询
SELECT address.city, address.zip FROM employees;
```

### 窗口函数

窗口函数在一组相关的行上执行计算,不改变结果集的行数。

**常用窗口函数:**

**1. 排名函数**
```sql
SELECT 
    name, salary,
    ROW_NUMBER() OVER (ORDER BY salary DESC) as row_num,
    RANK() OVER (ORDER BY salary DESC) as rank,
    DENSE_RANK() OVER (ORDER BY salary DESC) as dense_rank,
    NTILE(4) OVER (ORDER BY salary DESC) as quartile
FROM employees;
```

**2. 聚合窗口函数**
```sql
SELECT 
    name, department, salary,
    AVG(salary) OVER (PARTITION BY department) as dept_avg,
    SUM(salary) OVER (PARTITION BY department) as dept_total,
    MAX(salary) OVER (PARTITION BY department) as dept_max
FROM employees;
```

**3. 偏移函数**
```sql
SELECT 
    date, revenue,
    LAG(revenue, 1) OVER (ORDER BY date) as prev_revenue,
    LEAD(revenue, 1) OVER (ORDER BY date) as next_revenue,
    FIRST_VALUE(revenue) OVER (ORDER BY date) as first_revenue,
    LAST_VALUE(revenue) OVER (ORDER BY date) as last_revenue
FROM daily_sales;
```

**4. 框架子句**
```sql
-- 累计汇总
SELECT 
    date, amount,
    SUM(amount) OVER (ORDER BY date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_sum
FROM sales;

-- 移动平均
SELECT 
    date, amount,
    AVG(amount) OVER (ORDER BY date 
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as moving_avg_3
FROM sales;
```

### 事务表 (ACID)

Hive 3.0+ 支持完整的 ACID 事务,允许 UPDATE、DELETE、MERGE 操作。

**创建事务表:**
```sql
CREATE TABLE accounts (
    account_id INT,
    balance DOUBLE
) STORED AS ORC
TBLPROPERTIES (
    'transactional' = 'true',
    'orc.compress' = 'SNAPPY'
);
```

**事务操作:**
```sql
-- INSERT
INSERT INTO accounts VALUES (1, 1000.0);

-- UPDATE
UPDATE accounts SET balance = balance + 100 WHERE account_id = 1;

-- DELETE
DELETE FROM accounts WHERE balance < 0;

-- MERGE (UPSERT)
MERGE INTO accounts AS target
USING updates AS source
ON target.account_id = source.account_id
WHEN MATCHED THEN UPDATE SET balance = source.balance
WHEN NOT MATCHED THEN INSERT VALUES (source.account_id, source.balance);
```

**要求:**
- 必须使用 ORC 文件格式
- 必须设置 transactional=true
- 必须有主键或分桶

### 表优化 (Compaction)

事务表会产生 Delta 文件,需要定期合并。

**Compaction 类型:**
- **Minor Compaction**: 合并小的 Delta 文件
- **Major Compaction**: 合并所有 Delta 到 Base 文件

```sql
-- Minor Compaction
ALTER TABLE accounts COMPACT 'minor';

-- Major Compaction
ALTER TABLE accounts COMPACT 'major';

-- 查看 Compaction 状态
SHOW COMPACTIONS;
```

### 物化视图 (Materialized View)

预计算并存储查询结果,加速复杂查询。

```sql
-- 创建物化视图
CREATE MATERIALIZED VIEW sales_summary AS
SELECT 
    region, 
    product_category,
    SUM(amount) as total_sales,
    COUNT(*) as order_count
FROM sales
GROUP BY region, product_category;

-- 刷新物化视图
ALTER MATERIALIZED VIEW sales_summary REBUILD;

-- 查询自动使用物化视图
SELECT region, SUM(amount) FROM sales GROUP BY region;

-- 删除物化视图
DROP MATERIALIZED VIEW sales_summary;
```

**物化视图优势:**
- 预计算聚合结果
- 查询重写自动使用
- 支持增量刷新

### Hive 性能优化

**1. 文件格式优化**
```sql
-- ORC: 最佳列式存储格式
CREATE TABLE data STORED AS ORC TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'orc.stripe.size' = '268435456',
    'orc.compress.size' = '262144'
);

-- Parquet: 跨平台列式格式
CREATE TABLE data STORED AS PARQUET TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);
```

**2. 查询优化配置**
```sql
-- 启用 CBO (Cost-Based Optimizer)
SET hive.cbo.enable = true;
SET hive.compute.query.using.stats = true;
SET hive.stats.autogather = true;

-- 启用向量化执行
SET hive.vectorized.execution.enabled = true;
SET hive.vectorized.execution.reduce.enabled = true;

-- Map-side JOIN
SET hive.auto.convert.join = true;
SET hive.mapjoin.smalltable.filesize = 25000000;

-- 并行执行
SET hive.exec.parallel = true;
SET hive.exec.parallel.thread.number = 16;

-- 动态分区裁剪
SET hive.optimize.ppd = true;
```

**3. 表统计信息**
```sql
-- 收集表统计信息
ANALYZE TABLE sales COMPUTE STATISTICS;

-- 收集列统计信息
ANALYZE TABLE sales COMPUTE STATISTICS FOR COLUMNS;

-- 查看统计信息
DESCRIBE FORMATTED sales;
```

**4. 数据倾斜处理**
```sql
-- 启用倾斜处理
SET hive.groupby.skewindata = true;
SET hive.optimize.skewjoin = true;

-- 使用 DISTRIBUTE BY
SELECT department, COUNT(*)
FROM employees
DISTRIBUTE BY department
GROUP BY department;
```

### Hive 查询优化技巧

**1. 分区裁剪**
```sql
-- 好: WHERE 中使用分区列
SELECT * FROM sales WHERE year = 2024 AND month = 1;

-- 差: 不使用分区列
SELECT * FROM sales WHERE amount > 1000;
```

**2. 列裁剪**
```sql
-- 好: 只查询需要的列
SELECT order_id, amount FROM sales;

-- 差: SELECT *
SELECT * FROM sales;
```

**3. JOIN 优化**
```sql
-- 小表放左侧 (Map-side JOIN)
SELECT /*+ MAPJOIN(small_table) */ *
FROM small_table s JOIN large_table l ON s.id = l.id;

-- 使用分桶表 JOIN
SELECT * FROM users_bucketed u JOIN orders_bucketed o
ON u.user_id = o.user_id;
```

**4. 使用 EXPLAIN 分析**
```sql
EXPLAIN SELECT * FROM sales WHERE year = 2024;
EXPLAIN EXTENDED SELECT * FROM sales JOIN users;
```

---

## 📚 资源链接

### 传统大数据技术栈
- [Apache Spark 官方文档](https://spark.apache.org/docs/latest/)
- [Apache Hadoop 官方文档](https://hadoop.apache.org/docs/current/)
- [Apache Hive 官方文档](https://hive.apache.org/)
- [Apache HBase 官方文档](https://hbase.apache.org/)
- [Apache Phoenix 官方文档](https://phoenix.apache.org/)
- [HBase Coprocessor Guide](https://hbase.apache.org/book.html#cp)

### 现代大数据技术栈 ⭐ NEW!
- [Apache Flink 官方文档](https://flink.apache.org)
- [Apache Kafka 官方文档](https://kafka.apache.org)
- [Flink CDC 官方文档](https://ververica.github.io/flink-cdc-connectors/)
- [Apache Iceberg 官方文档](https://iceberg.apache.org)
- [ClickHouse 官方文档](https://clickhouse.com/docs)

### 推荐学习资源
- 📖 《Flink 实战》
- 📖 《Kafka 权威指南》
- 📖 《数据密集型应用系统设计》(DDIA)
- 📖 《ClickHouse 原理解析与应用实践》
- 🎓 尚硅谷大数据系列教程
- 🎓 黑马程序员大数据课程

### 项目文档
- 📘 [现代技术栈完整指南](TECH_STACK_GUIDE.md)
- 📘 [HDFS 高级特性指南](HDFS_GUIDE.md)
- 📘 [Hive 高级特性指南](HIVE_GUIDE.md)
- 📘 [Phoenix 使用指南](PHOENIX_GUIDE.md)
- 📘 [HBase 协处理器指南](COPROCESSOR_GUIDE.md)

## 📄 许可证
# spark-hadoop-hive-demo01
