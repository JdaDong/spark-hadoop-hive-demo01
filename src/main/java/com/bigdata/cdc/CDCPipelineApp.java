package com.bigdata.cdc;

/**
 * ============================================================
 * CDC 全链路深度实战 (Debezium + Flink CDC + Schema Registry)
 * ============================================================
 *
 * CDC (Change Data Capture) 是现代数据架构的基石：
 * - 实时数据同步: 数据库变更实时传播到下游系统
 * - 事件驱动架构: 数据变更即事件，驱动下游处理
 * - 零侵入: 读取 Binlog/WAL，不影响源库性能
 *
 * 本模块涵盖：
 * 1. Debezium 深度 (MySQL/PostgreSQL/MongoDB Connector)
 * 2. Flink CDC 3.0 (Pipeline + Transform)
 * 3. Schema Evolution 全链路
 * 4. Exactly-Once 语义保证
 * 5. CDC 运维 (断点续传/全量快照/增量切换)
 * 6. CDC 最佳实践 & 踩坑指南
 *
 * @author bigdata-team
 */
public class CDCPipelineApp {

    public static void main(String[] args) {
        System.out.println("╔═══════════════════════════════════════════════════════════╗");
        System.out.println("║   CDC 全链路深度实战 (Debezium + Flink CDC)                 ║");
        System.out.println("╚═══════════════════════════════════════════════════════════╝");

        // ============================================================
        // 第1部分: Debezium 深度
        // ============================================================
        demonstrateDebezium();

        // ============================================================
        // 第2部分: Flink CDC 3.0
        // ============================================================
        demonstrateFlinkCDC();

        // ============================================================
        // 第3部分: Schema Evolution 全链路
        // ============================================================
        demonstrateSchemaEvolution();

        // ============================================================
        // 第4部分: Exactly-Once 语义
        // ============================================================
        demonstrateExactlyOnce();

        // ============================================================
        // 第5部分: CDC 运维
        // ============================================================
        demonstrateCDCOps();

        // ============================================================
        // 第6部分: 最佳实践 & 踩坑指南
        // ============================================================
        demonstrateBestPractices();

        System.out.println("\n✅ CDC 全链路深度实战全部演示完成!");
    }

    // ============================================================
    //  第1部分: Debezium 深度
    // ============================================================
    private static void demonstrateDebezium() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🔄 第1部分: Debezium 深度实战");
        System.out.println("=".repeat(60));

        // 1.1 Debezium 架构
        System.out.println("\n🏗️ 1.1 Debezium 架构:");
        String debeziumArch = """
            ┌─────────────────────────────────────────────────────────────────┐
            │                    Debezium CDC 架构                              │
            ├─────────────────────────────────────────────────────────────────┤
            │                                                                 │
            │  Source DB                                                       │
            │  ┌──────────────┐                                               │
            │  │ MySQL        │──Binlog──┐                                    │
            │  │ (GTID mode)  │          │                                    │
            │  └──────────────┘          │                                    │
            │  ┌──────────────┐          │    ┌──────────────────────────┐   │
            │  │ PostgreSQL   │──WAL─────┼──→ │ Debezium Connector      │   │
            │  │ (pgoutput)   │          │    │ (Kafka Connect Plugin)  │   │
            │  └──────────────┘          │    │                         │   │
            │  ┌──────────────┐          │    │ ① 初始快照 (Snapshot)    │   │
            │  │ MongoDB      │──OpLog───┘    │ ② 增量捕获 (Streaming)  │   │
            │  │ (Change      │               │ ③ Schema 历史记录       │   │
            │  │  Streams)    │               │ ④ 断点续传 (Offset)     │   │
            │  └──────────────┘               └───────────┬─────────────┘   │
            │                                              │                  │
            │                              ┌───────────────┴────────────┐    │
            │                              │       Kafka Cluster        │    │
            │                              │                            │    │
            │                              │ Topic: mysql.ecommerce.orders│  │
            │                              │ Topic: mysql.ecommerce.users │  │
            │                              │ Topic: __debezium.schema    │  │
            │                              └──────┬────────────┬────────┘   │
            │                                     │            │             │
            │                              ┌──────┴──┐   ┌────┴────┐       │
            │                              │ Flink   │   │ Doris   │       │
            │                              │ CDC     │   │ Routine │       │
            │                              │ Consumer│   │ Load    │       │
            │                              └─────────┘   └─────────┘       │
            └─────────────────────────────────────────────────────────────────┘
            """;
        System.out.println(debeziumArch);

        // 1.2 Debezium MySQL Connector 配置
        System.out.println("\n⚙️ 1.2 Debezium MySQL Connector 完整配置:");
        String debeziumConfig = """
            // ============================================================
            // Debezium MySQL Connector 配置 (生产级)
            // 通过 Kafka Connect REST API 注册
            // ============================================================
            
            // POST http://kafka-connect:8083/connectors
            {
              "name": "mysql-ecommerce-cdc",
              "config": {
                // ---- 基本配置 ----
                "connector.class": "io.debezium.connector.mysql.MySqlConnector",
                "tasks.max": "1",
                
                // ---- 源数据库 ----
                "database.hostname": "mysql-master",
                "database.port": "3306",
                "database.user": "debezium",
                "database.password": "${vault:secret/mysql/debezium:password}",
                "database.server.id": "184054",            // 唯一 server ID
                
                // ---- Topic 前缀 ----
                "topic.prefix": "mysql.ecommerce",         // → mysql.ecommerce.{db}.{table}
                
                // ---- 捕获范围 ----
                "database.include.list": "ecommerce",
                "table.include.list": 
                    "ecommerce.orders,ecommerce.order_items," +
                    "ecommerce.users,ecommerce.products," +
                    "ecommerce.payments",
                "column.exclude.list": "ecommerce.users.password_hash",  // 排除敏感列
                
                // ---- 快照模式 ----
                "snapshot.mode": "initial",                // initial | schema_only | never
                // initial: 首次启动先全量快照，然后增量
                // schema_only: 只快照 schema，从当前位置开始增量
                // never: 不做快照，直接增量 (需确保 offset 有效)
                
                "snapshot.locking.mode": "minimal",        // minimal | extended | none
                // minimal: 只在读 schema 时短暂加锁
                
                // ---- Binlog 配置 ----
                "include.schema.changes": "true",
                "database.history.kafka.bootstrap.servers": "kafka:9092",
                "database.history.kafka.topic": "schema-changes.ecommerce",
                
                // ---- 序列化 ----
                "key.converter": "io.confluent.connect.avro.AvroConverter",
                "key.converter.schema.registry.url": "http://schema-registry:8081",
                "value.converter": "io.confluent.connect.avro.AvroConverter",
                "value.converter.schema.registry.url": "http://schema-registry:8081",
                
                // ---- 事件结构 ----
                "tombstones.on.delete": "true",            // DELETE 后发送 tombstone
                "provide.transaction.metadata": "true",    // 包含事务信息
                
                // ---- SMT (Single Message Transform) ----
                "transforms": "route,unwrap,addField",
                
                // 路由: 按表名路由到不同 Topic
                "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
                "transforms.route.regex": "mysql\\.ecommerce\\.(.*)",
                "transforms.route.replacement": "cdc.$1",
                
                // 展平: Debezium envelope → 扁平 JSON
                "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
                "transforms.unwrap.drop.tombstones": "false",
                "transforms.unwrap.delete.handling.mode": "rewrite",
                "transforms.unwrap.add.fields": "op,source.ts_ms,source.server,source.db,source.table",
                
                // 添加自定义字段
                "transforms.addField.type": "org.apache.kafka.connect.transforms.InsertField$Value",
                "transforms.addField.static.field": "cdc_ingestion_ts",
                "transforms.addField.static.value": "${datetime}",
                
                // ---- 错误处理 ----
                "errors.tolerance": "all",                 // all | none
                "errors.log.enable": "true",
                "errors.log.include.messages": "true",
                "errors.deadletterqueue.topic.name": "dlq.mysql-ecommerce",
                "errors.deadletterqueue.context.headers.enable": "true",
                
                // ---- 性能调优 ----
                "max.batch.size": "2048",                  // 每批最大记录数
                "max.queue.size": "8192",                  // 内部队列大小
                "poll.interval.ms": "100",                 // 轮询间隔
                
                // ---- 心跳 ----
                "heartbeat.interval.ms": "10000",          // 10s 心跳
                "heartbeat.action.query": 
                    "INSERT INTO debezium_heartbeat (ts) VALUES (NOW()) ON DUPLICATE KEY UPDATE ts=NOW()"
              }
            }
            """;
        System.out.println(debeziumConfig);

        // 1.3 Debezium 消息格式
        System.out.println("\n📨 1.3 Debezium 消息格式解析:");
        String messageFormat = """
            // Debezium CDC 消息结构 (Envelope 格式):
            {
              "schema": { /* Avro/JSON Schema */ },
              "payload": {
                // ---- 变更前的值 (UPDATE/DELETE 时有值) ----
                "before": {
                  "order_id": "ORD001",
                  "order_status": "PENDING",
                  "order_amount": 299.00,
                  "updated_at": 1705286400000
                },
                
                // ---- 变更后的值 (INSERT/UPDATE 时有值) ----
                "after": {
                  "order_id": "ORD001",
                  "order_status": "PAID",         // 状态变更
                  "order_amount": 299.00,
                  "updated_at": 1705290000000
                },
                
                // ---- 元数据 ----
                "source": {
                  "version": "2.5.0",
                  "connector": "mysql",
                  "name": "mysql.ecommerce",
                  "ts_ms": 1705290000123,           // binlog 事件时间
                  "snapshot": false,
                  "db": "ecommerce",
                  "table": "orders",
                  "server_id": 184054,
                  "gtid": "3E11FA47-71CA-11E1-9E33-C80AA9429562:42",
                  "file": "mysql-bin.000003",
                  "pos": 12345,
                  "row": 0,
                  "thread": 42
                },
                
                "op": "u",                           // c=INSERT, u=UPDATE, d=DELETE, r=READ(快照)
                "ts_ms": 1705290000456,              // Debezium 处理时间
                
                // ---- 事务信息 (可选) ----
                "transaction": {
                  "id": "file=mysql-bin.000003,pos=12300",
                  "total_order": 3,                  // 事务内第几条
                  "data_collection_order": 2
                }
              }
            }
            
            // 操作类型含义:
            // "op": "c" → CREATE (INSERT)
            // "op": "r" → READ   (初始快照)
            // "op": "u" → UPDATE
            // "op": "d" → DELETE (before 有值, after 为 null)
            """;
        System.out.println(messageFormat);
    }

    // ============================================================
    //  第2部分: Flink CDC 3.0
    // ============================================================
    private static void demonstrateFlinkCDC() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("⚡ 第2部分: Flink CDC 3.0 Pipeline");
        System.out.println("=".repeat(60));

        // 2.1 Flink CDC 3.0 Pipeline API
        System.out.println("\n🔗 2.1 Flink CDC 3.0 Pipeline API (全新!):");
        String flinkCDC3 = """
            // ============================================================
            // Flink CDC 3.0 核心特性:
            // 1. YAML Pipeline 定义 (无需写代码)
            // 2. Schema Evolution 自动同步
            // 3. 整库同步 (多表并行)
            // 4. Transform (ETL in CDC)
            // ============================================================
            
            // ---- YAML Pipeline 定义 ----
            // 文件: cdc-pipelines/mysql-to-doris.yaml
            
            source:
              type: mysql
              hostname: mysql-master
              port: 3306
              username: debezium
              password: ${MYSQL_PASSWORD}
              tables: ecommerce.orders, ecommerce.users, ecommerce.products
              server-id: 5400-5404                   # 范围，支持并行快照
              server-time-zone: Asia/Shanghai
              
              # 快照分片 (并行快照，大表必备)
              scan.incremental.snapshot.enabled: true
              scan.incremental.snapshot.chunk.size: 8096
              chunk-key.even-distribution.factor.lower-bound: 0.05
              chunk-key.even-distribution.factor.upper-bound: 100
            
            sink:
              type: doris
              fenodes: doris-fe:8030
              username: root
              password: ${DORIS_PASSWORD}
              
              # Doris 自动建表
              table.create.properties.replication_num: 1
              table.create.properties.light_schema_change: true
              
            transform:
              # ---- 表级转换 ----
              - source-table: ecommerce.orders
                projection: "*, DATE_FORMAT(created_at, 'yyyy-MM-dd') AS order_date"
                filter: "order_status != 'CANCELLED'"
                description: "过滤已取消订单，增加日期分区列"
                
              - source-table: ecommerce.users
                projection: >
                  user_id,
                  UPPER(user_name) AS user_name,
                  SUBSTR(phone, 1, 3) || '****' || SUBSTR(phone, 8) AS phone_masked,
                  city, province,
                  created_at
                filter: "status = 'active'"
                description: "用户表脱敏处理 (手机号掩码)"
            
            route:
              # ---- 目标表路由 ----
              - source-table: ecommerce.orders
                sink-table: ods_db.ods_orders
                
              - source-table: ecommerce.users
                sink-table: ods_db.ods_users
                
              - source-table: ecommerce.products
                sink-table: ods_db.ods_products
                
              # 通配符路由 (整库同步)
              # - source-table: ecommerce.\\.*
              #   sink-table: ods_db.ods_<TABLE>
            
            pipeline:
              name: MySQL-to-Doris-CDC
              parallelism: 4
              schema.change.behavior: EVOLVE          # EVOLVE | EXCEPTION | IGNORE
              # EVOLVE: 自动同步 DDL 到下游 (加列/改列名/加索引)
              # EXCEPTION: DDL 变更时报错停止
              # IGNORE: 忽略 DDL 变更
            
            # 启动命令:
            # bin/flink-cdc.sh mysql-to-doris.yaml
            """;
        System.out.println(flinkCDC3);

        // 2.2 Flink CDC Java API
        System.out.println("\n☕ 2.2 Flink CDC Java API:");
        String flinkCDCJava = """
            // ============================================================
            // Flink CDC Java API (精细控制)
            // ============================================================
            
            // 1. MySQL CDC Source (增量快照)
            MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("mysql-master")
                .port(3306)
                .databaseList("ecommerce")
                .tableList("ecommerce.orders", "ecommerce.order_items")
                .username("debezium")
                .password("password")
                .serverId("5400-5404")                // 并行快照 (5个并行)
                .deserializer(new JsonDebeziumDeserializationSchema())
                
                // 快照配置
                .startupOptions(StartupOptions.initial())  // initial | latest | timestamp | specificOffset
                .scanNewlyAddedTableEnabled(true)           // 自动发现新表
                
                // 增量快照分片
                .splitSize(8096)                            // 每个快照分片大小
                .splitMetaGroupSize(1000)                   // 分片元数据组大小
                .fetchSize(1024)                            // 每次 fetch 行数
                .connectTimeout(Duration.ofSeconds(30))
                .connectMaxRetries(3)
                
                // 水位线
                .closeIdleReaders(true)                     // 快照完成后关闭空闲 reader
                
                // Debezium 属性
                .debeziumProperties(debeziumProps())
                .build();
            
            // 2. Flink DataStream 处理
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.enableCheckpointing(60000);                 // 1分钟 checkpoint
            env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
            
            DataStreamSource<String> cdcStream = env.fromSource(
                mySqlSource,
                WatermarkStrategy.noWatermarks(),
                "MySQL CDC Source"
            );
            
            // 3. 数据处理 + 路由
            cdcStream
                .map(new CDCMessageParser())                // 解析 CDC 消息
                .keyBy(record -> record.getTable())         // 按表分区
                .process(new CDCRouter())                   // 路由到不同下游
                .addSink(dorisSink());                      // 写入 Doris
            
            // 4. 处理 Schema 变更
            private static Properties debeziumProps() {
                Properties props = new Properties();
                props.setProperty("decimal.handling.mode", "string");
                props.setProperty("bigint.unsigned.handling.mode", "long");
                props.setProperty("include.schema.changes", "true");
                // 处理 DDL: 自动传播到下游
                props.setProperty("schema.history.internal.kafka.bootstrap.servers", "kafka:9092");
                props.setProperty("schema.history.internal.kafka.topic", "schema-history");
                return props;
            }
            """;
        System.out.println(flinkCDCJava);

        // 2.3 增量快照算法
        System.out.println("\n📐 2.3 增量快照算法 (核心原理):");
        String incrementalSnapshot = """
            ┌──────────────────────────────────────────────────────────────┐
            │         Flink CDC 增量快照算法 (无锁全量+增量切换)              │
            ├──────────────────────────────────────────────────────────────┤
            │                                                              │
            │  传统方式: 全量快照 → 锁表 → 增量                             │
            │  问题: 大表锁表几小时，严重影响业务                             │
            │                                                              │
            │  Flink CDC 增量快照: 无锁!                                   │
            │                                                              │
            │  Phase 1: 分片快照 (并行读取)                                 │
            │  ┌─────────────────────────────────────────────────────┐    │
            │  │ 表: orders (1亿行)                                   │    │
            │  │                                                     │    │
            │  │ Split 1: PK [0, 2M)       ──→ Reader 1 SELECT ...  │    │
            │  │ Split 2: PK [2M, 4M)      ──→ Reader 2 SELECT ...  │    │
            │  │ Split 3: PK [4M, 6M)      ──→ Reader 3 SELECT ...  │    │
            │  │ ...                                                 │    │
            │  │ Split N: PK [98M, 100M)   ──→ Reader 4 SELECT ...  │    │
            │  │                                                     │    │
            │  │ 关键: 每个 Split 读取前记录 Binlog 位点                │    │
            │  │       读取完成后再记录一次 Binlog 位点                  │    │
            │  │       对比两次位点之间的 Binlog 来修正快照数据          │    │
            │  └─────────────────────────────────────────────────────┘    │
            │                                                              │
            │  Phase 2: Binlog 增量 (快照完成后无缝切换)                    │
            │  ┌─────────────────────────────────────────────────────┐    │
            │  │ 所有 Split 快照完成 → 记录最小 Binlog 位点            │    │
            │  │ 切换到纯 Binlog 读取模式 (CDC 增量)                   │    │
            │  │ 无缝衔接，不丢数据，不重复                             │    │
            │  └─────────────────────────────────────────────────────┘    │
            │                                                              │
            │  优势:                                                       │
            │  ① 无锁: 不影响源库业务                                      │
            │  ② 并行: 多个 Reader 并行快照，速度快 N 倍                    │
            │  ③ 断点: 按 Split 粒度 checkpoint，失败只重做当前 Split      │
            │  ④ 一致: Binlog 修正保证快照数据一致性                        │
            └──────────────────────────────────────────────────────────────┘
            """;
        System.out.println(incrementalSnapshot);
    }

    // ============================================================
    //  第3部分: Schema Evolution 全链路
    // ============================================================
    private static void demonstrateSchemaEvolution() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("📐 第3部分: Schema Evolution 全链路");
        System.out.println("=".repeat(60));

        String schemaEvolution = """
            ┌──────────────────────────────────────────────────────────────┐
            │              Schema Evolution 全链路处理                       │
            ├──────────────────────────────────────────────────────────────┤
            │                                                              │
            │  源表 DDL 变更                                               │
            │  ALTER TABLE orders ADD COLUMN coupon_code VARCHAR(32);     │
            │       │                                                      │
            │       ▼                                                      │
            │  ① Debezium 捕获 DDL                                        │
            │  ┌──────────────────────────────────────────────────────┐   │
            │  │ Topic: schema-changes.ecommerce                      │   │
            │  │ {                                                    │   │
            │  │   "databaseName": "ecommerce",                      │   │
            │  │   "ddl": "ALTER TABLE orders ADD COLUMN coupon_code │   │
            │  │           VARCHAR(32)",                              │   │
            │  │   "tableChanges": [{                                │   │
            │  │     "type": "ALTER",                                │   │
            │  │     "id": "ecommerce.orders",                       │   │
            │  │     "table": { "columns": [..., "coupon_code"] }    │   │
            │  │   }]                                                │   │
            │  │ }                                                    │   │
            │  └──────────────────────────┬───────────────────────────┘   │
            │                              │                              │
            │       ▼                      ▼                              │
            │  ② Schema Registry 更新      ③ Flink CDC 处理              │
            │  ┌──────────────────┐       ┌──────────────────────┐      │
            │  │ 注册新版本 Schema │       │ schema.change.behavior│     │
            │  │ v1 → v2         │       │                      │      │
            │  │ (BACKWARD 兼容) │       │ EVOLVE:               │      │
            │  │                  │       │  自动执行下游 DDL     │      │
            │  │ 旧消费者仍可读   │       │  ALTER TABLE ods_orders│     │
            │  │ (新字段默认null)  │       │  ADD COLUMN coupon_code│    │
            │  └──────────────────┘       └──────────────────────┘      │
            │                                                              │
            │  支持的 Schema 变更:                                         │
            │  ┌────────────────────┬──────────────┬──────────────┐      │
            │  │ DDL 类型            │ Flink CDC    │ Debezium     │      │
            │  ├────────────────────┼──────────────┼──────────────┤      │
            │  │ ADD COLUMN         │ ✅ 自动传播   │ ✅ 自动      │      │
            │  │ DROP COLUMN        │ ✅ 自动传播   │ ✅ 自动      │      │
            │  │ RENAME COLUMN      │ ✅ 自动传播   │ ✅ 自动      │      │
            │  │ MODIFY TYPE        │ ⚠️ 部分支持  │ ✅ 自动      │      │
            │  │ ADD INDEX          │ ✅ 忽略       │ ✅ 忽略      │      │
            │  │ RENAME TABLE       │ ⚠️ 需重启    │ ⚠️ 需重启   │      │
            │  │ CREATE TABLE (新表)│ ✅ 自动发现   │ ⚠️ 需配置   │      │
            │  └────────────────────┴──────────────┴──────────────┘      │
            └──────────────────────────────────────────────────────────────┘
            """;
        System.out.println(schemaEvolution);
    }

    // ============================================================
    //  第4部分: Exactly-Once 语义
    // ============================================================
    private static void demonstrateExactlyOnce() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🔒 第4部分: Exactly-Once 语义保证");
        System.out.println("=".repeat(60));

        String exactlyOnce = """
            ┌──────────────────────────────────────────────────────────────┐
            │               CDC Exactly-Once 语义保证                       │
            ├──────────────────────────────────────────────────────────────┤
            │                                                              │
            │  Source 端 (Exactly-Once Read):                              │
            │  ┌──────────────────────────────────────────────────────┐   │
            │  │ Flink CDC:                                           │   │
            │  │ ① Checkpoint 保存 Binlog 位点 (GTID/file+pos)       │   │
            │  │ ② 故障恢复从 Checkpoint 位点重放                      │   │
            │  │ ③ 增量快照: 每个 Split 独立 Checkpoint               │   │
            │  │                                                      │   │
            │  │ Debezium (Kafka Connect):                            │   │
            │  │ ① Offset 存储在 Kafka 内部 Topic                    │   │
            │  │ ② 故障恢复从 Offset 位点重放                         │   │
            │  │ ③ 可能有少量重复 (At-Least-Once)                     │   │
            │  └──────────────────────────────────────────────────────┘   │
            │                                                              │
            │  Transport 端 (Kafka Exactly-Once):                         │
            │  ┌──────────────────────────────────────────────────────┐   │
            │  │ Kafka 事务 Producer:                                  │   │
            │  │ ① enable.idempotence=true                           │   │
            │  │ ② transactional.id=<unique-id>                      │   │
            │  │ ③ 配合 Flink TwoPhaseCommitSinkFunction             │   │
            │  │                                                      │   │
            │  │ Consumer 隔离级别:                                    │   │
            │  │ isolation.level=read_committed                       │   │
            │  │ (只读已提交的事务消息)                                 │   │
            │  └──────────────────────────────────────────────────────┘   │
            │                                                              │
            │  Sink 端 (Exactly-Once Write):                              │
            │  ┌──────────────────────────────────────────────────────┐   │
            │  │ 方法1: 两阶段提交 (2PC)                              │   │
            │  │ → Flink Checkpoint 提交时，Sink 也提交               │   │
            │  │ → 适用: Kafka Sink, JDBC Sink (支持XA)              │   │
            │  │                                                      │   │
            │  │ 方法2: 幂等写入 (Idempotent Write) ← 推荐!          │   │
            │  │ → 利用主键 UPSERT (INSERT ON DUPLICATE KEY UPDATE)  │   │
            │  │ → 即使重复写入，结果一致                              │   │
            │  │ → 适用: Doris, StarRocks, HBase, Redis              │   │
            │  │                                                      │   │
            │  │ 方法3: 预写日志 (WAL)                                │   │
            │  │ → 先写 WAL，Checkpoint 成功后再提交到目标存储          │   │
            │  │ → 适用: 不支持事务的存储                              │   │
            │  └──────────────────────────────────────────────────────┘   │
            │                                                              │
            │  端到端 Exactly-Once 配置:                                   │
            │  ┌──────────────────────────────────────────────────────┐   │
            │  │ // Flink 配置                                        │   │
            │  │ env.enableCheckpointing(60000);                      │   │
            │  │ env.getCheckpointConfig()                            │   │
            │  │    .setCheckpointingMode(EXACTLY_ONCE);              │   │
            │  │ env.getCheckpointConfig()                            │   │
            │  │    .setMinPauseBetweenCheckpoints(30000);            │   │
            │  │ env.getCheckpointConfig()                            │   │
            │  │    .setCheckpointTimeout(120000);                    │   │
            │  │                                                      │   │
            │  │ // Doris Sink (幂等 UPSERT)                          │   │
            │  │ DorisExecutionOptions.builder()                      │   │
            │  │    .setStreamLoadProp(props)                         │   │
            │  │    .setDeletable(true)          // 支持 DELETE        │   │
            │  │    .setBatchMode(false)         // 非攒批，实时写入    │   │
            │  │    .enable2PC()                 // 两阶段提交         │   │
            │  │    .build();                                         │   │
            │  └──────────────────────────────────────────────────────┘   │
            └──────────────────────────────────────────────────────────────┘
            """;
        System.out.println(exactlyOnce);
    }

    // ============================================================
    //  第5部分: CDC 运维
    // ============================================================
    private static void demonstrateCDCOps() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🔧 第5部分: CDC 运维实战");
        System.out.println("=".repeat(60));

        String cdcOps = """
            # ============================================================
            # CDC 运维常见场景
            # ============================================================
            
            # 场景1: Binlog 过期 (位点失效)
            # ────────────────────────────────────────
            # 原因: MySQL binlog_expire_logs_seconds 过短，CDC 停机超过保留期
            # 解决:
            # 1. 重置为全量快照模式
            # 2. 修改 snapshot.mode=initial
            # 3. 重启 Connector
            # 预防: binlog_expire_logs_seconds >= 7天 (604800)
            
            # 场景2: 大表全量快照超时
            # ────────────────────────────────────────
            # 原因: 表数据量 > 10亿行，全量快照耗时过长
            # 解决:
            # 1. Flink CDC 增量快照: 并行分片 (parallelism=8)
            # 2. 调大 split-size: scan.incremental.snapshot.chunk.size=16384
            # 3. 分批快照: 先同步核心表，再逐步添加
            
            # 场景3: 源库主从切换
            # ────────────────────────────────────────
            # MySQL GTID 模式:
            # ① 使用 GTID (而非 file+pos)，主从切换自动续传
            # ② 确保 enforce_gtid_consistency=ON, gtid_mode=ON
            #
            # 非 GTID 模式:
            # ① 切换后需要手动校准 binlog 位点
            # ② 或重新全量快照
            
            # 场景4: CDC 延迟告警
            # ────────────────────────────────────────
            # Prometheus 监控指标:
            
            # Debezium 延迟:
            debezium_mysql_connector_metrics_seconds_behind_source
            # 告警: > 300 (5分钟) → P2, > 3600 (1小时) → P1
            
            # Flink CDC 延迟:
            flink_taskmanager_job_task_operator_currentEmitEventTimeLag
            # 告警: > 60000ms (1分钟) → P2
            
            # Kafka Consumer Lag:
            kafka_consumergroup_lag{topic="cdc.orders"}
            # 告警: > 10000 消息 → P2
            
            # 场景5: 数据一致性校验
            # ────────────────────────────────────────
            # 定期全量对账:
            
            # 1. 源表 count 对比
            SELECT COUNT(*) FROM mysql.ecommerce.orders;         -- 源
            SELECT COUNT(*) FROM doris.ods_db.ods_orders;        -- 目标
            
            # 2. 抽样 checksum 对比
            SELECT MD5(GROUP_CONCAT(
                order_id, order_status, order_amount
                ORDER BY order_id
            )) AS checksum
            FROM orders WHERE order_id BETWEEN 1000 AND 2000;
            
            # 3. 增量对账 (最近1小时变更)
            SELECT order_id, updated_at FROM orders
            WHERE updated_at > NOW() - INTERVAL 1 HOUR
            EXCEPT
            SELECT order_id, updated_at FROM ods_orders
            WHERE updated_at > NOW() - INTERVAL 1 HOUR;
            
            # 场景6: 断点续传 & Savepoint
            # ────────────────────────────────────────
            # Flink CDC Savepoint:
            # 停止作业 (保存状态)
            flink savepoint <job-id> s3://savepoints/cdc-mysql/
            flink stop --savepointPath s3://savepoints/ <job-id>
            
            # 从 Savepoint 恢复
            flink run -s s3://savepoints/cdc-mysql/savepoint-xxx \\
              -c com.bigdata.cdc.MysqlToDoris \\
              cdc-pipeline.jar
            """;
        System.out.println(cdcOps);
    }

    // ============================================================
    //  第6部分: 最佳实践 & 踩坑指南
    // ============================================================
    private static void demonstrateBestPractices() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("📝 第6部分: CDC 最佳实践 & 踩坑指南");
        System.out.println("=".repeat(60));

        String bestPractices = """
            ┌──────────────────────────────────────────────────────────────┐
            │                 CDC 最佳实践 & 踩坑指南                        │
            ├──────────────────────────────────────────────────────────────┤
            │                                                              │
            │  ✅ 最佳实践                                                 │
            │  ──────────────────────────────────────────────              │
            │  1. MySQL 配置:                                              │
            │     • binlog_format = ROW (必须)                            │
            │     • binlog_row_image = FULL (推荐, UPDATE 有完整前后值)     │
            │     • gtid_mode = ON (支持主从切换)                          │
            │     • binlog_expire_logs_seconds >= 604800 (7天)            │
            │     • 使用专用 CDC 只读账号，最小权限                          │
            │                                                              │
            │  2. Connector 设计:                                          │
            │     • 一个 Connector 对应一个数据库 (非一个表)                 │
            │     • table.include.list 精确指定表 (避免 .*)               │
            │     • column.exclude.list 排除大字段 (TEXT/BLOB)             │
            │     • 开启心跳 (heartbeat.interval.ms=10000)                │
            │     • 开启死信队列 (DLQ) 处理坏消息                          │
            │                                                              │
            │  3. 性能:                                                    │
            │     • Kafka Topic 分区数 = 表数量 (一表一分区)               │
            │     • Flink CDC 并行度 = min(表数, 可用slot)               │
            │     • max.batch.size 调大 (2048~4096) 提高吞吐              │
            │     • 大表快照: chunk.size=16384, parallelism=8             │
            │                                                              │
            │  4. 监控:                                                    │
            │     • 必监控: Binlog 延迟、Kafka Lag、Flink Checkpoint       │
            │     • 必告警: 延迟>5min, Lag>10000, Checkpoint 失败连续3次   │
            │     • 定期对账: 每天全量 count 对比 + 抽样 checksum          │
            │                                                              │
            │  ❌ 常见踩坑                                                 │
            │  ──────────────────────────────────────────────              │
            │  1. binlog_row_image=MINIMAL                                │
            │     → UPDATE 只有变更列, 缺少完整记录                        │
            │     → 下游 UPSERT 数据不完整                                │
            │     → 解决: 改为 FULL                                       │
            │                                                              │
            │  2. 时区问题                                                 │
            │     → Debezium 默认 UTC, 源库可能是 Asia/Shanghai           │
            │     → TIMESTAMP 类型差 8 小时                               │
            │     → 解决: database.connectionTimeZone=Asia/Shanghai       │
            │                                                              │
            │  3. decimal 精度丢失                                        │
            │     → Debezium 默认 decimal.handling.mode=precise (bytes)   │
            │     → 下游可能无法解析                                       │
            │     → 解决: decimal.handling.mode=string                    │
            │                                                              │
            │  4. 大事务阻塞                                               │
            │     → 源库执行大批量 UPDATE/DELETE (百万级)                   │
            │     → 单个 Binlog 事件过大, 内存溢出                         │
            │     → 解决: max.queue.size 调大, 或源库拆分批次              │
            │                                                              │
            │  5. Kafka 消息过大                                           │
            │     → 单条消息 > 1MB (Kafka 默认限制)                       │
            │     → 原因: BLOB/TEXT 大字段                                │
            │     → 解决: column.exclude.list 排除大字段                  │
            │             或调大 message.max.bytes                         │
            │                                                              │
            │  6. 权限不足                                                 │
            │     → GRANT SELECT, RELOAD, SHOW DATABASES,                 │
            │       REPLICATION SLAVE, REPLICATION CLIENT                  │
            │       ON *.* TO 'debezium'@'%';                             │
            │     → 缺少 REPLICATION 权限无法读 Binlog                    │
            └──────────────────────────────────────────────────────────────┘
            """;
        System.out.println(bestPractices);
    }
}
