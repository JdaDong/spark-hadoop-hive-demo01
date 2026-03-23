package com.bigdata.olap;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * ========================================================================
 * 实时 OLAP 引擎实战 - Apache Doris + StarRocks
 * ========================================================================
 *
 * 功能矩阵:
 * ┌─────────────────────────────────────────────────────────────────────┐
 * │  模块                │  功能                                        │
 * ├─────────────────────────────────────────────────────────────────────┤
 * │  1. Doris 表模型     │  Duplicate/Aggregate/Unique Key 三模型       │
 * │  2. Doris 物化视图   │  同步/异步物化视图 + 查询透明改写             │
 * │  3. Doris 数据导入   │  Stream Load/Broker Load/Routine Load        │
 * │  4. StarRocks 主键   │  Primary Key 表 + 实时 Upsert               │
 * │  5. StarRocks 查询   │  向量化引擎 + CBO + Pipeline 执行引擎        │
 * │  6. 湖仓一体         │  External Catalog (Hive/Iceberg/Hudi/Delta) │
 * │  7. 实时看板         │  多维分析 / 漏斗 / 留存 / 路径分析           │
 * │  8. 性能优化         │  Colocate Join / Bucket Shuffle / 分区裁剪  │
 * └─────────────────────────────────────────────────────────────────────┘
 *
 * 架构:
 *
 *   ┌──────────┐  Stream Load   ┌────────────────┐
 *   │  Kafka   │───────────────→│                │
 *   └──────────┘                │                │
 *   ┌──────────┐  Broker Load   │  Doris / SR    │──→  BI Dashboard
 *   │  HDFS    │───────────────→│  (FE + BE)     │──→  Ad-Hoc Query
 *   └──────────┘                │                │──→  Real-time Report
 *   ┌──────────┐  Catalog       │                │
 *   │  Hive    │───────────────→│                │
 *   └──────────┘                └────────────────┘
 */
public class RealtimeOlapApp {

    // ==================== 1. Doris 数据模型管理 ====================

    /**
     * Apache Doris 数据建模 - 三种表模型
     *
     * 1. Duplicate Model: 保留所有原始数据 (日志/明细)
     * 2. Aggregate Model: 预聚合 (指标汇总)
     * 3. Unique Model: 主键去重 (维度表/CDC)
     */
    static class DorisModelManager {

        private final String feHost;
        private final int fePort;
        private Connection conn;

        public DorisModelManager(String feHost, int fePort) {
            this.feHost = feHost;
            this.fePort = fePort;
        }

        /**
         * 获取连接
         */
        private Connection getConnection() throws SQLException {
            if (conn == null || conn.isClosed()) {
                String url = "jdbc:mysql://" + feHost + ":" + fePort + "/";
                conn = DriverManager.getConnection(url, "root", "");
            }
            return conn;
        }

        /**
         * 创建数据库 & 三种表模型
         */
        public List<String> generateTableDDL() {
            List<String> ddls = new ArrayList<>();

            System.out.println("📐 生成 Doris 数据模型 DDL...\n");

            // 创建数据库
            ddls.add("CREATE DATABASE IF NOT EXISTS bigdata_olap;");
            ddls.add("USE bigdata_olap;");

            // ===== 1. Duplicate Model (明细模型) - 用户行为日志 =====
            String duplicateTable = String.join("\n",
                    "-- ========================================",
                    "-- Duplicate Model: 用户行为日志 (保留全部明细)",
                    "-- ========================================",
                    "CREATE TABLE IF NOT EXISTS user_behavior_log (",
                    "    `event_time`     DATETIME        NOT NULL COMMENT '事件时间',",
                    "    `user_id`        BIGINT          NOT NULL COMMENT '用户ID',",
                    "    `event_type`     VARCHAR(32)     NOT NULL COMMENT '事件类型: pv/click/cart/buy',",
                    "    `page_id`        VARCHAR(64)     COMMENT '页面ID',",
                    "    `item_id`        BIGINT          COMMENT '商品ID',",
                    "    `category_id`    INT             COMMENT '品类ID',",
                    "    `channel`        VARCHAR(32)     COMMENT '渠道: app/web/h5/miniprogram',",
                    "    `device_type`    VARCHAR(16)     COMMENT '设备: ios/android/pc',",
                    "    `city`           VARCHAR(32)     COMMENT '城市',",
                    "    `session_id`     VARCHAR(64)     COMMENT '会话ID',",
                    "    `duration_ms`    INT             COMMENT '停留时长(毫秒)',",
                    "    `referrer`       VARCHAR(256)    COMMENT '来源URL'",
                    ") ENGINE=OLAP",
                    "DUPLICATE KEY(`event_time`, `user_id`, `event_type`)",
                    "COMMENT '用户行为日志明细表 (Duplicate Model)'",
                    "PARTITION BY RANGE(`event_time`) (",
                    "    PARTITION p20240101 VALUES LESS THAN ('2024-01-02'),",
                    "    PARTITION p20240102 VALUES LESS THAN ('2024-01-03'),",
                    "    PARTITION p20240103 VALUES LESS THAN ('2024-01-04')",
                    ")",
                    "DISTRIBUTED BY HASH(`user_id`) BUCKETS 16",
                    "PROPERTIES (",
                    "    'replication_num' = '3',",
                    "    'dynamic_partition.enable' = 'true',",
                    "    'dynamic_partition.time_unit' = 'DAY',",
                    "    'dynamic_partition.start' = '-30',",
                    "    'dynamic_partition.end' = '3',",
                    "    'dynamic_partition.prefix' = 'p',",
                    "    'dynamic_partition.buckets' = '16',",
                    "    'storage_medium' = 'SSD',",
                    "    'storage_cooldown_time' = '2024-02-01 00:00:00'",
                    ");");
            ddls.add(duplicateTable);
            System.out.println("  📋 Duplicate: user_behavior_log (动态分区/SSD-HDD 冷热分离)");

            // ===== 2. Aggregate Model (聚合模型) - 实时指标汇总 =====
            String aggregateTable = String.join("\n",
                    "-- ========================================",
                    "-- Aggregate Model: 实时指标聚合 (预计算)",
                    "-- ========================================",
                    "CREATE TABLE IF NOT EXISTS realtime_metrics_agg (",
                    "    `dt`                DATE            NOT NULL COMMENT '日期',",
                    "    `hour`              TINYINT         NOT NULL COMMENT '小时',",
                    "    `channel`           VARCHAR(32)     NOT NULL COMMENT '渠道',",
                    "    `category_id`       INT             NOT NULL COMMENT '品类ID',",
                    "    `pv`                BIGINT          SUM      COMMENT '页面浏览量',",
                    "    `uv`                BITMAP          BITMAP_UNION COMMENT '独立访客(精确去重)',",
                    "    `click_count`       BIGINT          SUM      COMMENT '点击次数',",
                    "    `cart_count`        BIGINT          SUM      COMMENT '加购次数',",
                    "    `order_count`       BIGINT          SUM      COMMENT '下单次数',",
                    "    `order_amount`      DECIMAL(18,2)   SUM      COMMENT '下单金额',",
                    "    `pay_count`         BIGINT          SUM      COMMENT '支付次数',",
                    "    `pay_amount`        DECIMAL(18,2)   SUM      COMMENT '支付金额',",
                    "    `max_duration`      INT             MAX      COMMENT '最大停留时长',",
                    "    `min_duration`      INT             MIN      COMMENT '最小停留时长',",
                    "    `user_set`          HLL             HLL_UNION COMMENT '用户集合(近似去重)'",
                    ") ENGINE=OLAP",
                    "AGGREGATE KEY(`dt`, `hour`, `channel`, `category_id`)",
                    "COMMENT '实时指标聚合表 (Aggregate Model - 预聚合)'",
                    "PARTITION BY RANGE(`dt`) (",
                    "    PARTITION p202401 VALUES LESS THAN ('2024-02-01'),",
                    "    PARTITION p202402 VALUES LESS THAN ('2024-03-01')",
                    ")",
                    "DISTRIBUTED BY HASH(`channel`, `category_id`) BUCKETS 8",
                    "PROPERTIES ('replication_num' = '3');");
            ddls.add(aggregateTable);
            System.out.println("  📋 Aggregate: realtime_metrics_agg (BITMAP精确去重 + HLL近似去重)");

            // ===== 3. Unique Model (唯一模型) - 用户画像 =====
            String uniqueTable = String.join("\n",
                    "-- ========================================",
                    "-- Unique Model: 用户画像 (主键唯一/实时更新)",
                    "-- ========================================",
                    "CREATE TABLE IF NOT EXISTS user_profile (",
                    "    `user_id`           BIGINT          NOT NULL COMMENT '用户ID',",
                    "    `username`          VARCHAR(64)     COMMENT '用户名',",
                    "    `gender`            TINYINT         COMMENT '性别: 0女 1男',",
                    "    `age`               TINYINT         COMMENT '年龄',",
                    "    `city`              VARCHAR(32)     COMMENT '城市',",
                    "    `vip_level`         TINYINT         COMMENT 'VIP等级',",
                    "    `reg_channel`       VARCHAR(32)     COMMENT '注册渠道',",
                    "    `total_orders`      INT             COMMENT '累计订单数',",
                    "    `total_amount`      DECIMAL(18,2)   COMMENT '累计消费金额',",
                    "    `last_login_time`   DATETIME        COMMENT '最后登录时间',",
                    "    `last_order_time`   DATETIME        COMMENT '最后下单时间',",
                    "    `rfm_score`         DECIMAL(6,2)    COMMENT 'RFM 综合评分',",
                    "    `user_tags`         VARCHAR(512)    COMMENT '用户标签(JSON)',",
                    "    `churn_probability` DECIMAL(4,3)    COMMENT '流失概率',",
                    "    `update_time`       DATETIME        DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间'",
                    ") ENGINE=OLAP",
                    "UNIQUE KEY(`user_id`)",
                    "COMMENT '用户画像表 (Unique Model - 实时Upsert)'",
                    "DISTRIBUTED BY HASH(`user_id`) BUCKETS 16",
                    "PROPERTIES (",
                    "    'replication_num' = '3',",
                    "    'enable_unique_key_merge_on_write' = 'true'",
                    ");");
            ddls.add(uniqueTable);
            System.out.println("  📋 Unique: user_profile (Merge-On-Write 实时更新)");

            return ddls;
        }

        /**
         * 创建物化视图 (Materialized View)
         */
        public List<String> generateMaterializedViews() {
            List<String> mvDDLs = new ArrayList<>();

            System.out.println("\n📐 创建物化视图...\n");

            // 同步物化视图 - 按小时汇总 PV/UV
            String syncMV = String.join("\n",
                    "-- 同步物化视图: 按小时/渠道汇总 (自动维护)",
                    "CREATE MATERIALIZED VIEW IF NOT EXISTS mv_hourly_channel_stats",
                    "AS",
                    "SELECT",
                    "    DATE_FORMAT(event_time, '%Y-%m-%d %H:00:00') AS hour_key,",
                    "    channel,",
                    "    COUNT(*) AS pv,",
                    "    COUNT(DISTINCT user_id) AS uv,",
                    "    SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) AS clicks,",
                    "    SUM(CASE WHEN event_type = 'buy' THEN 1 ELSE 0 END) AS buys,",
                    "    AVG(duration_ms) AS avg_duration",
                    "FROM user_behavior_log",
                    "GROUP BY",
                    "    DATE_FORMAT(event_time, '%Y-%m-%d %H:00:00'),",
                    "    channel;");
            mvDDLs.add(syncMV);
            System.out.println("  📊 同步MV: mv_hourly_channel_stats (小时/渠道 汇总)");

            // 异步物化视图 - 跨表关联的宽表
            String asyncMV = String.join("\n",
                    "-- 异步物化视图: 用户行为 + 画像 关联宽表",
                    "CREATE MATERIALIZED VIEW IF NOT EXISTS mv_user_behavior_profile",
                    "REFRESH ASYNC EVERY (INTERVAL 5 MINUTE)",
                    "AS",
                    "SELECT",
                    "    b.event_time,",
                    "    b.user_id,",
                    "    b.event_type,",
                    "    b.item_id,",
                    "    b.channel,",
                    "    p.gender,",
                    "    p.age,",
                    "    p.city,",
                    "    p.vip_level,",
                    "    p.rfm_score",
                    "FROM user_behavior_log b",
                    "LEFT JOIN user_profile p ON b.user_id = p.user_id;");
            mvDDLs.add(asyncMV);
            System.out.println("  📊 异步MV: mv_user_behavior_profile (5分钟刷新/行为+画像宽表)");

            return mvDDLs;
        }

        /**
         * 数据导入方式
         */
        public Map<String, String> generateLoadCommands() {
            Map<String, String> commands = new LinkedHashMap<>();

            System.out.println("\n📥 数据导入方式...\n");

            // 1. Stream Load (实时写入)
            commands.put("Stream Load (实时/HTTP)", String.join("\n",
                    "# Stream Load - 实时小批量写入 (推荐 <1GB)",
                    "curl -u root: -H 'format: json' \\",
                    "  -H 'strip_outer_array: true' \\",
                    "  -H 'max_filter_ratio: 0.1' \\",
                    "  -T /data/user_behavior.json \\",
                    "  http://" + feHost + ":8040/api/bigdata_olap/user_behavior_log/_stream_load"
            ));
            System.out.println("  📥 Stream Load: HTTP 实时写入 (毫秒级延迟)");

            // 2. Routine Load (Kafka 持续消费)
            commands.put("Routine Load (Kafka)", String.join("\n",
                    "-- Routine Load - 持续消费 Kafka",
                    "CREATE ROUTINE LOAD bigdata_olap.kafka_user_behavior",
                    "ON user_behavior_log",
                    "COLUMNS TERMINATED BY ','",
                    "PROPERTIES (",
                    "    'format' = 'json',",
                    "    'jsonpaths' = '[\"$.event_time\",\"$.user_id\",\"$.event_type\",",
                    "                   \"$.page_id\",\"$.item_id\",\"$.category_id\",",
                    "                   \"$.channel\",\"$.device_type\",\"$.city\"]',",
                    "    'max_error_number' = '1000',",
                    "    'max_batch_interval' = '10',",
                    "    'max_batch_rows' = '200000',",
                    "    'desired_concurrent_number' = '3'",
                    ")",
                    "FROM KAFKA (",
                    "    'kafka_broker_list' = 'kafka:9092',",
                    "    'kafka_topic' = 'user_events',",
                    "    'property.group.id' = 'doris_consumer_group',",
                    "    'property.kafka_default_offsets' = 'OFFSET_END'",
                    ");"
            ));
            System.out.println("  📥 Routine Load: Kafka → Doris 持续导入");

            // 3. Broker Load (HDFS 批量导入)
            commands.put("Broker Load (HDFS)", String.join("\n",
                    "-- Broker Load - HDFS 大批量导入 (推荐 >1GB)",
                    "LOAD LABEL bigdata_olap.load_user_behavior_20240101",
                    "(",
                    "    DATA INFILE('hdfs://namenode:8020/data/warehouse/ods/user_behavior/dt=2024-01-01/*')",
                    "    INTO TABLE user_behavior_log",
                    "    FORMAT AS 'parquet'",
                    "    (event_time, user_id, event_type, page_id, item_id,",
                    "     category_id, channel, device_type, city, session_id, duration_ms, referrer)",
                    ")",
                    "WITH BROKER 'hdfs_broker'",
                    "PROPERTIES (",
                    "    'timeout' = '3600',",
                    "    'max_filter_ratio' = '0.05'",
                    ");"
            ));
            System.out.println("  📥 Broker Load: HDFS → Doris 批量导入 (T+1)");

            // 4. INSERT INTO SELECT (内部ETL)
            commands.put("INSERT SELECT (内部ETL)", String.join("\n",
                    "-- INSERT INTO SELECT - 内部 ETL 加工",
                    "INSERT INTO realtime_metrics_agg",
                    "SELECT",
                    "    DATE(event_time) AS dt,",
                    "    HOUR(event_time) AS hour,",
                    "    channel,",
                    "    category_id,",
                    "    COUNT(*) AS pv,",
                    "    BITMAP_UNION(TO_BITMAP(user_id)) AS uv,",
                    "    SUM(CASE WHEN event_type='click' THEN 1 ELSE 0 END),",
                    "    SUM(CASE WHEN event_type='cart' THEN 1 ELSE 0 END),",
                    "    SUM(CASE WHEN event_type='buy' THEN 1 ELSE 0 END),",
                    "    SUM(CASE WHEN event_type='buy' THEN amount ELSE 0 END),",
                    "    SUM(CASE WHEN event_type='buy' THEN 1 ELSE 0 END),",
                    "    SUM(CASE WHEN event_type='buy' THEN amount ELSE 0 END),",
                    "    MAX(duration_ms),",
                    "    MIN(duration_ms),",
                    "    HLL_HASH(user_id)",
                    "FROM user_behavior_log",
                    "WHERE DATE(event_time) = CURDATE()",
                    "GROUP BY DATE(event_time), HOUR(event_time), channel, category_id;"
            ));
            System.out.println("  📥 INSERT SELECT: 内部 ETL 加工汇总");

            return commands;
        }
    }

    // ==================== 2. StarRocks 高级功能 ====================

    /**
     * StarRocks 高级特性 - Primary Key / 向量化 / CBO / Pipeline
     */
    static class StarRocksAdvancedEngine {

        /**
         * StarRocks Primary Key 表 (实时 Upsert)
         */
        public List<String> generateStarRocksDDL() {
            List<String> ddls = new ArrayList<>();

            System.out.println("\n📐 StarRocks 高级建模...\n");

            // Primary Key 表 - 订单实时更新
            String pkTable = String.join("\n",
                    "-- ========================================",
                    "-- StarRocks Primary Key: 订单表 (实时 Upsert)",
                    "-- ========================================",
                    "CREATE TABLE IF NOT EXISTS sr_order_realtime (",
                    "    `order_id`          BIGINT          NOT NULL COMMENT '订单ID',",
                    "    `user_id`           BIGINT          NOT NULL COMMENT '用户ID',",
                    "    `order_status`      TINYINT         COMMENT '订单状态: 0待付/1已付/2发货/3完成/4退款',",
                    "    `total_amount`      DECIMAL(18,2)   COMMENT '订单金额',",
                    "    `pay_amount`        DECIMAL(18,2)   COMMENT '实付金额',",
                    "    `coupon_amount`     DECIMAL(18,2)   COMMENT '优惠金额',",
                    "    `pay_type`          TINYINT         COMMENT '支付方式: 1微信/2支付宝/3银行卡',",
                    "    `channel`           VARCHAR(32)     COMMENT '渠道',",
                    "    `city`              VARCHAR(32)     COMMENT '城市',",
                    "    `create_time`       DATETIME        NOT NULL COMMENT '创建时间',",
                    "    `update_time`       DATETIME        COMMENT '更新时间',",
                    "    INDEX idx_user (user_id) USING BITMAP,",
                    "    INDEX idx_status (order_status) USING BITMAP,",
                    "    INDEX idx_city (city) USING BITMAP",
                    ") ENGINE=OLAP",
                    "PRIMARY KEY(`order_id`)",
                    "PARTITION BY RANGE(`create_time`) (",
                    "    PARTITION p20240101 VALUES LESS THAN ('2024-01-02'),",
                    "    PARTITION p20240102 VALUES LESS THAN ('2024-01-03')",
                    ")",
                    "DISTRIBUTED BY HASH(`order_id`) BUCKETS 16",
                    "ORDER BY(`user_id`, `create_time`)",
                    "PROPERTIES (",
                    "    'replication_num' = '3',",
                    "    'enable_persistent_index' = 'true',",
                    "    'bloom_filter_columns' = 'order_id, user_id'",
                    ");");
            ddls.add(pkTable);
            System.out.println("  📋 Primary Key: sr_order_realtime (持久化索引 + Bloom Filter)");

            // Colocate Group - 加速 Join
            String colocateTable = String.join("\n",
                    "-- ========================================",
                    "-- Colocate Join: 订单明细 (与订单表同分布)",
                    "-- ========================================",
                    "CREATE TABLE IF NOT EXISTS sr_order_detail (",
                    "    `detail_id`         BIGINT          NOT NULL COMMENT '明细ID',",
                    "    `order_id`          BIGINT          NOT NULL COMMENT '订单ID',",
                    "    `item_id`           BIGINT          COMMENT '商品ID',",
                    "    `item_name`         VARCHAR(128)    COMMENT '商品名',",
                    "    `quantity`          INT             COMMENT '数量',",
                    "    `unit_price`        DECIMAL(10,2)   COMMENT '单价',",
                    "    `total_price`       DECIMAL(18,2)   COMMENT '小计',",
                    "    `create_time`       DATETIME        NOT NULL COMMENT '创建时间'",
                    ") ENGINE=OLAP",
                    "PRIMARY KEY(`detail_id`)",
                    "PARTITION BY RANGE(`create_time`) (",
                    "    PARTITION p20240101 VALUES LESS THAN ('2024-01-02')",
                    ")",
                    "DISTRIBUTED BY HASH(`order_id`) BUCKETS 16",
                    "PROPERTIES (",
                    "    'replication_num' = '3',",
                    "    'colocate_with' = 'order_group'",
                    ");");
            ddls.add(colocateTable);
            System.out.println("  📋 Colocate: sr_order_detail (同分布本地Join/零Shuffle)");

            return ddls;
        }

        /**
         * 湖仓一体 - External Catalog
         */
        public List<String> generateExternalCatalogs() {
            List<String> catalogs = new ArrayList<>();

            System.out.println("\n🏠 湖仓一体 - External Catalog...\n");

            // Hive Catalog
            catalogs.add(String.join("\n",
                    "-- Hive External Catalog (查询 Hive 数据仓库)",
                    "CREATE EXTERNAL CATALOG hive_catalog",
                    "PROPERTIES (",
                    "    'type' = 'hive',",
                    "    'hive.metastore.uris' = 'thrift://hive-metastore:9083',",
                    "    'hive.metastore.type' = 'hive'",
                    ");",
                    "",
                    "-- 使用示例: 直接查询 Hive 表",
                    "SELECT * FROM hive_catalog.dw_db.dim_user LIMIT 100;"
            ));
            System.out.println("  🔗 Hive Catalog: 直接查询 Hive Metastore 表");

            // Iceberg Catalog
            catalogs.add(String.join("\n",
                    "-- Iceberg External Catalog",
                    "CREATE EXTERNAL CATALOG iceberg_catalog",
                    "PROPERTIES (",
                    "    'type' = 'iceberg',",
                    "    'iceberg.catalog.type' = 'hive',",
                    "    'hive.metastore.uris' = 'thrift://hive-metastore:9083'",
                    ");",
                    "",
                    "-- Time Travel 查询",
                    "SELECT * FROM iceberg_catalog.lakehouse.user_events",
                    "FOR VERSION AS OF 12345678;"
            ));
            System.out.println("  🔗 Iceberg Catalog: Time Travel + Schema Evolution");

            // Hudi Catalog
            catalogs.add(String.join("\n",
                    "-- Hudi External Catalog",
                    "CREATE EXTERNAL CATALOG hudi_catalog",
                    "PROPERTIES (",
                    "    'type' = 'hudi',",
                    "    'hive.metastore.uris' = 'thrift://hive-metastore:9083'",
                    ");",
                    "",
                    "-- 增量查询",
                    "SELECT * FROM hudi_catalog.cdc_db.order_changelog",
                    "WHERE _hoodie_commit_time > '20240101120000';"
            ));
            System.out.println("  🔗 Hudi Catalog: 增量查询 CDC 变更日志");

            // Delta Lake Catalog
            catalogs.add(String.join("\n",
                    "-- Delta Lake External Catalog",
                    "CREATE EXTERNAL CATALOG delta_catalog",
                    "PROPERTIES (",
                    "    'type' = 'deltalake',",
                    "    'hive.metastore.uris' = 'thrift://hive-metastore:9083'",
                    ");",
                    "",
                    "-- 查询 Delta Lake 表",
                    "SELECT * FROM delta_catalog.medallion.gold_sales_summary;"
            ));
            System.out.println("  🔗 Delta Lake Catalog: 查询 Medallion 架构数据");

            return catalogs;
        }
    }

    // ==================== 3. 实时多维分析引擎 ====================

    /**
     * 实时分析查询集 - 漏斗/留存/路径/TopN/同环比
     */
    static class RealtimeAnalyticsEngine {

        /**
         * 生成实时分析 SQL 集
         */
        public Map<String, String> generateAnalyticsQueries() {
            Map<String, String> queries = new LinkedHashMap<>();

            System.out.println("\n📊 实时多维分析查询...\n");

            // 1. 实时大盘 - 核心指标
            queries.put("实时大盘", String.join("\n",
                    "-- ========================================",
                    "-- 实时大盘: 当日核心经营指标",
                    "-- ========================================",
                    "SELECT",
                    "    DATE(NOW()) AS dt,",
                    "    -- 流量指标",
                    "    COUNT(*) AS total_pv,",
                    "    COUNT(DISTINCT user_id) AS total_uv,",
                    "    -- 转化指标",
                    "    SUM(CASE WHEN event_type='click' THEN 1 ELSE 0 END) AS clicks,",
                    "    SUM(CASE WHEN event_type='cart' THEN 1 ELSE 0 END) AS cart_adds,",
                    "    SUM(CASE WHEN event_type='buy' THEN 1 ELSE 0 END) AS orders,",
                    "    -- 转化率",
                    "    ROUND(SUM(CASE WHEN event_type='click' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS click_rate,",
                    "    ROUND(SUM(CASE WHEN event_type='buy' THEN 1 ELSE 0 END) * 100.0",
                    "        / NULLIF(SUM(CASE WHEN event_type='click' THEN 1 ELSE 0 END), 0), 2) AS buy_rate,",
                    "    -- 时间维度",
                    "    HOUR(NOW()) AS current_hour,",
                    "    ROUND(COUNT(*) * 24.0 / (HOUR(NOW()) + 1), 0) AS predicted_daily_pv",
                    "FROM user_behavior_log",
                    "WHERE DATE(event_time) = DATE(NOW());"
            ));
            System.out.println("  📊 实时大盘: PV/UV/转化率/预估日总量");

            // 2. 转化漏斗分析
            queries.put("转化漏斗", String.join("\n",
                    "-- ========================================",
                    "-- 转化漏斗: 浏览 → 点击 → 加购 → 下单 → 支付",
                    "-- ========================================",
                    "WITH funnel_data AS (",
                    "    SELECT",
                    "        user_id,",
                    "        MAX(CASE WHEN event_type = 'pv' THEN 1 ELSE 0 END) AS step1_view,",
                    "        MAX(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) AS step2_click,",
                    "        MAX(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) AS step3_cart,",
                    "        MAX(CASE WHEN event_type = 'buy' THEN 1 ELSE 0 END) AS step4_buy",
                    "    FROM user_behavior_log",
                    "    WHERE DATE(event_time) = DATE(NOW())",
                    "    GROUP BY user_id",
                    ")",
                    "SELECT",
                    "    '浏览' AS step, COUNT(*) AS users, 100.0 AS rate",
                    "FROM funnel_data WHERE step1_view = 1",
                    "UNION ALL",
                    "SELECT '点击', COUNT(*),",
                    "    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM funnel_data WHERE step1_view=1), 1)",
                    "FROM funnel_data WHERE step2_click = 1",
                    "UNION ALL",
                    "SELECT '加购', COUNT(*),",
                    "    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM funnel_data WHERE step1_view=1), 1)",
                    "FROM funnel_data WHERE step3_cart = 1",
                    "UNION ALL",
                    "SELECT '下单', COUNT(*),",
                    "    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM funnel_data WHERE step1_view=1), 1)",
                    "FROM funnel_data WHERE step4_buy = 1;"
            ));
            System.out.println("  📊 转化漏斗: 4步漏斗分析 (浏览→点击→加购→下单)");

            // 3. 用户留存分析
            queries.put("用户留存", String.join("\n",
                    "-- ========================================",
                    "-- 用户留存分析: 次日/3日/7日/30日留存率",
                    "-- ========================================",
                    "WITH first_visit AS (",
                    "    SELECT user_id, MIN(DATE(event_time)) AS first_date",
                    "    FROM user_behavior_log",
                    "    GROUP BY user_id",
                    "),",
                    "retention_calc AS (",
                    "    SELECT",
                    "        f.first_date,",
                    "        f.user_id,",
                    "        DATEDIFF(DATE(b.event_time), f.first_date) AS day_diff",
                    "    FROM first_visit f",
                    "    JOIN user_behavior_log b ON f.user_id = b.user_id",
                    ")",
                    "SELECT",
                    "    first_date,",
                    "    COUNT(DISTINCT user_id) AS new_users,",
                    "    COUNT(DISTINCT CASE WHEN day_diff = 1 THEN user_id END) AS day1_retained,",
                    "    COUNT(DISTINCT CASE WHEN day_diff = 3 THEN user_id END) AS day3_retained,",
                    "    COUNT(DISTINCT CASE WHEN day_diff = 7 THEN user_id END) AS day7_retained,",
                    "    COUNT(DISTINCT CASE WHEN day_diff = 30 THEN user_id END) AS day30_retained,",
                    "    ROUND(COUNT(DISTINCT CASE WHEN day_diff=1 THEN user_id END)*100.0",
                    "        / COUNT(DISTINCT user_id), 1) AS day1_rate,",
                    "    ROUND(COUNT(DISTINCT CASE WHEN day_diff=7 THEN user_id END)*100.0",
                    "        / COUNT(DISTINCT user_id), 1) AS day7_rate",
                    "FROM retention_calc",
                    "GROUP BY first_date",
                    "ORDER BY first_date DESC",
                    "LIMIT 30;"
            ));
            System.out.println("  📊 用户留存: 次日/3日/7日/30日留存率");

            // 4. 实时 TopN 排行
            queries.put("实时TopN排行", String.join("\n",
                    "-- ========================================",
                    "-- 实时 Top 排行: 热门商品 / 热门品类 / 热门城市",
                    "-- ========================================",
                    "-- 热销商品 Top20",
                    "SELECT",
                    "    item_id,",
                    "    COUNT(CASE WHEN event_type='buy' THEN 1 END) AS buy_count,",
                    "    COUNT(CASE WHEN event_type='click' THEN 1 END) AS click_count,",
                    "    COUNT(DISTINCT user_id) AS buyer_count,",
                    "    ROUND(COUNT(CASE WHEN event_type='buy' THEN 1 END) * 100.0 /",
                    "        NULLIF(COUNT(CASE WHEN event_type='click' THEN 1 END), 0), 2) AS convert_rate",
                    "FROM user_behavior_log",
                    "WHERE DATE(event_time) = DATE(NOW())",
                    "    AND item_id IS NOT NULL",
                    "GROUP BY item_id",
                    "ORDER BY buy_count DESC",
                    "LIMIT 20;"
            ));
            System.out.println("  📊 实时TopN: 热销商品/品类/城市排行");

            // 5. 同环比分析
            queries.put("同环比分析", String.join("\n",
                    "-- ========================================",
                    "-- 同环比分析: 日/周/月 同比 & 环比",
                    "-- ========================================",
                    "WITH daily_metrics AS (",
                    "    SELECT",
                    "        DATE(event_time) AS dt,",
                    "        COUNT(*) AS pv,",
                    "        COUNT(DISTINCT user_id) AS uv,",
                    "        SUM(CASE WHEN event_type='buy' THEN 1 ELSE 0 END) AS orders",
                    "    FROM user_behavior_log",
                    "    WHERE event_time >= DATE_SUB(NOW(), INTERVAL 60 DAY)",
                    "    GROUP BY DATE(event_time)",
                    ")",
                    "SELECT",
                    "    t.dt,",
                    "    t.pv,",
                    "    t.uv,",
                    "    t.orders,",
                    "    -- 日环比 (与昨日对比)",
                    "    ROUND((t.pv - y.pv) * 100.0 / NULLIF(y.pv, 0), 1) AS pv_dod,",
                    "    ROUND((t.orders - y.orders) * 100.0 / NULLIF(y.orders, 0), 1) AS orders_dod,",
                    "    -- 周同比 (与上周同日对比)",
                    "    ROUND((t.pv - w.pv) * 100.0 / NULLIF(w.pv, 0), 1) AS pv_wow",
                    "FROM daily_metrics t",
                    "LEFT JOIN daily_metrics y ON y.dt = DATE_SUB(t.dt, INTERVAL 1 DAY)",
                    "LEFT JOIN daily_metrics w ON w.dt = DATE_SUB(t.dt, INTERVAL 7 DAY)",
                    "ORDER BY t.dt DESC",
                    "LIMIT 14;"
            ));
            System.out.println("  📊 同环比: 日环比/周同比 PV/UV/订单趋势");

            // 6. 用户路径分析
            queries.put("用户路径分析", String.join("\n",
                    "-- ========================================",
                    "-- 用户路径分析: 行为序列挖掘",
                    "-- ========================================",
                    "WITH user_paths AS (",
                    "    SELECT",
                    "        user_id,",
                    "        session_id,",
                    "        GROUP_CONCAT(event_type ORDER BY event_time SEPARATOR ' → ') AS path,",
                    "        COUNT(*) AS path_length,",
                    "        MAX(CASE WHEN event_type='buy' THEN 1 ELSE 0 END) AS has_conversion",
                    "    FROM user_behavior_log",
                    "    WHERE DATE(event_time) = DATE(NOW())",
                    "        AND session_id IS NOT NULL",
                    "    GROUP BY user_id, session_id",
                    ")",
                    "SELECT",
                    "    path,",
                    "    COUNT(*) AS sessions,",
                    "    SUM(has_conversion) AS conversions,",
                    "    ROUND(SUM(has_conversion)*100.0/COUNT(*), 1) AS conversion_rate,",
                    "    ROUND(AVG(path_length), 1) AS avg_steps",
                    "FROM user_paths",
                    "GROUP BY path",
                    "HAVING COUNT(*) >= 10",
                    "ORDER BY sessions DESC",
                    "LIMIT 20;"
            ));
            System.out.println("  📊 路径分析: 高频行为路径 + 转化率");

            return queries;
        }
    }

    // ==================== 4. OLAP 性能优化 ====================

    /**
     * OLAP 查询性能优化策略
     */
    static class OlapPerformanceOptimizer {

        /**
         * 性能优化建议
         */
        public void generateOptimizationGuide() {
            System.out.println("\n⚡ OLAP 性能优化最佳实践\n");

            System.out.println("┌────────────────────────────────────────────────────────────┐");
            System.out.println("│  优化维度        │  策略                                    │");
            System.out.println("├────────────────────────────────────────────────────────────┤");

            // 1. 分区裁剪
            System.out.println("│  分区裁剪        │  按日期分区 + 动态分区自动管理            │");
            System.out.println("│                  │  查询必带分区条件 (dt=xxxx)              │");

            // 2. Bucket 优化
            System.out.println("│  分桶策略        │  高频 Join 字段做分桶键                  │");
            System.out.println("│                  │  桶数 = 数据量(GB) × 2 (建议8-128)      │");

            // 3. Colocate Join
            System.out.println("│  Colocate Join   │  关联表同分桶键/同桶数/同副本            │");
            System.out.println("│                  │  避免 Shuffle, 本地 Join                 │");

            // 4. 索引
            System.out.println("│  索引优化        │  BITMAP: 低基数字段 (status/gender)      │");
            System.out.println("│                  │  Bloom Filter: 高基数 (user_id/order_id) │");
            System.out.println("│                  │  前缀索引: 排序键 (sort key)             │");

            // 5. 物化视图
            System.out.println("│  物化视图        │  高频聚合查询创建同步 MV                  │");
            System.out.println("│                  │  跨表 Join 创建异步 MV                   │");
            System.out.println("│                  │  查询自动改写 (Query Rewrite)            │");

            // 6. 查询优化
            System.out.println("│  查询调优        │  避免 SELECT * , 只查必要列              │");
            System.out.println("│                  │  Runtime Filter 自动下推                 │");
            System.out.println("│                  │  Pipeline 并行执行引擎                   │");

            // 7. 冷热分离
            System.out.println("│  冷热分离        │  近期数据: SSD (高速查询)                │");
            System.out.println("│                  │  历史数据: HDD (降低成本)                │");
            System.out.println("│                  │  storage_cooldown_time 自动迁移          │");

            System.out.println("└────────────────────────────────────────────────────────────┘");

            // 生成优化 SQL
            System.out.println("\n--- 性能优化 SQL 示例 ---\n");

            System.out.println("-- 1. 查看查询 Profile (慢查询分析)");
            System.out.println("SHOW QUERY PROFILE '/query_id';");
            System.out.println("ANALYZE PROFILE '/query_id';\n");

            System.out.println("-- 2. 查看执行计划");
            System.out.println("EXPLAIN ANALYZE SELECT * FROM user_behavior_log WHERE dt='2024-01-01';\n");

            System.out.println("-- 3. 修改 Session 变量优化");
            System.out.println("SET parallel_fragment_exec_instance_num = 8;");
            System.out.println("SET query_timeout = 300;");
            System.out.println("SET enable_vectorized_engine = true;");
            System.out.println("SET batch_size = 4096;");
            System.out.println("SET enable_runtime_filter = true;\n");

            System.out.println("-- 4. 表数据均衡检查");
            System.out.println("SHOW DATA FROM user_behavior_log;");
            System.out.println("ADMIN SHOW REPLICA DISTRIBUTION FROM user_behavior_log;\n");

            System.out.println("-- 5. Compaction 手动触发");
            System.out.println("-- 当写入频繁导致小文件过多时");
            System.out.println("ALTER TABLE user_behavior_log COMPACT;");
        }
    }

    // ==================== Main 方法 ====================

    public static void main(String[] args) {
        System.out.println("╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║       实时 OLAP 引擎实战 - Apache Doris + StarRocks          ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝\n");

        // ===== 1. Doris 数据模型 =====
        System.out.println("━━━━━━ 1. Doris 三种表模型 ━━━━━━\n");
        DorisModelManager dorisManager = new DorisModelManager("doris-fe", 9030);

        List<String> tableDDLs = dorisManager.generateTableDDL();
        System.out.println("\n📝 共生成 " + tableDDLs.size() + " 条 DDL:");
        tableDDLs.forEach(ddl -> {
            String firstLine = ddl.split("\n")[0];
            if (firstLine.startsWith("--")) System.out.println("  " + firstLine);
        });

        // ===== 2. 物化视图 =====
        System.out.println("\n━━━━━━ 2. 物化视图 ━━━━━━");
        List<String> mvDDLs = dorisManager.generateMaterializedViews();

        // ===== 3. 数据导入 =====
        System.out.println("\n━━━━━━ 3. 数据导入方式 ━━━━━━");
        Map<String, String> loadCmds = dorisManager.generateLoadCommands();
        System.out.println("\n📥 共 " + loadCmds.size() + " 种导入方式");

        // ===== 4. StarRocks 高级功能 =====
        System.out.println("\n━━━━━━ 4. StarRocks 高级特性 ━━━━━━");
        StarRocksAdvancedEngine srEngine = new StarRocksAdvancedEngine();

        List<String> srDDLs = srEngine.generateStarRocksDDL();
        List<String> catalogs = srEngine.generateExternalCatalogs();
        System.out.println("\n📝 共生成 " + srDDLs.size() + " 张 SR 表, " + catalogs.size() + " 个 External Catalog");

        // ===== 5. 实时多维分析 =====
        System.out.println("\n━━━━━━ 5. 实时多维分析查询 ━━━━━━");
        RealtimeAnalyticsEngine analyticsEngine = new RealtimeAnalyticsEngine();

        Map<String, String> analytics = analyticsEngine.generateAnalyticsQueries();
        System.out.println("\n📊 共生成 " + analytics.size() + " 个分析场景");

        // ===== 6. 性能优化 =====
        System.out.println("\n━━━━━━ 6. 性能优化指南 ━━━━━━");
        OlapPerformanceOptimizer optimizer = new OlapPerformanceOptimizer();
        optimizer.generateOptimizationGuide();

        // ===== 总结 =====
        System.out.println("\n" + "=".repeat(60));
        System.out.println("📊 实时 OLAP 引擎总结:");
        System.out.println("  Doris 表模型: Duplicate(明细) + Aggregate(聚合) + Unique(去重)");
        System.out.println("  StarRocks: Primary Key(实时更新) + Colocate(本地Join)");
        System.out.println("  物化视图: 同步MV(自动维护) + 异步MV(定时刷新)");
        System.out.println("  数据导入: Stream Load(实时) + Routine Load(Kafka) + Broker Load(HDFS)");
        System.out.println("  湖仓一体: Hive/Iceberg/Hudi/Delta Lake External Catalog");
        System.out.println("  分析场景: 实时大盘/漏斗/留存/TopN/同环比/路径分析");
        System.out.println("=".repeat(60));

        System.out.println("\n🎉 实时 OLAP 引擎演示完成!");
    }
}
