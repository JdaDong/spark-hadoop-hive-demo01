package com.bigdata.flink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import static org.apache.flink.table.api.Expressions.*;

/**
 * Flink Table API & SQL 高级示例
 * 
 * 涵盖内容:
 * 1. Table API 基础操作 (创建表、投影、过滤、聚合)
 * 2. SQL 查询 (DDL、DML、复杂查询)
 * 3. 窗口函数 (Tumble、Hop、Session、Cumulate)
 * 4. Temporal Join (时态表连接)
 * 5. Pattern Recognition (MATCH_RECOGNIZE)
 * 6. Kafka Source & Sink
 * 7. ClickHouse Sink
 * 8. Upsert Kafka
 * 9. Mini-Batch 优化
 * 10. 自定义函数 (UDF/UDAF/UDTF)
 */
public class FlinkTableApiJavaApp {

    public static void main(String[] args) throws Exception {
        System.out.println("=== Flink Table API & SQL 高级示例 ===\n");

        // 1. 创建执行环境
        createEnvironmentDemo();

        // 2. Table API 基础操作
        tableApiBasicsDemo();

        // 3. SQL DDL 操作
        sqlDDLDemo();

        // 4. 窗口函数
        windowFunctionsDemo();

        // 5. Temporal Join
        temporalJoinDemo();

        // 6. Pattern Recognition
        patternRecognitionDemo();

        // 7. Kafka Source & Sink
        kafkaIntegrationDemo();

        // 8. ClickHouse Sink
        clickhouseSinkDemo();

        // 9. Mini-Batch 优化
        miniBatchOptimizationDemo();

        // 10. 自定义函数
        userDefinedFunctionsDemo();

        System.out.println("\n=== 所有示例完成 ===");
    }

    // ==================== 1. 创建执行环境 ====================
    static void createEnvironmentDemo() {
        System.out.println("--- 1. 创建 Flink Table 执行环境 ---");

        // 方式1: 基于 StreamExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 方式2: 纯 Table API 环境 (批处理)
        TableEnvironment batchTableEnv = TableEnvironment.create(
            EnvironmentSettings.newInstance()
                .inBatchMode()
                .build()
        );

        // 方式3: 流模式 Table 环境
        TableEnvironment streamTableEnv = TableEnvironment.create(
            EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build()
        );

        // 配置参数
        tableEnv.getConfig().set("table.exec.state.ttl", "3600000");         // 状态过期时间
        tableEnv.getConfig().set("table.exec.mini-batch.enabled", "true");   // 启用 mini-batch
        tableEnv.getConfig().set("table.exec.mini-batch.allow-latency", "5s"); // mini-batch 延迟
        tableEnv.getConfig().set("table.exec.mini-batch.size", "5000");      // mini-batch 大小
        tableEnv.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE"); // 两阶段聚合
        tableEnv.getConfig().set("table.optimizer.distinct-agg.split.enabled", "true"); // 去重聚合拆分

        System.out.println("  执行环境创建成功");
        System.out.println("  - 流模式 StreamTableEnvironment");
        System.out.println("  - 批模式 TableEnvironment");
        System.out.println("  - Mini-batch 优化已启用\n");
    }

    // ==================== 2. Table API 基础操作 ====================
    static void tableApiBasicsDemo() {
        System.out.println("--- 2. Table API 基础操作 ---");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 使用 SQL 创建数据源表
        tableEnv.executeSql(
            "CREATE TABLE orders (" +
            "  order_id BIGINT," +
            "  user_id BIGINT," +
            "  product STRING," +
            "  amount DECIMAL(10, 2)," +
            "  order_time TIMESTAMP(3)," +
            "  WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND" +
            ") WITH (" +
            "  'connector' = 'datagen'," +
            "  'rows-per-second' = '10'," +
            "  'fields.order_id.kind' = 'sequence'," +
            "  'fields.order_id.start' = '1'," +
            "  'fields.order_id.end' = '1000'," +
            "  'fields.user_id.min' = '1'," +
            "  'fields.user_id.max' = '100'," +
            "  'fields.amount.min' = '10'," +
            "  'fields.amount.max' = '1000'" +
            ")"
        );

        // Table API 操作
        Table orders = tableEnv.from("orders");

        // 投影 (SELECT)
        Table projected = orders.select(
            $("order_id"),
            $("user_id"),
            $("product"),
            $("amount"),
            $("order_time")
        );

        // 过滤 (WHERE)
        Table filtered = orders.where(
            $("amount").isGreater(100)
                .and($("user_id").isLess(50))
        );

        // 聚合 (GROUP BY)
        Table aggregated = orders
            .groupBy($("user_id"))
            .select(
                $("user_id"),
                $("amount").sum().as("total_amount"),
                $("order_id").count().as("order_count"),
                $("amount").avg().as("avg_amount"),
                $("amount").max().as("max_amount"),
                $("amount").min().as("min_amount")
            );

        // 排序 + 限制 (ORDER BY + LIMIT, 仅批处理)
        // Table sorted = orders.orderBy($("amount").desc()).fetch(10);

        // 去重 (DISTINCT)
        Table distinctUsers = orders
            .select($("user_id"))
            .distinct();

        // 连接 (JOIN)
        tableEnv.executeSql(
            "CREATE TABLE users (" +
            "  user_id BIGINT," +
            "  username STRING," +
            "  city STRING" +
            ") WITH (" +
            "  'connector' = 'datagen'," +
            "  'rows-per-second' = '1'," +
            "  'fields.user_id.min' = '1'," +
            "  'fields.user_id.max' = '100'" +
            ")"
        );

        Table users = tableEnv.from("users");

        // Inner Join
        Table joined = orders.join(users)
            .where($("orders.user_id").isEqual($("users.user_id")))
            .select(
                $("orders.order_id"),
                $("users.username"),
                $("orders.amount")
            );

        // Left Outer Join
        Table leftJoined = orders.leftOuterJoin(users,
            $("orders.user_id").isEqual($("users.user_id"))
        );

        // Union
        // Table unioned = table1.unionAll(table2);

        // CASE WHEN 表达式
        Table withCategory = orders.select(
            $("order_id"),
            $("amount"),
            ifThenElse(
                $("amount").isGreater(500), "高额订单",
                ifThenElse(
                    $("amount").isGreater(200), "中额订单",
                    "低额订单"
                )
            ).as("category")
        );

        System.out.println("  Table API 基础操作示例:");
        System.out.println("  - 投影 (select)");
        System.out.println("  - 过滤 (where)");
        System.out.println("  - 聚合 (groupBy + select)");
        System.out.println("  - 去重 (distinct)");
        System.out.println("  - 连接 (join / leftOuterJoin)");
        System.out.println("  - 条件表达式 (ifThenElse)\n");
    }

    // ==================== 3. SQL DDL 操作 ====================
    static void sqlDDLDemo() {
        System.out.println("--- 3. SQL DDL 操作 ---");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 创建 Catalog
        String createCatalogSQL = 
            "CREATE CATALOG my_catalog WITH (" +
            "  'type' = 'generic_in_memory'" +
            ")";
        // tableEnv.executeSql(createCatalogSQL);

        // 创建数据库
        String createDatabaseSQL =
            "CREATE DATABASE IF NOT EXISTS my_db";
        // tableEnv.executeSql(createDatabaseSQL);

        // 创建 Kafka Source 表 (完整DDL)
        String createKafkaSourceSQL =
            "CREATE TABLE kafka_orders (" +
            "  order_id BIGINT," +
            "  user_id BIGINT," +
            "  product STRING," +
            "  amount DECIMAL(10, 2)," +
            "  order_time TIMESTAMP(3)," +
            "  -- 计算列" +
            "  order_date AS CAST(order_time AS DATE)," +
            "  order_hour AS HOUR(order_time)," +
            "  -- 处理时间" +
            "  proc_time AS PROCTIME()," +
            "  -- Watermark" +
            "  WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND" +
            ") WITH (" +
            "  'connector' = 'kafka'," +
            "  'topic' = 'orders'," +
            "  'properties.bootstrap.servers' = 'localhost:9092'," +
            "  'properties.group.id' = 'flink-sql-consumer'," +
            "  'scan.startup.mode' = 'latest-offset'," +
            "  'format' = 'json'," +
            "  'json.fail-on-missing-field' = 'false'," +
            "  'json.ignore-parse-errors' = 'true'" +
            ")";

        // 创建 Upsert Kafka Sink (带主键)
        String createUpsertKafkaSinkSQL =
            "CREATE TABLE kafka_user_stats (" +
            "  user_id BIGINT," +
            "  total_amount DECIMAL(10, 2)," +
            "  order_count BIGINT," +
            "  last_order_time TIMESTAMP(3)," +
            "  PRIMARY KEY (user_id) NOT ENFORCED" +
            ") WITH (" +
            "  'connector' = 'upsert-kafka'," +
            "  'topic' = 'user-stats'," +
            "  'properties.bootstrap.servers' = 'localhost:9092'," +
            "  'key.format' = 'json'," +
            "  'value.format' = 'json'" +
            ")";

        // 创建 JDBC Sink 表 (MySQL)
        String createJdbcSinkSQL =
            "CREATE TABLE mysql_orders (" +
            "  order_id BIGINT," +
            "  user_id BIGINT," +
            "  product STRING," +
            "  amount DECIMAL(10, 2)," +
            "  order_time TIMESTAMP(3)," +
            "  PRIMARY KEY (order_id) NOT ENFORCED" +
            ") WITH (" +
            "  'connector' = 'jdbc'," +
            "  'url' = 'jdbc:mysql://localhost:3306/bigdata'," +
            "  'table-name' = 'orders'," +
            "  'driver' = 'com.mysql.cj.jdbc.Driver'," +
            "  'username' = 'root'," +
            "  'password' = 'password'," +
            "  'sink.buffer-flush.max-rows' = '1000'," +
            "  'sink.buffer-flush.interval' = '2s'," +
            "  'sink.max-retries' = '3'" +
            ")";

        // 创建 Filesystem Sink (Hive格式)
        String createFileSinkSQL =
            "CREATE TABLE hive_orders (" +
            "  order_id BIGINT," +
            "  user_id BIGINT," +
            "  product STRING," +
            "  amount DECIMAL(10, 2)," +
            "  order_time TIMESTAMP(3)," +
            "  dt STRING" +
            ") PARTITIONED BY (dt) WITH (" +
            "  'connector' = 'filesystem'," +
            "  'path' = 'hdfs:///data/warehouse/orders'," +
            "  'format' = 'parquet'," +
            "  'sink.partition-commit.trigger' = 'partition-time'," +
            "  'sink.partition-commit.delay' = '1 h'," +
            "  'sink.partition-commit.policy.kind' = 'success-file'," +
            "  'sink.rolling-policy.file-size' = '128MB'," +
            "  'sink.rolling-policy.rollover-interval' = '30 min'" +
            ")";

        // 创建 Elasticsearch Sink
        String createEsSinkSQL =
            "CREATE TABLE es_orders (" +
            "  order_id BIGINT," +
            "  user_id BIGINT," +
            "  product STRING," +
            "  amount DECIMAL(10, 2)," +
            "  order_time TIMESTAMP(3)," +
            "  PRIMARY KEY (order_id) NOT ENFORCED" +
            ") WITH (" +
            "  'connector' = 'elasticsearch-7'," +
            "  'hosts' = 'http://localhost:9200'," +
            "  'index' = 'orders'," +
            "  'sink.bulk-flush.max-actions' = '1000'," +
            "  'sink.bulk-flush.interval' = '1s'" +
            ")";

        // 创建临时视图
        String createViewSQL =
            "CREATE TEMPORARY VIEW high_value_orders AS " +
            "SELECT * FROM orders WHERE amount > 500";

        System.out.println("  SQL DDL 示例:");
        System.out.println("  - CREATE CATALOG (内存/Hive/JDBC)");
        System.out.println("  - CREATE DATABASE");
        System.out.println("  - Kafka Source 表 (计算列 + Watermark)");
        System.out.println("  - Upsert Kafka Sink (带主键)");
        System.out.println("  - JDBC Sink 表 (MySQL)");
        System.out.println("  - Filesystem Sink (Parquet + 分区提交)");
        System.out.println("  - Elasticsearch Sink");
        System.out.println("  - 临时视图 (TEMPORARY VIEW)\n");
    }

    // ==================== 4. 窗口函数 ====================
    static void windowFunctionsDemo() {
        System.out.println("--- 4. 窗口函数 ---");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 创建测试数据源
        tableEnv.executeSql(
            "CREATE TABLE sensor_data (" +
            "  sensor_id STRING," +
            "  temperature DOUBLE," +
            "  humidity DOUBLE," +
            "  ts TIMESTAMP(3)," +
            "  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND" +
            ") WITH (" +
            "  'connector' = 'datagen'," +
            "  'rows-per-second' = '10'," +
            "  'fields.temperature.min' = '20'," +
            "  'fields.temperature.max' = '40'," +
            "  'fields.humidity.min' = '30'," +
            "  'fields.humidity.max' = '90'" +
            ")"
        );

        // ========== 4.1 Tumble 窗口 (滚动窗口) ==========
        String tumbleWindowSQL =
            "SELECT " +
            "  sensor_id," +
            "  TUMBLE_START(ts, INTERVAL '1' MINUTE) AS window_start," +
            "  TUMBLE_END(ts, INTERVAL '1' MINUTE) AS window_end," +
            "  COUNT(*) AS event_count," +
            "  AVG(temperature) AS avg_temp," +
            "  MAX(temperature) AS max_temp," +
            "  MIN(temperature) AS min_temp " +
            "FROM sensor_data " +
            "GROUP BY sensor_id, TUMBLE(ts, INTERVAL '1' MINUTE)";

        // ========== 4.2 Hop 窗口 (滑动窗口) ==========
        String hopWindowSQL =
            "SELECT " +
            "  sensor_id," +
            "  HOP_START(ts, INTERVAL '30' SECOND, INTERVAL '1' MINUTE) AS window_start," +
            "  HOP_END(ts, INTERVAL '30' SECOND, INTERVAL '1' MINUTE) AS window_end," +
            "  AVG(temperature) AS avg_temp " +
            "FROM sensor_data " +
            "GROUP BY sensor_id, HOP(ts, INTERVAL '30' SECOND, INTERVAL '1' MINUTE)";

        // ========== 4.3 Session 窗口 (会话窗口) ==========
        String sessionWindowSQL =
            "SELECT " +
            "  sensor_id," +
            "  SESSION_START(ts, INTERVAL '10' SECOND) AS session_start," +
            "  SESSION_END(ts, INTERVAL '10' SECOND) AS session_end," +
            "  COUNT(*) AS event_count " +
            "FROM sensor_data " +
            "GROUP BY sensor_id, SESSION(ts, INTERVAL '10' SECOND)";

        // ========== 4.4 Cumulate 窗口 (累积窗口, Flink 1.13+) ==========
        String cumulateWindowSQL =
            "SELECT " +
            "  sensor_id," +
            "  window_start," +
            "  window_end," +
            "  COUNT(*) AS event_count," +
            "  SUM(temperature) AS total_temp " +
            "FROM TABLE(" +
            "  CUMULATE(TABLE sensor_data, DESCRIPTOR(ts), " +
            "    INTERVAL '10' SECOND, INTERVAL '1' MINUTE)" +
            ") " +
            "GROUP BY sensor_id, window_start, window_end";

        // ========== 4.5 TVF 窗口 (Table-Valued Function, 新版推荐) ==========
        // Tumble TVF
        String tumbleTvfSQL =
            "SELECT " +
            "  sensor_id," +
            "  window_start," +
            "  window_end," +
            "  COUNT(*) AS cnt," +
            "  AVG(temperature) AS avg_temp " +
            "FROM TABLE(" +
            "  TUMBLE(TABLE sensor_data, DESCRIPTOR(ts), INTERVAL '1' MINUTE)" +
            ") " +
            "GROUP BY sensor_id, window_start, window_end";

        // Hop TVF
        String hopTvfSQL =
            "SELECT " +
            "  sensor_id," +
            "  window_start," +
            "  window_end," +
            "  AVG(temperature) AS avg_temp " +
            "FROM TABLE(" +
            "  HOP(TABLE sensor_data, DESCRIPTOR(ts), " +
            "    INTERVAL '30' SECOND, INTERVAL '1' MINUTE)" +
            ") " +
            "GROUP BY sensor_id, window_start, window_end";

        // ========== 4.6 Window Top-N (窗口内排名) ==========
        String windowTopNSQL =
            "SELECT * FROM (" +
            "  SELECT " +
            "    sensor_id," +
            "    window_start," +
            "    window_end," +
            "    avg_temp," +
            "    ROW_NUMBER() OVER (PARTITION BY window_start, window_end " +
            "      ORDER BY avg_temp DESC) AS rn " +
            "  FROM (" +
            "    SELECT " +
            "      sensor_id," +
            "      window_start," +
            "      window_end," +
            "      AVG(temperature) AS avg_temp " +
            "    FROM TABLE(" +
            "      TUMBLE(TABLE sensor_data, DESCRIPTOR(ts), INTERVAL '1' MINUTE)" +
            "    ) " +
            "    GROUP BY sensor_id, window_start, window_end" +
            "  )" +
            ") WHERE rn <= 3";

        // ========== 4.7 Over 窗口 (分析窗口) ==========
        String overWindowSQL =
            "SELECT " +
            "  sensor_id," +
            "  ts," +
            "  temperature," +
            "  AVG(temperature) OVER w AS avg_temp," +
            "  MAX(temperature) OVER w AS max_temp," +
            "  COUNT(*) OVER w AS cnt," +
            "  -- 前3行到当前行的滑动平均" +
            "  AVG(temperature) OVER (" +
            "    PARTITION BY sensor_id ORDER BY ts " +
            "    ROWS BETWEEN 3 PRECEDING AND CURRENT ROW" +
            "  ) AS moving_avg " +
            "FROM sensor_data " +
            "WINDOW w AS (" +
            "  PARTITION BY sensor_id ORDER BY ts " +
            "  RANGE BETWEEN INTERVAL '1' MINUTE PRECEDING AND CURRENT ROW" +
            ")";

        System.out.println("  窗口函数示例:");
        System.out.println("  - Tumble 窗口 (滚动窗口, 1分钟)");
        System.out.println("  - Hop 窗口 (滑动窗口, 30秒滑动/1分钟窗口)");
        System.out.println("  - Session 窗口 (会话窗口, 10秒间隔)");
        System.out.println("  - Cumulate 窗口 (累积窗口, 10秒步长/1分钟最大)");
        System.out.println("  - TVF 窗口 (新版推荐方式)");
        System.out.println("  - Window Top-N (窗口内排名)");
        System.out.println("  - Over 窗口 (分析窗口/滑动平均)\n");
    }

    // ==================== 5. Temporal Join ====================
    static void temporalJoinDemo() {
        System.out.println("--- 5. Temporal Join (时态表连接) ---");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 订单流 (事实表)
        String createOrdersSQL =
            "CREATE TABLE orders_stream (" +
            "  order_id BIGINT," +
            "  product_id BIGINT," +
            "  quantity INT," +
            "  order_time TIMESTAMP(3)," +
            "  WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND" +
            ") WITH (" +
            "  'connector' = 'datagen'," +
            "  'rows-per-second' = '5'," +
            "  'fields.product_id.min' = '1'," +
            "  'fields.product_id.max' = '10'," +
            "  'fields.quantity.min' = '1'," +
            "  'fields.quantity.max' = '20'" +
            ")";

        // 产品维度表 (缓慢变化维) - Temporal Table
        String createProductsSQL =
            "CREATE TABLE products (" +
            "  product_id BIGINT," +
            "  product_name STRING," +
            "  price DECIMAL(10, 2)," +
            "  update_time TIMESTAMP(3)," +
            "  WATERMARK FOR update_time AS update_time - INTERVAL '5' SECOND," +
            "  PRIMARY KEY (product_id) NOT ENFORCED" +
            ") WITH (" +
            "  'connector' = 'datagen'," +
            "  'rows-per-second' = '1'," +
            "  'fields.product_id.min' = '1'," +
            "  'fields.product_id.max' = '10'," +
            "  'fields.price.min' = '10'," +
            "  'fields.price.max' = '100'" +
            ")";

        // ========== 5.1 Event-Time Temporal Join ==========
        // 基于事件时间的时态连接: 订单按下单时间关联产品当时的价格
        String eventTimeTemporalJoinSQL =
            "SELECT " +
            "  o.order_id," +
            "  o.product_id," +
            "  p.product_name," +
            "  o.quantity," +
            "  p.price," +
            "  o.quantity * p.price AS total_amount," +
            "  o.order_time " +
            "FROM orders_stream AS o " +
            "JOIN products FOR SYSTEM_TIME AS OF o.order_time AS p " +
            "ON o.product_id = p.product_id";

        // ========== 5.2 Processing-Time Temporal Join (Lookup Join) ==========
        // 基于处理时间的 Lookup Join: 实时关联 MySQL 维度表
        String lookupJoinSQL =
            "-- 需要先创建 JDBC 维度表\n" +
            "-- CREATE TABLE dim_products (\n" +
            "--   product_id BIGINT,\n" +
            "--   product_name STRING,\n" +
            "--   price DECIMAL(10, 2),\n" +
            "--   PRIMARY KEY (product_id) NOT ENFORCED\n" +
            "-- ) WITH (\n" +
            "--   'connector' = 'jdbc',\n" +
            "--   'url' = 'jdbc:mysql://localhost:3306/bigdata',\n" +
            "--   'table-name' = 'products',\n" +
            "--   'lookup.cache.max-rows' = '5000',\n" +
            "--   'lookup.cache.ttl' = '10min',\n" +
            "--   'lookup.max-retries' = '3'\n" +
            "-- );\n" +
            "-- \n" +
            "-- SELECT o.*, p.product_name, p.price\n" +
            "-- FROM orders_stream AS o\n" +
            "-- JOIN dim_products FOR SYSTEM_TIME AS OF o.proc_time AS p\n" +
            "-- ON o.product_id = p.product_id";

        // ========== 5.3 Interval Join (区间连接) ==========
        String intervalJoinSQL =
            "-- 订单和支付在5分钟内匹配\n" +
            "-- SELECT o.order_id, o.amount, p.pay_time\n" +
            "-- FROM orders_stream o, payments_stream p\n" +
            "-- WHERE o.order_id = p.order_id\n" +
            "-- AND p.pay_time BETWEEN o.order_time AND o.order_time + INTERVAL '5' MINUTE";

        System.out.println("  Temporal Join 示例:");
        System.out.println("  - Event-Time Temporal Join (事件时间时态连接)");
        System.out.println("  - Processing-Time Lookup Join (处理时间维表关联)");
        System.out.println("  - Interval Join (区间连接, 订单-支付匹配)\n");
    }

    // ==================== 6. Pattern Recognition ====================
    static void patternRecognitionDemo() {
        System.out.println("--- 6. Pattern Recognition (MATCH_RECOGNIZE) ---");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 股票价格流
        tableEnv.executeSql(
            "CREATE TABLE stock_prices (" +
            "  symbol STRING," +
            "  price DOUBLE," +
            "  tax DOUBLE," +
            "  ts TIMESTAMP(3)," +
            "  WATERMARK FOR ts AS ts - INTERVAL '3' SECOND" +
            ") WITH (" +
            "  'connector' = 'datagen'," +
            "  'rows-per-second' = '10'," +
            "  'fields.price.min' = '10'," +
            "  'fields.price.max' = '200'," +
            "  'fields.tax.min' = '0'," +
            "  'fields.tax.max' = '5'" +
            ")"
        );

        // ========== 6.1 检测连续上涨模式 (V型反弹) ==========
        String vShapePatternSQL =
            "SELECT * FROM stock_prices " +
            "MATCH_RECOGNIZE (" +
            "  PARTITION BY symbol " +
            "  ORDER BY ts " +
            "  MEASURES " +
            "    STRT.price AS start_price," +
            "    LAST(DOWN.price) AS bottom_price," +
            "    LAST(UP.price) AS end_price," +
            "    STRT.ts AS start_time," +
            "    LAST(UP.ts) AS end_time " +
            "  ONE ROW PER MATCH " +
            "  AFTER MATCH SKIP TO LAST UP " +
            "  PATTERN (STRT DOWN+ UP+) " +
            "  DEFINE " +
            "    DOWN AS DOWN.price < PREV(DOWN.price, 1)," +
            "    UP AS UP.price > PREV(UP.price, 1)" +
            ") AS T";

        // ========== 6.2 检测连续登录失败 ==========
        String loginFailPatternSQL =
            "-- 检测连续3次登录失败的用户\n" +
            "-- SELECT * FROM login_events\n" +
            "-- MATCH_RECOGNIZE (\n" +
            "--   PARTITION BY user_id\n" +
            "--   ORDER BY ts\n" +
            "--   MEASURES\n" +
            "--     FIRST(FAIL.ts) AS first_fail_time,\n" +
            "--     LAST(FAIL.ts) AS last_fail_time,\n" +
            "--     COUNT(FAIL.ts) AS fail_count\n" +
            "--   ONE ROW PER MATCH\n" +
            "--   AFTER MATCH SKIP PAST LAST ROW\n" +
            "--   PATTERN (FAIL{3,})\n" +
            "--   DEFINE\n" +
            "--     FAIL AS FAIL.status = 'FAILED'\n" +
            "-- ) AS T";

        // ========== 6.3 检测超时未支付 ==========
        String timeoutPatternSQL =
            "-- 订单超过30分钟未支付\n" +
            "-- SELECT * FROM order_events\n" +
            "-- MATCH_RECOGNIZE (\n" +
            "--   PARTITION BY order_id\n" +
            "--   ORDER BY ts\n" +
            "--   MEASURES\n" +
            "--     CREATE_ORDER.ts AS create_time,\n" +
            "--     LAST(NO_PAY.ts) AS timeout_time\n" +
            "--   ONE ROW PER MATCH\n" +
            "--   AFTER MATCH SKIP PAST LAST ROW\n" +
            "--   PATTERN (CREATE_ORDER NO_PAY*) WITHIN INTERVAL '30' MINUTE\n" +
            "--   DEFINE\n" +
            "--     CREATE_ORDER AS CREATE_ORDER.event_type = 'create',\n" +
            "--     NO_PAY AS NO_PAY.event_type <> 'pay'\n" +
            "-- ) AS T";

        System.out.println("  MATCH_RECOGNIZE 示例:");
        System.out.println("  - V型反弹检测 (先跌后涨)");
        System.out.println("  - 连续登录失败检测 (3次以上)");
        System.out.println("  - 超时未支付检测 (30分钟内)\n");
    }

    // ==================== 7. Kafka 集成 ====================
    static void kafkaIntegrationDemo() {
        System.out.println("--- 7. Kafka Source & Sink 集成 ---");

        // Kafka Source (JSON 格式)
        String kafkaJsonSourceSQL =
            "CREATE TABLE kafka_json_source (" +
            "  user_id BIGINT," +
            "  item_id BIGINT," +
            "  behavior STRING," +
            "  ts TIMESTAMP(3) METADATA FROM 'timestamp'," +
            "  `partition` BIGINT METADATA VIRTUAL," +
            "  `offset` BIGINT METADATA VIRTUAL," +
            "  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND" +
            ") WITH (" +
            "  'connector' = 'kafka'," +
            "  'topic' = 'user_behavior'," +
            "  'properties.bootstrap.servers' = 'localhost:9092'," +
            "  'properties.group.id' = 'flink-consumer'," +
            "  'scan.startup.mode' = 'latest-offset'," +
            "  'format' = 'json'" +
            ")";

        // Kafka Source (Avro 格式 + Schema Registry)
        String kafkaAvroSourceSQL =
            "CREATE TABLE kafka_avro_source (" +
            "  user_id BIGINT," +
            "  item_id BIGINT," +
            "  behavior STRING," +
            "  ts TIMESTAMP(3)" +
            ") WITH (" +
            "  'connector' = 'kafka'," +
            "  'topic' = 'user_behavior_avro'," +
            "  'properties.bootstrap.servers' = 'localhost:9092'," +
            "  'format' = 'avro-confluent'," +
            "  'avro-confluent.url' = 'http://localhost:8081'" +
            ")";

        // Kafka Source (Canal JSON - CDC)
        String kafkaCanalSourceSQL =
            "CREATE TABLE kafka_canal_source (" +
            "  id BIGINT," +
            "  name STRING," +
            "  age INT," +
            "  update_time TIMESTAMP(3)" +
            ") WITH (" +
            "  'connector' = 'kafka'," +
            "  'topic' = 'mysql_cdc'," +
            "  'properties.bootstrap.servers' = 'localhost:9092'," +
            "  'format' = 'canal-json'" +
            ")";

        // Kafka Source (Debezium JSON - CDC)
        String kafkaDebeziumSourceSQL =
            "CREATE TABLE kafka_debezium_source (" +
            "  id BIGINT," +
            "  name STRING," +
            "  age INT," +
            "  update_time TIMESTAMP(3)" +
            ") WITH (" +
            "  'connector' = 'kafka'," +
            "  'topic' = 'mysql_dbz'," +
            "  'properties.bootstrap.servers' = 'localhost:9092'," +
            "  'format' = 'debezium-json'" +
            ")";

        // Kafka Sink (JSON)
        String kafkaJsonSinkSQL =
            "CREATE TABLE kafka_json_sink (" +
            "  user_id BIGINT," +
            "  behavior STRING," +
            "  cnt BIGINT," +
            "  window_start TIMESTAMP(3)," +
            "  window_end TIMESTAMP(3)" +
            ") WITH (" +
            "  'connector' = 'kafka'," +
            "  'topic' = 'user_behavior_stats'," +
            "  'properties.bootstrap.servers' = 'localhost:9092'," +
            "  'format' = 'json'," +
            "  'sink.partitioner' = 'round-robin'" +
            ")";

        // 多 Topic 写入
        String kafkaMultiTopicSinkSQL =
            "CREATE TABLE kafka_multi_sink (" +
            "  user_id BIGINT," +
            "  behavior STRING," +
            "  ts TIMESTAMP(3)," +
            "  -- 动态 topic" +
            "  topic STRING METADATA" +
            ") WITH (" +
            "  'connector' = 'kafka'," +
            "  'properties.bootstrap.servers' = 'localhost:9092'," +
            "  'format' = 'json'," +
            "  'topic' = 'default-topic'" +
            ")";

        System.out.println("  Kafka 集成示例:");
        System.out.println("  - JSON 格式 Source (含 Metadata)");
        System.out.println("  - Avro 格式 + Schema Registry");
        System.out.println("  - Canal JSON (MySQL CDC)");
        System.out.println("  - Debezium JSON (通用 CDC)");
        System.out.println("  - JSON Sink (带分区器)");
        System.out.println("  - 多 Topic 动态路由\n");
    }

    // ==================== 8. ClickHouse Sink ====================
    static void clickhouseSinkDemo() {
        System.out.println("--- 8. ClickHouse Sink ---");

        // ClickHouse JDBC Sink
        String clickhouseJdbcSinkSQL =
            "CREATE TABLE ck_user_behavior (" +
            "  user_id BIGINT," +
            "  behavior STRING," +
            "  cnt BIGINT," +
            "  dt STRING," +
            "  PRIMARY KEY (user_id, behavior, dt) NOT ENFORCED" +
            ") WITH (" +
            "  'connector' = 'jdbc'," +
            "  'url' = 'jdbc:clickhouse://localhost:8123/default'," +
            "  'table-name' = 'user_behavior_stats'," +
            "  'driver' = 'com.clickhouse.jdbc.ClickHouseDriver'," +
            "  'username' = 'default'," +
            "  'password' = ''," +
            "  'sink.buffer-flush.max-rows' = '10000'," +
            "  'sink.buffer-flush.interval' = '5s'," +
            "  'sink.max-retries' = '3'" +
            ")";

        // 完整的 ETL 管道: Kafka → Flink SQL → ClickHouse
        String etlPipelineSQL =
            "-- 完整 ETL 管道示例:\n" +
            "-- INSERT INTO ck_user_behavior\n" +
            "-- SELECT\n" +
            "--   user_id,\n" +
            "--   behavior,\n" +
            "--   COUNT(*) AS cnt,\n" +
            "--   DATE_FORMAT(TUMBLE_START(ts, INTERVAL '1' MINUTE), 'yyyy-MM-dd') AS dt\n" +
            "-- FROM kafka_json_source\n" +
            "-- GROUP BY\n" +
            "--   user_id,\n" +
            "--   behavior,\n" +
            "--   TUMBLE(ts, INTERVAL '1' MINUTE)";

        System.out.println("  ClickHouse Sink 示例:");
        System.out.println("  - JDBC Connector 写入");
        System.out.println("  - Kafka → Flink SQL → ClickHouse ETL 管道\n");
    }

    // ==================== 9. Mini-Batch 优化 ====================
    static void miniBatchOptimizationDemo() {
        System.out.println("--- 9. Mini-Batch 优化 & 性能调优 ---");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // ========== 9.1 Mini-Batch 聚合 ==========
        // 减少状态访问频率,提升吞吐
        tableEnv.getConfig().set("table.exec.mini-batch.enabled", "true");
        tableEnv.getConfig().set("table.exec.mini-batch.allow-latency", "5s");
        tableEnv.getConfig().set("table.exec.mini-batch.size", "5000");

        // ========== 9.2 两阶段聚合 ==========
        // 减少数据倾斜,类似 MapReduce 的 Combiner
        tableEnv.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");

        // ========== 9.3 Split Distinct 优化 ==========
        // 将 COUNT(DISTINCT) 拆分为两层聚合
        tableEnv.getConfig().set("table.optimizer.distinct-agg.split.enabled", "true");
        tableEnv.getConfig().set("table.optimizer.distinct-agg.split.bucket-num", "1024");

        // ========== 9.4 状态 TTL ==========
        // 过期状态自动清理
        tableEnv.getConfig().set("table.exec.state.ttl", "86400000"); // 24小时

        // ========== 9.5 Join Hint ==========
        String joinHintSQL =
            "-- Broadcast Join (小表广播)\n" +
            "-- SELECT /*+ BROADCAST(dim) */ *\n" +
            "-- FROM orders o\n" +
            "-- JOIN dim_products dim ON o.product_id = dim.product_id\n" +
            "--\n" +
            "-- Shuffle Hash Join\n" +
            "-- SELECT /*+ SHUFFLE_HASH(o) */ *\n" +
            "-- FROM orders o\n" +
            "-- JOIN dim_products dim ON o.product_id = dim.product_id\n" +
            "--\n" +
            "-- Nest Loop Join\n" +
            "-- SELECT /*+ NEST_LOOP(dim) */ *\n" +
            "-- FROM orders o\n" +
            "-- JOIN dim_products dim ON o.product_id = dim.product_id";

        // ========== 9.6 Top-N 优化 ==========
        String topNSQL =
            "-- 去重 (Deduplication)\n" +
            "SELECT * FROM (\n" +
            "  SELECT *,\n" +
            "    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY ts DESC) AS rn\n" +
            "  FROM orders\n" +
            ") WHERE rn = 1\n" +
            "\n" +
            "-- Top-N\n" +
            "SELECT * FROM (\n" +
            "  SELECT *,\n" +
            "    ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC) AS rn\n" +
            "  FROM products\n" +
            ") WHERE rn <= 10";

        System.out.println("  性能优化示例:");
        System.out.println("  - Mini-Batch 聚合 (5秒延迟/5000条)");
        System.out.println("  - 两阶段聚合 (TWO_PHASE)");
        System.out.println("  - Split Distinct (1024桶)");
        System.out.println("  - 状态 TTL (24小时)");
        System.out.println("  - Join Hint (Broadcast/ShuffleHash/NestLoop)");
        System.out.println("  - Top-N / 去重 优化\n");
    }

    // ==================== 10. 自定义函数 ====================
    static void userDefinedFunctionsDemo() {
        System.out.println("--- 10. 自定义函数 (UDF/UDAF/UDTF) ---");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // ========== 10.1 注册 UDF (标量函数) ==========
        // tableEnv.createTemporaryFunction("my_upper", MyUpperFunction.class);
        // tableEnv.createTemporaryFunction("hash_code", HashFunction.class);

        // ========== 10.2 注册 UDAF (聚合函数) ==========
        // tableEnv.createTemporaryFunction("weighted_avg", WeightedAvgFunction.class);

        // ========== 10.3 注册 UDTF (表函数) ==========
        // tableEnv.createTemporaryFunction("split_func", SplitFunction.class);

        System.out.println("  自定义函数示例:");
        System.out.println("  - UDF (标量函数): my_upper, hash_code");
        System.out.println("  - UDAF (聚合函数): weighted_avg");
        System.out.println("  - UDTF (表函数): split_func");

        // UDF SQL 使用示例
        String udfUsageSQL =
            "-- 标量函数\n" +
            "-- SELECT my_upper(name) FROM users\n" +
            "--\n" +
            "-- 聚合函数\n" +
            "-- SELECT weighted_avg(price, quantity) FROM orders GROUP BY user_id\n" +
            "--\n" +
            "-- 表函数 (LATERAL TABLE)\n" +
            "-- SELECT user_id, word\n" +
            "-- FROM orders, LATERAL TABLE(split_func(tags)) AS T(word)";

        System.out.println("  - SQL 使用: SELECT my_upper(name), LATERAL TABLE(split_func(tags))");
        System.out.println();
    }
}
