package com.bigdata.spark;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.*;
import org.apache.spark.sql.types.*;

import static org.apache.spark.sql.functions.*;

/**
 * Spark Structured Streaming 高级示例
 * 
 * 涵盖内容:
 * 1. 基础流处理 (Socket / Rate / File Source)
 * 2. Kafka Source & Sink (完整集成)
 * 3. 窗口操作 (Tumbling, Sliding, Session)
 * 4. 水位线 (Watermark) 处理迟到数据
 * 5. 输出模式 (Append / Complete / Update)
 * 6. 有状态处理 (mapGroupsWithState / flatMapGroupsWithState)
 * 7. 流-流连接 (Stream-Stream Join)
 * 8. 流-批连接 (Stream-Static Join)
 * 9. ForeachBatch Sink (自定义输出)
 * 10. 性能调优
 * 11. 实战: Kafka → 聚合 → ClickHouse
 * 12. 实战: 实时指标仪表盘
 */
public class SparkStructuredStreamingJavaApp {

    public static void main(String[] args) throws Exception {
        System.out.println("=== Spark Structured Streaming 高级示例 ===\n");
        basicStreamingDemo();
        kafkaIntegrationDemo();
        windowOperationsDemo();
        statefulProcessingDemo();
        streamJoinDemo();
        foreachBatchSinkDemo();
        performanceTuningDemo();
        realtimeETLDemo();
        System.out.println("\n=== 所有 Structured Streaming 示例完成 ===");
    }

    // ==================== 1. 基础流处理 ====================
    static void basicStreamingDemo() {
        System.out.println("--- 1. 基础流处理 ---");

        SparkSession spark = SparkSession.builder()
            .appName("StructuredStreaming-Basic")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate();

        // Rate Source (测试用, 自动生成数据)
        Dataset<Row> rateDF = spark.readStream()
            .format("rate")
            .option("rowsPerSecond", 10)
            .option("numPartitions", 4)
            .load();

        // File Source (监控目录新文件)
        StructType schema = new StructType()
            .add("user_id", DataTypes.LongType)
            .add("action", DataTypes.StringType)
            .add("timestamp", DataTypes.TimestampType);

        Dataset<Row> fileDF = spark.readStream()
            .format("json")
            .schema(schema)
            .option("maxFilesPerTrigger", 1)
            .load("/data/input/");

        // 基本转换
        Dataset<Row> processed = rateDF
            .withColumn("doubled", col("value").multiply(2))
            .withColumn("category",
                when(col("value").mod(3).equalTo(0), "A")
                .when(col("value").mod(3).equalTo(1), "B")
                .otherwise("C"))
            .filter(col("value").gt(5));

        // 输出到控制台
        // StreamingQuery q = processed.writeStream()
        //     .outputMode("append")
        //     .format("console")
        //     .option("truncate", false)
        //     .trigger(Trigger.ProcessingTime("5 seconds"))
        //     .start();

        spark.stop();
        System.out.println("  基础: Rate Source, File Source, 转换, Console Sink\n");
    }

    // ==================== 2. Kafka 集成 ====================
    static void kafkaIntegrationDemo() {
        System.out.println("--- 2. Kafka Source & Sink ---");

        SparkSession spark = SparkSession.builder()
            .appName("StructuredStreaming-Kafka")
            .master("local[*]")
            .getOrCreate();

        // ========== Kafka Source ==========
        // 订阅单个 topic
        Dataset<Row> kafkaDF = spark.readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "user_events")
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .option("maxOffsetsPerTrigger", 10000)
            .option("kafka.max.poll.records", "500")
            .load();

        // 订阅多个 topic (模式匹配)
        Dataset<Row> multiTopicDF = spark.readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribePattern", "user_events_.*")
            .option("startingOffsets", "earliest")
            .load();

        // 解析 Kafka 消息 (key, value, topic, partition, offset, timestamp)
        StructType eventSchema = new StructType()
            .add("user_id", DataTypes.LongType)
            .add("event_type", DataTypes.StringType)
            .add("page", DataTypes.StringType)
            .add("duration", DataTypes.IntegerType)
            .add("event_time", DataTypes.TimestampType);

        Dataset<Row> parsedDF = kafkaDF
            .selectExpr("CAST(key AS STRING) AS key",
                        "CAST(value AS STRING) AS json_value",
                        "topic", "partition", "offset", "timestamp")
            .select(
                col("key"),
                from_json(col("json_value"), eventSchema).as("data"),
                col("topic"), col("partition"), col("offset"), col("timestamp")
            )
            .select("key", "data.*", "topic", "partition", "offset", "timestamp");

        // ========== 聚合计算 ==========
        Dataset<Row> eventCounts = parsedDF
            .withWatermark("event_time", "10 minutes")
            .groupBy(
                window(col("event_time"), "5 minutes"),
                col("event_type")
            )
            .agg(
                count("*").as("event_count"),
                approx_count_distinct("user_id").as("unique_users"),
                avg("duration").as("avg_duration")
            );

        // ========== Kafka Sink ==========
        // 写入 Kafka (需要 key + value)
        // eventCounts
        //     .selectExpr(
        //         "CAST(event_type AS STRING) AS key",
        //         "to_json(struct(*)) AS value"
        //     )
        //     .writeStream()
        //     .format("kafka")
        //     .option("kafka.bootstrap.servers", "localhost:9092")
        //     .option("topic", "event_stats")
        //     .option("checkpointLocation", "/tmp/checkpoint/kafka-sink")
        //     .outputMode("update")
        //     .trigger(Trigger.ProcessingTime("10 seconds"))
        //     .start();

        spark.stop();
        System.out.println("  Kafka集成: Source(单/多topic) → JSON解析 → 窗口聚合 → Kafka Sink\n");
    }

    // ==================== 3. 窗口操作 ====================
    static void windowOperationsDemo() {
        System.out.println("--- 3. 窗口操作 & 水位线 ---");

        SparkSession spark = SparkSession.builder()
            .appName("StructuredStreaming-Window")
            .master("local[*]")
            .getOrCreate();

        Dataset<Row> rateDF = spark.readStream()
            .format("rate")
            .option("rowsPerSecond", 100)
            .load()
            .withColumn("event_time", col("timestamp"))
            .withColumn("user_id", expr("CAST(value % 100 AS LONG)"))
            .withColumn("amount", expr("CAST(rand() * 1000 AS DECIMAL(10,2))"));

        // ========== 3.1 滚动窗口 (Tumbling) ==========
        Dataset<Row> tumblingWindow = rateDF
            .withWatermark("event_time", "10 minutes")
            .groupBy(
                window(col("event_time"), "5 minutes"),
                col("user_id")
            )
            .agg(
                count("*").as("txn_count"),
                sum("amount").as("total_amount"),
                avg("amount").as("avg_amount"),
                max("amount").as("max_amount")
            );

        // ========== 3.2 滑动窗口 (Sliding) ==========
        Dataset<Row> slidingWindow = rateDF
            .withWatermark("event_time", "10 minutes")
            .groupBy(
                window(col("event_time"), "10 minutes", "2 minutes"),
                col("user_id")
            )
            .count();

        // ========== 3.3 会话窗口 (Session, Spark 3.2+) ==========
        // Dataset<Row> sessionWindow = rateDF
        //     .withWatermark("event_time", "10 minutes")
        //     .groupBy(
        //         session_window(col("event_time"), "5 minutes"),
        //         col("user_id")
        //     )
        //     .count();

        // ========== 3.4 水位线处理迟到数据 ==========
        Dataset<Row> withWatermark = rateDF
            .withWatermark("event_time", "30 minutes")  // 允许30分钟迟到
            .groupBy(
                window(col("event_time"), "5 minutes")
            )
            .agg(
                count("*").as("count"),
                sum("amount").as("total")
            );

        // ========== 3.5 窗口排名 (Top-N) ==========
        Dataset<Row> windowAgg = rateDF
            .withWatermark("event_time", "10 minutes")
            .groupBy(
                window(col("event_time"), "1 hour"),
                col("user_id")
            )
            .agg(sum("amount").as("total_amount"));

        // 窗口内 Top 10 用户
        // 注意: 需要在 foreachBatch 中使用批处理窗口函数
        // windowAgg.writeStream()
        //     .outputMode("complete")
        //     .foreachBatch((batchDF, batchId) -> {
        //         Dataset<Row> ranked = batchDF
        //             .withColumn("rank",
        //                 row_number().over(
        //                     Window.partitionBy("window")
        //                           .orderBy(col("total_amount").desc())))
        //             .filter(col("rank").leq(10));
        //         ranked.show(false);
        //     })
        //     .start();

        spark.stop();
        System.out.println("  窗口操作:");
        System.out.println("  - Tumbling Window (5分钟)");
        System.out.println("  - Sliding Window (10分钟/2分钟步长)");
        System.out.println("  - Session Window (5分钟间隔)");
        System.out.println("  - Watermark (30分钟迟到容忍)");
        System.out.println("  - Window Top-N (窗口排名)\n");
    }

    // ==================== 4. 有状态处理 ====================
    static void statefulProcessingDemo() {
        System.out.println("--- 4. 有状态处理 ---");

        // ========== 4.1 mapGroupsWithState ==========
        // 用途: 自定义状态管理,一对一输出
        // 示例: 用户会话跟踪
        String mapGroupsExample = 
            "// 定义状态类\n" +
            "public class UserSession implements Serializable {\n" +
            "    String userId;\n" +
            "    long startTime;\n" +
            "    long lastActivity;\n" +
            "    int eventCount;\n" +
            "}\n\n" +
            "// mapGroupsWithState\n" +
            "dataset\n" +
            "  .groupByKey(row -> row.getString(0), Encoders.STRING())\n" +
            "  .mapGroupsWithState(\n" +
            "    (MapGroupsWithStateFunction<String, Row, UserSession, UserSession>)\n" +
            "    (userId, events, state) -> {\n" +
            "      UserSession session = state.exists() ? state.get() : new UserSession();\n" +
            "      session.userId = userId;\n" +
            "      while (events.hasNext()) {\n" +
            "        Row event = events.next();\n" +
            "        session.lastActivity = event.getTimestamp(1).getTime();\n" +
            "        session.eventCount++;\n" +
            "      }\n" +
            "      // 设置超时\n" +
            "      state.setTimeoutDuration(\"30 minutes\");\n" +
            "      state.update(session);\n" +
            "      return session;\n" +
            "    },\n" +
            "    Encoders.bean(UserSession.class),\n" +
            "    Encoders.bean(UserSession.class),\n" +
            "    GroupStateTimeout.ProcessingTimeTimeout()\n" +
            "  )";

        // ========== 4.2 flatMapGroupsWithState ==========
        // 用途: 自定义状态管理,一对多输出
        // 示例: 检测用户行为序列 (浏览→加购→下单)
        String flatMapGroupsExample =
            "// flatMapGroupsWithState\n" +
            "dataset\n" +
            "  .groupByKey(row -> row.getString(0), Encoders.STRING())\n" +
            "  .flatMapGroupsWithState(\n" +
            "    (FlatMapGroupsWithStateFunction<String, Row, List<String>, Alert>)\n" +
            "    (userId, events, state) -> {\n" +
            "      List<String> history = state.exists() ? state.get() : new ArrayList<>();\n" +
            "      List<Alert> alerts = new ArrayList<>();\n" +
            "      while (events.hasNext()) {\n" +
            "        Row event = events.next();\n" +
            "        String action = event.getString(1);\n" +
            "        history.add(action);\n" +
            "        // 检测模式: view → cart → order\n" +
            "        if (history.size() >= 3) {\n" +
            "          int n = history.size();\n" +
            "          if (\"view\".equals(history.get(n-3))\n" +
            "              && \"cart\".equals(history.get(n-2))\n" +
            "              && \"order\".equals(history.get(n-1))) {\n" +
            "            alerts.add(new Alert(userId, \"CONVERSION\"));\n" +
            "          }\n" +
            "        }\n" +
            "      }\n" +
            "      state.update(history);\n" +
            "      return alerts.iterator();\n" +
            "    },\n" +
            "    OutputMode.Append(),\n" +
            "    Encoders.bean(Alert.class),\n" +
            "    Encoders.javaSerialization(List.class),\n" +
            "    GroupStateTimeout.ProcessingTimeTimeout()\n" +
            "  )";

        System.out.println("  有状态处理:");
        System.out.println("  - mapGroupsWithState: 用户会话跟踪 (一对一)");
        System.out.println("  - flatMapGroupsWithState: 行为序列检测 (一对多)");
        System.out.println("  - GroupStateTimeout: 处理时间/事件时间超时\n");
    }

    // ==================== 5. 流连接 ====================
    static void streamJoinDemo() {
        System.out.println("--- 5. 流连接操作 ---");

        SparkSession spark = SparkSession.builder()
            .appName("StructuredStreaming-Join")
            .master("local[*]")
            .getOrCreate();

        // ========== 5.1 Stream-Stream Inner Join ==========
        Dataset<Row> ordersStream = spark.readStream()
            .format("rate").option("rowsPerSecond", 10).load()
            .withColumn("order_id", col("value"))
            .withColumn("user_id", expr("value % 100"))
            .withColumn("order_time", col("timestamp"))
            .withWatermark("order_time", "10 minutes");

        Dataset<Row> paymentsStream = spark.readStream()
            .format("rate").option("rowsPerSecond", 5).load()
            .withColumn("payment_order_id", col("value"))
            .withColumn("pay_amount", expr("CAST(rand() * 1000 AS DECIMAL(10,2))"))
            .withColumn("pay_time", col("timestamp"))
            .withWatermark("pay_time", "10 minutes");

        // 订单和支付在10分钟内匹配
        Dataset<Row> orderPaymentJoin = ordersStream.join(
            paymentsStream,
            expr("order_id = payment_order_id AND " +
                 "pay_time >= order_time AND " +
                 "pay_time <= order_time + interval 10 minutes"),
            "inner"
        );

        // ========== 5.2 Stream-Stream Left Outer Join ==========
        // 找到未支付的订单
        Dataset<Row> leftJoin = ordersStream.join(
            paymentsStream,
            expr("order_id = payment_order_id AND " +
                 "pay_time >= order_time AND " +
                 "pay_time <= order_time + interval 30 minutes"),
            "leftOuter"
        );

        // ========== 5.3 Stream-Static (Batch) Join ==========
        // 流数据与静态维度表关联
        Dataset<Row> userDimension = spark.read()
            .format("json")
            .load("/data/users/");

        // Stream Join Static (支持 inner/left)
        // Dataset<Row> enrichedOrders = ordersStream.join(
        //     userDimension,
        //     ordersStream.col("user_id").equalTo(userDimension.col("id")),
        //     "left"
        // );

        spark.stop();
        System.out.println("  流连接:");
        System.out.println("  - Stream-Stream Inner Join (10分钟窗口)");
        System.out.println("  - Stream-Stream Left Outer Join (未支付检测)");
        System.out.println("  - Stream-Static Join (维表关联)\n");
    }

    // ==================== 6. ForeachBatch Sink ====================
    static void foreachBatchSinkDemo() {
        System.out.println("--- 6. ForeachBatch 自定义 Sink ---");

        SparkSession spark = SparkSession.builder()
            .appName("StructuredStreaming-ForeachBatch")
            .master("local[*]")
            .getOrCreate();

        Dataset<Row> rateDF = spark.readStream()
            .format("rate")
            .option("rowsPerSecond", 100)
            .load();

        // ========== 6.1 ForeachBatch: 写入 MySQL ==========
        String mysqlExample =
            "rateDF.writeStream()\n" +
            "  .foreachBatch((batchDF, batchId) -> {\n" +
            "    batchDF.write()\n" +
            "      .format(\"jdbc\")\n" +
            "      .option(\"url\", \"jdbc:mysql://localhost:3306/bigdata\")\n" +
            "      .option(\"dbtable\", \"stream_data\")\n" +
            "      .option(\"user\", \"root\")\n" +
            "      .option(\"password\", \"password\")\n" +
            "      .mode(SaveMode.Append)\n" +
            "      .save();\n" +
            "  })\n" +
            "  .option(\"checkpointLocation\", \"/tmp/cp/mysql\")\n" +
            "  .start();";

        // ========== 6.2 ForeachBatch: 写入 ClickHouse ==========
        String clickhouseExample =
            "rateDF.writeStream()\n" +
            "  .foreachBatch((batchDF, batchId) -> {\n" +
            "    batchDF.write()\n" +
            "      .format(\"jdbc\")\n" +
            "      .option(\"url\", \"jdbc:clickhouse://localhost:8123/default\")\n" +
            "      .option(\"dbtable\", \"stream_metrics\")\n" +
            "      .option(\"driver\", \"com.clickhouse.jdbc.ClickHouseDriver\")\n" +
            "      .mode(SaveMode.Append)\n" +
            "      .save();\n" +
            "  })\n" +
            "  .trigger(Trigger.ProcessingTime(\"10 seconds\"))\n" +
            "  .start();";

        // ========== 6.3 ForeachBatch: 写入 HBase ==========
        String hbaseExample =
            "rateDF.writeStream()\n" +
            "  .foreachBatch((batchDF, batchId) -> {\n" +
            "    batchDF.foreachPartition(partition -> {\n" +
            "      Connection conn = ConnectionFactory.createConnection(hbaseConf);\n" +
            "      Table table = conn.getTable(TableName.valueOf(\"stream_data\"));\n" +
            "      List<Put> puts = new ArrayList<>();\n" +
            "      while (partition.hasNext()) {\n" +
            "        Row row = partition.next();\n" +
            "        Put put = new Put(Bytes.toBytes(row.getLong(0)));\n" +
            "        put.addColumn(CF, COL, Bytes.toBytes(row.getString(1)));\n" +
            "        puts.add(put);\n" +
            "      }\n" +
            "      table.put(puts);\n" +
            "      table.close();\n" +
            "      conn.close();\n" +
            "    });\n" +
            "  })\n" +
            "  .start();";

        // ========== 6.4 ForeachBatch: 多路输出 ==========
        String multiSinkExample =
            "rateDF.writeStream()\n" +
            "  .foreachBatch((batchDF, batchId) -> {\n" +
            "    // 缓存以复用\n" +
            "    batchDF.persist();\n" +
            "    \n" +
            "    // 输出到 MySQL\n" +
            "    batchDF.write().format(\"jdbc\")...save();\n" +
            "    \n" +
            "    // 输出到 Redis (热数据缓存)\n" +
            "    batchDF.foreachPartition(writeToRedis);\n" +
            "    \n" +
            "    // 输出到 Kafka (下游消费)\n" +
            "    batchDF.selectExpr(\"to_json(struct(*)) AS value\")\n" +
            "           .write().format(\"kafka\")...save();\n" +
            "    \n" +
            "    batchDF.unpersist();\n" +
            "  })\n" +
            "  .start();";

        spark.stop();
        System.out.println("  ForeachBatch Sink:");
        System.out.println("  - 写入 MySQL (JDBC)");
        System.out.println("  - 写入 ClickHouse (JDBC)");
        System.out.println("  - 写入 HBase (Native API)");
        System.out.println("  - 多路输出 (MySQL + Redis + Kafka)\n");
    }

    // ==================== 7. 性能调优 ====================
    static void performanceTuningDemo() {
        System.out.println("--- 7. 性能调优 ---");

        SparkSession spark = SparkSession.builder()
            .appName("StructuredStreaming-Tuning")
            .master("local[*]")
            // ========== 核心调优参数 ==========
            .config("spark.sql.shuffle.partitions", "200")         // Shuffle 分区数
            .config("spark.sql.streaming.stateStore.providerClass",
                "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
            .config("spark.sql.streaming.stateStore.maintenanceInterval", "60s")
            .config("spark.sql.streaming.minBatchesToRetain", "100") // 保留的微批数
            .config("spark.sql.streaming.noDataMicroBatches.enabled", "false") // 无数据不触发
            .config("spark.sql.streaming.metricsEnabled", "true")    // 启用指标
            // ========== 状态存储优化 ==========
            .config("spark.sql.streaming.stateStore.rocksdb.changelog", "true")
            .config("spark.sql.streaming.stateStore.rocksdb.compactOnCommit", "false")
            // ========== Kafka 优化 ==========
            .config("spark.streaming.kafka.maxRatePerPartition", "10000")
            .config("spark.streaming.backpressure.enabled", "true")
            // ========== 内存优化 ==========
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "8g")
            .config("spark.executor.memoryOverhead", "2g")
            .config("spark.memory.fraction", "0.8")
            .config("spark.memory.storageFraction", "0.3")
            .getOrCreate();

        // Trigger 策略对比
        String triggerExamples =
            "// 1. Processing Time (固定间隔微批)\n" +
            "Trigger.ProcessingTime(\"10 seconds\")\n\n" +
            "// 2. Once (只执行一次, 适合补数据)\n" +
            "Trigger.Once()\n\n" +
            "// 3. AvailableNow (处理所有可用数据后停止, Spark 3.3+)\n" +
            "Trigger.AvailableNow()\n\n" +
            "// 4. Continuous (连续处理, 实验性, 毫秒级延迟)\n" +
            "Trigger.Continuous(\"1 second\")";

        spark.stop();
        System.out.println("  性能调优:");
        System.out.println("  - shuffle.partitions: 200");
        System.out.println("  - stateStore: HDFS/RocksDB");
        System.out.println("  - noDataMicroBatches: false");
        System.out.println("  - backpressure: enabled");
        System.out.println("  - Trigger: ProcessingTime/Once/AvailableNow/Continuous\n");
    }

    // ==================== 8. 实战: 实时 ETL ====================
    static void realtimeETLDemo() {
        System.out.println("--- 8. 实战: Kafka → 转换 → 多路输出 ---");

        String etlPipeline =
            "// 完整 ETL 管道\n" +
            "SparkSession spark = SparkSession.builder()...\n\n" +
            "// 1. 从 Kafka 读取\n" +
            "Dataset<Row> kafkaDF = spark.readStream()\n" +
            "  .format(\"kafka\")\n" +
            "  .option(\"subscribe\", \"raw_events\")\n" +
            "  .load()\n" +
            "  .selectExpr(\"CAST(value AS STRING)\")\n" +
            "  .select(from_json(col(\"value\"), schema).as(\"data\"))\n" +
            "  .select(\"data.*\");\n\n" +
            "// 2. 数据清洗\n" +
            "Dataset<Row> cleaned = kafkaDF\n" +
            "  .filter(col(\"user_id\").isNotNull())\n" +
            "  .filter(col(\"event_time\").gt(\"2020-01-01\"))\n" +
            "  .withColumn(\"event_date\", to_date(col(\"event_time\")))\n" +
            "  .withColumn(\"event_hour\", hour(col(\"event_time\")))\n" +
            "  .dropDuplicates(\"event_id\");\n\n" +
            "// 3. 维度关联\n" +
            "Dataset<Row> userDim = spark.read().parquet(\"/data/dim/users\");\n" +
            "Dataset<Row> enriched = cleaned.join(userDim, \"user_id\");\n\n" +
            "// 4. 多路输出 (ForeachBatch)\n" +
            "enriched.writeStream()\n" +
            "  .foreachBatch((batchDF, batchId) -> {\n" +
            "    batchDF.persist();\n" +
            "    // ODS层: 写入 HDFS Parquet\n" +
            "    batchDF.write().partitionBy(\"event_date\")\n" +
            "      .parquet(\"/data/ods/events\");\n" +
            "    // DWD层: 写入 Iceberg\n" +
            "    batchDF.writeTo(\"iceberg.dwd.events\").append();\n" +
            "    // 实时: 写入 Kafka\n" +
            "    batchDF.selectExpr(\"to_json(struct(*)) AS value\")\n" +
            "      .write().format(\"kafka\")\n" +
            "      .option(\"topic\", \"enriched_events\").save();\n" +
            "    batchDF.unpersist();\n" +
            "  })\n" +
            "  .trigger(Trigger.ProcessingTime(\"30 seconds\"))\n" +
            "  .option(\"checkpointLocation\", \"/tmp/cp/etl\")\n" +
            "  .start()\n" +
            "  .awaitTermination();";

        System.out.println("  实时ETL管道:");
        System.out.println("  Kafka → 清洗 → 维度关联 → HDFS(ODS) + Iceberg(DWD) + Kafka(实时)");
        System.out.println("  - 去重: dropDuplicates");
        System.out.println("  - 维表: Stream-Static Join");
        System.out.println("  - 输出: ForeachBatch 多路写入\n");
    }
}
