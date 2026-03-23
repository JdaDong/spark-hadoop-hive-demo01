package com.bigdata.datalake;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.expressions.Window;
import static org.apache.spark.sql.functions.*;

import java.util.*;

/**
 * ============================================================
 * 数据湖实战 - Delta Lake + Apache Hudi
 * ============================================================
 * 覆盖:
 *   1. Delta Lake 表操作 (CRUD / MERGE / Time Travel / Schema Evolution)
 *   2. Apache Hudi MOR & COW 表 (Upsert / Incremental Query)
 *   3. 数据湖分层架构 (Bronze→Silver→Gold)
 *   4. Change Data Feed (CDC → 数据湖)
 *   5. 数据湖表优化 (Compaction / Z-Order / Vacuum)
 *   6. 与 Hive Metastore 集成
 *   7. Streaming 写入数据湖
 *
 * 运行: spark-submit --class com.bigdata.datalake.DataLakeApp target/xxx.jar
 */
public class DataLakeApp {

    private static SparkSession spark;
    private static final String LAKE_BASE = "hdfs:///data-lake";
    private static final String BRONZE = LAKE_BASE + "/bronze";
    private static final String SILVER = LAKE_BASE + "/silver";
    private static final String GOLD = LAKE_BASE + "/gold";

    public static void main(String[] args) {
        System.out.println("╔══════════════════════════════════════════════════════╗");
        System.out.println("║   数据湖实战 - Delta Lake + Apache Hudi             ║");
        System.out.println("║   Bronze → Silver → Gold 分层架构                   ║");
        System.out.println("╚══════════════════════════════════════════════════════╝\n");

        spark = SparkSession.builder()
                .appName("DataLake-DeltaHudi-Demo")
                .config("spark.sql.extensions",
                        "io.delta.sql.DeltaSparkSessionExtension," +
                        "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog",
                        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.sql.warehouse.dir", "hdfs:///user/hive/warehouse")
                .enableHiveSupport()
                .getOrCreate();

        // ==================== Part 1: Delta Lake ====================
        demoDeltaLake();

        // ==================== Part 2: Apache Hudi ====================
        demoApacheHudi();

        // ==================== Part 3: 湖仓分层架构 ====================
        demoLakehouseArchitecture();

        spark.stop();
        System.out.println("\n✅ 数据湖实战全部完成!");
    }

    // ==================== 1. Delta Lake 实战 ====================

    static void demoDeltaLake() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("📦 Part 1: Delta Lake 实战");
        System.out.println("=".repeat(60));

        // ---- 1.1 创建 Delta 表 ----
        System.out.println("\n🔹 1.1 创建 Delta 表 (订单表)");
        Dataset<Row> orders = spark.sql(
            "SELECT * FROM VALUES " +
            "(1, 1001, 'iPhone 15', 7999.00, 2, '2024-01-01', 'completed'), " +
            "(2, 1002, 'MacBook Pro', 14999.00, 1, '2024-01-01', 'completed'), " +
            "(3, 1003, 'AirPods Pro', 1799.00, 3, '2024-01-02', 'pending'), " +
            "(4, 1001, 'iPad Air', 4799.00, 1, '2024-01-02', 'shipped'), " +
            "(5, 1004, 'Apple Watch', 2999.00, 1, '2024-01-03', 'completed') " +
            "AS orders(order_id, user_id, product, price, quantity, order_date, status)"
        );

        String deltaOrdersPath = BRONZE + "/delta/orders";
        orders.write()
                .format("delta")
                .mode(SaveMode.Overwrite)
                .partitionBy("order_date")
                .save(deltaOrdersPath);

        System.out.println("   ✅ Delta 表已创建: " + deltaOrdersPath);
        spark.read().format("delta").load(deltaOrdersPath).show();

        // ---- 1.2 MERGE INTO (Upsert) ----
        System.out.println("🔹 1.2 Delta MERGE INTO (Upsert 合并写入)");

        // 注册为临时视图
        spark.read().format("delta").load(deltaOrdersPath)
                .createOrReplaceTempView("delta_orders");

        // 新增和更新数据
        Dataset<Row> updates = spark.sql(
            "SELECT * FROM VALUES " +
            "(3, 1003, 'AirPods Pro', 1799.00, 3, '2024-01-02', 'completed'), " +  // 更新: pending→completed
            "(6, 1005, 'HomePod Mini', 749.00, 2, '2024-01-03', 'pending') " +      // 新增
            "AS updates(order_id, user_id, product, price, quantity, order_date, status)"
        );
        updates.createOrReplaceTempView("order_updates");

        spark.sql(
            "MERGE INTO delta.`" + deltaOrdersPath + "` AS target " +
            "USING order_updates AS source " +
            "ON target.order_id = source.order_id " +
            "WHEN MATCHED THEN UPDATE SET * " +
            "WHEN NOT MATCHED THEN INSERT *"
        );

        System.out.println("   ✅ MERGE 完成 (1条更新 + 1条新增)");
        spark.read().format("delta").load(deltaOrdersPath)
                .orderBy("order_id").show();

        // ---- 1.3 Time Travel (时间旅行) ----
        System.out.println("🔹 1.3 Delta Time Travel (历史版本查询)");

        // 查看版本历史
        spark.sql("DESCRIBE HISTORY delta.`" + deltaOrdersPath + "`")
                .select("version", "timestamp", "operation", "operationMetrics")
                .show(false);

        // 读取特定版本
        Dataset<Row> version0 = spark.read().format("delta")
                .option("versionAsOf", 0)
                .load(deltaOrdersPath);
        System.out.println("   📌 Version 0 (初始版本) 行数: " + version0.count());

        Dataset<Row> version1 = spark.read().format("delta")
                .option("versionAsOf", 1)
                .load(deltaOrdersPath);
        System.out.println("   📌 Version 1 (MERGE后) 行数: " + version1.count());

        // ---- 1.4 Schema Evolution (模式演进) ----
        System.out.println("\n🔹 1.4 Delta Schema Evolution (模式演进)");

        Dataset<Row> newOrders = spark.sql(
            "SELECT 7 AS order_id, 1006 AS user_id, 'Vision Pro' AS product, " +
            "27999.00 AS price, 1 AS quantity, '2024-01-04' AS order_date, " +
            "'pending' AS status, 'credit_card' AS payment_method, " +
            "'2024-01-04 10:30:00' AS created_at"
        );

        newOrders.write().format("delta")
                .mode(SaveMode.Append)
                .option("mergeSchema", "true")  // 自动合并 Schema
                .save(deltaOrdersPath);

        System.out.println("   ✅ Schema 自动演进: 新增 payment_method, created_at 列");
        spark.read().format("delta").load(deltaOrdersPath).printSchema();

        // ---- 1.5 Change Data Feed (CDF) ----
        System.out.println("🔹 1.5 Delta Change Data Feed (变更数据捕获)");

        // 启用 CDF
        spark.sql("ALTER TABLE delta.`" + deltaOrdersPath +
                "` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)");

        // 做一次更新来产生 CDF 数据
        spark.sql("UPDATE delta.`" + deltaOrdersPath +
                "` SET status = 'cancelled' WHERE order_id = 4");

        // 读取变更数据
        Dataset<Row> changes = spark.read().format("delta")
                .option("readChangeFeed", "true")
                .option("startingVersion", 2)
                .load(deltaOrdersPath);

        System.out.println("   📋 变更数据:");
        changes.select("order_id", "status", "_change_type", "_commit_version")
                .show();

        // ---- 1.6 表优化 ----
        System.out.println("🔹 1.6 Delta 表优化 (Compaction / Z-Order / Vacuum)");

        // OPTIMIZE - 小文件合并
        spark.sql("OPTIMIZE delta.`" + deltaOrdersPath + "`");
        System.out.println("   ✅ OPTIMIZE: 小文件合并完成");

        // Z-ORDER - 多维聚簇
        spark.sql("OPTIMIZE delta.`" + deltaOrdersPath + "` ZORDER BY (user_id, product)");
        System.out.println("   ✅ Z-ORDER: 按 user_id, product 聚簇优化");

        // VACUUM - 清理过期文件 (保留7天)
        spark.sql("SET spark.databricks.delta.retentionDurationCheck.enabled = false");
        spark.sql("VACUUM delta.`" + deltaOrdersPath + "` RETAIN 168 HOURS");
        System.out.println("   ✅ VACUUM: 清理 7 天前的过期文件");
    }

    // ==================== 2. Apache Hudi 实战 ====================

    static void demoApacheHudi() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🔥 Part 2: Apache Hudi 实战");
        System.out.println("=".repeat(60));

        // ---- 2.1 COW (Copy-On-Write) 表 ----
        System.out.println("\n🔹 2.1 Hudi COW 表 (适合读多写少)");

        String hudiCOWPath = BRONZE + "/hudi/users_cow";
        Map<String, String> hudiCOWOptions = new HashMap<>();
        hudiCOWOptions.put("hoodie.table.name", "users_cow");
        hudiCOWOptions.put("hoodie.datasource.write.table.type", "COPY_ON_WRITE");
        hudiCOWOptions.put("hoodie.datasource.write.recordkey.field", "user_id");
        hudiCOWOptions.put("hoodie.datasource.write.precombine.field", "updated_at");
        hudiCOWOptions.put("hoodie.datasource.write.partitionpath.field", "register_date");
        hudiCOWOptions.put("hoodie.datasource.write.operation", "upsert");
        hudiCOWOptions.put("hoodie.upsert.shuffle.parallelism", "2");
        hudiCOWOptions.put("hoodie.insert.shuffle.parallelism", "2");

        Dataset<Row> users = spark.sql(
            "SELECT * FROM VALUES " +
            "(1001, '张三', 28, 'M', 'VIP', '2024-01-01', '2024-01-15 10:00:00'), " +
            "(1002, '李四', 32, 'F', 'Normal', '2024-01-01', '2024-01-15 10:00:00'), " +
            "(1003, '王五', 25, 'M', 'VIP', '2024-01-02', '2024-01-15 10:00:00'), " +
            "(1004, '赵六', 30, 'F', 'Normal', '2024-01-02', '2024-01-15 10:00:00') " +
            "AS users(user_id, name, age, gender, level, register_date, updated_at)"
        );

        DataFrameWriter<Row> writer = users.write().format("hudi")
                .mode(SaveMode.Overwrite);
        hudiCOWOptions.forEach(writer::option);
        writer.save(hudiCOWPath);

        System.out.println("   ✅ Hudi COW 表已创建: " + hudiCOWPath);
        spark.read().format("hudi").load(hudiCOWPath)
                .select("user_id", "name", "age", "level", "_hoodie_commit_time")
                .show();

        // ---- 2.2 Hudi Upsert ----
        System.out.println("🔹 2.2 Hudi Upsert (增量更新)");

        Dataset<Row> updates = spark.sql(
            "SELECT * FROM VALUES " +
            "(1002, '李四', 32, 'F', 'VIP', '2024-01-01', '2024-01-16 10:00:00'), " +      // 升级为VIP
            "(1005, '钱七', 27, 'M', 'Normal', '2024-01-03', '2024-01-16 10:00:00') " +     // 新用户
            "AS users(user_id, name, age, gender, level, register_date, updated_at)"
        );

        DataFrameWriter<Row> updateWriter = updates.write().format("hudi")
                .mode(SaveMode.Append);
        hudiCOWOptions.forEach(updateWriter::option);
        updateWriter.save(hudiCOWPath);

        System.out.println("   ✅ Upsert 完成 (1更新 + 1新增)");
        spark.read().format("hudi").load(hudiCOWPath)
                .select("user_id", "name", "level", "_hoodie_commit_time")
                .orderBy("user_id").show();

        // ---- 2.3 MOR (Merge-On-Read) 表 ----
        System.out.println("🔹 2.3 Hudi MOR 表 (适合写多读少 / 近实时)");

        String hudiMORPath = BRONZE + "/hudi/events_mor";
        Map<String, String> hudiMOROptions = new HashMap<>();
        hudiMOROptions.put("hoodie.table.name", "events_mor");
        hudiMOROptions.put("hoodie.datasource.write.table.type", "MERGE_ON_READ");
        hudiMOROptions.put("hoodie.datasource.write.recordkey.field", "event_id");
        hudiMOROptions.put("hoodie.datasource.write.precombine.field", "event_time");
        hudiMOROptions.put("hoodie.datasource.write.partitionpath.field", "event_date");
        hudiMOROptions.put("hoodie.datasource.write.operation", "upsert");
        hudiMOROptions.put("hoodie.compact.inline", "true");
        hudiMOROptions.put("hoodie.compact.inline.max.delta.commits", "3");

        Dataset<Row> events = spark.sql(
            "SELECT * FROM VALUES " +
            "('e001', 1001, 'page_view', '/home', '2024-01-15 10:00:00', '2024-01-15'), " +
            "('e002', 1001, 'click', '/product/1', '2024-01-15 10:01:00', '2024-01-15'), " +
            "('e003', 1002, 'page_view', '/home', '2024-01-15 10:02:00', '2024-01-15'), " +
            "('e004', 1001, 'purchase', '/checkout', '2024-01-15 10:05:00', '2024-01-15'), " +
            "('e005', 1003, 'page_view', '/category', '2024-01-15 10:10:00', '2024-01-15') " +
            "AS events(event_id, user_id, event_type, page, event_time, event_date)"
        );

        DataFrameWriter<Row> morWriter = events.write().format("hudi")
                .mode(SaveMode.Overwrite);
        hudiMOROptions.forEach(morWriter::option);
        morWriter.save(hudiMORPath);
        System.out.println("   ✅ Hudi MOR 表已创建: " + hudiMORPath);

        // MOR 实时视图 vs 读优化视图
        System.out.println("\n   📋 实时视图 (Realtime View) - 包含最新 Log 数据:");
        spark.read().format("hudi").load(hudiMORPath).show();

        System.out.println("   📋 读优化视图 (Read-Optimized View) - 仅 Parquet:");
        spark.read().format("hudi")
                .option("hoodie.datasource.query.type", "read_optimized")
                .load(hudiMORPath).show();

        // ---- 2.4 增量查询 ----
        System.out.println("🔹 2.4 Hudi 增量查询 (Incremental Query)");

        // 获取最早的 commit 时间
        String beginTime = spark.read().format("hudi").load(hudiCOWPath)
                .select("_hoodie_commit_time")
                .orderBy("_hoodie_commit_time")
                .first().getString(0);

        Dataset<Row> incrementalDF = spark.read().format("hudi")
                .option("hoodie.datasource.query.type", "incremental")
                .option("hoodie.datasource.read.begin.instanttime", beginTime)
                .load(hudiCOWPath);

        System.out.println("   📋 增量数据 (自 " + beginTime + " 以来的变更):");
        incrementalDF.select("user_id", "name", "level", "_hoodie_commit_time")
                .orderBy("_hoodie_commit_time").show();

        // ---- 2.5 Hudi 与 Hive 集成 ----
        System.out.println("🔹 2.5 Hudi 同步到 Hive Metastore");

        Map<String, String> hiveSyncOptions = new HashMap<>(hudiCOWOptions);
        hiveSyncOptions.put("hoodie.datasource.hive_sync.enable", "true");
        hiveSyncOptions.put("hoodie.datasource.hive_sync.database", "dw");
        hiveSyncOptions.put("hoodie.datasource.hive_sync.table", "dim_users_hudi");
        hiveSyncOptions.put("hoodie.datasource.hive_sync.mode", "hms");
        hiveSyncOptions.put("hoodie.datasource.hive_sync.partition_fields", "register_date");
        hiveSyncOptions.put("hoodie.datasource.hive_sync.partition_extractor_class",
                "org.apache.hudi.hive.MultiPartKeysValueExtractor");

        System.out.println("   ✅ Hive 同步配置已准备 (数据库: dw, 表: dim_users_hudi)");
    }

    // ==================== 3. 湖仓分层架构 ====================

    static void demoLakehouseArchitecture() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🏗️ Part 3: 湖仓分层架构 (Medallion Architecture)");
        System.out.println("=".repeat(60));

        // ---- Bronze 层: 原始数据落湖 ----
        System.out.println("\n🥉 Bronze 层 (原始数据 - 保留完整原始信息)");

        Dataset<Row> rawOrders = spark.sql(
            "SELECT * FROM VALUES " +
            "(1, 1001, 'p001', 2, 7999.00, '2024-01-15 10:00:00', 'completed', 'src_mysql', '2024-01-15'), " +
            "(2, 1002, 'p002', 1, 14999.00, '2024-01-15 10:05:00', 'pending', 'src_mysql', '2024-01-15'), " +
            "(3, NULL, 'p001', -1, 7999.00, '2024-01-15 10:10:00', 'unknown', 'src_mysql', '2024-01-15'), " +
            "(4, 1003, 'p003', 3, 1799.00, '2024-01-15 10:15:00', 'completed', 'src_api', '2024-01-15'), " +
            "(5, 1001, 'p001', 2, 7999.00, '2024-01-15 10:00:00', 'completed', 'src_mysql', '2024-01-15') " +  // 重复数据
            "AS orders(order_id, user_id, product_id, quantity, price, order_time, status, source, dt)"
        );

        // Bronze: 原样落湖 + 添加采集元数据
        Dataset<Row> bronzeOrders = rawOrders
                .withColumn("_ingestion_time", current_timestamp())
                .withColumn("_source_file", lit("mysql_binlog_2024011510"))
                .withColumn("_batch_id", lit("batch_20240115_001"));

        bronzeOrders.write().format("delta")
                .mode(SaveMode.Overwrite)
                .partitionBy("dt")
                .save(BRONZE + "/delta/orders_raw");

        System.out.println("   ✅ Bronze 写入完成: " + BRONZE + "/delta/orders_raw");
        System.out.println("   📊 行数: " + bronzeOrders.count() + " (含脏数据和重复数据)");

        // ---- Silver 层: 清洗去重标准化 ----
        System.out.println("\n🥈 Silver 层 (清洗数据 - 去重/去脏/标准化)");

        Dataset<Row> silverOrders = spark.read().format("delta").load(BRONZE + "/delta/orders_raw");

        silverOrders = silverOrders
                // 1. 去重 (按 order_id 取最新)
                .withColumn("rn", row_number().over(
                        Window.partitionBy("order_id").orderBy(col("order_time").desc())))
                .filter(col("rn").equalTo(1)).drop("rn")
                // 2. 去空值
                .filter(col("user_id").isNotNull())
                // 3. 数据修正 (quantity > 0)
                .filter(col("quantity").gt(0))
                // 4. 状态标准化
                .withColumn("status", when(col("status").equalTo("unknown"), "pending")
                        .otherwise(col("status")))
                // 5. 计算派生字段
                .withColumn("total_amount", col("price").multiply(col("quantity")))
                .withColumn("_cleaned_time", current_timestamp())
                // 6. 删除采集元数据
                .drop("_ingestion_time", "_source_file", "_batch_id");

        silverOrders.write().format("delta")
                .mode(SaveMode.Overwrite)
                .partitionBy("dt")
                .save(SILVER + "/delta/orders_cleaned");

        System.out.println("   ✅ Silver 写入完成: " + SILVER + "/delta/orders_cleaned");
        System.out.println("   📊 行数: " + silverOrders.count() + " (去重去脏后)");
        silverOrders.show();

        // ---- Gold 层: 业务聚合指标 ----
        System.out.println("🥇 Gold 层 (业务指标 - 聚合/宽表/报表)");

        Dataset<Row> goldDailyStats = spark.read().format("delta")
                .load(SILVER + "/delta/orders_cleaned")
                .groupBy("dt")
                .agg(
                        count("order_id").alias("order_count"),
                        countDistinct("user_id").alias("buyer_count"),
                        sum("total_amount").alias("gmv"),
                        avg("total_amount").alias("avg_order_amount"),
                        max("total_amount").alias("max_order_amount"),
                        sum(when(col("status").equalTo("completed"), 1).otherwise(0)).alias("completed_count")
                )
                .withColumn("completion_rate",
                        col("completed_count").divide(col("order_count")))
                .withColumn("arpu",
                        col("gmv").divide(col("buyer_count")))
                .withColumn("_aggregated_time", current_timestamp());

        goldDailyStats.write().format("delta")
                .mode(SaveMode.Overwrite)
                .save(GOLD + "/delta/daily_order_stats");

        System.out.println("   ✅ Gold 写入完成: " + GOLD + "/delta/daily_order_stats");
        goldDailyStats.show();

        // ---- 数据质量检查 ----
        System.out.println("📊 数据质量检查 (Bronze vs Silver vs Gold)");
        long bronzeCount = spark.read().format("delta").load(BRONZE + "/delta/orders_raw").count();
        long silverCount = spark.read().format("delta").load(SILVER + "/delta/orders_cleaned").count();
        long goldCount = spark.read().format("delta").load(GOLD + "/delta/daily_order_stats").count();

        System.out.printf("   Bronze: %d 行 → Silver: %d 行 (过滤 %.1f%%) → Gold: %d 行%n",
                bronzeCount, silverCount,
                (1 - (double) silverCount / bronzeCount) * 100,
                goldCount);
    }
}
