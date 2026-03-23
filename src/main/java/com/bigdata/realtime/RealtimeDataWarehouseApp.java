package com.bigdata.realtime;

import java.sql.*;
import java.util.*;

/**
 * 实时数仓实战: Kafka → Flink → ClickHouse 完整管道
 * 
 * 架构:
 *   数据源(MySQL) → Canal/Flink CDC → Kafka → Flink SQL → ClickHouse → Grafana
 * 
 * 数仓分层:
 *   ODS层: Kafka 原始数据 (JSON)
 *   DWD层: Flink SQL 清洗转换 → Kafka (明细事实表)
 *   DWS层: Flink SQL 聚合 → ClickHouse (汇总表)
 *   ADS层: ClickHouse 物化视图 (应用指标)
 * 
 * 涵盖内容:
 * 1. ODS层: Kafka Topic 设计与数据接入
 * 2. DWD层: Flink SQL 实时数据清洗
 * 3. DWS层: Flink SQL 实时聚合 → ClickHouse
 * 4. ADS层: ClickHouse 物化视图与查询
 * 5. 维度表关联: Flink Lookup Join
 * 6. 实时指标: PV/UV/GMV/转化率
 * 7. 实时大屏 SQL 查询
 * 8. 数据质量监控
 */
public class RealtimeDataWarehouseApp {

    // ClickHouse 连接配置
    private static final String CK_URL = "jdbc:clickhouse://localhost:8123/realtime_dw";
    private static final String CK_USER = "default";
    private static final String CK_PASSWORD = "";

    public static void main(String[] args) throws Exception {
        System.out.println("=== 实时数仓实战: Kafka → Flink → ClickHouse ===\n");

        // 1. 创建 ClickHouse 数仓表
        createClickHouseTables();

        // 2. 打印 Flink SQL 作业定义
        printFlinkODSLayer();
        printFlinkDWDLayer();
        printFlinkDWSLayer();

        // 3. 打印 ADS 层查询
        printADSQueries();

        // 4. 数据质量监控
        printDataQualityMonitor();

        System.out.println("\n=== 实时数仓架构搭建完成 ===");
    }

    // ==================== 1. ClickHouse 建表 ====================
    static void createClickHouseTables() throws Exception {
        System.out.println("--- 1. ClickHouse 数仓建表 ---");

        String[] ddlStatements = {
            // ========== 创建数据库 ==========
            "CREATE DATABASE IF NOT EXISTS realtime_dw",

            // ========== DWS层: 用户行为汇总表 (ReplacingMergeTree) ==========
            "CREATE TABLE IF NOT EXISTS realtime_dw.dws_user_behavior_1min (" +
            "  window_start DateTime," +
            "  window_end DateTime," +
            "  user_id UInt64," +
            "  pv UInt64," +
            "  uv UInt64," +
            "  cart_count UInt64," +
            "  order_count UInt64," +
            "  pay_count UInt64," +
            "  pay_amount Decimal(18, 2)," +
            "  dt Date" +
            ") ENGINE = ReplacingMergeTree()" +
            " PARTITION BY dt" +
            " ORDER BY (dt, window_start, user_id)" +
            " TTL dt + INTERVAL 90 DAY",

            // ========== DWS层: 商品销售汇总表 ==========
            "CREATE TABLE IF NOT EXISTS realtime_dw.dws_product_sale_1h (" +
            "  window_start DateTime," +
            "  window_end DateTime," +
            "  product_id UInt64," +
            "  product_name String," +
            "  category String," +
            "  order_count UInt64," +
            "  order_amount Decimal(18, 2)," +
            "  pay_count UInt64," +
            "  pay_amount Decimal(18, 2)," +
            "  refund_count UInt64," +
            "  refund_amount Decimal(18, 2)," +
            "  dt Date" +
            ") ENGINE = ReplacingMergeTree()" +
            " PARTITION BY dt" +
            " ORDER BY (dt, window_start, product_id)" +
            " TTL dt + INTERVAL 180 DAY",

            // ========== DWS层: 地区销售汇总表 ==========
            "CREATE TABLE IF NOT EXISTS realtime_dw.dws_region_sale_1h (" +
            "  window_start DateTime," +
            "  window_end DateTime," +
            "  province String," +
            "  city String," +
            "  order_count UInt64," +
            "  order_amount Decimal(18, 2)," +
            "  user_count UInt64," +
            "  dt Date" +
            ") ENGINE = SummingMergeTree()" +
            " PARTITION BY dt" +
            " ORDER BY (dt, window_start, province, city)",

            // ========== ADS层: 实时大屏指标 (AggregatingMergeTree) ==========
            "CREATE TABLE IF NOT EXISTS realtime_dw.ads_realtime_dashboard (" +
            "  stat_time DateTime," +
            "  metric_name String," +
            "  metric_value Float64," +
            "  dt Date" +
            ") ENGINE = ReplacingMergeTree()" +
            " PARTITION BY dt" +
            " ORDER BY (dt, stat_time, metric_name)",

            // ========== ADS层: 实时 UV 使用 HyperLogLog ==========
            "CREATE TABLE IF NOT EXISTS realtime_dw.ads_uv_bitmap (" +
            "  dt Date," +
            "  hour UInt8," +
            "  uv_state AggregateFunction(uniq, UInt64)" +
            ") ENGINE = AggregatingMergeTree()" +
            " PARTITION BY dt" +
            " ORDER BY (dt, hour)",

            // ========== 物化视图: 每小时自动聚合 ==========
            "CREATE MATERIALIZED VIEW IF NOT EXISTS realtime_dw.mv_hourly_stats " +
            "TO realtime_dw.ads_realtime_dashboard AS " +
            "SELECT " +
            "  toStartOfHour(window_start) AS stat_time," +
            "  'hourly_gmv' AS metric_name," +
            "  sum(pay_amount) AS metric_value," +
            "  toDate(window_start) AS dt " +
            "FROM realtime_dw.dws_user_behavior_1min " +
            "GROUP BY toStartOfHour(window_start), toDate(window_start)"
        };

        // 注意: 实际运行需要 ClickHouse 服务
        System.out.println("  建表 DDL (共 " + ddlStatements.length + " 条):");
        for (int i = 0; i < ddlStatements.length; i++) {
            String tableName = extractTableName(ddlStatements[i]);
            System.out.println("  [" + (i + 1) + "] " + tableName);
        }

        // 实际执行
        // try (Connection conn = DriverManager.getConnection(CK_URL, CK_USER, CK_PASSWORD)) {
        //     for (String ddl : ddlStatements) {
        //         conn.createStatement().execute(ddl);
        //     }
        // }
        System.out.println();
    }

    // ==================== 2. ODS 层 ====================
    static void printFlinkODSLayer() {
        System.out.println("--- 2. ODS层: Kafka 数据接入 ---");

        String odsSQL =
            "-- ========== ODS层: 用户行为日志 ==========\n" +
            "CREATE TABLE ods_user_behavior (\n" +
            "  user_id BIGINT,\n" +
            "  item_id BIGINT,\n" +
            "  category_id BIGINT,\n" +
            "  behavior STRING,        -- pv/cart/fav/buy\n" +
            "  ts BIGINT,              -- 时间戳(秒)\n" +
            "  event_time AS TO_TIMESTAMP_LTZ(ts, 0),\n" +
            "  proc_time AS PROCTIME(),\n" +
            "  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND\n" +
            ") WITH (\n" +
            "  'connector' = 'kafka',\n" +
            "  'topic' = 'ods_user_behavior',\n" +
            "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
            "  'properties.group.id' = 'flink-ods',\n" +
            "  'scan.startup.mode' = 'latest-offset',\n" +
            "  'format' = 'json'\n" +
            ");\n\n" +
            "-- ========== ODS层: 订单数据 (CDC) ==========\n" +
            "CREATE TABLE ods_order_info (\n" +
            "  id BIGINT,\n" +
            "  user_id BIGINT,\n" +
            "  product_id BIGINT,\n" +
            "  province_id INT,\n" +
            "  order_status STRING,\n" +
            "  total_amount DECIMAL(16, 2),\n" +
            "  create_time TIMESTAMP(3),\n" +
            "  update_time TIMESTAMP(3),\n" +
            "  WATERMARK FOR create_time AS create_time - INTERVAL '5' SECOND,\n" +
            "  PRIMARY KEY (id) NOT ENFORCED\n" +
            ") WITH (\n" +
            "  'connector' = 'mysql-cdc',\n" +
            "  'hostname' = 'localhost',\n" +
            "  'port' = '3306',\n" +
            "  'username' = 'root',\n" +
            "  'password' = 'password',\n" +
            "  'database-name' = 'ecommerce',\n" +
            "  'table-name' = 'order_info'\n" +
            ");\n\n" +
            "-- ========== ODS层: 支付数据 (CDC) ==========\n" +
            "CREATE TABLE ods_payment_info (\n" +
            "  id BIGINT,\n" +
            "  order_id BIGINT,\n" +
            "  user_id BIGINT,\n" +
            "  pay_amount DECIMAL(16, 2),\n" +
            "  pay_type STRING,\n" +
            "  pay_time TIMESTAMP(3),\n" +
            "  WATERMARK FOR pay_time AS pay_time - INTERVAL '5' SECOND,\n" +
            "  PRIMARY KEY (id) NOT ENFORCED\n" +
            ") WITH (\n" +
            "  'connector' = 'mysql-cdc',\n" +
            "  'hostname' = 'localhost',\n" +
            "  'port' = '3306',\n" +
            "  'username' = 'root',\n" +
            "  'password' = 'password',\n" +
            "  'database-name' = 'ecommerce',\n" +
            "  'table-name' = 'payment_info'\n" +
            ");";

        System.out.println("  ODS层数据源:");
        System.out.println("  - ods_user_behavior: Kafka JSON (用户行为)");
        System.out.println("  - ods_order_info: MySQL CDC (订单)");
        System.out.println("  - ods_payment_info: MySQL CDC (支付)\n");
    }

    // ==================== 3. DWD 层 ====================
    static void printFlinkDWDLayer() {
        System.out.println("--- 3. DWD层: 数据清洗与关联 ---");

        String dwdSQL =
            "-- ========== 维度表: 商品 (Lookup Join) ==========\n" +
            "CREATE TABLE dim_product (\n" +
            "  id BIGINT,\n" +
            "  product_name STRING,\n" +
            "  category STRING,\n" +
            "  brand STRING,\n" +
            "  price DECIMAL(10, 2),\n" +
            "  PRIMARY KEY (id) NOT ENFORCED\n" +
            ") WITH (\n" +
            "  'connector' = 'jdbc',\n" +
            "  'url' = 'jdbc:mysql://localhost:3306/ecommerce',\n" +
            "  'table-name' = 'product_info',\n" +
            "  'lookup.cache.max-rows' = '5000',\n" +
            "  'lookup.cache.ttl' = '1h'\n" +
            ");\n\n" +
            "-- ========== 维度表: 地区 ==========\n" +
            "CREATE TABLE dim_region (\n" +
            "  id INT,\n" +
            "  province STRING,\n" +
            "  city STRING,\n" +
            "  PRIMARY KEY (id) NOT ENFORCED\n" +
            ") WITH (\n" +
            "  'connector' = 'jdbc',\n" +
            "  'url' = 'jdbc:mysql://localhost:3306/ecommerce',\n" +
            "  'table-name' = 'region_info',\n" +
            "  'lookup.cache.max-rows' = '1000',\n" +
            "  'lookup.cache.ttl' = '24h'\n" +
            ");\n\n" +
            "-- ========== DWD: 订单宽表 ==========\n" +
            "CREATE TABLE dwd_order_detail (\n" +
            "  order_id BIGINT,\n" +
            "  user_id BIGINT,\n" +
            "  product_id BIGINT,\n" +
            "  product_name STRING,\n" +
            "  category STRING,\n" +
            "  province STRING,\n" +
            "  city STRING,\n" +
            "  order_status STRING,\n" +
            "  total_amount DECIMAL(16, 2),\n" +
            "  create_time TIMESTAMP(3),\n" +
            "  PRIMARY KEY (order_id) NOT ENFORCED\n" +
            ") WITH (\n" +
            "  'connector' = 'upsert-kafka',\n" +
            "  'topic' = 'dwd_order_detail',\n" +
            "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
            "  'key.format' = 'json',\n" +
            "  'value.format' = 'json'\n" +
            ");\n\n" +
            "-- ========== DWD ETL: 订单 + 商品 + 地区 ==========\n" +
            "INSERT INTO dwd_order_detail\n" +
            "SELECT\n" +
            "  o.id AS order_id,\n" +
            "  o.user_id,\n" +
            "  o.product_id,\n" +
            "  p.product_name,\n" +
            "  p.category,\n" +
            "  r.province,\n" +
            "  r.city,\n" +
            "  o.order_status,\n" +
            "  o.total_amount,\n" +
            "  o.create_time\n" +
            "FROM ods_order_info AS o\n" +
            "LEFT JOIN dim_product FOR SYSTEM_TIME AS OF o.proc_time AS p\n" +
            "  ON o.product_id = p.id\n" +
            "LEFT JOIN dim_region FOR SYSTEM_TIME AS OF o.proc_time AS r\n" +
            "  ON o.province_id = r.id;";

        System.out.println("  DWD层:");
        System.out.println("  - dim_product: 商品维表 (Lookup Join, 缓存5000行/1h)");
        System.out.println("  - dim_region: 地区维表 (Lookup Join, 缓存1000行/24h)");
        System.out.println("  - dwd_order_detail: 订单宽表 (Upsert Kafka)");
        System.out.println("  - ETL: 订单 LEFT JOIN 商品 LEFT JOIN 地区\n");
    }

    // ==================== 4. DWS 层 ====================
    static void printFlinkDWSLayer() {
        System.out.println("--- 4. DWS层: 实时聚合 → ClickHouse ---");

        String dwsSQL =
            "-- ========== DWS: ClickHouse JDBC Sink ==========\n" +
            "CREATE TABLE dws_user_behavior_1min (\n" +
            "  window_start TIMESTAMP(3),\n" +
            "  window_end TIMESTAMP(3),\n" +
            "  user_id BIGINT,\n" +
            "  pv BIGINT,\n" +
            "  uv BIGINT,\n" +
            "  cart_count BIGINT,\n" +
            "  order_count BIGINT,\n" +
            "  pay_count BIGINT,\n" +
            "  pay_amount DECIMAL(18, 2),\n" +
            "  dt STRING,\n" +
            "  PRIMARY KEY (dt, window_start, user_id) NOT ENFORCED\n" +
            ") WITH (\n" +
            "  'connector' = 'jdbc',\n" +
            "  'url' = 'jdbc:clickhouse://localhost:8123/realtime_dw',\n" +
            "  'table-name' = 'dws_user_behavior_1min',\n" +
            "  'driver' = 'com.clickhouse.jdbc.ClickHouseDriver',\n" +
            "  'sink.buffer-flush.max-rows' = '5000',\n" +
            "  'sink.buffer-flush.interval' = '5s'\n" +
            ");\n\n" +
            "-- ========== DWS聚合逻辑 ==========\n" +
            "INSERT INTO dws_user_behavior_1min\n" +
            "SELECT\n" +
            "  window_start,\n" +
            "  window_end,\n" +
            "  user_id,\n" +
            "  SUM(IF(behavior = 'pv', 1, 0)) AS pv,\n" +
            "  1 AS uv,\n" +
            "  SUM(IF(behavior = 'cart', 1, 0)) AS cart_count,\n" +
            "  SUM(IF(behavior = 'buy', 1, 0)) AS order_count,\n" +
            "  SUM(IF(behavior = 'pay', 1, 0)) AS pay_count,\n" +
            "  SUM(IF(behavior = 'pay', amount, 0)) AS pay_amount,\n" +
            "  DATE_FORMAT(window_start, 'yyyy-MM-dd') AS dt\n" +
            "FROM TABLE(\n" +
            "  TUMBLE(TABLE ods_user_behavior, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)\n" +
            ")\n" +
            "GROUP BY window_start, window_end, user_id;";

        System.out.println("  DWS层:");
        System.out.println("  - 1分钟滚动窗口聚合");
        System.out.println("  - 指标: PV/UV/加购/下单/支付/GMV");
        System.out.println("  - Sink: ClickHouse JDBC (5000行/5秒 刷新)\n");
    }

    // ==================== 5. ADS 层查询 ====================
    static void printADSQueries() {
        System.out.println("--- 5. ADS层: ClickHouse 实时查询 ---");

        String[] queries = {
            // 实时 GMV
            "-- 今日实时 GMV\n" +
            "SELECT\n" +
            "  toDate(window_start) AS dt,\n" +
            "  sum(pay_amount) AS gmv,\n" +
            "  sum(order_count) AS total_orders,\n" +
            "  sum(pv) AS total_pv,\n" +
            "  uniq(user_id) AS total_uv\n" +
            "FROM realtime_dw.dws_user_behavior_1min\n" +
            "WHERE dt = today()\n" +
            "GROUP BY dt;",

            // 每小时趋势
            "-- 每小时趋势\n" +
            "SELECT\n" +
            "  toStartOfHour(window_start) AS hour,\n" +
            "  sum(pay_amount) AS hourly_gmv,\n" +
            "  sum(order_count) AS hourly_orders,\n" +
            "  uniq(user_id) AS hourly_uv\n" +
            "FROM realtime_dw.dws_user_behavior_1min\n" +
            "WHERE dt = today()\n" +
            "GROUP BY hour\n" +
            "ORDER BY hour;",

            // 商品TOP10
            "-- 实时商品销售 TOP 10\n" +
            "SELECT\n" +
            "  product_name,\n" +
            "  category,\n" +
            "  sum(order_count) AS sales,\n" +
            "  sum(pay_amount) AS revenue\n" +
            "FROM realtime_dw.dws_product_sale_1h\n" +
            "WHERE dt = today()\n" +
            "GROUP BY product_name, category\n" +
            "ORDER BY revenue DESC\n" +
            "LIMIT 10;",

            // 地区分布
            "-- 实时地区销售分布\n" +
            "SELECT\n" +
            "  province,\n" +
            "  sum(order_count) AS orders,\n" +
            "  sum(order_amount) AS amount,\n" +
            "  sum(user_count) AS users\n" +
            "FROM realtime_dw.dws_region_sale_1h\n" +
            "WHERE dt = today()\n" +
            "GROUP BY province\n" +
            "ORDER BY amount DESC;",

            // 转化率漏斗
            "-- 实时转化率漏斗\n" +
            "SELECT\n" +
            "  sum(pv) AS total_pv,\n" +
            "  uniq(user_id) AS total_uv,\n" +
            "  sum(cart_count) AS total_cart,\n" +
            "  sum(order_count) AS total_order,\n" +
            "  sum(pay_count) AS total_pay,\n" +
            "  round(sum(cart_count) / sum(pv) * 100, 2) AS pv_to_cart_rate,\n" +
            "  round(sum(order_count) / sum(cart_count) * 100, 2) AS cart_to_order_rate,\n" +
            "  round(sum(pay_count) / sum(order_count) * 100, 2) AS order_to_pay_rate,\n" +
            "  round(sum(pay_count) / sum(pv) * 100, 4) AS overall_conversion_rate\n" +
            "FROM realtime_dw.dws_user_behavior_1min\n" +
            "WHERE dt = today();",

            // 同比环比
            "-- 今日 vs 昨日 同比\n" +
            "SELECT\n" +
            "  toStartOfHour(window_start) AS hour,\n" +
            "  sumIf(pay_amount, dt = today()) AS today_gmv,\n" +
            "  sumIf(pay_amount, dt = yesterday()) AS yesterday_gmv,\n" +
            "  round((sumIf(pay_amount, dt = today()) - sumIf(pay_amount, dt = yesterday()))\n" +
            "    / sumIf(pay_amount, dt = yesterday()) * 100, 2) AS growth_rate\n" +
            "FROM realtime_dw.dws_user_behavior_1min\n" +
            "WHERE dt IN (today(), yesterday())\n" +
            "GROUP BY hour\n" +
            "ORDER BY hour;"
        };

        System.out.println("  ADS层查询 (共 " + queries.length + " 条):");
        String[] names = {"实时GMV大盘", "每小时趋势", "商品TOP10", "地区分布", "转化率漏斗", "同比环比"};
        for (int i = 0; i < names.length; i++) {
            System.out.println("  [" + (i + 1) + "] " + names[i]);
        }
        System.out.println();
    }

    // ==================== 6. 数据质量监控 ====================
    static void printDataQualityMonitor() {
        System.out.println("--- 6. 数据质量监控 ---");

        String monitorSQL =
            "-- ========== 数据延迟监控 ==========\n" +
            "SELECT\n" +
            "  max(window_end) AS latest_window,\n" +
            "  now() AS current_time,\n" +
            "  dateDiff('minute', max(window_end), now()) AS delay_minutes\n" +
            "FROM realtime_dw.dws_user_behavior_1min\n" +
            "WHERE dt = today();\n\n" +
            "-- ========== 数据量波动检测 ==========\n" +
            "SELECT\n" +
            "  toStartOfHour(window_start) AS hour,\n" +
            "  count(*) AS row_count,\n" +
            "  -- 与前一小时对比\n" +
            "  lagInFrame(count(*)) OVER (ORDER BY toStartOfHour(window_start)) AS prev_count,\n" +
            "  round((count(*) - lagInFrame(count(*)) OVER (ORDER BY toStartOfHour(window_start)))\n" +
            "    / lagInFrame(count(*)) OVER (ORDER BY toStartOfHour(window_start)) * 100, 2)\n" +
            "    AS change_rate\n" +
            "FROM realtime_dw.dws_user_behavior_1min\n" +
            "WHERE dt = today()\n" +
            "GROUP BY hour\n" +
            "ORDER BY hour;\n\n" +
            "-- ========== 空值比例检测 ==========\n" +
            "SELECT\n" +
            "  count(*) AS total,\n" +
            "  countIf(user_id = 0) AS null_user,\n" +
            "  countIf(pay_amount = 0 AND pay_count > 0) AS zero_amount,\n" +
            "  round(countIf(user_id = 0) / count(*) * 100, 2) AS null_rate\n" +
            "FROM realtime_dw.dws_user_behavior_1min\n" +
            "WHERE dt = today();";

        System.out.println("  数据质量监控:");
        System.out.println("  - 数据延迟: 最新窗口 vs 当前时间");
        System.out.println("  - 数据量波动: 小时级环比");
        System.out.println("  - 空值比例: user_id/amount 空值检测\n");
    }

    // 辅助方法: 提取表名
    private static String extractTableName(String ddl) {
        if (ddl.contains("DATABASE")) return "DATABASE realtime_dw";
        if (ddl.contains("MATERIALIZED VIEW")) {
            int idx = ddl.indexOf("realtime_dw.mv_");
            int end = ddl.indexOf(" ", idx);
            return ddl.substring(idx, end);
        }
        if (ddl.contains("TABLE")) {
            int idx = ddl.indexOf("realtime_dw.");
            if (idx > 0) {
                int end = ddl.indexOf(" ", idx);
                if (end > 0) return ddl.substring(idx, end);
                return ddl.substring(idx, ddl.indexOf("(", idx)).trim();
            }
        }
        return ddl.substring(0, Math.min(60, ddl.length()));
    }
}
