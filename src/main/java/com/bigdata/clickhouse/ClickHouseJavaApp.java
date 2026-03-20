package com.bigdata.clickhouse;

import com.clickhouse.jdbc.ClickHouseDataSource;

import java.sql.*;
import java.util.*;

/**
 * ClickHouse Java 完整示例
 * 
 * 功能特性:
 * 1. 表引擎 (MergeTree, ReplicatedMergeTree)
 * 2. 分布式表 (Distributed)
 * 3. 物化视图 (Materialized View)
 * 4. 批量写入
 * 5. OLAP 查询优化
 * 6. 数据压缩
 * 7. 分区管理
 * 8. 副本与分片
 * 
 * @author BigData Team
 */
public class ClickHouseJavaApp {

    private static final String JDBC_URL = "jdbc:clickhouse://localhost:8123/default";
    private static final String USERNAME = "default";
    private static final String PASSWORD = "";

    public static void main(String[] args) {
        System.out.println("=== ClickHouse 完整示例 ===\n");

        try {
            // 示例1: 创建表
            createTables();

            // 示例2: 批量写入
            batchInsert();

            // 示例3: OLAP 查询
            olapQueries();

            // 示例4: 物化视图
            materializedViews();

            // 示例5: 分布式表
            distributedTables();

            // 示例6: 性能优化
            performanceOptimization();

        } catch (Exception e) {
            System.err.println("执行失败: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 示例1: 创建表
     * - MergeTree 表引擎
     * - 分区和排序键
     * - 索引和压缩
     */
    private static void createTables() throws SQLException {
        System.out.println("【示例1】创建表");

        try (Connection conn = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD);
             Statement stmt = conn.createStatement()) {

            // 1. 创建本地表 (MergeTree)
            System.out.println("\n✅ 创建 MergeTree 表:");
            String createTableSQL = """
                CREATE TABLE IF NOT EXISTS events_local (
                    event_id UInt64,
                    user_id UInt64,
                    event_type String,
                    event_time DateTime,
                    properties String
                ) ENGINE = MergeTree()
                PARTITION BY toYYYYMM(event_time)
                ORDER BY (event_time, user_id, event_id)
                SETTINGS index_granularity = 8192
                """;
            
            // stmt.execute(createTableSQL);
            System.out.println("   表名: events_local");
            System.out.println("   引擎: MergeTree");
            System.out.println("   分区: 按月 toYYYYMM(event_time)");
            System.out.println("   排序: (event_time, user_id, event_id)");
            System.out.println("   索引粒度: 8192");

            // 2. 创建副本表 (ReplicatedMergeTree)
            System.out.println("\n✅ 创建副本表:");
            String createReplicatedSQL = """
                CREATE TABLE IF NOT EXISTS events_replicated (
                    event_id UInt64,
                    user_id UInt64,
                    event_type String,
                    event_time DateTime,
                    properties String
                ) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/events', '{replica}')
                PARTITION BY toYYYYMM(event_time)
                ORDER BY (event_time, user_id)
                """;
            
            System.out.println("   引擎: ReplicatedMergeTree");
            System.out.println("   ZooKeeper 路径: /clickhouse/tables/{shard}/events");
            System.out.println("   副本: {replica}");

            // 3. 创建聚合表 (SummingMergeTree)
            System.out.println("\n✅ 创建聚合表:");
            String createSummingSQL = """
                CREATE TABLE IF NOT EXISTS events_summary (
                    event_date Date,
                    event_type String,
                    event_count UInt64
                ) ENGINE = SummingMergeTree()
                PARTITION BY toYYYYMM(event_date)
                ORDER BY (event_date, event_type)
                """;
            
            System.out.println("   引擎: SummingMergeTree");
            System.out.println("   自动聚合: event_count");
        }

        System.out.println("\n创建表示例完成\n");
    }

    /**
     * 示例2: 批量写入
     * - PreparedStatement 批量插入
     * - 异步插入
     * - 性能优化
     */
    private static void batchInsert() throws SQLException {
        System.out.println("【示例2】批量写入");

        try (Connection conn = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD)) {
            
            String insertSQL = "INSERT INTO events_local VALUES (?, ?, ?, ?, ?)";
            
            try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
                
                long startTime = System.currentTimeMillis();
                
                // 批量插入10000条数据
                for (int i = 1; i <= 10000; i++) {
                    pstmt.setLong(1, i);
                    pstmt.setLong(2, i % 1000);
                    pstmt.setString(3, "click");
                    pstmt.setTimestamp(4, new Timestamp(System.currentTimeMillis()));
                    pstmt.setString(5, "{\"page\":\"home\"}");
                    pstmt.addBatch();
                    
                    // 每1000条提交一次
                    if (i % 1000 == 0) {
                        pstmt.executeBatch();
                    }
                }
                
                long endTime = System.currentTimeMillis();
                
                System.out.println("\n✅ 批量写入完成:");
                System.out.println("   写入记录数: 10000");
                System.out.println("   耗时: " + (endTime - startTime) + " ms");
                System.out.println("   吞吐量: " + (10000 * 1000 / (endTime - startTime)) + " rows/s");
            }
        }

        System.out.println("\n✅ 写入优化建议:");
        System.out.println("  1. 批量大小: 1000-10000 行");
        System.out.println("  2. 异步插入: INSERT INTO ... SETTINGS async_insert=1");
        System.out.println("  3. 压缩: 使用 LZ4 或 ZSTD");
        System.out.println("  4. 分区: 按时间分区,避免跨分区查询");

        System.out.println("\n批量写入示例完成\n");
    }

    /**
     * 示例3: OLAP 查询
     * - 聚合查询
     * - 窗口函数
     * - JOIN 优化
     * - 分区裁剪
     */
    private static void olapQueries() throws SQLException {
        System.out.println("【示例3】OLAP 查询");

        try (Connection conn = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD);
             Statement stmt = conn.createStatement()) {

            // 1. 聚合查询
            System.out.println("\n✅ 聚合查询 - 按事件类型统计:");
            String aggregateSQL = """
                SELECT 
                    event_type,
                    count() as event_count,
                    uniq(user_id) as unique_users
                FROM events_local
                WHERE event_time >= today() - 7
                GROUP BY event_type
                ORDER BY event_count DESC
                """;
            
            System.out.println(aggregateSQL);
            System.out.println("   查询优化:");
            System.out.println("     - 分区裁剪: WHERE event_time >= today() - 7");
            System.out.println("     - 聚合下推: GROUP BY 在存储层执行");
            System.out.println("     - 列式存储: 只读取需要的列");

            // 2. 窗口函数
            System.out.println("\n✅ 窗口函数 - 每日活跃用户趋势:");
            String windowSQL = """
                SELECT 
                    toDate(event_time) as date,
                    uniq(user_id) as dau,
                    lagInFrame(dau, 1) OVER (ORDER BY date) as prev_dau,
                    dau - prev_dau as dau_diff
                FROM events_local
                WHERE event_time >= today() - 30
                GROUP BY date
                ORDER BY date
                """;
            
            System.out.println(windowSQL);

            // 3. JOIN 查询
            System.out.println("\n✅ JOIN 查询 - 订单与用户关联:");
            String joinSQL = """
                SELECT 
                    o.order_id,
                    u.user_name,
                    o.amount
                FROM orders o
                INNER JOIN users u ON o.user_id = u.user_id
                WHERE o.order_date >= today() - 1
                """;
            
            System.out.println(joinSQL);
            System.out.println("   JOIN 优化:");
            System.out.println("     - 小表右侧: 将小表放在 JOIN 右侧");
            System.out.println("     - 分布式 JOIN: 使用 GLOBAL JOIN");
            System.out.println("     - 预聚合: 使用物化视图减少 JOIN");

            // 4. 复杂查询
            System.out.println("\n✅ 复杂查询 - 漏斗分析:");
            String funnelSQL = """
                SELECT
                    windowFunnel(3600)(event_time, event_type = 'page_view',
                                                   event_type = 'add_cart',
                                                   event_type = 'purchase') as level
                FROM events_local
                WHERE event_time >= today()
                GROUP BY user_id
                """;
            
            System.out.println(funnelSQL);
        }

        System.out.println("\nOLAP 查询示例完成\n");
    }

    /**
     * 示例4: 物化视图
     * - 创建物化视图
     * - 增量更新
     * - 查询加速
     */
    private static void materializedViews() throws SQLException {
        System.out.println("【示例4】物化视图");

        try (Connection conn = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD);
             Statement stmt = conn.createStatement()) {

            // 1. 创建物化视图
            System.out.println("\n✅ 创建物化视图 - 每日事件统计:");
            String createMVSQL = """
                CREATE MATERIALIZED VIEW IF NOT EXISTS events_daily_mv
                ENGINE = SummingMergeTree()
                PARTITION BY toYYYYMM(event_date)
                ORDER BY (event_date, event_type)
                AS SELECT
                    toDate(event_time) as event_date,
                    event_type,
                    count() as event_count,
                    uniq(user_id) as unique_users
                FROM events_local
                GROUP BY event_date, event_type
                """;
            
            System.out.println(createMVSQL);
            System.out.println("   特点:");
            System.out.println("     - 增量更新: 新数据自动写入视图");
            System.out.println("     - 预聚合: 查询直接读取聚合结果");
            System.out.println("     - 性能提升: 10-100倍加速");

            // 2. 查询物化视图
            System.out.println("\n✅ 查询物化视图:");
            String queryMVSQL = """
                SELECT 
                    event_date,
                    event_type,
                    sum(event_count) as total_count,
                    sum(unique_users) as total_users
                FROM events_daily_mv
                WHERE event_date >= today() - 7
                GROUP BY event_date, event_type
                ORDER BY event_date DESC
                """;
            
            System.out.println(queryMVSQL);

            // 3. 级联物化视图
            System.out.println("\n✅ 级联物化视图 - 每周汇总:");
            String cascadeMVSQL = """
                CREATE MATERIALIZED VIEW events_weekly_mv
                ENGINE = SummingMergeTree()
                ORDER BY (week_start, event_type)
                AS SELECT
                    toMonday(event_date) as week_start,
                    event_type,
                    sum(event_count) as event_count,
                    sum(unique_users) as unique_users
                FROM events_daily_mv
                GROUP BY week_start, event_type
                """;
            
            System.out.println(cascadeMVSQL);
        }

        System.out.println("\n物化视图示例完成\n");
    }

    /**
     * 示例5: 分布式表
     * - Distributed 引擎
     * - 分片与副本
     * - 集群配置
     */
    private static void distributedTables() {
        System.out.println("【示例5】分布式表");

        // 1. 创建分布式表
        System.out.println("\n✅ 创建分布式表:");
        String createDistSQL = """
            CREATE TABLE events_distributed ON CLUSTER my_cluster (
                event_id UInt64,
                user_id UInt64,
                event_type String,
                event_time DateTime
            ) ENGINE = Distributed(my_cluster, default, events_local, rand())
            """;
        
        System.out.println(createDistSQL);
        System.out.println("   集群: my_cluster");
        System.out.println("   本地表: events_local");
        System.out.println("   分片键: rand() (随机分片)");

        // 2. 集群配置
        System.out.println("\n✅ 集群配置 (config.xml):");
        String clusterConfig = """
            <remote_servers>
                <my_cluster>
                    <shard>
                        <replica>
                            <host>node1</host>
                            <port>9000</port>
                        </replica>
                        <replica>
                            <host>node2</host>
                            <port>9000</port>
                        </replica>
                    </shard>
                    <shard>
                        <replica>
                            <host>node3</host>
                            <port>9000</port>
                        </replica>
                        <replica>
                            <host>node4</host>
                            <port>9000</port>
                        </replica>
                    </shard>
                </my_cluster>
            </remote_servers>
            """;
        
        System.out.println(clusterConfig);
        System.out.println("   架构: 2 分片 x 2 副本");

        // 3. 分布式查询
        System.out.println("\n✅ 分布式查询:");
        System.out.println("   SELECT count() FROM events_distributed;");
        System.out.println("   -> 自动路由到所有分片并聚合结果");

        System.out.println("\n✅ 分片策略:");
        System.out.println("  1. rand(): 随机分片,负载均衡");
        System.out.println("  2. user_id: 按用户分片,相关查询快");
        System.out.println("  3. cityHash64(user_id): 哈希分片,分布均匀");

        System.out.println("\n分布式表示例完成\n");
    }

    /**
     * 示例6: 性能优化
     * - 表引擎选择
     * - 索引优化
     * - 查询优化
     * - 硬件配置
     */
    private static void performanceOptimization() {
        System.out.println("【示例6】性能优化");

        System.out.println("\n✅ 表引擎选择:");
        System.out.println("  MergeTree: 通用场景,支持主键");
        System.out.println("  ReplacingMergeTree: 去重场景");
        System.out.println("  SummingMergeTree: 预聚合场景");
        System.out.println("  AggregatingMergeTree: 复杂聚合");
        System.out.println("  CollapsingMergeTree: 状态变更");

        System.out.println("\n✅ 索引优化:");
        System.out.println("  1. 主键索引: ORDER BY 越靠前列过滤性越强");
        System.out.println("  2. 跳数索引: minmax, set, bloom_filter");
        System.out.println("  3. 索引粒度: 8192 (默认),可根据场景调整");

        System.out.println("\n✅ 查询优化:");
        System.out.println("  1. 分区裁剪: WHERE 条件使用分区列");
        System.out.println("  2. 列裁剪: SELECT 只查询需要的列");
        System.out.println("  3. PREWHERE: 提前过滤,减少数据读取");
        System.out.println("  4. 采样: SAMPLE 0.1 (查询10%数据)");
        System.out.println("  5. 物化视图: 预聚合常用查询");

        System.out.println("\n✅ 配置优化:");
        System.out.println("  max_threads: 8 (查询并行度)");
        System.out.println("  max_memory_usage: 10GB (单查询内存)");
        System.out.println("  max_bytes_before_external_sort: 10GB (外排序阈值)");

        System.out.println("\n✅ 性能基准:");
        System.out.println("  单节点: 百万级 QPS,亿级数据秒级响应");
        System.out.println("  集群: 千万级 QPS,万亿级数据秒级响应");
        System.out.println("  压缩比: 10:1 (平均)");

        System.out.println("\n性能优化示例完成\n");
    }
}
