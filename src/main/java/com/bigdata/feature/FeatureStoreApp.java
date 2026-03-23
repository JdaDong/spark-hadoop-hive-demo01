package com.bigdata.feature;

/**
 * ============================================================
 * 实时特征工程 & Feature Store (Feast) 实战
 * ============================================================
 *
 * 核心问题：
 * - 训练/推理特征不一致 (Training-Serving Skew) 是 ML 最大痛点
 * - Feature Store 统一管理离线/在线特征，保证一致性
 *
 * 本模块涵盖：
 * 1. Feature Store 架构 (Feast 框架)
 * 2. 离线特征 (Batch Features from Spark/Doris)
 * 3. 在线特征 (Real-time Features from Flink)
 * 4. 流批统一特征 Pipeline
 * 5. Feature Serving & Discovery
 * 6. 特征监控 & 漂移检测
 *
 * @author bigdata-team
 */
public class FeatureStoreApp {

    public static void main(String[] args) {
        System.out.println("╔═══════════════════════════════════════════════════════════╗");
        System.out.println("║   实时特征工程 & Feature Store 深度实战                      ║");
        System.out.println("╚═══════════════════════════════════════════════════════════╝");

        // ============================================================
        // 第1部分: Feature Store 架构
        // ============================================================
        demonstrateFeatureStoreArchitecture();

        // ============================================================
        // 第2部分: Feast 项目配置
        // ============================================================
        demonstrateFeastConfig();

        // ============================================================
        // 第3部分: 特征定义与注册
        // ============================================================
        demonstrateFeatureDefinition();

        // ============================================================
        // 第4部分: 流批统一特征 Pipeline
        // ============================================================
        demonstrateStreamBatchPipeline();

        // ============================================================
        // 第5部分: Feature Serving 在线推理
        // ============================================================
        demonstrateFeatureServing();

        // ============================================================
        // 第6部分: 特征监控 & 漂移检测
        // ============================================================
        demonstrateFeatureMonitoring();

        System.out.println("\n✅ 实时特征工程 & Feature Store 全部演示完成!");
    }

    // ============================================================
    //  第1部分: Feature Store 架构
    // ============================================================
    private static void demonstrateFeatureStoreArchitecture() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🏗️ 第1部分: Feature Store 架构");
        System.out.println("=".repeat(60));

        String architecture = """
            ┌─────────────────────────────────────────────────────────────────┐
            │                Feature Store 全景架构 (Feast)                    │
            ├─────────────────────────────────────────────────────────────────┤
            │                                                                 │
            │  ┌────────────────────────────────────────────────────────┐    │
            │  │                  特征定义层 (Registry)                  │    │
            │  │  feature_store.yaml → Entity / FeatureView / Service   │    │
            │  │  存储: S3/GCS/本地文件/PostgreSQL                       │    │
            │  └────────────────────────┬───────────────────────────────┘    │
            │                           │                                     │
            │  ┌────────────────────────┴───────────────────────────────┐    │
            │  │               特征计算层 (Transformation)               │    │
            │  │                                                        │    │
            │  │  离线特征 (Batch)          实时特征 (Stream)             │    │
            │  │  ┌──────────────┐         ┌──────────────────┐        │    │
            │  │  │ Spark SQL    │         │ Flink SQL/DataStream│      │    │
            │  │  │ dbt          │         │ Kafka Streams      │      │    │
            │  │  │ 调度: Airflow│         │ 调度: 持续运行      │      │    │
            │  │  │              │         │                    │        │    │
            │  │  │ 日/小时聚合  │         │ 秒级/分钟级窗口    │        │    │
            │  │  └──────┬───────┘         └────────┬──────────┘        │    │
            │  │         │                          │                    │    │
            │  └─────────┼──────────────────────────┼────────────────────┘    │
            │            │                          │                         │
            │  ┌─────────┴──────────────────────────┴────────────────────┐   │
            │  │               特征存储层 (Offline / Online Store)        │   │
            │  │                                                         │   │
            │  │  Offline Store         Online Store                     │   │
            │  │  ┌──────────────┐     ┌──────────────────┐             │   │
            │  │  │ Doris/Hive   │     │ Redis Cluster    │             │   │
            │  │  │ S3/Parquet   │     │ DynamoDB         │             │   │
            │  │  │ BigQuery     │     │ Bigtable         │             │   │
            │  │  │              │     │                  │             │   │
            │  │  │ 训练数据获取 │     │ 在线推理 <10ms   │             │   │
            │  │  └──────────────┘     └──────────────────┘             │   │
            │  └─────────────────────────────────────────────────────────┘   │
            │                           │                                     │
            │  ┌────────────────────────┴───────────────────────────────┐    │
            │  │               特征服务层 (Feature Serving)              │    │
            │  │                                                        │    │
            │  │  训练获取                 在线推理                      │    │
            │  │  get_historical_features  get_online_features          │    │
            │  │  (Point-in-Time Join)     (Redis Lookup)               │    │
            │  │  → 训练集 DataFrame       → 推理请求补全               │    │
            │  │                                                        │    │
            │  │  Python SDK / Java SDK / REST API / gRPC               │    │
            │  └────────────────────────────────────────────────────────┘    │
            └─────────────────────────────────────────────────────────────────┘
            
            核心价值:
            ┌────────────────────┬──────────────────────────────────────────┐
            │ 问题                │ Feature Store 解决方案                    │
            ├────────────────────┼──────────────────────────────────────────┤
            │ Training-Serving   │ 统一特征定义，同一份代码同时服务于            │
            │ Skew (训练推理偏差) │ 训练数据生成和在线推理                      │
            │                    │                                          │
            │ 特征重复计算        │ 特征注册中心，搜索+复用已有特征             │
            │                    │ 团队A的特征可以直接被团队B使用               │
            │                    │                                          │
            │ Point-in-Time      │ 自动处理时间点连接，防止未来数据泄露          │
            │ Correctness        │ 确保训练数据反映历史真实状态                 │
            │                    │                                          │
            │ 在线低延迟          │ 预物化到 Redis，在线查询 <5ms              │
            └────────────────────┴──────────────────────────────────────────┘
            """;
        System.out.println(architecture);
    }

    // ============================================================
    //  第2部分: Feast 项目配置
    // ============================================================
    private static void demonstrateFeastConfig() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("⚙️ 第2部分: Feast 项目配置");
        System.out.println("=".repeat(60));

        String feastConfig = """
            # ============================================================
            # Feast 项目配置
            # 文件: feature_repo/feature_store.yaml
            # ============================================================
            project: ecommerce_features
            provider: local                        # local | aws | gcp
            
            registry:
              registry_type: sql
              path: postgresql://feast:feast123@localhost:5432/feast_registry
              cache_ttl_seconds: 60
            
            offline_store:
              type: spark                          # spark | file | bigquery | redshift
              spark_conf:
                spark.master: "local[*]"
                spark.sql.catalog.hive: org.apache.spark.sql.hive.HiveExternalCatalog
                spark.sql.warehouse.dir: "hdfs://namenode:9000/user/hive/warehouse"
            
            online_store:
              type: redis                          # redis | dynamodb | datastore | sqlite
              connection_string: "redis://redis-cluster:6379"
              key_ttl_seconds: 86400               # 24小时过期
            
            entity_key_serialization_version: 2
            
            # ============================================================
            # 另一种配置: 使用 Doris 作为离线存储
            # ============================================================
            # offline_store:
            #   type: custom
            #   module: feast_doris
            #   connection_string: "jdbc:mysql://doris-fe:9030/feature_db"
            #   engine: doris
            
            # ============================================================
            # Feast 命令行工具
            # ============================================================
            # pip install feast[redis,spark]
            
            # 初始化项目
            # feast init ecommerce_features
            
            # 应用特征定义 (注册到 Registry)
            # feast apply
            
            # 物化特征到 Online Store (Redis)
            # feast materialize 2024-01-01T00:00:00 2024-01-15T00:00:00
            
            # 增量物化 (从上次截止时间到现在)
            # feast materialize-incremental $(date +%Y-%m-%dT%H:%M:%S)
            
            # 查看注册的特征
            # feast feature-views list
            # feast entities list
            
            # Feast UI (Web 管理界面)
            # feast ui --port 8888
            """;
        System.out.println(feastConfig);
    }

    // ============================================================
    //  第3部分: 特征定义与注册
    // ============================================================
    private static void demonstrateFeatureDefinition() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("📋 第3部分: 特征定义与注册");
        System.out.println("=".repeat(60));

        String featureDefinition = """
            # ============================================================
            # Feast 特征定义 (Python)
            # 文件: feature_repo/features.py
            # ============================================================
            from feast import (
                Entity, FeatureView, FeatureService,
                Field, FileSource, PushSource,
                BatchFeatureView, StreamFeatureView
            )
            from feast.types import Float32, Float64, Int64, String, UnixTimestamp
            from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import (
                SparkSource
            )
            from datetime import timedelta
            
            # ============================================================
            # Entity 定义 (业务实体)
            # ============================================================
            user_entity = Entity(
                name="user",
                join_keys=["user_id"],
                description="电商用户实体"
            )
            
            product_entity = Entity(
                name="product",
                join_keys=["product_id"],
                description="商品实体"
            )
            
            # ============================================================
            # 离线特征视图 (Batch) - 日级聚合
            # ============================================================
            
            # 数据源: Doris / Hive 离线表
            user_order_stats_source = SparkSource(
                name="user_order_stats",
                query=\"\"\"
                    SELECT
                        user_id,
                        -- 订单统计
                        COUNT(order_id) AS order_count_30d,
                        SUM(order_amount) AS total_amount_30d,
                        AVG(order_amount) AS avg_order_amount_30d,
                        MAX(order_amount) AS max_order_amount_30d,
                        STDDEV(order_amount) AS std_order_amount_30d,
                        
                        -- 时间特征
                        DATEDIFF(CURRENT_DATE, MAX(order_date)) AS days_since_last_order,
                        DATEDIFF(CURRENT_DATE, MIN(order_date)) AS days_since_first_order,
                        
                        -- 品类偏好 (Top3 品类占比)
                        MAX(CASE WHEN cat_rank=1 THEN category END) AS top1_category,
                        MAX(CASE WHEN cat_rank=1 THEN cat_ratio END) AS top1_category_ratio,
                        
                        -- 支付偏好
                        MAX(CASE WHEN pay_rank=1 THEN payment_method END) AS preferred_payment,
                        
                        -- RFM 特征
                        NTILE(5) OVER (ORDER BY DATEDIFF(CURRENT_DATE, MAX(order_date)))
                            AS recency_score,
                        NTILE(5) OVER (ORDER BY COUNT(order_id))
                            AS frequency_score,
                        NTILE(5) OVER (ORDER BY SUM(order_amount))
                            AS monetary_score,
                        
                        event_timestamp
                    FROM dwd.dwd_user_order_agg_30d
                    WHERE dt = '${ds}'
                    GROUP BY user_id, event_timestamp
                \"\"\",
                timestamp_field="event_timestamp"
            )
            
            user_order_features = BatchFeatureView(
                name="user_order_features",
                entities=[user_entity],
                ttl=timedelta(days=2),             # 特征有效期
                schema=[
                    Field(name="order_count_30d", dtype=Int64),
                    Field(name="total_amount_30d", dtype=Float64),
                    Field(name="avg_order_amount_30d", dtype=Float64),
                    Field(name="max_order_amount_30d", dtype=Float64),
                    Field(name="std_order_amount_30d", dtype=Float64),
                    Field(name="days_since_last_order", dtype=Int64),
                    Field(name="days_since_first_order", dtype=Int64),
                    Field(name="top1_category", dtype=String),
                    Field(name="top1_category_ratio", dtype=Float64),
                    Field(name="preferred_payment", dtype=String),
                    Field(name="recency_score", dtype=Int64),
                    Field(name="frequency_score", dtype=Int64),
                    Field(name="monetary_score", dtype=Int64),
                ],
                source=user_order_stats_source,
                online=True,                       # 物化到 Redis
                tags={"team": "user-growth", "domain": "user"},
                description="用户30天订单行为特征 (日更新)"
            )
            
            # ============================================================
            # 实时特征视图 (Stream) - 分钟级/秒级
            # ============================================================
            
            user_realtime_source = PushSource(
                name="user_realtime_events",
                batch_source=SparkSource(
                    name="user_realtime_batch",
                    table="feature_db.user_realtime_features"
                )
            )
            
            user_realtime_features = StreamFeatureView(
                name="user_realtime_features",
                entities=[user_entity],
                ttl=timedelta(hours=1),
                schema=[
                    Field(name="click_count_10min", dtype=Int64),
                    Field(name="cart_count_10min", dtype=Int64),
                    Field(name="search_count_10min", dtype=Int64),
                    Field(name="page_view_count_10min", dtype=Int64),
                    Field(name="active_duration_sec_10min", dtype=Int64),
                    Field(name="current_page_category", dtype=String),
                    Field(name="is_returning_visitor", dtype=Int64),
                ],
                source=user_realtime_source,
                online=True,
                tags={"team": "recommendation", "latency": "realtime"},
                description="用户10分钟窗口实时行为特征"
            )
            
            # ============================================================
            # Feature Service (特征服务 - 组合多个 FeatureView)
            # ============================================================
            
            recommendation_feature_service = FeatureService(
                name="recommendation_v2",
                features=[
                    user_order_features[["order_count_30d", "avg_order_amount_30d",
                                         "top1_category", "recency_score"]],
                    user_realtime_features[["click_count_10min", "cart_count_10min",
                                            "current_page_category"]],
                ],
                description="推荐模型 v2 特征服务",
                tags={"model": "recommendation-v2", "team": "recommendation"}
            )
            
            fraud_detection_service = FeatureService(
                name="fraud_detection_v1",
                features=[
                    user_order_features[["order_count_30d", "total_amount_30d",
                                         "std_order_amount_30d", "days_since_first_order"]],
                    user_realtime_features[["click_count_10min", "page_view_count_10min"]],
                ],
                description="风控模型 v1 特征服务"
            )
            """;
        System.out.println(featureDefinition);
    }

    // ============================================================
    //  第4部分: 流批统一特征 Pipeline
    // ============================================================
    private static void demonstrateStreamBatchPipeline() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🔄 第4部分: 流批统一特征 Pipeline");
        System.out.println("=".repeat(60));

        String streamBatchPipeline = """
            ┌──────────────────────────────────────────────────────────────┐
            │              流批统一特征计算架构                               │
            ├──────────────────────────────────────────────────────────────┤
            │                                                              │
            │  实时路径 (Stream Path):                                      │
            │  Kafka → Flink SQL → Redis (Online Store)                   │
            │                                                              │
            │  离线路径 (Batch Path):                                       │
            │  Hive/Doris → Spark SQL → Parquet/Doris (Offline Store)    │
            │                        → Redis (Online Store 物化)          │
            │                                                              │
            │  关键: 两条路径使用相同的特征计算逻辑!                          │
            └──────────────────────────────────────────────────────────────┘
            
            // ============================================================
            // Flink 实时特征计算 (10分钟滑动窗口)
            // ============================================================
            
            // Flink SQL:
            CREATE TABLE user_events (
                user_id STRING,
                event_type STRING,        -- click/cart/search/page_view
                page_category STRING,
                event_time TIMESTAMP(3),
                WATERMARK FOR event_time AS event_time - INTERVAL '30' SECOND
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'user.behavior.events',
                'properties.bootstrap.servers' = 'kafka:9092',
                'format' = 'json',
                'scan.startup.mode' = 'latest-offset'
            );
            
            -- 10分钟滑动窗口聚合
            CREATE TEMPORARY VIEW user_window_features AS
            SELECT
                user_id,
                window_start,
                window_end,
                COUNT(CASE WHEN event_type = 'click' THEN 1 END)     AS click_count_10min,
                COUNT(CASE WHEN event_type = 'cart' THEN 1 END)      AS cart_count_10min,
                COUNT(CASE WHEN event_type = 'search' THEN 1 END)    AS search_count_10min,
                COUNT(CASE WHEN event_type = 'page_view' THEN 1 END) AS page_view_count_10min,
                TIMESTAMPDIFF(SECOND, MIN(event_time), MAX(event_time)) AS active_duration_sec,
                LAST_VALUE(page_category)                              AS current_page_category
            FROM TABLE(
                HOP(TABLE user_events, DESCRIPTOR(event_time),
                    INTERVAL '1' MINUTE,             -- slide
                    INTERVAL '10' MINUTES)           -- window size
            )
            GROUP BY user_id, window_start, window_end;
            
            -- 写入 Redis (通过 Feast Push API 或直接 Redis Sink)
            CREATE TABLE redis_sink (
                user_id STRING,
                click_count_10min BIGINT,
                cart_count_10min BIGINT,
                search_count_10min BIGINT,
                page_view_count_10min BIGINT,
                active_duration_sec BIGINT,
                current_page_category STRING,
                PRIMARY KEY (user_id) NOT ENFORCED
            ) WITH (
                'connector' = 'redis',
                'host' = 'redis-cluster',
                'port' = '6379',
                'command' = 'HSET',
                'key-prefix' = 'feast:user_realtime_features:'
            );
            
            INSERT INTO redis_sink
            SELECT user_id,
                   click_count_10min, cart_count_10min,
                   search_count_10min, page_view_count_10min,
                   active_duration_sec, current_page_category
            FROM user_window_features;
            
            // ============================================================
            // Spark 离线特征回填 (Backfill)
            // ============================================================
            
            // Spark SQL (与 Flink SQL 逻辑一致):
            SELECT
                user_id,
                COUNT(CASE WHEN event_type = 'click' THEN 1 END)     AS click_count_10min,
                COUNT(CASE WHEN event_type = 'cart' THEN 1 END)      AS cart_count_10min,
                -- ... (相同聚合逻辑)
                window.start AS window_start
            FROM user_events_history
            GROUP BY
                user_id,
                window(event_time, '10 minutes', '1 minute')
            
            // 关键: 用统一 SQL 模板生成 Flink 和 Spark 代码
            // 工具: SQLMesh / Jinja2 模板
            """;
        System.out.println(streamBatchPipeline);
    }

    // ============================================================
    //  第5部分: Feature Serving 在线推理
    // ============================================================
    private static void demonstrateFeatureServing() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🚀 第5部分: Feature Serving 在线推理");
        System.out.println("=".repeat(60));

        String featureServing = """
            // ============================================================
            // 在线推理时获取特征 (Python)
            // ============================================================
            
            from feast import FeatureStore
            
            store = FeatureStore(repo_path="feature_repo/")
            
            # ---- 在线获取 (Online Serving) ----
            # 推荐系统接收到用户请求时，从 Redis 获取特征
            online_features = store.get_online_features(
                features=[
                    "user_order_features:order_count_30d",
                    "user_order_features:avg_order_amount_30d",
                    "user_order_features:top1_category",
                    "user_order_features:recency_score",
                    "user_realtime_features:click_count_10min",
                    "user_realtime_features:cart_count_10min",
                    "user_realtime_features:current_page_category",
                ],
                entity_rows=[
                    {"user_id": "U001"},
                    {"user_id": "U002"},
                ]
            ).to_dict()
            
            # 输出:
            # {
            #   "user_id": ["U001", "U002"],
            #   "order_count_30d": [15, 3],
            #   "avg_order_amount_30d": [256.80, 89.50],
            #   "top1_category": ["Electronics", "Books"],
            #   "recency_score": [5, 2],
            #   "click_count_10min": [12, 0],
            #   "cart_count_10min": [2, 0],
            #   "current_page_category": ["手机", null]
            # }
            
            # ---- 通过 Feature Service 获取 ----
            features = store.get_online_features(
                feature_service=store.get_feature_service("recommendation_v2"),
                entity_rows=[{"user_id": "U001"}]
            )
            
            # ---- 训练数据获取 (Historical/Offline) ----
            # Point-in-Time Join: 确保不使用未来数据
            import pandas as pd
            
            entity_df = pd.DataFrame({
                "user_id": ["U001", "U002", "U003"],
                "event_timestamp": [
                    pd.Timestamp("2024-01-10"),
                    pd.Timestamp("2024-01-11"),
                    pd.Timestamp("2024-01-12"),
                ],
                "label": [1, 0, 1]                 # 是否购买
            })
            
            training_df = store.get_historical_features(
                entity_df=entity_df,
                features=[
                    "user_order_features:order_count_30d",
                    "user_order_features:avg_order_amount_30d",
                    "user_order_features:recency_score",
                    "user_order_features:frequency_score",
                    "user_order_features:monetary_score",
                ],
            ).to_df()
            
            # 输出: 每个用户在对应 event_timestamp 时间点的特征
            # 确保 U001 在 2024-01-10 这一刻看到的只是 2024-01-10 之前的数据
            
            // ============================================================
            // Java SDK 在线获取 (REST API 方式)
            // ============================================================
            
            // Feast 提供 Feature Server (Go/Python)
            // 启动: feast serve --port 6566
            
            // Java 调用示例:
            OkHttpClient client = new OkHttpClient();
            
            String requestBody = new JsonObject()
                .add("feature_service", "recommendation_v2")
                .add("entities", new JsonObject()
                    .add("user_id", new JsonArray().add("U001").add("U002")))
                .toString();
            
            Request request = new Request.Builder()
                .url("http://feast-server:6566/get-online-features")
                .post(RequestBody.create(requestBody, MediaType.parse("application/json")))
                .build();
            
            Response response = client.newCall(request).execute();
            // 延迟: P99 < 5ms (Redis 直读)
            """;
        System.out.println(featureServing);
    }

    // ============================================================
    //  第6部分: 特征监控 & 漂移检测
    // ============================================================
    private static void demonstrateFeatureMonitoring() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("📊 第6部分: 特征监控 & 漂移检测");
        System.out.println("=".repeat(60));

        String featureMonitoring = """
            ┌──────────────────────────────────────────────────────────────┐
            │                 特征监控 & 漂移检测体系                        │
            ├──────────────────────────────────────────────────────────────┤
            │                                                              │
            │  1. 特征新鲜度监控                                            │
            │  ┌──────────────────────────────────────────────────┐       │
            │  │ 指标                │ 阈值     │ 告警级别          │       │
            │  ├───────────────────┼─────────┼──────────────────┤       │
            │  │ 离线特征延迟        │ > 2h    │ P2 Warning       │       │
            │  │ 实时特征延迟        │ > 5min  │ P1 Critical      │       │
            │  │ Redis 缓存命中率   │ < 95%   │ P2 Warning       │       │
            │  │ 特征空值率          │ > 5%    │ P2 Warning       │       │
            │  │ 特征覆盖率          │ < 90%   │ P1 Critical      │       │
            │  └──────────────────────────────────────────────────┘       │
            │                                                              │
            │  2. 特征分布漂移检测                                          │
            │  ┌──────────────────────────────────────────────────┐       │
            │  │ 检测方法           │ 适用类型  │ 阈值              │       │
            │  ├──────────────────┼─────────┼───────────────────┤       │
            │  │ PSI              │ 分类特征 │ > 0.25 = 严重漂移  │       │
            │  │ (Population      │          │ 0.1~0.25 = 中度   │       │
            │  │  Stability Index)│          │ < 0.1 = 稳定      │       │
            │  │                  │          │                   │       │
            │  │ KS Test          │ 连续特征 │ p-value < 0.05    │       │
            │  │ (Kolmogorov-     │          │ = 显著漂移         │       │
            │  │  Smirnov)        │          │                   │       │
            │  │                  │          │                   │       │
            │  │ Chi-Square       │ 分类特征 │ p-value < 0.05    │       │
            │  │                  │          │                   │       │
            │  │ Wasserstein      │ 连续特征 │ > 2σ baseline     │       │
            │  │ Distance         │          │                   │       │
            │  └──────────────────────────────────────────────────┘       │
            │                                                              │
            │  3. 特征质量看板 (Grafana)                                    │
            │  ┌──────────────────────────────────────────────────┐       │
            │  │  ┌─────────────────┐  ┌─────────────────┐      │       │
            │  │  │ 特征覆盖率      │  │ 空值率趋势       │      │       │
            │  │  │ ████████░ 96.5% │  │ ▁▂▁▁▂▅▁▁ 1.2%  │      │       │
            │  │  └─────────────────┘  └─────────────────┘      │       │
            │  │  ┌─────────────────┐  ┌─────────────────┐      │       │
            │  │  │ PSI 漂移指数    │  │ 特征延迟 P99     │      │       │
            │  │  │ ▁▁▁▂▁▁▅▁ 0.08  │  │ ▂▂▃▂▂▂▂▂ 3ms   │      │       │
            │  │  └─────────────────┘  └─────────────────┘      │       │
            │  └──────────────────────────────────────────────────┘       │
            └──────────────────────────────────────────────────────────────┘
            
            // ============================================================
            // Prometheus 特征监控指标
            // ============================================================
            
            # Feast Feature Server 暴露的 Prometheus 指标:
            
            # 在线查询延迟
            feast_feature_server_request_latency_seconds{
              feature_service="recommendation_v2",
              quantile="0.99"
            }
            
            # 特征空值率
            feast_feature_null_ratio{
              feature_view="user_order_features",
              feature="order_count_30d"
            }
            
            # Redis 缓存命中率
            feast_online_store_cache_hit_ratio{
              feature_view="user_realtime_features"
            }
            
            # 自定义漂移检测指标 (PSI)
            feast_feature_psi{
              feature_view="user_order_features",
              feature="avg_order_amount_30d"
            }
            
            # Grafana 告警规则:
            # IF feast_feature_psi > 0.25 FOR 1h → P1 告警
            # IF feast_feature_null_ratio > 0.05 FOR 30min → P2 告警
            # IF feast_feature_server_request_latency_seconds{q="0.99"} > 0.01 → P2 告警
            
            // ============================================================
            // Airflow DAG: 每日特征质量检查
            // ============================================================
            
            # @dag(schedule="0 8 * * *", catchup=False)
            # def feature_quality_check():
            #     @task
            #     def check_freshness():
            #         \"\"\"检查所有特征视图的新鲜度\"\"\"
            #         store = FeatureStore(repo_path="feature_repo/")
            #         for fv in store.list_feature_views():
            #             last_update = get_last_materialize_time(fv.name)
            #             if (now() - last_update) > fv.ttl:
            #                 alert(f"Feature {fv.name} 已过期!")
            #
            #     @task
            #     def check_drift():
            #         \"\"\"检查特征分布漂移\"\"\"
            #         for feature in MONITORED_FEATURES:
            #             current = get_current_distribution(feature)
            #             baseline = get_baseline_distribution(feature)
            #             psi = calculate_psi(current, baseline)
            #             if psi > 0.25:
            #                 alert(f"Feature {feature} PSI={psi:.3f} 严重漂移!")
            #
            #     check_freshness() >> check_drift()
            """;
        System.out.println(featureMonitoring);
    }
}
