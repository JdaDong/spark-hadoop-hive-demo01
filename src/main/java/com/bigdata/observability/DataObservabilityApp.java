package com.bigdata.observability;

import java.util.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * ============================================================================
 * 数据可观测性平台 - OpenLineage + Marquez 血缘追踪
 * ============================================================================
 *
 * 一、OpenLineage 标准
 *    1. 事件模型 (RunEvent: START/COMPLETE/FAIL/ABORT)
 *    2. Job / Dataset / Run 三大实体
 *    3. Facets 扩展 (Schema/Source/Stats/Quality/Lineage)
 *    4. 集成 (Spark/Flink/Airflow/dbt)
 *
 * 二、Marquez 元数据服务
 *    1. 架构 & API
 *    2. Dataset 管理 (字段血缘/版本历史)
 *    3. Job 管理 (执行历史/依赖图)
 *    4. Namespace 多租户
 *    5. 搜索 & 发现
 *
 * 三、血缘追踪实战
 *    1. Spark OpenLineage 集成 (自动采集)
 *    2. Flink OpenLineage 集成
 *    3. Airflow OpenLineage Provider
 *    4. dbt OpenLineage 集成
 *    5. 跨系统端到端血缘
 *
 * 四、数据可观测性指标
 *    1. 数据质量指标 (完整性/准确性/一致性/时效性)
 *    2. Pipeline 健康度 (成功率/延迟/SLA)
 *    3. 数据漂移检测 (Schema Drift/Distribution Drift)
 *    4. 影响分析 (上游变更 → 下游影响)
 *    5. 根因分析 (数据异常 → 溯源定位)
 *
 * 五、告警 & 治理
 *    1. SLA 告警 (延迟/失败/数据质量)
 *    2. 自动化修复 (重试/回滚/降级)
 *    3. 数据合规 (PII 追踪/GDPR)
 *    4. 运营仪表盘 (Grafana)
 *
 * 技术栈: OpenLineage 1.7 + Marquez 0.45 + Spark/Flink/Airflow/dbt
 * ============================================================================
 */
public class DataObservabilityApp {

    public static void main(String[] args) {
        System.out.println("╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║      数据可观测性平台 - OpenLineage + Marquez 血缘追踪       ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝");

        // 一、OpenLineage 标准
        demoOpenLineageEventModel();
        demoOpenLineageFacets();

        // 二、Marquez 元数据服务
        demoMarquezArchitecture();
        demoMarquezApi();
        demoMarquezDatasetManagement();
        demoMarquezJobManagement();

        // 三、血缘追踪实战
        demoSparkOpenLineage();
        demoFlinkOpenLineage();
        demoAirflowOpenLineage();
        demoDbtOpenLineage();
        demoCrossSystemLineage();

        // 四、可观测性指标
        demoDataQualityMetrics();
        demoPipelineHealth();
        demoDataDriftDetection();
        demoImpactAnalysis();
        demoRootCauseAnalysis();

        // 五、告警 & 治理
        demoSlaAlerting();
        demoComplianceTracking();
        demoOperationsDashboard();

        System.out.println("\n✅ 数据可观测性平台演示完成！");
    }

    // ====================== 一、OpenLineage 标准 ======================

    /**
     * 1.1 OpenLineage 事件模型
     */
    static void demoOpenLineageEventModel() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("📊 1.1 OpenLineage 事件模型");
        System.out.println("=".repeat(60));

        String eventModel = String.join("\n",
            "OpenLineage 核心概念:",
            "",
            "┌─────────────────────────────────────────────────────────┐",
            "│                  OpenLineage Event                      │",
            "│                                                         │",
            "│  RunEvent {                                             │",
            "│    eventType: START | COMPLETE | FAIL | ABORT           │",
            "│    eventTime: 2024-03-23T02:00:00Z                     │",
            "│    producer: https://airflow.company.com                │",
            "│                                                         │",
            "│    run: {                                               │",
            "│      runId: UUID                                        │",
            "│      facets: { nominalTime, parent, ... }              │",
            "│    }                                                    │",
            "│                                                         │",
            "│    job: {                                               │",
            "│      namespace: \"prod-airflow\"                         │",
            "│      name: \"elt_ecommerce_pipeline.dbt_run\"           │",
            "│      facets: { sourceCode, sql, ... }                  │",
            "│    }                                                    │",
            "│                                                         │",
            "│    inputs: [                                            │",
            "│      { namespace: \"postgres\", name: \"raw.orders\",      │",
            "│        facets: { schema, dataSource, ... } }           │",
            "│    ]                                                    │",
            "│                                                         │",
            "│    outputs: [                                           │",
            "│      { namespace: \"doris\", name: \"core.fct_orders\",    │",
            "│        facets: { schema, columnLineage, stats, ... } } │",
            "│    ]                                                    │",
            "│  }                                                      │",
            "└─────────────────────────────────────────────────────────┘"
        );
        System.out.println(eventModel);

        // 完整事件 JSON 示例
        String eventJson = String.join("\n",
            "",
            "// OpenLineage RunEvent JSON 示例:",
            "{",
            "  \"eventType\": \"COMPLETE\",",
            "  \"eventTime\": \"2024-03-23T02:15:00.000Z\",",
            "  \"producer\": \"https://github.com/apache/airflow/\",",
            "  \"schemaURL\": \"https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent\",",
            "  ",
            "  \"run\": {",
            "    \"runId\": \"a1b2c3d4-e5f6-7890-abcd-ef1234567890\",",
            "    \"facets\": {",
            "      \"nominalTime\": {",
            "        \"nominalStartTime\": \"2024-03-23T02:00:00Z\",",
            "        \"nominalEndTime\": \"2024-03-23T02:15:00Z\"",
            "      },",
            "      \"parent\": {",
            "        \"run\": { \"runId\": \"parent-run-uuid\" },",
            "        \"job\": { \"namespace\": \"prod-airflow\", \"name\": \"elt_pipeline\" }",
            "      }",
            "    }",
            "  },",
            "  ",
            "  \"job\": {",
            "    \"namespace\": \"prod-dbt\",",
            "    \"name\": \"ecommerce_analytics.fct_orders\",",
            "    \"facets\": {",
            "      \"sourceCodeLocation\": {",
            "        \"type\": \"git\",",
            "        \"url\": \"https://git.company.com/dbt/ecommerce/models/marts/core/fct_orders.sql\",",
            "        \"branch\": \"main\",",
            "        \"version\": \"abc1234\"",
            "      },",
            "      \"sql\": {",
            "        \"query\": \"SELECT o.order_id, ... FROM stg_orders o JOIN dim_users u ...\"",
            "      }",
            "    }",
            "  },",
            "  ",
            "  \"inputs\": [",
            "    {",
            "      \"namespace\": \"postgres://analytics-db:5432\",",
            "      \"name\": \"staging.stg_ecommerce__orders\",",
            "      \"facets\": {",
            "        \"schema\": {",
            "          \"fields\": [",
            "            { \"name\": \"order_id\", \"type\": \"BIGINT\" },",
            "            { \"name\": \"user_id\", \"type\": \"BIGINT\" },",
            "            { \"name\": \"order_amount\", \"type\": \"DECIMAL(10,2)\" },",
            "            { \"name\": \"order_status\", \"type\": \"VARCHAR\" }",
            "          ]",
            "        },",
            "        \"dataSource\": {",
            "          \"name\": \"analytics-postgres\",",
            "          \"uri\": \"postgres://analytics-db:5432/analytics\"",
            "        }",
            "      }",
            "    },",
            "    {",
            "      \"namespace\": \"postgres://analytics-db:5432\",",
            "      \"name\": \"core.dim_users\",",
            "      \"facets\": { \"schema\": { \"fields\": [\"...\"] } }",
            "    }",
            "  ],",
            "  ",
            "  \"outputs\": [",
            "    {",
            "      \"namespace\": \"doris://doris-fe:9030\",",
            "      \"name\": \"core.fct_orders\",",
            "      \"facets\": {",
            "        \"schema\": {",
            "          \"fields\": [",
            "            { \"name\": \"order_id\", \"type\": \"BIGINT\" },",
            "            { \"name\": \"user_name\", \"type\": \"VARCHAR\" },",
            "            { \"name\": \"net_amount\", \"type\": \"DECIMAL(10,2)\" }",
            "          ]",
            "        },",
            "        \"columnLineage\": {",
            "          \"fields\": {",
            "            \"net_amount\": {",
            "              \"inputFields\": [",
            "                { \"namespace\": \"postgres\", \"name\": \"stg_orders\", \"field\": \"order_amount\" },",
            "                { \"namespace\": \"postgres\", \"name\": \"stg_orders\", \"field\": \"discount_amount\" }",
            "              ],",
            "              \"transformationDescription\": \"order_amount - discount_amount\",",
            "              \"transformationType\": \"IDENTITY\"",
            "            }",
            "          }",
            "        },",
            "        \"outputStatistics\": {",
            "          \"rowCount\": 1584320,",
            "          \"bytes\": 524288000",
            "        }",
            "      }",
            "    }",
            "  ]",
            "}"
        );
        System.out.println(eventJson);
    }

    /**
     * 1.2 OpenLineage Facets
     */
    static void demoOpenLineageFacets() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🧩 1.2 OpenLineage Facets 扩展");
        System.out.println("=".repeat(60));

        String facets = String.join("\n",
            "OpenLineage Facets 分类:",
            "",
            "┌─────────────────────┬──────────────────────────────────────────┐",
            "│ Facet 类型          │ 说明                                     │",
            "├─────────────────────┼──────────────────────────────────────────┤",
            "│ Run Facets          │                                          │",
            "│  nominalTime        │ 计划执行时间                             │",
            "│  parent             │ 父 Job/Run (DAG 中的上游)               │",
            "│  errorMessage       │ 失败时的错误信息                         │",
            "│  processing_engine  │ 引擎信息 (Spark 3.2.1)                  │",
            "├─────────────────────┼──────────────────────────────────────────┤",
            "│ Job Facets          │                                          │",
            "│  sourceCodeLocation │ 源码位置 (Git URL + commit)             │",
            "│  sql                │ 执行的 SQL 语句                          │",
            "│  ownership          │ 作业归属 (team/owner)                   │",
            "│  documentation      │ 作业说明文档                             │",
            "├─────────────────────┼──────────────────────────────────────────┤",
            "│ Dataset Facets      │                                          │",
            "│  schema             │ 字段名 + 类型 + 描述                    │",
            "│  dataSource         │ 数据源连接信息 (URI)                    │",
            "│  columnLineage      │ 字段级血缘 (field → field)             │",
            "│  outputStatistics   │ 输出统计 (行数/字节/文件数)            │",
            "│  dataQualityMetrics │ 质量指标 (null率/唯一率/异常值)         │",
            "│  symlinks           │ 数据集别名/引用                         │",
            "│  storage            │ 存储格式 (parquet/delta/iceberg)        │",
            "│  lifecycleChange    │ 生命周期 (CREATE/ALTER/DROP/TRUNCATE)   │",
            "└─────────────────────┴──────────────────────────────────────────┘",
            "",
            "// 自定义 Facet 示例: 数据分级",
            "\"customFacets\": {",
            "  \"dataClassification\": {",
            "    \"_producer\": \"https://company.com/governance\",",
            "    \"_schemaURL\": \"https://company.com/facets/DataClassification.json\",",
            "    \"level\": \"CONFIDENTIAL\",",
            "    \"piiFields\": [\"user_name\", \"phone_number\", \"email\"],",
            "    \"retentionDays\": 365,",
            "    \"encryptionRequired\": true",
            "  }",
            "}"
        );
        System.out.println(facets);
    }

    // ====================== 二、Marquez 元数据服务 ======================

    /**
     * 2.1 Marquez 架构
     */
    static void demoMarquezArchitecture() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🏗️ 2.1 Marquez 元数据服务架构");
        System.out.println("=".repeat(60));

        String architecture = String.join("\n",
            "Marquez 架构:",
            "",
            "┌───────────────────────────────────────────────────────────┐",
            "│                    数据生产者                              │",
            "│  ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐          │",
            "│  │Spark │ │Flink │ │Airflw│ │ dbt  │ │Custom│          │",
            "│  └──┬───┘ └──┬───┘ └──┬───┘ └──┬───┘ └──┬───┘          │",
            "│     │OpenLineage Events│         │         │              │",
            "└─────┼────────┼────────┼─────────┼─────────┼──────────────┘",
            "      ▼        ▼        ▼         ▼         ▼",
            "┌───────────────────────────────────────────────────────────┐",
            "│                  Marquez API Server                       │",
            "│  ┌──────────────────────────────────────────────────┐    │",
            "│  │ POST /api/v1/lineage   (接收 OpenLineage Event)  │    │",
            "│  │ GET  /api/v1/namespaces/{ns}/datasets            │    │",
            "│  │ GET  /api/v1/namespaces/{ns}/jobs                │    │",
            "│  │ GET  /api/v1/lineage?nodeId=...                  │    │",
            "│  │ GET  /api/v1/search?q=...                        │    │",
            "│  └──────────────────────────────────────────────────┘    │",
            "│                         ↓                                │",
            "│  ┌──────────────────────────────────────────────────┐    │",
            "│  │              PostgreSQL (元数据存储)               │    │",
            "│  │  datasets / jobs / runs / lineage_events         │    │",
            "│  │  dataset_versions / job_versions / facets        │    │",
            "│  └──────────────────────────────────────────────────┘    │",
            "└───────────────────────────────────────────────────────────┘",
            "                         ↓",
            "┌───────────────────────────────────────────────────────────┐",
            "│                  Marquez Web UI                           │",
            "│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐   │",
            "│  │ 数据集   │ │ 作业列表 │ │ 血缘图   │ │ 搜索发现 │   │",
            "│  │ 浏览     │ │ 执行历史 │ │ (DAG)    │ │          │   │",
            "│  └──────────┘ └──────────┘ └──────────┘ └──────────┘   │",
            "└───────────────────────────────────────────────────────────┘",
            "",
            "# Docker Compose 部署:",
            "# marquez-api:  http://localhost:5001",
            "# marquez-web:  http://localhost:3001"
        );
        System.out.println(architecture);
    }

    /**
     * 2.2 Marquez API
     */
    static void demoMarquezApi() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🔌 2.2 Marquez API 接口");
        System.out.println("=".repeat(60));

        String api = String.join("\n",
            "// ===== Marquez REST API =====",
            "",
            "// 1. 发送 OpenLineage 事件",
            "POST /api/v1/lineage",
            "Content-Type: application/json",
            "Body: { OpenLineage RunEvent JSON }",
            "",
            "// 2. 查询数据集列表",
            "GET /api/v1/namespaces/prod-dbt/datasets",
            "Response: {",
            "  \"datasets\": [",
            "    {",
            "      \"id\": { \"namespace\": \"prod-dbt\", \"name\": \"core.fct_orders\" },",
            "      \"type\": \"DB_TABLE\",",
            "      \"createdAt\": \"2024-03-01T00:00:00Z\",",
            "      \"updatedAt\": \"2024-03-23T02:15:00Z\",",
            "      \"currentVersion\": \"v-abc123\",",
            "      \"fields\": [",
            "        { \"name\": \"order_id\", \"type\": \"BIGINT\", \"tags\": [\"PK\"] },",
            "        { \"name\": \"user_name\", \"type\": \"VARCHAR\", \"tags\": [\"PII\"] },",
            "        { \"name\": \"net_amount\", \"type\": \"DECIMAL\" }",
            "      ],",
            "      \"tags\": [\"production\", \"core\", \"pii\"]",
            "    }",
            "  ]",
            "}",
            "",
            "// 3. 查询数据集血缘",
            "GET /api/v1/lineage?nodeId=dataset:prod-dbt:core.fct_orders&depth=5",
            "Response: {",
            "  \"graph\": [",
            "    {",
            "      \"id\": \"dataset:prod-dbt:core.fct_orders\",",
            "      \"type\": \"DATASET\",",
            "      \"inEdges\": [\"job:prod-dbt:ecommerce_analytics.fct_orders\"],",
            "      \"outEdges\": [\"job:prod-airflow:generate_daily_report\"]",
            "    },",
            "    {",
            "      \"id\": \"job:prod-dbt:ecommerce_analytics.fct_orders\",",
            "      \"type\": \"JOB\",",
            "      \"inEdges\": [\"dataset:postgres:staging.stg_ecommerce__orders\",",
            "                   \"dataset:postgres:core.dim_users\"],",
            "      \"outEdges\": [\"dataset:prod-dbt:core.fct_orders\"]",
            "    }",
            "  ]",
            "}",
            "",
            "// 4. 查询作业执行历史",
            "GET /api/v1/namespaces/prod-dbt/jobs/ecommerce_analytics.fct_orders/runs",
            "Response: {",
            "  \"runs\": [",
            "    {",
            "      \"id\": \"run-uuid-001\",",
            "      \"state\": \"COMPLETED\",",
            "      \"startedAt\": \"2024-03-23T02:00:00Z\",",
            "      \"endedAt\": \"2024-03-23T02:15:00Z\",",
            "      \"durationMs\": 900000",
            "    }",
            "  ]",
            "}",
            "",
            "// 5. 搜索数据集",
            "GET /api/v1/search?q=orders&filter=DATASET&sort=UPDATE_AT",
            "",
            "// 6. 字段级血缘",
            "GET /api/v1/column-lineage?nodeId=dataset:prod-dbt:core.fct_orders&depth=10"
        );
        System.out.println(api);
    }

    /**
     * 2.3 Dataset 管理
     */
    static void demoMarquezDatasetManagement() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("📊 2.3 Marquez Dataset 管理");
        System.out.println("=".repeat(60));

        String dataset = String.join("\n",
            "// 数据集版本管理:",
            "GET /api/v1/namespaces/prod-dbt/datasets/core.fct_orders/versions",
            "",
            "// Response: 每次 Schema/数据变更自动创建新版本",
            "{",
            "  \"versions\": [",
            "    {",
            "      \"version\": \"v-003\",",
            "      \"createdAt\": \"2024-03-23T02:15:00Z\",",
            "      \"fields\": [",
            "        { \"name\": \"order_id\", \"type\": \"BIGINT\" },",
            "        { \"name\": \"net_amount\", \"type\": \"DECIMAL(10,2)\" },",
            "        { \"name\": \"user_cohort_month\", \"type\": \"VARCHAR\" }  // 新增字段",
            "      ],",
            "      \"run\": { \"runId\": \"run-uuid-003\" }",
            "    },",
            "    {",
            "      \"version\": \"v-002\",",
            "      \"createdAt\": \"2024-03-22T02:15:00Z\",",
            "      \"fields\": [",
            "        { \"name\": \"order_id\", \"type\": \"BIGINT\" },",
            "        { \"name\": \"net_amount\", \"type\": \"DECIMAL(10,2)\" }",
            "      ]",
            "    }",
            "  ]",
            "}",
            "",
            "// Schema Diff (版本对比):",
            "// v-002 → v-003:",
            "//   + user_cohort_month VARCHAR  (新增)",
            "//   ~ (无修改字段)",
            "//   - (无删除字段)"
        );
        System.out.println(dataset);
    }

    /**
     * 2.4 Job 管理
     */
    static void demoMarquezJobManagement() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("⚙️ 2.4 Marquez Job 管理");
        System.out.println("=".repeat(60));

        String job = String.join("\n",
            "// 作业依赖图:",
            "GET /api/v1/lineage?nodeId=job:prod-airflow:elt_ecommerce_pipeline&depth=10",
            "",
            "// 血缘 DAG 可视化:",
            "//",
            "// MySQL(orders) ──Airbyte──→ raw.orders ──dbt──→ stg_orders ──→ fct_orders",
            "// MySQL(users)  ──Airbyte──→ raw.users  ──dbt──→ stg_users  ──→ dim_users ↗",
            "// MySQL(products)─Airbyte──→ raw.products─dbt──→ dim_products ──────────────↗",
            "//                                                                     │",
            "//                                                             fin_revenue_daily",
            "//                                                             mkt_user_cohorts",
            "//                                                             daily_report (BI)",
            "",
            "// 作业健康统计:",
            "// ┌────────────────────────────────┬───────┬───────┬───────┐",
            "// │ Job                            │ 成功  │ 失败  │ 平均耗时│",
            "// ├────────────────────────────────┼───────┼───────┼───────┤",
            "// │ airbyte.ecommerce_sync         │ 29/30 │ 1/30  │ 12min  │",
            "// │ dbt.stg_ecommerce__orders      │ 30/30 │ 0/30  │ 45s    │",
            "// │ dbt.fct_orders                 │ 28/30 │ 2/30  │ 3min   │",
            "// │ spark.user_behavior_analysis   │ 30/30 │ 0/30  │ 25min  │",
            "// └────────────────────────────────┴───────┴───────┴───────┘"
        );
        System.out.println(job);
    }

    // ====================== 三、血缘追踪实战 ======================

    /**
     * 3.1 Spark OpenLineage 集成
     */
    static void demoSparkOpenLineage() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("⚡ 3.1 Spark OpenLineage 集成");
        System.out.println("=".repeat(60));

        String sparkConfig = String.join("\n",
            "# Spark 集成 OpenLineage (自动血缘采集):",
            "",
            "# 方式1: spark-submit 参数",
            "spark-submit \\",
            "  --packages io.openlineage:openlineage-spark_2.12:1.7.0 \\",
            "  --conf spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener \\",
            "  --conf spark.openlineage.transport.type=http \\",
            "  --conf spark.openlineage.transport.url=http://marquez:5001/api/v1/lineage \\",
            "  --conf spark.openlineage.namespace=prod-spark \\",
            "  --conf spark.openlineage.parentJobName=elt_ecommerce_pipeline \\",
            "  --conf spark.openlineage.parentRunId=parent-run-uuid \\",
            "  --class com.bigdata.spark.SparkHiveJavaApp \\",
            "  target/spark-hadoop-hive-demo01-1.0-SNAPSHOT.jar",
            "",
            "# 方式2: spark-defaults.conf",
            "spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener",
            "spark.openlineage.transport.type=http",
            "spark.openlineage.transport.url=http://marquez:5001/api/v1/lineage",
            "spark.openlineage.namespace=prod-spark",
            "",
            "# 方式3: Kafka Transport (高吞吐)",
            "spark.openlineage.transport.type=kafka",
            "spark.openlineage.transport.topicName=openlineage.events",
            "spark.openlineage.transport.properties.bootstrap.servers=kafka:9092",
            "",
            "# 自动采集的血缘信息:",
            "# - 读取的表/文件 → inputs",
            "# - 写入的表/文件 → outputs",
            "# - Schema (列名/类型)",
            "# - 列级血缘 (column lineage)",
            "# - 执行统计 (行数/耗时)"
        );
        System.out.println(sparkConfig);

        // Spark Java 代码集成
        String sparkCode = String.join("\n",
            "",
            "// Java 代码中手动发送 OpenLineage 事件:",
            "import io.openlineage.client.OpenLineage;",
            "import io.openlineage.client.OpenLineageClient;",
            "import io.openlineage.client.transports.HttpTransport;",
            "",
            "OpenLineageClient client = OpenLineageClient.builder()",
            "    .transport(HttpTransport.builder()",
            "        .uri(URI.create(\"http://marquez:5001/api/v1/lineage\"))",
            "        .build())",
            "    .build();",
            "",
            "OpenLineage ol = new OpenLineage(URI.create(\"https://my-spark-app\"));",
            "",
            "// 创建 Run Event",
            "OpenLineage.RunEvent event = ol.newRunEventBuilder()",
            "    .eventType(OpenLineage.RunEvent.EventType.COMPLETE)",
            "    .eventTime(ZonedDateTime.now())",
            "    .run(ol.newRunBuilder().runId(UUID.randomUUID()).build())",
            "    .job(ol.newJobBuilder()",
            "        .namespace(\"prod-spark\")",
            "        .name(\"user_behavior_analysis\")",
            "        .build())",
            "    .inputs(List.of(",
            "        ol.newInputDatasetBuilder()",
            "            .namespace(\"hive://hive-metastore:9083\")",
            "            .name(\"raw.user_events\")",
            "            .facets(ol.newDatasetFacetsBuilder()",
            "                .schema(ol.newSchemaDatasetFacetBuilder()",
            "                    .fields(List.of(",
            "                        ol.newSchemaDatasetFacetFieldsBuilder()",
            "                            .name(\"event_id\").type(\"BIGINT\").build(),",
            "                        ol.newSchemaDatasetFacetFieldsBuilder()",
            "                            .name(\"user_id\").type(\"BIGINT\").build()",
            "                    )).build())",
            "                .build())",
            "            .build()",
            "    ))",
            "    .outputs(List.of(",
            "        ol.newOutputDatasetBuilder()",
            "            .namespace(\"doris://doris-fe:9030\")",
            "            .name(\"analytics.user_behavior_summary\")",
            "            .build()",
            "    ))",
            "    .build();",
            "",
            "client.emit(event);"
        );
        System.out.println(sparkCode);
    }

    /**
     * 3.2 Flink OpenLineage 集成
     */
    static void demoFlinkOpenLineage() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🌊 3.2 Flink OpenLineage 集成");
        System.out.println("=".repeat(60));

        String flinkConfig = String.join("\n",
            "# Flink OpenLineage 集成 (Flink 1.17+):",
            "",
            "# flink-conf.yaml",
            "execution.attached: true",
            "openlineage.transport.type: http",
            "openlineage.transport.url: http://marquez:5001/api/v1/lineage",
            "openlineage.namespace: prod-flink",
            "",
            "# 或通过 FlinkDeployment CRD:",
            "flinkConfiguration:",
            "  openlineage.transport.type: http",
            "  openlineage.transport.url: http://marquez:5001/api/v1/lineage",
            "  openlineage.namespace: prod-flink",
            "",
            "# Flink SQL 自动采集血缘:",
            "# INSERT INTO doris_orders",
            "# SELECT * FROM kafka_source",
            "# → 自动生成: kafka_source(input) → flink_job → doris_orders(output)",
            "",
            "# Flink DataStream API 手动标注:",
            "# 使用 OpenLineage Flink Listener 自动采集",
            "# 或通过 Custom OpenLineage Transport 发送事件"
        );
        System.out.println(flinkConfig);
    }

    /**
     * 3.3 Airflow OpenLineage 集成
     */
    static void demoAirflowOpenLineage() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🌀 3.3 Airflow OpenLineage Provider");
        System.out.println("=".repeat(60));

        String airflowConfig = String.join("\n",
            "# Airflow OpenLineage Provider:",
            "# pip install apache-airflow-providers-openlineage",
            "",
            "# airflow.cfg",
            "[openlineage]",
            "transport = {\"type\": \"http\", \"url\": \"http://marquez:5001/api/v1/lineage\"}",
            "namespace = prod-airflow",
            "disabled_for_operators = \"BashOperator\"  # 排除不需要的 Operator",
            "",
            "# 或环境变量:",
            "OPENLINEAGE_URL=http://marquez:5001",
            "OPENLINEAGE_NAMESPACE=prod-airflow",
            "",
            "# 自动采集:",
            "# - DAG 级别: DAG 开始/结束事件",
            "# - Task 级别: 每个 Task 的 inputs/outputs",
            "# - 支持的 Operator:",
            "#   PostgresOperator → 自动解析 SQL 获取 input/output 表",
            "#   BigQueryOperator → 自动解析 BQ 表",
            "#   SparkSubmitOperator → 委托给 Spark Listener",
            "#   AirbyteTriggerSyncOperator → Airbyte 连接血缘",
            "",
            "# DAG 中自定义 Lineage (inlet/outlet):",
            "from airflow.lineage import AUTO",
            "",
            "task = PythonOperator(",
            "    task_id='process_data',",
            "    python_callable=my_func,",
            "    inlets=[Dataset('postgres://db/raw.orders')],",
            "    outlets=[Dataset('doris://doris/analytics.daily_summary')],",
            ")"
        );
        System.out.println(airflowConfig);
    }

    /**
     * 3.4 dbt OpenLineage 集成
     */
    static void demoDbtOpenLineage() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🔧 3.4 dbt OpenLineage 集成");
        System.out.println("=".repeat(60));

        String dbtConfig = String.join("\n",
            "# dbt 原生支持 OpenLineage (通过 dbt-core 1.7+):",
            "",
            "# 环境变量配置:",
            "OPENLINEAGE_URL=http://marquez:5001",
            "OPENLINEAGE_NAMESPACE=prod-dbt",
            "",
            "# 或 profiles.yml:",
            "config:",
            "  send_anonymous_usage_stats: False",
            "  openlineage:",
            "    url: http://marquez:5001",
            "    namespace: prod-dbt",
            "",
            "# dbt 自动发送的事件:",
            "# 1. dbt run → 每个模型生成 START + COMPLETE/FAIL 事件",
            "# 2. inputs: ref() / source() 引用的上游模型/源表",
            "# 3. outputs: 当前模型生成的表",
            "# 4. facets: schema (列信息) + sql (查询) + columnLineage",
            "",
            "# 示例: dbt run --select fct_orders",
            "# 生成事件:",
            "#   Job: prod-dbt/ecommerce_analytics.fct_orders",
            "#   Inputs: stg_ecommerce__orders, dim_users, dim_products",
            "#   Outputs: core.fct_orders",
            "#   Column Lineage: net_amount ← (order_amount - discount_amount)",
            "",
            "# 与 Airflow Cosmos 联合使用时:",
            "# Airflow 发送 DAG 级别血缘",
            "# dbt (通过 Cosmos) 发送模型级别血缘",
            "# → 自动关联 parent runId, 形成完整 DAG"
        );
        System.out.println(dbtConfig);
    }

    /**
     * 3.5 跨系统端到端血缘
     */
    static void demoCrossSystemLineage() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🔗 3.5 跨系统端到端血缘");
        System.out.println("=".repeat(60));

        String crossLineage = String.join("\n",
            "端到端血缘追踪 (全链路):",
            "",
            "MySQL ecommerce.orders",
            "  │ (Airbyte CDC)",
            "  ▼",
            "PostgreSQL raw.raw_orders        ← Airbyte 发送 OL 事件",
            "  │ (dbt staging)",
            "  ▼",
            "PostgreSQL staging.stg_orders     ← dbt 发送 OL 事件",
            "  │ (dbt intermediate)",
            "  ▼",
            "PostgreSQL (ephemeral) int_orders  ← dbt 发送 OL 事件",
            "  │ (dbt marts)",
            "  ▼",
            "Doris core.fct_orders             ← dbt 发送 OL 事件",
            "  │ (Spark 分析)",
            "  ▼",
            "Doris analytics.user_behavior     ← Spark 发送 OL 事件",
            "  │ (BI 报表)",
            "  ▼",
            "Metabase daily_report             ← 手动标注 OL 事件",
            "",
            "字段级血缘示例:",
            "core.fct_orders.net_amount",
            "  ← staging.stg_orders.order_amount - staging.stg_orders.discount_amount",
            "  ← raw.raw_orders.order_amount / 100",
            "  ← MySQL ecommerce.orders.order_amount (分)",
            "",
            "# Marquez UI 血缘图:",
            "# 在 http://localhost:3001 可视化查看完整血缘 DAG",
            "# 支持: 上游溯源 / 下游影响 / 字段级钻取"
        );
        System.out.println(crossLineage);
    }

    // ====================== 四、可观测性指标 ======================

    /**
     * 4.1 数据质量指标
     */
    static void demoDataQualityMetrics() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("📏 4.1 数据质量指标 (四维度)");
        System.out.println("=".repeat(60));

        String metrics = String.join("\n",
            "数据质量四维度:",
            "",
            "┌──────────────┬───────────────────────────────────────────────┐",
            "│ 维度         │ 指标                                         │",
            "├──────────────┼───────────────────────────────────────────────┤",
            "│ 完整性       │ NULL 率: SUM(CASE WHEN col IS NULL)/COUNT(*) │",
            "│ Completeness │ 行数波动: ABS(今日-昨日)/昨日 < 10%         │",
            "│              │ 字段覆盖: 非空字段比例 > 99%                 │",
            "├──────────────┼───────────────────────────────────────────────┤",
            "│ 准确性       │ 值域检查: order_amount >= 0                  │",
            "│ Accuracy     │ 格式校验: email/phone 正则匹配              │",
            "│              │ 引用完整性: 外键关系 100% 匹配              │",
            "├──────────────┼───────────────────────────────────────────────┤",
            "│ 一致性       │ 跨系统对账: 源端行数 vs 目标行数 差异<0.1%  │",
            "│ Consistency  │ 聚合验证: 明细金额之和 = 汇总金额           │",
            "│              │ 唯一约束: 主键唯一率 100%                    │",
            "├──────────────┼───────────────────────────────────────────────┤",
            "│ 时效性       │ 数据新鲜度: 最新数据 < SLA 时间             │",
            "│ Timeliness   │ Pipeline 延迟: 实际完成时间 - 计划时间      │",
            "│              │ 端到端延迟: 源数据产生 → 可查询 < 15min     │",
            "└──────────────┴───────────────────────────────────────────────┘",
            "",
            "// OpenLineage dataQualityMetrics Facet:",
            "\"dataQualityMetrics\": {",
            "  \"rowCount\": 1584320,",
            "  \"bytes\": 524288000,",
            "  \"columnMetrics\": {",
            "    \"order_id\": {",
            "      \"nullCount\": 0, \"distinctCount\": 1584320, \"sum\": null,",
            "      \"min\": \"1\", \"max\": \"1584320\", \"quantiles\": {}",
            "    },",
            "    \"order_amount\": {",
            "      \"nullCount\": 12, \"distinctCount\": 45678, \"sum\": 89567890.50,",
            "      \"min\": \"0.01\", \"max\": \"99999.99\",",
            "      \"quantiles\": {\"0.25\": \"45.00\", \"0.50\": \"128.50\", \"0.75\": \"389.00\"}",
            "    }",
            "  }",
            "}"
        );
        System.out.println(metrics);
    }

    /**
     * 4.2 Pipeline 健康度
     */
    static void demoPipelineHealth() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("💚 4.2 Pipeline 健康度");
        System.out.println("=".repeat(60));

        String health = String.join("\n",
            "Pipeline 健康度仪表盘:",
            "",
            "┌────────────────────────────────────────────────────────┐",
            "│  ELT Pipeline 健康度  (过去 30 天)                     │",
            "│                                                        │",
            "│  整体 SLA 达成率:  96.7% (29/30)  🟢                  │",
            "│                                                        │",
            "│  ┌────────────────────────┬──────┬──────┬──────────┐  │",
            "│  │ Stage                  │ 成功率│ P95  │ SLA      │  │",
            "│  ├────────────────────────┼──────┼──────┼──────────┤  │",
            "│  │ 1.Airbyte EL           │ 96.7%│ 15m  │ < 30m 🟢│  │",
            "│  │ 2.dbt staging          │ 100% │ 2m   │ < 5m  🟢│  │",
            "│  │ 3.dbt marts            │ 93.3%│ 8m   │ < 15m 🟢│  │",
            "│  │ 4.dbt test             │ 100% │ 3m   │ < 10m 🟢│  │",
            "│  │ 端到端                 │ 93.3%│ 28m  │ < 60m 🟢│  │",
            "│  └────────────────────────┴──────┴──────┴──────────┘  │",
            "│                                                        │",
            "│  失败分析:                                             │",
            "│  - 03/15: Airbyte MySQL 连接超时 (已重试成功)         │",
            "│  - 03/20: dbt fct_orders Schema 变更 (手动修复)       │",
            "└────────────────────────────────────────────────────────┘",
            "",
            "# Prometheus 指标:",
            "# openlineage_run_duration_seconds{job='fct_orders', status='COMPLETED'}",
            "# openlineage_run_total{status='FAILED'}",
            "# openlineage_dataset_freshness_seconds{dataset='fct_orders'}"
        );
        System.out.println(health);
    }

    /**
     * 4.3 数据漂移检测
     */
    static void demoDataDriftDetection() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("📉 4.3 数据漂移检测");
        System.out.println("=".repeat(60));

        String drift = String.join("\n",
            "数据漂移类型:",
            "",
            "1. Schema Drift (结构漂移):",
            "   - 新增字段: raw.orders +user_agent (可接受)",
            "   - 删除字段: raw.orders -discount_amount (危险!)",
            "   - 类型变更: order_amount INT → VARCHAR (危险!)",
            "   → dbt contract enforced 自动拦截",
            "",
            "2. Distribution Drift (分布漂移):",
            "   - 数值偏移: order_amount P50 从 ¥128 → ¥45 (异常!)",
            "   - 空值率突增: user_city NULL 率从 2% → 35% (异常!)",
            "   - 值域变化: order_status 出现新值 'partial_refund'",
            "",
            "3. Volume Drift (数量漂移):",
            "   - 行数骤降: 日订单从 50000 → 5000 (异常!)",
            "   - 行数暴增: 日订单从 50000 → 500000 (大促?)",
            "",
            "// 检测 SQL (dbt test):",
            "-- 分布漂移检测:",
            "WITH today AS (",
            "    SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY order_amount) AS p50",
            "    FROM {{ ref('fct_orders') }}",
            "    WHERE ordered_date = CURRENT_DATE - 1",
            "),",
            "baseline AS (",
            "    SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY order_amount) AS p50",
            "    FROM {{ ref('fct_orders') }}",
            "    WHERE ordered_date BETWEEN CURRENT_DATE - 30 AND CURRENT_DATE - 2",
            ")",
            "SELECT * FROM today t, baseline b",
            "WHERE ABS(t.p50 - b.p50) / NULLIF(b.p50, 0) > 0.5  -- 偏差 > 50%",
            "",
            "// 自动告警: 漂移 → Slack + PagerDuty"
        );
        System.out.println(drift);
    }

    /**
     * 4.4 影响分析
     */
    static void demoImpactAnalysis() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("💥 4.4 影响分析 (上游变更 → 下游影响)");
        System.out.println("=".repeat(60));

        String impact = String.join("\n",
            "场景: 源表 MySQL ecommerce.orders 新增字段 'coupon_id'",
            "",
            "影响分析 (基于 Marquez 血缘图):",
            "",
            "MySQL ecommerce.orders (Schema 变更)",
            "  │",
            "  ├──→ raw.raw_orders          (Airbyte 自动同步新字段) ✅",
            "  │    ├──→ stg_ecommerce__orders (dbt view, 需更新) ⚠️",
            "  │    │    ├──→ int_orders__pivoted (ephemeral) ⚠️",
            "  │    │    │    └──→ fct_orders     (table) ⚠️",
            "  │    │    │         ├──→ fin_revenue_daily ⚠️",
            "  │    │    │         ├──→ mkt_user_cohorts ⚠️",
            "  │    │    │         └──→ BI daily_report   ⚠️",
            "  │    │    └──→ dim_orders_snapshot (快照) ⚠️",
            "  │    └──→ spark.user_behavior_analysis ⚠️",
            "  │",
            "  └──→ Flink ecommerce_streaming (CDC, 自动) ✅",
            "",
            "影响范围: 9 个下游 Dataset, 6 个下游 Job",
            "",
            "// Marquez API 查询下游影响:",
            "GET /api/v1/lineage?nodeId=dataset:mysql:ecommerce.orders&depth=10&direction=downstream",
            "",
            "// 变更通知:",
            "// → Slack: '@data-platform Schema 变更: ecommerce.orders +coupon_id'",
            "// → 受影响团队: core-analytics, marketing, finance",
            "// → 推荐操作: 更新 stg 模型, 重新运行 dbt build"
        );
        System.out.println(impact);
    }

    /**
     * 4.5 根因分析
     */
    static void demoRootCauseAnalysis() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🔍 4.5 根因分析 (数据异常 → 溯源定位)");
        System.out.println("=".repeat(60));

        String rca = String.join("\n",
            "场景: BI 报表显示 3月23日 日收入骤降 80%",
            "",
            "根因分析步骤:",
            "",
            "1. 定位异常 Dataset:",
            "   BI daily_report ← fin_revenue_daily ← fct_orders",
            "   → fct_orders 3/23 数据量异常: 仅 8432 条 (正常 50000+)",
            "",
            "2. 上游溯源 (Marquez 血缘):",
            "   fct_orders ← stg_ecommerce__orders ← raw.raw_orders",
            "   → raw.raw_orders 3/23 数据正常: 50000+ 条 ✅",
            "   → stg_ecommerce__orders 3/23 仅 8432 条 ❌",
            "",
            "3. 钻取 stg 模型:",
            "   → SQL: WHERE created_at >= '{{ var(\"start_date\") }}'",
            "   → 发现 var('start_date') 被意外修改为 '2024-03-22'",
            "   → 导致只取了一天的增量数据 (而非全量)",
            "",
            "4. 根因确认:",
            "   dbt_project.yml 中 start_date 变量被 PR #142 修改",
            "   → 提交者: developer_a, 时间: 3/22 18:00",
            "   → Code Review 未发现此变更",
            "",
            "5. 修复:",
            "   → 恢复 start_date = '2024-01-01'",
            "   → dbt run --select stg_ecommerce__orders+ --full-refresh",
            "   → 验证 fct_orders 行数恢复正常",
            "",
            "// 时间线:",
            "// 3/22 18:00 - PR #142 合并 (修改 start_date)",
            "// 3/23 02:00 - ELT Pipeline 运行 (数据异常)",
            "// 3/23 02:15 - dbt test 通过 (未覆盖行数检查 ❌)",
            "// 3/23 09:00 - BI 团队发现日收入异常",
            "// 3/23 09:30 - Marquez 血缘溯源定位根因",
            "// 3/23 10:00 - 修复发布, 数据恢复",
            "",
            "改进措施:",
            "// 1. 添加行数波动测试 (dbt test)",
            "// 2. dbt_project.yml 变量变更需要额外 Review",
            "// 3. 数据质量 SLA 告警 (行数骤降自动告警)"
        );
        System.out.println(rca);
    }

    // ====================== 五、告警 & 治理 ======================

    /**
     * 5.1 SLA 告警
     */
    static void demoSlaAlerting() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🔔 5.1 SLA 告警体系");
        System.out.println("=".repeat(60));

        String alerting = String.join("\n",
            "SLA 告警规则:",
            "",
            "┌──────────────────────┬──────────┬──────────┬──────────────┐",
            "│ 告警类型             │ 阈值     │ 级别     │ 通知方式     │",
            "├──────────────────────┼──────────┼──────────┼──────────────┤",
            "│ Pipeline 延迟        │ > 60min  │ P1       │ PagerDuty    │",
            "│ Pipeline 失败        │ 连续2次  │ P1       │ PagerDuty    │",
            "│ 数据质量 error       │ 任意     │ P2       │ Slack + Email│",
            "│ 数据新鲜度           │ > 24h    │ P2       │ Slack        │",
            "│ Schema 变更          │ 任意     │ P3       │ Slack        │",
            "│ 行数异常 (>50%偏差)  │ > 50%    │ P2       │ Slack + Email│",
            "│ 分布漂移             │ P50>50%  │ P3       │ Slack        │",
            "└──────────────────────┴──────────┴──────────┴──────────────┘",
            "",
            "# Prometheus AlertManager 规则:",
            "groups:",
            "  - name: data_pipeline_alerts",
            "    rules:",
            "      - alert: PipelineDelayExceeded",
            "        expr: openlineage_run_duration_seconds > 3600",
            "        for: 5m",
            "        labels:",
            "          severity: critical",
            "          team: data-platform",
            "        annotations:",
            "          summary: 'Pipeline {{ $labels.job }} 延迟超过 1 小时'",
            "          description: '当前耗时: {{ $value }}s'",
            "",
            "      - alert: DataFreshnessExpired",
            "        expr: time() - openlineage_dataset_last_updated_timestamp > 86400",
            "        for: 10m",
            "        labels:",
            "          severity: warning",
            "        annotations:",
            "          summary: '数据集 {{ $labels.dataset }} 超过 24 小时未更新'"
        );
        System.out.println(alerting);
    }

    /**
     * 5.2 合规追踪
     */
    static void demoComplianceTracking() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🛡️ 5.2 数据合规追踪 (PII / GDPR)");
        System.out.println("=".repeat(60));

        String compliance = String.join("\n",
            "PII 数据血缘追踪:",
            "",
            "// 通过 Marquez Tag 标记 PII 字段:",
            "// Dataset fct_orders → Column user_name → Tag: PII",
            "",
            "// PII 字段传播追踪:",
            "MySQL users.real_name (PII)",
            "  → raw.raw_users.real_name (PII)",
            "    → stg_users.user_name (PII, 已脱敏: 张*)",
            "      → dim_users.user_name (PII, 已脱敏)",
            "        → fct_orders.user_name (PII, 已脱敏)",
            "          → daily_report (PII, 已脱敏) ✅",
            "",
            "// GDPR 数据删除请求处理:",
            "// 1. 收到删除请求: user_id = 1001",
            "// 2. 通过 Marquez 血缘查询所有包含 user_id 的 Dataset:",
            "GET /api/v1/search?q=user_id&filter=DATASET",
            "// 结果: raw_users, stg_users, dim_users, fct_orders, ...",
            "// 3. 对每个 Dataset 执行删除:",
            "DELETE FROM dim_users WHERE user_id = 1001;",
            "DELETE FROM fct_orders WHERE user_id = 1001;",
            "// 4. 审计记录: GDPR deletion request fulfilled for user 1001",
            "",
            "// 数据保留策略:",
            "// ┌──────────────┬──────────┬───────────────┐",
            "// │ 数据分级     │ 保留期限 │ 自动清理      │",
            "// ├──────────────┼──────────┼───────────────┤",
            "// │ PII 原始数据 │ 90 天    │ ✅ 自动删除   │",
            "// │ 脱敏后数据   │ 365 天   │ ✅ 自动归档   │",
            "// │ 聚合指标     │ 无限     │ ❌ 永久保留   │",
            "// └──────────────┴──────────┴───────────────┘"
        );
        System.out.println(compliance);
    }

    /**
     * 5.3 运营仪表盘
     */
    static void demoOperationsDashboard() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("📊 5.3 运营仪表盘 (Grafana)");
        System.out.println("=".repeat(60));

        String dashboard = String.join("\n",
            "Grafana 数据可观测性仪表盘:",
            "",
            "┌────────────────────────────────────────────────────────────┐",
            "│  📊 数据平台运营仪表盘 (实时)                             │",
            "│                                                            │",
            "│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐      │",
            "│  │ 活跃 Pipeline│ │ 今日运行次数 │ │ SLA 达成率   │      │",
            "│  │    42        │ │    156       │ │   97.2%      │      │",
            "│  └──────────────┘ └──────────────┘ └──────────────┘      │",
            "│                                                            │",
            "│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐      │",
            "│  │ 数据新鲜度   │ │ 质量测试通过 │ │ 活跃告警     │      │",
            "│  │ 98.5% 🟢    │ │ 234/240 🟢  │ │   2 ⚠️       │      │",
            "│  └──────────────┘ └──────────────┘ └──────────────┘      │",
            "│                                                            │",
            "│  Pipeline 执行耗时趋势 (7天):                             │",
            "│  40min ─────────────────────────────────────               │",
            "│  30min ───────╲──────────────────────/──────               │",
            "│  20min ────────╲────────────────────/───────               │",
            "│  10min ─────────╲──────────────────/────────               │",
            "│         Mon  Tue  Wed  Thu  Fri  Sat  Sun                  │",
            "│                                                            │",
            "│  数据集数量增长:                                           │",
            "│  ┌──────────┬──────┬──────┬──────┐                        │",
            "│  │ Namespace│ 数据集│ 作业 │ 运行 │                        │",
            "│  ├──────────┼──────┼──────┼──────┤                        │",
            "│  │ prod-dbt │ 45   │ 45   │ 1350 │                        │",
            "│  │ prod-spark│ 12  │ 8    │ 240  │                        │",
            "│  │ prod-flink│ 6   │ 6    │ ∞    │                        │",
            "│  │ prod-airbyte│8  │ 8    │ 240  │                        │",
            "│  └──────────┴──────┴──────┴──────┘                        │",
            "└────────────────────────────────────────────────────────────┘",
            "",
            "# Grafana 数据源: Prometheus + Marquez API",
            "# 面板类型: Stat/Graph/Table/Flowchart",
            "",
            "# Docker Compose 启动 Marquez:",
            "# marquez-api:     http://localhost:5001",
            "# marquez-web:     http://localhost:3001",
            "# marquez-db:      PostgreSQL (内部)",
            "# grafana:         http://localhost:3000 (已有)"
        );
        System.out.println(dashboard);
    }
}
