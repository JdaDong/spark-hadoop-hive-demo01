package com.bigdata.orchestration;

import java.util.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * ============================================================================
 * 数据编排平台 - dbt + Airbyte 现代 ELT 体系
 * ============================================================================
 *
 * 一、dbt (Data Build Tool) 数据转换层
 *    1. 项目结构 & 模型层次 (staging/intermediate/marts)
 *    2. 增量模型 (incremental + merge策略)
 *    3. 快照 (SCD Type 2 缓慢变化维)
 *    4. 数据测试 (generic/singular/custom)
 *    5. 宏 (Jinja2 模板 + 自定义宏)
 *    6. 文档 & 数据血缘 (DAG 依赖图)
 *    7. dbt Packages (dbt_utils/dbt_expectations)
 *    8. Hooks & Operations (pre/post-hook)
 *
 * 二、Airbyte 数据集成层 (EL)
 *    1. Source Connector (MySQL CDC / REST API / S3)
 *    2. Destination Connector (Doris / PostgreSQL)
 *    3. 同步模式 (Full Refresh / Incremental / CDC)
 *    4. 连接配置 & Catalog 管理
 *    5. API 编程接口
 *
 * 三、编排集成
 *    1. Airflow + dbt (Cosmos / BashOperator)
 *    2. Airbyte + Airflow 触发同步
 *    3. 端到端 ELT Pipeline
 *    4. 数据质量门禁 (dbt test → 告警)
 *    5. 环境管理 (dev/staging/prod)
 *
 * 四、数据契约 & 治理
 *    1. Schema Contract (dbt 模型契约)
 *    2. Data Freshness (源数据新鲜度)
 *    3. 变更管理 (PR Review / CI/CD)
 *
 * 技术栈: dbt-core 1.7 + Airbyte 0.50 + Airflow 2.7 + Jinja2
 * ============================================================================
 */
public class DataOrchestrationApp {

    public static void main(String[] args) {
        System.out.println("╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║          数据编排平台 - dbt + Airbyte 现代 ELT 体系          ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝");

        // 一、dbt 数据转换层
        demoDbtProjectStructure();
        demoDbtModels();
        demoDbtIncrementalModels();
        demoDbtSnapshots();
        demoDbtTests();
        demoDbtMacros();
        demoDbtPackages();

        // 二、Airbyte 数据集成层
        demoAirbyteSourceConnectors();
        demoAirbyteDestinationConnectors();
        demoAirbyteSyncModes();
        demoAirbyteApiIntegration();

        // 三、编排集成
        demoAirflowDbtIntegration();
        demoEndToEndPipeline();
        demoDataQualityGate();
        demoEnvironmentManagement();

        // 四、数据契约
        demoSchemaContract();
        demoDataFreshness();
        demoCiCdPipeline();

        System.out.println("\n✅ 数据编排平台演示完成！");
    }

    // ====================== 一、dbt 数据转换层 ======================

    /**
     * 1.1 dbt 项目结构 - 企业级目录规范
     */
    static void demoDbtProjectStructure() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("📦 1.1 dbt 项目结构 - 企业级目录规范");
        System.out.println("=".repeat(60));

        String dbtProjectYml = String.join("\n",
            "# dbt_project.yml - 项目核心配置",
            "name: 'ecommerce_analytics'",
            "version: '1.0.0'",
            "config-version: 2",
            "",
            "profile: 'ecommerce'",
            "",
            "model-paths: [\"models\"]",
            "analysis-paths: [\"analyses\"]",
            "test-paths: [\"tests\"]",
            "seed-paths: [\"seeds\"]",
            "macro-paths: [\"macros\"]",
            "snapshot-paths: [\"snapshots\"]",
            "",
            "target-path: \"target\"",
            "clean-targets: [\"target\", \"dbt_packages\"]",
            "",
            "models:",
            "  ecommerce_analytics:",
            "    staging:",
            "      +materialized: view",
            "      +schema: staging",
            "      +tags: ['staging', 'daily']",
            "    intermediate:",
            "      +materialized: ephemeral",
            "      +tags: ['intermediate']",
            "    marts:",
            "      +materialized: table",
            "      +tags: ['marts']",
            "      core:",
            "        +schema: core",
            "      marketing:",
            "        +schema: marketing",
            "      finance:",
            "        +schema: finance",
            "    metrics:",
            "      +materialized: table",
            "      +schema: metrics",
            "",
            "vars:",
            "  start_date: '2024-01-01'",
            "  default_country: 'CN'",
            "  enable_debug: false",
            "",
            "on-run-start:",
            "  - \"{{ log('dbt run started at ' ~ run_started_at, info=True) }}\"",
            "  - \"CREATE SCHEMA IF NOT EXISTS {{ target.schema }}_staging\"",
            "",
            "on-run-end:",
            "  - \"{{ log('dbt run completed, ' ~ results|length ~ ' models', info=True) }}\""
        );
        System.out.println(dbtProjectYml);

        String projectTree = String.join("\n",
            "",
            "📁 企业级 dbt 项目目录:",
            "ecommerce_analytics/",
            "├── dbt_project.yml              # 项目配置",
            "├── profiles.yml                 # 连接配置 (不提交 Git)",
            "├── packages.yml                 # 第三方包依赖",
            "├── models/",
            "│   ├── staging/                 # Layer 1: 源数据清洗 (view)",
            "│   │   ├── _staging__sources.yml      # 源表声明",
            "│   │   ├── _staging__models.yml        # 模型文档",
            "│   │   ├── stg_ecommerce__orders.sql   # 订单清洗",
            "│   │   ├── stg_ecommerce__users.sql    # 用户清洗",
            "│   │   └── stg_ecommerce__products.sql # 商品清洗",
            "│   ├── intermediate/            # Layer 2: 中间转换 (ephemeral)",
            "│   │   ├── int_orders__pivoted.sql     # 订单透视",
            "│   │   └── int_users__enriched.sql     # 用户增强",
            "│   └── marts/                   # Layer 3: 业务集市 (table)",
            "│       ├── core/dim_users.sql / fct_orders.sql",
            "│       ├── marketing/mkt_user_cohorts.sql",
            "│       └── finance/fin_revenue_daily.sql",
            "├── seeds/                       # 静态维度数据 (CSV)",
            "├── snapshots/                   # SCD Type 2 快照",
            "├── tests/                       # 自定义测试",
            "├── macros/                      # Jinja2 宏",
            "└── analyses/                    # Ad-hoc 分析"
        );
        System.out.println(projectTree);

        String profilesYml = String.join("\n",
            "",
            "# profiles.yml - 多环境连接配置",
            "ecommerce:",
            "  target: dev",
            "  outputs:",
            "    dev:",
            "      type: postgres",
            "      host: localhost",
            "      port: 5432",
            "      user: dbt_dev",
            "      password: \"{{ env_var('DBT_DEV_PASSWORD') }}\"",
            "      dbname: analytics_dev",
            "      schema: dbt_dev",
            "      threads: 4",
            "    staging:",
            "      type: postgres",
            "      host: staging-db.internal",
            "      threads: 8",
            "    prod:",
            "      type: doris           # Apache Doris 适配器",
            "      host: doris-fe",
            "      port: 9030",
            "      user: dbt_prod",
            "      password: \"{{ env_var('DBT_PROD_PASSWORD') }}\"",
            "      dbname: analytics",
            "      schema: dbt_prod",
            "      threads: 16"
        );
        System.out.println(profilesYml);
    }

    /**
     * 1.2 dbt 模型 - 三层架构 (staging → intermediate → marts)
     */
    static void demoDbtModels() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("📊 1.2 dbt 模型 - 三层架构");
        System.out.println("=".repeat(60));

        // Layer 1: sources 声明
        String sourcesYml = String.join("\n",
            "# models/staging/_staging__sources.yml",
            "version: 2",
            "sources:",
            "  - name: ecommerce",
            "    description: \"电商业务数据库 (MySQL CDC → 仓库)\"",
            "    database: raw",
            "    schema: ecommerce",
            "    loader: airbyte",
            "    freshness:",
            "      warn_after: {count: 12, period: hour}",
            "      error_after: {count: 24, period: hour}",
            "    loaded_at_field: _airbyte_emitted_at",
            "    tables:",
            "      - name: orders",
            "        columns:",
            "          - name: order_id",
            "            tests: [unique, not_null]",
            "          - name: order_amount",
            "            tests:",
            "              - not_null",
            "              - dbt_expectations.expect_column_values_to_be_between:",
            "                  min_value: 0",
            "                  max_value: 1000000",
            "      - name: users",
            "        columns:",
            "          - name: user_id",
            "            tests: [unique, not_null]",
            "      - name: products"
        );
        System.out.println(sourcesYml);

        // staging 模型
        String stgOrders = String.join("\n",
            "",
            "-- models/staging/stg_ecommerce__orders.sql",
            "{{ config(materialized='view', tags=['staging', 'daily']) }}",
            "",
            "WITH source AS (",
            "    SELECT * FROM {{ source('ecommerce', 'orders') }}",
            "),",
            "renamed AS (",
            "    SELECT",
            "        order_id,",
            "        user_id,",
            "        product_id,",
            "        {{ cents_to_dollars('order_amount') }} AS order_amount,",
            "        {{ cents_to_dollars('discount_amount') }} AS discount_amount,",
            "        CASE order_status",
            "            WHEN 0 THEN 'pending'",
            "            WHEN 1 THEN 'paid'",
            "            WHEN 2 THEN 'shipped'",
            "            WHEN 3 THEN 'delivered'",
            "            WHEN 4 THEN 'cancelled'",
            "            WHEN 5 THEN 'refunded'",
            "        END AS order_status,",
            "        LOWER(TRIM(channel)) AS order_channel,",
            "        CAST(created_at AS TIMESTAMP) AS ordered_at,",
            "        CAST(paid_at AS TIMESTAMP) AS paid_at,",
            "        _airbyte_emitted_at AS _loaded_at",
            "    FROM source",
            "    WHERE order_id IS NOT NULL",
            "      AND created_at >= '{{ var(\"start_date\") }}'",
            ")",
            "SELECT * FROM renamed"
        );
        System.out.println(stgOrders);

        // Layer 2: intermediate
        String intOrders = String.join("\n",
            "",
            "-- models/intermediate/int_orders__pivoted.sql",
            "{{ config(materialized='ephemeral') }}",
            "",
            "WITH orders AS (",
            "    SELECT * FROM {{ ref('stg_ecommerce__orders') }}",
            "),",
            "user_summary AS (",
            "    SELECT",
            "        user_id,",
            "        COUNT(*) AS total_orders,",
            "        COUNT(CASE WHEN order_status='delivered' THEN 1 END) AS delivered_orders,",
            "        COUNT(CASE WHEN order_status='cancelled' THEN 1 END) AS cancelled_orders,",
            "        SUM(order_amount) AS total_amount,",
            "        AVG(order_amount) AS avg_order_amount,",
            "        MIN(ordered_at) AS first_order_at,",
            "        MAX(ordered_at) AS last_order_at,",
            "        COUNT(CASE WHEN order_channel='app' THEN 1 END) AS app_orders,",
            "        COUNT(CASE WHEN order_channel='web' THEN 1 END) AS web_orders",
            "    FROM orders",
            "    GROUP BY user_id",
            ")",
            "SELECT * FROM user_summary"
        );
        System.out.println(intOrders);

        // Layer 3: marts 事实表
        String fctOrders = String.join("\n",
            "",
            "-- models/marts/core/fct_orders.sql",
            "{{ config(",
            "    materialized='table',",
            "    schema='core',",
            "    partition_by={'field': 'ordered_date', 'data_type': 'date', 'granularity': 'month'},",
            "    cluster_by=['order_status', 'order_channel'],",
            "    contract={'enforced': true}",
            ") }}",
            "",
            "WITH orders AS ( SELECT * FROM {{ ref('stg_ecommerce__orders') }} ),",
            "users AS ( SELECT * FROM {{ ref('dim_users') }} ),",
            "products AS ( SELECT * FROM {{ ref('dim_products') }} ),",
            "final AS (",
            "    SELECT",
            "        o.order_id, o.order_status, o.order_channel,",
            "        DATE(o.ordered_at) AS ordered_date,",
            "        o.user_id, u.user_name, u.user_level, u.user_cohort_month,",
            "        o.product_id, p.product_name, p.category_name, p.brand_name,",
            "        o.order_amount, o.discount_amount,",
            "        o.order_amount - o.discount_amount AS net_amount,",
            "        o.ordered_at, o.paid_at,",
            "        DATEDIFF('minute', o.ordered_at, o.paid_at) AS minutes_to_pay,",
            "        CASE WHEN o.order_amount > 1000 THEN true ELSE false END AS is_high_value",
            "    FROM orders o",
            "    LEFT JOIN users u ON o.user_id = u.user_id",
            "    LEFT JOIN products p ON o.product_id = p.product_id",
            ")",
            "SELECT * FROM final"
        );
        System.out.println(fctOrders);
    }

    /**
     * 1.3 dbt 增量模型 - 高性能增量更新
     */
    static void demoDbtIncrementalModels() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("⚡ 1.3 dbt 增量模型 - 高性能增量更新");
        System.out.println("=".repeat(60));

        String incrementalModel = String.join("\n",
            "-- models/marts/core/fct_user_events.sql",
            "{{ config(",
            "    materialized='incremental',",
            "    unique_key='event_id',",
            "    incremental_strategy='merge',",
            "    partition_by={'field': 'event_date', 'data_type': 'date'},",
            "    on_schema_change='append_new_columns',",
            "    tags=['incremental', 'hourly']",
            ") }}",
            "",
            "WITH source_events AS (",
            "    SELECT",
            "        event_id, user_id, event_type, event_properties,",
            "        platform, device_type, session_id,",
            "        CAST(event_timestamp AS TIMESTAMP) AS event_at,",
            "        DATE(event_timestamp) AS event_date",
            "    FROM {{ source('ecommerce', 'user_events') }}",
            "    {% if is_incremental() %}",
            "        WHERE event_timestamp >= (",
            "            SELECT DATEADD('hour', -3, MAX(event_at)) FROM {{ this }}",
            "        )",
            "    {% endif %}",
            "),",
            "enriched AS (",
            "    SELECT *,",
            "        ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_at) AS event_seq,",
            "        COUNT(*) OVER (PARTITION BY user_id, event_date) AS daily_event_count,",
            "        DATEDIFF('second',",
            "            LAG(event_at) OVER (PARTITION BY session_id ORDER BY event_at),",
            "            event_at) AS seconds_since_last",
            "    FROM source_events",
            ")",
            "SELECT * FROM enriched"
        );
        System.out.println(incrementalModel);

        String strategies = String.join("\n",
            "",
            "增量策略对比:",
            "┌──────────────────┬──────────────┬────────────────┬──────────────┐",
            "│ 策略             │ merge        │ delete+insert  │ append       │",
            "├──────────────────┼──────────────┼────────────────┼──────────────┤",
            "│ 适用场景         │ upsert       │ 分区替换       │ 只追加       │",
            "│ unique_key       │ ✅ 必须       │ ✅ 必须         │ ❌ 不需要    │",
            "│ 支持更新         │ ✅            │ ✅              │ ❌           │",
            "│ 支持删除         │ ❌            │ ✅              │ ❌           │",
            "│ 性能             │ 中等         │ 快 (批量)      │ 最快         │",
            "│ 典型用途         │ 用户画像     │ 日分区全量     │ 日志/事件流  │",
            "└──────────────────┴──────────────┴────────────────┴──────────────┘"
        );
        System.out.println(strategies);
    }

    /**
     * 1.4 dbt 快照 - SCD Type 2 缓慢变化维
     */
    static void demoDbtSnapshots() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("📸 1.4 dbt 快照 - SCD Type 2");
        System.out.println("=".repeat(60));

        String snapshot = String.join("\n",
            "-- snapshots/snap_users.sql",
            "{% snapshot snap_users %}",
            "{{ config(",
            "    target_schema='snapshots',",
            "    unique_key='user_id',",
            "    strategy='check',",
            "    check_cols=['user_level', 'user_city', 'membership_status', 'credit_score'],",
            "    invalidate_hard_deletes=True",
            ") }}",
            "",
            "SELECT user_id, user_name, user_level, user_city,",
            "    membership_status, credit_score, email, registered_at,",
            "    CURRENT_TIMESTAMP AS _snapshot_loaded_at",
            "FROM {{ source('ecommerce', 'users') }}",
            "{% endsnapshot %}",
            "",
            "-- 快照结果 (自动添加 SCD 列):",
            "-- user_id | user_name | ... | dbt_valid_from | dbt_valid_to | dbt_scd_id",
            "--    1001 | 张三      | ... | 2024-01-01     | 2024-06-15   | md5(...)",
            "--    1001 | 张三      | ... | 2024-06-15     | NULL         | md5(...)",
            "--   (等级 silver → gold, dbt_valid_to=NULL 表示当前有效)"
        );
        System.out.println(snapshot);

        String snapshotQuery = String.join("\n",
            "",
            "-- 基于快照的历史分析:",
            "-- 1. 查看用户在某时间点状态",
            "SELECT * FROM {{ ref('snap_users') }}",
            "WHERE user_id = 1001",
            "  AND '2024-03-15' BETWEEN dbt_valid_from AND COALESCE(dbt_valid_to, '9999-12-31')",
            "",
            "-- 2. 会员等级变迁追踪",
            "SELECT user_id, user_level, dbt_valid_from AS level_start,",
            "    DATEDIFF('day', dbt_valid_from, COALESCE(dbt_valid_to, CURRENT_DATE)) AS days",
            "FROM {{ ref('snap_users') }}",
            "ORDER BY user_id, dbt_valid_from"
        );
        System.out.println(snapshotQuery);
    }

    /**
     * 1.5 dbt 测试 - 数据质量保障
     */
    static void demoDbtTests() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🧪 1.5 dbt 测试 - 数据质量保障");
        System.out.println("=".repeat(60));

        String schemaTests = String.join("\n",
            "# models/marts/core/_core__models.yml",
            "version: 2",
            "models:",
            "  - name: fct_orders",
            "    description: \"订单事实宽表\"",
            "    config:",
            "      contract: {enforced: true}",
            "    columns:",
            "      - name: order_id",
            "        data_type: bigint",
            "        constraints: [{type: not_null}, {type: primary_key}]",
            "        tests: [unique, not_null]",
            "      - name: user_id",
            "        tests:",
            "          - not_null",
            "          - relationships:",
            "              to: ref('dim_users')",
            "              field: user_id",
            "      - name: order_amount",
            "        tests:",
            "          - not_null",
            "          - dbt_expectations.expect_column_values_to_be_between:",
            "              min_value: 0",
            "              max_value: 999999.99",
            "      - name: order_status",
            "        tests:",
            "          - accepted_values:",
            "              values: ['pending','paid','shipped','delivered','cancelled','refunded']"
        );
        System.out.println(schemaTests);

        String singularTest = String.join("\n",
            "",
            "-- tests/assert_daily_revenue_positive.sql",
            "-- singular 测试: 每日收入必须为正",
            "WITH daily AS (",
            "    SELECT ordered_date, SUM(net_amount) AS revenue, COUNT(*) AS cnt",
            "    FROM {{ ref('fct_orders') }}",
            "    WHERE order_status NOT IN ('cancelled', 'refunded')",
            "    GROUP BY ordered_date",
            ")",
            "SELECT * FROM daily WHERE revenue <= 0 OR cnt = 0",
            "",
            "-- tests/assert_no_orphan_orders.sql",
            "-- 孤儿订单检查",
            "SELECT o.order_id FROM {{ ref('fct_orders') }} o",
            "LEFT JOIN {{ ref('dim_users') }} u ON o.user_id = u.user_id",
            "WHERE u.user_id IS NULL"
        );
        System.out.println(singularTest);

        String genericTest = String.join("\n",
            "",
            "-- macros/tests/test_row_count_ratio.sql",
            "{% test row_count_ratio(model, compare_model, min_ratio, max_ratio) %}",
            "WITH m AS (SELECT COUNT(*) AS cnt FROM {{ model }}),",
            "     c AS (SELECT COUNT(*) AS cnt FROM {{ compare_model }})",
            "SELECT m.cnt, c.cnt, CAST(m.cnt AS FLOAT)/NULLIF(c.cnt,0) AS ratio",
            "FROM m CROSS JOIN c",
            "WHERE ratio NOT BETWEEN {{ min_ratio }} AND {{ max_ratio }}",
            "{% endtest %}"
        );
        System.out.println(genericTest);
    }

    /**
     * 1.6 dbt 宏 - Jinja2 模板引擎
     */
    static void demoDbtMacros() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🔧 1.6 dbt 宏 - Jinja2 模板引擎");
        System.out.println("=".repeat(60));

        String macros = String.join("\n",
            "-- macros/cents_to_dollars.sql",
            "{% macro cents_to_dollars(column_name, precision=2) %}",
            "    ROUND(CAST({{ column_name }} AS DECIMAL(18,4)) / 100, {{ precision }})",
            "{% endmacro %}",
            "",
            "-- macros/generate_schema_name.sql (覆盖默认逻辑)",
            "{% macro generate_schema_name(custom_schema_name, node) -%}",
            "    {%- set default_schema = target.schema -%}",
            "    {%- if custom_schema_name is none -%} {{ default_schema }}",
            "    {%- elif target.name == 'prod' -%} {{ custom_schema_name | trim }}",
            "    {%- else -%} {{ default_schema }}_{{ custom_schema_name | trim }}",
            "    {%- endif -%}",
            "{%- endmacro %}",
            "",
            "-- macros/date_spine.sql (日期维度生成)",
            "{% macro generate_date_spine(start_date, end_date) %}",
            "WITH date_spine AS (",
            "    {{ dbt_utils.date_spine(",
            "        datepart='day',",
            "        start_date=\"cast('\" ~ start_date ~ \"' as date)\",",
            "        end_date=\"cast('\" ~ end_date ~ \"' as date)\"",
            "    ) }}",
            "),",
            "enriched AS (",
            "    SELECT date_day AS date_key,",
            "        EXTRACT(YEAR FROM date_day) AS year,",
            "        EXTRACT(MONTH FROM date_day) AS month,",
            "        EXTRACT(DOW FROM date_day) AS day_of_week,",
            "        CASE WHEN EXTRACT(DOW FROM date_day) IN (0,6) THEN true ELSE false END AS is_weekend",
            "    FROM date_spine",
            ")",
            "SELECT * FROM enriched",
            "{% endmacro %}",
            "",
            "-- macros/audit_helper.sql (模型变更审计)",
            "{% macro audit_model_changes(model_a, model_b, primary_key, columns) %}",
            "WITH new_records AS (",
            "    SELECT 'NEW' AS change_type, a.*",
            "    FROM {{ model_a }} a LEFT JOIN {{ model_b }} b ON a.{{ primary_key }}=b.{{ primary_key }}",
            "    WHERE b.{{ primary_key }} IS NULL",
            "),",
            "deleted_records AS (",
            "    SELECT 'DELETED', b.* FROM {{ model_b }} b",
            "    LEFT JOIN {{ model_a }} a ON a.{{ primary_key }}=b.{{ primary_key }}",
            "    WHERE a.{{ primary_key }} IS NULL",
            "),",
            "changed_records AS (",
            "    SELECT 'CHANGED', a.* FROM {{ model_a }} a",
            "    INNER JOIN {{ model_b }} b ON a.{{ primary_key }}=b.{{ primary_key }}",
            "    WHERE {% for col in columns %} a.{{ col }}!=b.{{ col }} {{ 'OR' if not loop.last }} {% endfor %}",
            ")",
            "SELECT * FROM new_records UNION ALL",
            "SELECT * FROM deleted_records UNION ALL",
            "SELECT * FROM changed_records",
            "{% endmacro %}"
        );
        System.out.println(macros);
    }

    /**
     * 1.7 dbt Packages 第三方包
     */
    static void demoDbtPackages() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("📦 1.7 dbt Packages 第三方包");
        System.out.println("=".repeat(60));

        String packages = String.join("\n",
            "# packages.yml",
            "packages:",
            "  - package: dbt-labs/dbt_utils           # 通用工具 (surrogate_key/date_spine/pivot)",
            "    version: [\">=1.1.0\", \"<2.0.0\"]",
            "  - package: calogica/dbt_expectations     # Great Expectations 风格测试",
            "    version: [\">=0.10.0\", \"<0.11.0\"]",
            "  - package: dbt-labs/audit_helper          # 模型审计对比",
            "    version: [\">=0.9.0\", \"<1.0.0\"]",
            "  - package: dbt-labs/codegen               # 代码自动生成",
            "    version: [\">=0.12.0\", \"<1.0.0\"]",
            "  - package: elementary-data/elementary     # 数据可观测性",
            "    version: [\">=0.13.0\", \"<0.14.0\"]",
            "",
            "# 安装: dbt deps",
            "",
            "-- dbt_utils 常用功能:",
            "-- 1. {{ dbt_utils.generate_surrogate_key(['order_id', 'product_id']) }}",
            "-- 2. {{ dbt_utils.pivot('channel', dbt_utils.get_column_values(ref('orders'),'channel')) }}",
            "-- 3. {{ dbt_utils.star(ref('dim_users'), except=['password_hash']) }}",
            "-- 4. {{ dbt_utils.union_relations([ref('stg_a'), ref('stg_b')]) }}"
        );
        System.out.println(packages);
    }

    // ====================== 二、Airbyte 数据集成层 ======================

    /**
     * 2.1 Airbyte Source Connectors
     */
    static void demoAirbyteSourceConnectors() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🔌 2.1 Airbyte Source Connectors");
        System.out.println("=".repeat(60));

        String mysqlCdc = String.join("\n",
            "// MySQL CDC Source (基于 Debezium)",
            "{",
            "  \"name\": \"ecommerce-mysql-cdc\",",
            "  \"connectionConfiguration\": {",
            "    \"host\": \"mysql\", \"port\": 3306, \"database\": \"ecommerce\",",
            "    \"username\": \"airbyte_cdc\",",
            "    \"replication_method\": {",
            "      \"method\": \"CDC\",",
            "      \"initial_waiting_seconds\": 300,",
            "      \"server_time_zone\": \"Asia/Shanghai\",",
            "      \"additional_properties\": {",
            "        \"snapshot.mode\": \"initial\",",
            "        \"snapshot.locking.mode\": \"minimal\",",
            "        \"tombstones.on.delete\": true,",
            "        \"decimal.handling.mode\": \"string\",",
            "        \"heartbeat.interval.ms\": 10000",
            "      }",
            "    },",
            "    \"tables_to_sync\": [\"orders\",\"order_items\",\"users\",\"products\",\"payments\",\"user_events\"]",
            "  }",
            "}"
        );
        System.out.println(mysqlCdc);

        String apiSource = String.join("\n",
            "",
            "// REST API Source (通用 HTTP)",
            "{",
            "  \"name\": \"third-party-api\",",
            "  \"connectionConfiguration\": {",
            "    \"url_base\": \"https://api.partner.com/v2\",",
            "    \"authenticator\": { \"type\": \"BearerAuthenticator\", \"api_token\": \"${API_TOKEN}\" },",
            "    \"streams\": [{",
            "      \"name\": \"campaign_metrics\",",
            "      \"url_path\": \"/campaigns/metrics\",",
            "      \"paginator\": { \"type\": \"CursorPagination\", \"cursor_value\": \"{{ response.next_cursor }}\", \"page_size\": 100 },",
            "      \"incremental_sync\": {",
            "        \"type\": \"DatetimeBasedCursor\",",
            "        \"cursor_field\": \"updated_at\",",
            "        \"start_datetime\": \"2024-01-01T00:00:00Z\",",
            "        \"step\": \"P7D\"",
            "      },",
            "      \"rate_limiter\": { \"requests_per_window\": 100, \"window_size_in_seconds\": 60 }",
            "    }]",
            "  }",
            "}"
        );
        System.out.println(apiSource);

        String s3Source = String.join("\n",
            "",
            "// S3/MinIO Source (文件数据湖)",
            "{",
            "  \"name\": \"data-lake-s3\",",
            "  \"connectionConfiguration\": {",
            "    \"bucket\": \"data-lake-raw\",",
            "    \"endpoint\": \"http://minio:9000\",",
            "    \"streams\": [",
            "      { \"name\": \"clickstream_logs\", \"format\": { \"filetype\": \"parquet\" },",
            "        \"globs\": [\"clickstream/dt=*/part-*.parquet\"] },",
            "      { \"name\": \"marketing_csv\", \"format\": { \"filetype\": \"csv\", \"delimiter\": \",\" },",
            "        \"globs\": [\"marketing/campaign_*.csv\"] }",
            "    ]",
            "  }",
            "}"
        );
        System.out.println(s3Source);
    }

    /**
     * 2.2 Airbyte Destination Connectors
     */
    static void demoAirbyteDestinationConnectors() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🎯 2.2 Airbyte Destination Connectors");
        System.out.println("=".repeat(60));

        String doris = String.join("\n",
            "// Doris Destination (Stream Load 写入)",
            "{",
            "  \"name\": \"analytics-doris\",",
            "  \"connectionConfiguration\": {",
            "    \"host\": \"doris-fe\", \"http_port\": 8030, \"query_port\": 9030,",
            "    \"username\": \"root\", \"database\": \"raw\",",
            "    \"loading_method\": {",
            "      \"method\": \"STREAM_LOAD\", \"format\": \"json\",",
            "      \"batch_size\": 10000, \"batch_interval_ms\": 5000",
            "    }",
            "  }",
            "}",
            "",
            "// PostgreSQL Destination (dbt 数据仓库)",
            "{",
            "  \"name\": \"analytics-postgres\",",
            "  \"connectionConfiguration\": {",
            "    \"host\": \"analytics-db\", \"port\": 5432,",
            "    \"database\": \"analytics\", \"schema\": \"raw\",",
            "    \"raw_data_schema\": \"_airbyte_raw\",",
            "    \"jdbc_url_params\": \"reWriteBatchedInserts=true\"",
            "  }",
            "}"
        );
        System.out.println(doris);
    }

    /**
     * 2.3 Airbyte 同步模式
     */
    static void demoAirbyteSyncModes() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🔄 2.3 Airbyte 同步模式对比");
        System.out.println("=".repeat(60));

        String modes = String.join("\n",
            "┌─────────────────┬──────────────┬────────────────┬────────────────┐",
            "│ 模式            │ Full Refresh │ Incr Append    │ Incr Dedup(CDC)│",
            "├─────────────────┼──────────────┼────────────────┼────────────────┤",
            "│ 读取方式        │ 全量扫描     │ 基于游标增量   │ Binlog CDC     │",
            "│ 支持更新/删除   │ ✅ (覆盖)    │ ❌ (只追加)    │ ✅ (完整)      │",
            "│ 性能            │ 慢           │ 快             │ 最快           │",
            "│ 源端负载        │ 高           │ 中             │ 低 (读Binlog)  │",
            "│ 典型用途        │ 小维度表     │ 事件日志       │ 订单/用户      │",
            "└─────────────────┴──────────────┴────────────────┴────────────────┘"
        );
        System.out.println(modes);

        String connectionConfig = String.join("\n",
            "",
            "// Connection 配置 (Source → Destination)",
            "{",
            "  \"name\": \"ecommerce-to-analytics\",",
            "  \"schedule\": { \"scheduleType\": \"cron\", \"cronExpression\": \"0 */2 * * *\" },",
            "  \"syncCatalog\": { \"streams\": [",
            "    { \"stream\": {\"name\": \"orders\"}, \"config\": {",
            "        \"syncMode\": \"incremental\", \"destinationSyncMode\": \"append_dedup\",",
            "        \"cursorField\": [\"updated_at\"], \"primaryKey\": [[\"order_id\"]] }},",
            "    { \"stream\": {\"name\": \"user_events\"}, \"config\": {",
            "        \"syncMode\": \"incremental\", \"destinationSyncMode\": \"append\",",
            "        \"cursorField\": [\"event_timestamp\"] }},",
            "    { \"stream\": {\"name\": \"product_categories\"}, \"config\": {",
            "        \"syncMode\": \"full_refresh\", \"destinationSyncMode\": \"overwrite\" }}",
            "  ]},",
            "  \"notificationSettings\": {",
            "    \"sendOnFailure\": { \"notificationType\": [\"slack\",\"email\"] }",
            "  }",
            "}"
        );
        System.out.println(connectionConfig);
    }

    /**
     * 2.4 Airbyte API 编程集成
     */
    static void demoAirbyteApiIntegration() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🔌 2.4 Airbyte API 编程集成");
        System.out.println("=".repeat(60));

        String api = String.join("\n",
            "// Airbyte API v1 集成",
            "",
            "// 1. 触发同步: POST /api/v1/connections/sync",
            "//    Body: { \"connectionId\": \"${CONN_ID}\" }",
            "//    Response: { \"job\": { \"id\": 12345, \"status\": \"pending\" } }",
            "",
            "// 2. 查询状态: POST /api/v1/jobs/get",
            "//    Body: { \"id\": 12345 }",
            "//    Response: { \"job\": { \"status\": \"succeeded\" },",
            "//      \"attempts\": [{ \"totalStats\": { \"recordsEmitted\": 158432 } }] }",
            "",
            "// 3. 重置 Connection: POST /api/v1/connections/reset",
            "",
            "// Java 客户端封装:",
            "public class AirbyteClient {",
            "    private final String baseUrl;",
            "    private final HttpClient httpClient;",
            "",
            "    public long triggerSync(String connectionId) {",
            "        // POST /api/v1/connections/sync → return jobId",
            "    }",
            "    public String waitForCompletion(long jobId, Duration timeout) {",
            "        // 10s 轮询 until succeeded/failed/timeout",
            "    }",
            "    public SyncStats getSyncStats(long jobId) {",
            "        // 解析 recordsSynced, bytesSynced",
            "    }",
            "}"
        );
        System.out.println(api);
    }

    // ====================== 三、编排集成 ======================

    /**
     * 3.1 Airflow + dbt 集成
     */
    static void demoAirflowDbtIntegration() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🔄 3.1 Airflow + dbt 集成编排");
        System.out.println("=".repeat(60));

        String dag = String.join("\n",
            "# airflow/dags/dbt_elt_pipeline_dag.py",
            "# Airflow DAG: Airbyte EL → dbt Transform → 质量检查 → 通知",
            "",
            "from datetime import datetime, timedelta",
            "from airflow import DAG",
            "from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator",
            "from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor",
            "from airflow.utils.task_group import TaskGroup",
            "from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig",
            "",
            "dag = DAG(",
            "    dag_id='elt_ecommerce_pipeline',",
            "    schedule_interval='0 2 * * *',   # 每日 02:00",
            "    start_date=datetime(2024, 1, 1),",
            "    catchup=False,",
            "    max_active_runs=1,",
            "    tags=['elt', 'dbt', 'airbyte', 'production'],",
            ")",
            "",
            "# ===== Stage 1: Airbyte EL =====",
            "with TaskGroup('extract_load', dag=dag) as el:",
            "    trigger = AirbyteTriggerSyncOperator(",
            "        task_id='trigger_sync',",
            "        airbyte_conn_id='airbyte_default',",
            "        connection_id='{{ var.value.airbyte_conn_id }}',",
            "        asynchronous=True, timeout=3600",
            "    )",
            "    wait = AirbyteJobSensor(",
            "        task_id='wait_sync',",
            "        airbyte_conn_id='airbyte_default',",
            "        airbyte_job_id=\"{{ ti.xcom_pull('extract_load.trigger_sync') }}\",",
            "        timeout=3600, poke_interval=30",
            "    )",
            "    freshness = BashOperator(",
            "        task_id='check_freshness',",
            "        bash_command='cd /opt/dbt && dbt source freshness --target prod'",
            "    )",
            "    trigger >> wait >> freshness",
            "",
            "# ===== Stage 2: dbt Transform (Cosmos) =====",
            "with TaskGroup('transform', dag=dag) as transform:",
            "    dbt_staging = DbtTaskGroup(",
            "        group_id='staging',",
            "        project_config=ProjectConfig('/opt/dbt/ecommerce_analytics'),",
            "        profile_config=ProfileConfig(profile_name='ecommerce', target_name='prod'),",
            "        render_config=RenderConfig(select=['tag:staging']),",
            "    )",
            "    dbt_marts = DbtTaskGroup(",
            "        group_id='marts',",
            "        project_config=ProjectConfig('/opt/dbt/ecommerce_analytics'),",
            "        profile_config=ProfileConfig(profile_name='ecommerce', target_name='prod'),",
            "        render_config=RenderConfig(select=['tag:marts']),",
            "    )",
            "    dbt_staging >> dbt_marts",
            "",
            "# ===== Stage 3: 质量门禁 =====",
            "dbt_test = BashOperator(",
            "    task_id='dbt_test',",
            "    bash_command='cd /opt/dbt && dbt test --target prod --store-failures',",
            "    dag=dag",
            ")",
            "",
            "# ===== Stage 4: 通知 =====",
            "notify = SlackWebhookOperator(",
            "    task_id='notify', slack_webhook_conn_id='slack_default',",
            "    message='✅ ELT Pipeline 完成: {{ ds }}', dag=dag",
            ")",
            "",
            "el >> transform >> dbt_test >> notify"
        );
        System.out.println(dag);
    }

    /**
     * 3.2 端到端 ELT Pipeline 架构
     */
    static void demoEndToEndPipeline() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🚀 3.2 端到端 ELT Pipeline 架构");
        System.out.println("=".repeat(60));

        String pipeline = String.join("\n",
            "┌───────────────────────────────────────────────────────────┐",
            "│                    数据源 (Sources)                       │",
            "│  MySQL(CDC) │ REST API │ S3/MinIO │ Kafka(Events)        │",
            "└──────┬──────────┬──────────┬──────────┬──────────────────┘",
            "       ▼          ▼          ▼          ▼",
            "┌───────────────────────────────────────────────────────────┐",
            "│              Airbyte (Extract + Load)                     │",
            "│  CDC Connector │ HTTP Connector │ S3 Conn │ Kafka Conn   │",
            "└──────────────────────┬────────────────────────────────────┘",
            "                       ▼",
            "┌───────────────────────────────────────────────────────────┐",
            "│              Landing Zone (Raw Schema)                    │",
            "│  raw_orders │ raw_users │ raw_events │ raw_campaigns     │",
            "└──────────────────────┬────────────────────────────────────┘",
            "                       ▼",
            "┌───────────────────────────────────────────────────────────┐",
            "│                  dbt (Transform)                          │",
            "│  staging → intermediate → marts                          │",
            "│  (view)    (ephemeral)    (table)                        │",
            "│  ✅ dbt test → 质量门禁                                  │",
            "│  📖 dbt docs → 文档 & 血缘                              │",
            "└──────────────────────┬────────────────────────────────────┘",
            "                       ▼",
            "┌───────────────────────────────────────────────────────────┐",
            "│                  消费层 (Serve)                           │",
            "│  BI报表(Metabase) │ 数据产品 │ ML特征 │ 数据API          │",
            "└───────────────────────────────────────────────────────────┘",
            "",
            "调度: Airflow DAG (每日 02:00)",
            "监控: OpenLineage + Marquez (血缘追踪)"
        );
        System.out.println(pipeline);
    }

    /**
     * 3.3 数据质量门禁
     */
    static void demoDataQualityGate() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🛡️ 3.3 数据质量门禁");
        System.out.println("=".repeat(60));

        String qualityGate = String.join("\n",
            "质量规则矩阵:",
            "┌──────────────────┬──────────┬────────────────────────────────┐",
            "│ 检查类型         │ 级别     │ 规则                           │",
            "├──────────────────┼──────────┼────────────────────────────────┤",
            "│ 唯一性           │ error    │ 主键唯一                       │",
            "│ 非空             │ error    │ 关键字段非空                   │",
            "│ 引用完整性       │ error    │ 外键关系正确                   │",
            "│ 值域检查         │ error    │ 金额≥0, 状态在枚举内          │",
            "│ 行数比例         │ warn     │ 上下游行数比 0.95~1.05         │",
            "│ 数据新鲜度       │ warn     │ 源数据 < 24 小时               │",
            "│ 业务规则         │ error    │ 日收入>0, 无孤儿订单           │",
            "│ Schema 变更      │ error    │ 模型契约校验                   │",
            "└──────────────────┴──────────┴────────────────────────────────┘",
            "",
            "# 门禁决策逻辑 (Airflow BranchPythonOperator):",
            "def evaluate_test_results(**context):",
            "    results = json.load(open('/tmp/test_results.json'))",
            "    errors = [r for r in results['results']",
            "              if r['status']=='fail' and r.get('severity','error')=='error']",
            "    warnings = [r for r in results['results']",
            "                if r['status']=='fail' and r.get('severity')=='warn']",
            "    context['ti'].xcom_push('tests_failed', [e['unique_id'] for e in errors])",
            "    return 'notify_failure' if errors else 'notify_success'"
        );
        System.out.println(qualityGate);
    }

    /**
     * 3.4 环境管理
     */
    static void demoEnvironmentManagement() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🌍 3.4 环境管理 (dev/staging/prod)");
        System.out.println("=".repeat(60));

        String envMgmt = String.join("\n",
            "环境管理策略:",
            "┌─────────┬─────────────────┬──────────────┬──────────────────┐",
            "│ 环境    │ 数据库          │ dbt target   │ 用途             │",
            "├─────────┼─────────────────┼──────────────┼──────────────────┤",
            "│ dev     │ analytics_dev   │ dbt_dev      │ 开发调试         │",
            "│ staging │ analytics_stg   │ dbt_staging  │ 集成测试         │",
            "│ prod    │ analytics       │ dbt_prod     │ 生产环境         │",
            "└─────────┴─────────────────┴──────────────┴──────────────────┘",
            "",
            "# 运行命令:",
            "dbt run --target dev                    # 开发环境",
            "dbt run --target staging --full-refresh # 集成测试 (全量)",
            "dbt run --target prod --select tag:daily # 生产每日任务",
            "",
            "# CI/CD 中的 dbt 检查 (PR → merge 前):",
            "dbt compile --target staging         # 编译检查",
            "dbt run --target staging --defer --state ./prod_manifest  # 只跑变更模型",
            "dbt test --target staging --select state:modified+  # 只测变更模型",
            "",
            "# Slim CI (仅构建变更模型):",
            "dbt ls --target staging --select state:modified --state ./prod_manifest",
            "dbt build --target staging --select state:modified+ --defer --state ./prod_manifest"
        );
        System.out.println(envMgmt);
    }

    // ====================== 四、数据契约 & 治理 ======================

    /**
     * 4.1 Schema Contract (dbt v1.5+)
     */
    static void demoSchemaContract() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("📝 4.1 Schema Contract (dbt 模型契约)");
        System.out.println("=".repeat(60));

        String contract = String.join("\n",
            "# dbt v1.5+ 模型契约: 强制 Schema 一致性",
            "",
            "models:",
            "  - name: fct_orders",
            "    config:",
            "      contract:",
            "        enforced: true       # 启用契约",
            "    columns:",
            "      - name: order_id",
            "        data_type: bigint      # 类型强制",
            "        constraints:",
            "          - type: not_null",
            "          - type: primary_key",
            "      - name: order_amount",
            "        data_type: decimal(10,2)",
            "        constraints:",
            "          - type: not_null",
            "          - type: check",
            "            expression: \"order_amount >= 0\"",
            "      - name: ordered_date",
            "        data_type: date",
            "        constraints:",
            "          - type: not_null",
            "",
            "# 契约违规报错:",
            "# Compilation Error: contract violation for fct_orders",
            "#   - column 'new_column' not in contract",
            "#   - column 'order_amount' expected decimal(10,2) got varchar",
            "",
            "# 数据访问策略 (group + access):",
            "models:",
            "  - name: fct_orders",
            "    access: public            # public / protected / private",
            "    group: core_analytics      # 所属分组",
            "    # public: 任何 dbt 项目可 ref",
            "    # protected: 同 group 可 ref",
            "    # private: 同目录可 ref"
        );
        System.out.println(contract);
    }

    /**
     * 4.2 Data Freshness
     */
    static void demoDataFreshness() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("⏰ 4.2 Data Freshness (数据新鲜度)");
        System.out.println("=".repeat(60));

        String freshness = String.join("\n",
            "# dbt source freshness 配置:",
            "sources:",
            "  - name: ecommerce",
            "    freshness:",
            "      warn_after: {count: 12, period: hour}   # 12小时未更新 → warn",
            "      error_after: {count: 24, period: hour}  # 24小时未更新 → error",
            "    loaded_at_field: _airbyte_emitted_at       # 加载时间字段",
            "",
            "# 运行新鲜度检查:",
            "dbt source freshness --target prod",
            "",
            "# 输出示例:",
            "# source ecommerce.orders   | max_loaded_at: 2024-03-23 01:30:00",
            "#                           | snapshotted_at: 2024-03-23 02:00:00",
            "#                           | age: 0.5 hours   | status: pass ✅",
            "# source ecommerce.users    | max_loaded_at: 2024-03-22 08:00:00",
            "#                           | age: 18 hours    | status: warn ⚠️",
            "",
            "# Airflow 集成: 新鲜度失败 → 跳过 dbt run → 告警",
            "freshness_check >> branch_on_freshness >> [run_dbt, skip_and_alert]"
        );
        System.out.println(freshness);
    }

    /**
     * 4.3 CI/CD Pipeline
     */
    static void demoCiCdPipeline() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🔄 4.3 CI/CD Pipeline (GitHub Actions)");
        System.out.println("=".repeat(60));

        String cicd = String.join("\n",
            "# .github/workflows/dbt-ci.yml",
            "name: dbt CI",
            "on:",
            "  pull_request:",
            "    paths: ['dbt/**']",
            "",
            "jobs:",
            "  dbt-ci:",
            "    runs-on: ubuntu-latest",
            "    steps:",
            "      - uses: actions/checkout@v4",
            "      - name: Setup Python",
            "        uses: actions/setup-python@v4",
            "        with: { python-version: '3.10' }",
            "      - name: Install dbt",
            "        run: pip install dbt-core dbt-postgres dbt-doris",
            "      - name: dbt deps",
            "        run: cd dbt && dbt deps",
            "      - name: dbt compile",
            "        run: cd dbt && dbt compile --target staging",
            "      - name: Slim CI (只构建变更模型)",
            "        run: |",
            "          cd dbt",
            "          dbt build --target staging \\",
            "            --select state:modified+ \\",
            "            --defer --state ./prod_manifest \\",
            "            --fail-fast",
            "      - name: dbt test",
            "        run: cd dbt && dbt test --target staging --select state:modified+",
            "      - name: Upload artifacts",
            "        uses: actions/upload-artifact@v3",
            "        with:",
            "          name: dbt-artifacts",
            "          path: dbt/target/",
            "",
            "# PR 自动评论 dbt 变更摘要:",
            "# - 新增模型: 3 个",
            "# - 修改模型: 2 个",
            "# - 测试通过: 15/15 ✅",
            "# - Schema 变更: 无 ✅"
        );
        System.out.println(cicd);
    }
}
