package com.bigdata.contract;

/**
 * ============================================================
 * 数据契约 & Data Mesh & Data API 实战
 * ============================================================
 *
 * 核心理念：
 * 数据即产品 (Data as a Product)
 * - 数据不再是副产品，而是有明确所有者、契约、SLA 的产品
 * - Data Mesh: 去中心化数据架构，领域团队拥有自己的数据
 * - Data Contract: 生产者与消费者之间的正式协议
 *
 * 本模块涵盖：
 * 1. Data Contract 规范 (YAML/JSON Schema 定义)
 * 2. Data Mesh 四大原则实现
 * 3. Data API 服务化 (GraphQL/REST/gRPC)
 * 4. Schema Registry 集中管理
 * 5. 契约测试 & 合规验证
 * 6. Data Product Canvas (数据产品画布)
 *
 * @author bigdata-team
 */
public class DataContractApp {

    public static void main(String[] args) {
        System.out.println("╔═══════════════════════════════════════════════════════════╗");
        System.out.println("║   数据契约 & Data Mesh & Data API 深度实战                   ║");
        System.out.println("╚═══════════════════════════════════════════════════════════╝");

        // ============================================================
        // 第1部分: Data Contract 规范定义
        // ============================================================
        demonstrateDataContract();

        // ============================================================
        // 第2部分: Data Mesh 四大原则
        // ============================================================
        demonstrateDataMesh();

        // ============================================================
        // 第3部分: Schema Registry 集中管理
        // ============================================================
        demonstrateSchemaRegistry();

        // ============================================================
        // 第4部分: Data API 服务化
        // ============================================================
        demonstrateDataAPI();

        // ============================================================
        // 第5部分: 契约测试 & 合规验证
        // ============================================================
        demonstrateContractTesting();

        // ============================================================
        // 第6部分: Data Product Canvas
        // ============================================================
        demonstrateDataProductCanvas();

        System.out.println("\n✅ 数据契约 & Data Mesh & Data API 全部演示完成!");
    }

    // ============================================================
    //  第1部分: Data Contract 规范定义
    // ============================================================
    private static void demonstrateDataContract() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("📜 第1部分: Data Contract 规范定义");
        System.out.println("=".repeat(60));

        /*
         * ┌─────────────────────────────────────────────────────────────┐
         * │ Data Contract 是什么？                                       │
         * │                                                             │
         * │ 数据生产者和消费者之间的正式协议，定义：                          │
         * │ - 数据结构 (Schema)                                         │
         * │ - 数据质量 (Quality SLA)                                    │
         * │ - 数据语义 (Business Semantics)                             │
         * │ - 数据所有权 (Ownership)                                    │
         * │ - 变更策略 (Evolution Policy)                               │
         * │ - 访问权限 (Access Control)                                  │
         * └─────────────────────────────────────────────────────────────┘
         */

        // 1.1 Data Contract YAML 规范 (datacontract-specification v0.9.3)
        System.out.println("\n📋 1.1 Data Contract YAML 规范:");
        String dataContractYaml = """
            # ============================================================
            # Data Contract Specification v0.9.3
            # 文件: datacontracts/orders-contract.yaml
            # ============================================================
            dataContractSpecification: 0.9.3
            id: urn:datacontract:ecommerce:orders:v2
            
            info:
              title: "电商订单数据契约"
              version: "2.1.0"
              status: active                    # draft → active → deprecated → retired
              description: |
                电商平台订单域核心数据产品。
                提供标准化的订单事实数据，支撑下游分析、风控、营销等场景。
              owner: order-domain-team
              contact:
                name: "订单域数据团队"
                email: order-data@company.com
                slack: "#order-data-support"
              links:
                documentation: https://wiki.company.com/data-products/orders
                dashboard: https://grafana.company.com/d/orders-health
            
            # ============================================================
            # 服务器定义 - 数据在哪里
            # ============================================================
            servers:
              production:
                type: kafka
                host: kafka-cluster.prod:9092
                topic: ecommerce.orders.v2
                format: avro
                description: "实时订单流 (Kafka Avro)"
              
              warehouse:
                type: bigquery
                project: company-data-prod
                dataset: ecommerce_orders
                description: "离线数仓 (BigQuery / 可替换为 Doris)"
              
              api:
                type: rest
                url: https://data-api.company.com/v2/orders
                description: "数据 API 服务"
            
            # ============================================================
            # 数据模型定义 - Schema
            # ============================================================
            models:
              orders:
                description: "订单事实表"
                type: table
                fields:
                  order_id:
                    type: string
                    format: uuid
                    required: true
                    unique: true
                    primary: true
                    description: "订单唯一标识"
                    pii: false
                    classification: internal
                    
                  user_id:
                    type: string
                    required: true
                    description: "用户ID"
                    pii: true                   # 个人信息标记
                    classification: confidential
                    
                  order_amount:
                    type: decimal
                    precision: 10
                    scale: 2
                    required: true
                    description: "订单金额（元）"
                    minimum: 0.01
                    maximum: 999999.99
                    
                  order_status:
                    type: string
                    required: true
                    description: "订单状态"
                    enum:
                      - PENDING      # 待支付
                      - PAID         # 已支付
                      - SHIPPED      # 已发货
                      - DELIVERED    # 已送达
                      - CANCELLED    # 已取消
                      - REFUNDED     # 已退款
                    
                  payment_method:
                    type: string
                    description: "支付方式"
                    enum: [WECHAT, ALIPAY, CREDIT_CARD, DEBIT_CARD, COD]
                    
                  shipping_address:
                    type: object
                    description: "收货地址"
                    pii: true
                    classification: restricted
                    fields:
                      province:
                        type: string
                        required: true
                      city:
                        type: string
                        required: true
                      district:
                        type: string
                      detail:
                        type: string
                        pii: true
                    
                  items:
                    type: array
                    description: "订单商品列表"
                    items:
                      type: object
                      fields:
                        sku_id:
                          type: string
                          required: true
                        product_name:
                          type: string
                        quantity:
                          type: integer
                          minimum: 1
                        unit_price:
                          type: decimal
                          precision: 10
                          scale: 2
                    
                  created_at:
                    type: timestamp
                    required: true
                    description: "创建时间 (UTC)"
                    
                  updated_at:
                    type: timestamp
                    required: true
                    description: "最后更新时间"
                    
                  _metadata:
                    type: object
                    description: "元数据 (不在契约保证范围内)"
                    fields:
                      source_system:
                        type: string
                      ingestion_timestamp:
                        type: timestamp
                      contract_version:
                        type: string
            
            # ============================================================
            # 数据质量 SLA
            # ============================================================
            quality:
              type: SodaCL                      # 质量检查引擎
              specification:
                # ---- 完整性 ----
                checks for orders:
                  - row_count > 0
                  - missing_count(order_id) = 0
                  - missing_count(user_id) = 0
                  - missing_count(order_amount) = 0
                  - duplicate_count(order_id) = 0
                  
                # ---- 准确性 ----
                  - valid_min(order_amount) >= 0.01
                  - valid_max(order_amount) <= 999999.99
                  - invalid_count(order_status) = 0:
                      valid values: [PENDING, PAID, SHIPPED, DELIVERED, CANCELLED, REFUNDED]
                  
                # ---- 时效性 ----
                  - freshness(updated_at) < 1h    # 数据延迟 < 1小时
                  
                # ---- 一致性 ----
                  - avg(order_amount) between 50 and 5000
                  - schema:
                      fail:
                        when mismatching columns: any
                        
            # ============================================================
            # 服务水平协议 (SLA)
            # ============================================================
            servicelevels:
              availability:
                description: "数据可用性"
                percentage: 99.9%
                
              retention:
                description: "数据保留期"
                period: 3y
                unlimited: false
                
              latency:
                description: "数据延迟"
                threshold: 5min                   # 实时流延迟
                sourceTimestampField: created_at
                processedTimestampField: _metadata.ingestion_timestamp
                
              freshness:
                description: "数据新鲜度"
                threshold: 1h                     # 批处理最大延迟
                timestampField: updated_at
                
              frequency:
                description: "更新频率"
                type: streaming                   # streaming | hourly | daily
                cron: null
                
              support:
                description: "支持响应"
                time: 30min                       # P1 问题响应时间
                responseTime: 4h                  # 解决时间
                
            # ============================================================
            # Schema 演进策略
            # ============================================================
            terms:
              usage:
                - "仅限公司内部业务分析使用"
                - "禁止导出 PII 字段到外部系统"
                - "下游需注册为正式消费者"
              limitations:
                - "每秒最大查询 1000 QPS (API)"
                - "批量导出需提前申请"
              billing:
                model: free                       # free | cost-center | chargeback
                costCenter: "CC-DATA-001"
            """;
        System.out.println(dataContractYaml);

        // 1.2 Schema 演进策略
        System.out.println("\n📋 1.2 Schema 演进策略:");
        String schemaEvolutionPolicy = """
            ┌──────────────────────────────────────────────────────────────┐
            │                   Schema 演进策略                             │
            ├──────────────────┬───────────────────────────────────────────┤
            │ 兼容性级别         │ 规则                                     │
            ├──────────────────┼───────────────────────────────────────────┤
            │ BACKWARD (默认)   │ 新 Schema 可以读旧数据                     │
            │                  │ → 允许: 删除字段、添加可选字段               │
            │                  │ → 禁止: 添加必选字段、修改字段类型           │
            │                  │                                           │
            │ FORWARD          │ 旧 Schema 可以读新数据                     │
            │                  │ → 允许: 添加字段、删除可选字段               │
            │                  │ → 禁止: 删除必选字段                        │
            │                  │                                           │
            │ FULL             │ 双向兼容                                   │
            │                  │ → 最严格，只允许添加/删除可选字段            │
            │                  │                                           │
            │ NONE             │ 无兼容性保证                               │
            │                  │ → 需要消费者自行适配                        │
            ├──────────────────┼───────────────────────────────────────────┤
            │ 版本策略            │ 语义版本号 (SemVer)                      │
            │   major          │ 破坏性变更 → 新 Topic/新 API endpoint     │
            │   minor          │ 向后兼容新特性 (如添加可选字段)              │
            │   patch          │ 质量/文档修复                              │
            ├──────────────────┼───────────────────────────────────────────┤
            │ 变更流程            │ 1. 提交 PR 修改 contract YAML            │
            │                  │ 2. CI 自动兼容性检查 (schema-registry)     │
            │                  │ 3. 通知下游消费者 (Slack + Email)          │
            │                  │ 4. 消费者确认窗口期 (7天)                   │
            │                  │ 5. 合并部署 + 发布变更日志                  │
            └──────────────────┴───────────────────────────────────────────┘
            """;
        System.out.println(schemaEvolutionPolicy);

        // 1.3 Data Contract CLI 工具链
        System.out.println("\n📋 1.3 Data Contract CLI 工具链:");
        String contractCli = """
            # ============================================================
            # Data Contract CLI (datacontract-cli) - 契约管理工具
            # ============================================================
            
            # 安装
            pip install datacontract-cli
            
            # 1. 初始化契约
            datacontract init --template orders
            
            # 2. 验证契约语法
            datacontract lint datacontracts/orders-contract.yaml
            
            # 3. 测试数据质量 (连接实际数据源)
            datacontract test datacontracts/orders-contract.yaml \\
              --server warehouse \\
              --publish-to-marquez http://localhost:5001
            
            # 4. 对比两个版本的兼容性
            datacontract diff \\
              --old datacontracts/orders-contract-v1.yaml \\
              --new datacontracts/orders-contract-v2.yaml
            
            # 输出示例:
            # BREAKING: Field 'shipping_address.detail' type changed: string → object
            # COMPATIBLE: New optional field 'coupon_code' added
            # COMPATIBLE: New enum value 'PARTIAL_REFUND' added to 'order_status'
            
            # 5. 生成文档
            datacontract catalog \\
              --input datacontracts/ \\
              --output docs/data-catalog/ \\
              --format html
            
            # 6. 从已有数据源逆向生成契约
            datacontract import \\
              --source bigquery \\
              --project company-data-prod \\
              --dataset ecommerce_orders \\
              --output datacontracts/orders-contract.yaml
            
            # 7. 导出为其他格式
            datacontract export \\
              --input datacontracts/orders-contract.yaml \\
              --format avro > schemas/orders.avsc
            datacontract export --format dbt > models/schema.yml
            datacontract export --format jsonschema > schemas/orders.json
            datacontract export --format openapi > api/orders-openapi.yaml
            
            # 8. CI/CD 集成 (GitHub Actions)
            # .github/workflows/contract-ci.yaml
            # name: Data Contract CI
            # on:
            #   pull_request:
            #     paths: ['datacontracts/**']
            # jobs:
            #   validate:
            #     steps:
            #       - uses: datacontract/datacontract-cli-action@v1
            #         with:
            #           command: test
            #           contract: datacontracts/orders-contract.yaml
            #       - uses: datacontract/datacontract-cli-action@v1
            #         with:
            #           command: diff
            #           breaking-changes-fail: true
            """;
        System.out.println(contractCli);
    }

    // ============================================================
    //  第2部分: Data Mesh 四大原则
    // ============================================================
    private static void demonstrateDataMesh() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🌐 第2部分: Data Mesh 四大原则");
        System.out.println("=".repeat(60));

        /*
         * ┌──────────────────────────────────────────────────────────────┐
         * │                Data Mesh 四大原则                              │
         * ├──────────────────────────────────────────────────────────────┤
         * │                                                              │
         * │  ① 领域所有权              ② 数据即产品                        │
         * │  (Domain Ownership)       (Data as a Product)               │
         * │  ┌─────────┐              ┌─────────┐                       │
         * │  │ 订单域    │              │ SLA     │                       │
         * │  │ 用户域    │              │ Schema  │                       │
         * │  │ 商品域    │              │ Quality │                       │
         * │  │ 支付域    │              │ Docs    │                       │
         * │  └─────────┘              └─────────┘                       │
         * │                                                              │
         * │  ③ 自服务平台              ④ 联邦治理                          │
         * │  (Self-serve Platform)    (Federated Governance)            │
         * │  ┌─────────┐              ┌─────────┐                       │
         * │  │ 一键建表  │              │ 全局标准 │                       │
         * │  │ 自动血缘  │              │ 本地自治 │                       │
         * │  │ 质量门禁  │              │ 互操作  │                       │
         * │  │ API 暴露  │              │ 合规    │                       │
         * │  └─────────┘              └─────────┘                       │
         * └──────────────────────────────────────────────────────────────┘
         */

        // 2.1 领域数据产品架构
        System.out.println("\n🏢 2.1 领域数据产品架构:");
        String domainArchitecture = """
            # ============================================================
            # Data Mesh 领域数据产品定义
            # 文件: data-products/order-domain/product.yaml
            # ============================================================
            apiVersion: datamesh.io/v1alpha1
            kind: DataProduct
            metadata:
              name: order-analytics
              namespace: order-domain
              labels:
                domain: order
                team: order-analytics-team
                tier: gold                        # bronze → silver → gold
                
            spec:
              # ---- 数据产品基本信息 ----
              description: |
                订单分析数据产品，提供标准化的订单宽表和聚合指标。
                面向分析师、数据科学家、BI 看板。
              owner:
                team: order-analytics-team
                contact: order-data@company.com
                oncall: "#order-oncall"
                
              # ---- 输入端口 (数据来源) ----
              inputPorts:
                - name: raw-orders
                  type: kafka
                  server: kafka-cluster.prod:9092
                  topic: mysql.ecommerce.orders    # Debezium CDC
                  format: avro
                  
                - name: raw-payments
                  type: kafka
                  topic: mysql.ecommerce.payments
                  format: avro
                  
                - name: user-profiles
                  type: rest-api
                  url: https://data-api.company.com/v2/users
                  owner: user-domain
                  
              # ---- 输出端口 (数据产品暴露) ----
              outputPorts:
                - name: order-wide-table
                  type: doris-table
                  server: doris-fe.prod:9030
                  database: dw_order
                  table: dwd_order_wide
                  format: columnar
                  contract: datacontracts/orders-wide-contract.yaml
                  
                - name: order-metrics-api
                  type: rest-api
                  url: https://data-api.company.com/v2/order-metrics
                  format: json
                  rateLimit: 1000/s
                  
                - name: order-events-stream
                  type: kafka
                  topic: dp.order-domain.order-analytics.v2
                  format: avro
                  
                - name: order-feature-store
                  type: feast
                  featureView: order_features
                  description: "订单特征 (给 ML 模型使用)"
                  
              # ---- 转换逻辑 ----
              transformations:
                engine: dbt
                project: dbt-order-domain
                models:
                  - staging/stg_orders.sql
                  - intermediate/int_orders_enriched.sql
                  - marts/fct_orders_wide.sql
                  - marts/dim_order_metrics_daily.sql
                schedule:
                  type: streaming + batch
                  streaming: "Flink CDC → Doris 实时宽表"
                  batch: "每天 06:00 dbt 全量刷新聚合指标"
                  
              # ---- 数据质量 ----
              quality:
                sla:
                  availability: 99.9%
                  freshness: 5min (streaming) / 1h (batch)
                  completeness: ">99.5%"
                checks:
                  - type: great_expectations
                    suite: order_quality_suite
                  - type: soda
                    config: soda/orders-checks.yaml
                    
              # ---- 可观测性 ----
              observability:
                lineage:
                  provider: openlineage
                  marquezNamespace: order-domain
                metrics:
                  provider: prometheus
                  dashboard: grafana.company.com/d/order-product
                alerts:
                  - type: freshness
                    threshold: 30min
                    severity: P2
                    channel: "#order-oncall"
            """;
        System.out.println(domainArchitecture);

        // 2.2 Data Mesh 自服务数据平台
        System.out.println("\n🛠️ 2.2 自服务数据平台架构:");
        String selfServePlatform = """
            ┌─────────────────────────────────────────────────────────────────┐
            │                    自服务数据平台 (Self-Serve Platform)           │
            ├─────────────────────────────────────────────────────────────────┤
            │                                                                 │
            │  ┌─────────────┐  ┌──────────────┐  ┌─────────────────┐       │
            │  │ 数据产品门户  │  │ API 网关      │  │ 数据目录搜索     │       │
            │  │ (Portal)    │  │ (Kong/Envoy) │  │ (DataHub/Amundsen)│     │
            │  └──────┬──────┘  └──────┬───────┘  └───────┬─────────┘       │
            │         │                │                   │                  │
            │  ┌──────┴──────────────────┴───────────────────┴──────────┐     │
            │  │              平台抽象层 (Platform Mesh Layer)            │     │
            │  ├──────────────────────────────────────────────────────────┤     │
            │  │                                                        │     │
            │  │  ┌──────────┐  ┌──────────┐  ┌──────────┐            │     │
            │  │  │ 模板工厂  │  │ 契约注册  │  │ 质量引擎  │            │     │
            │  │  │          │  │          │  │          │            │     │
            │  │  │ dbt模板   │  │ Schema   │  │ SodaCL   │            │     │
            │  │  │ Flink模板 │  │ Registry │  │ GE       │            │     │
            │  │  │ Airflow  │  │ Contract │  │ dbt test │            │     │
            │  │  │ 模板     │  │  Store   │  │          │            │     │
            │  │  └──────────┘  └──────────┘  └──────────┘            │     │
            │  │                                                        │     │
            │  │  ┌──────────┐  ┌──────────┐  ┌──────────┐            │     │
            │  │  │ 权限中心  │  │ 计算引擎  │  │ 存储抽象  │            │     │
            │  │  │          │  │          │  │          │            │     │
            │  │  │ Ranger   │  │ Spark    │  │ Doris    │            │     │
            │  │  │ OPA      │  │ Flink    │  │ Hive     │            │     │
            │  │  │ RBAC/ABAC│  │ dbt      │  │ Kafka    │            │     │
            │  │  └──────────┘  └──────────┘  └──────────┘            │     │
            │  │                                                        │     │
            │  └────────────────────────────────────────────────────────┘     │
            │                                                                 │
            │  ┌─────────────────────────────────────────────────────────┐    │
            │  │                 基础设施层 (Infra)                       │    │
            │  │  K8s │ Terraform │ ArgoCD │ Prometheus │ OpenLineage   │    │
            │  └─────────────────────────────────────────────────────────┘    │
            └─────────────────────────────────────────────────────────────────┘
            """;
        System.out.println(selfServePlatform);

        // 2.3 联邦治理 - 全局标准 + 本地自治
        System.out.println("\n⚖️ 2.3 联邦治理模型:");
        String federatedGovernance = """
            # ============================================================
            # 联邦治理策略定义
            # 文件: governance/federated-policies.yaml
            # ============================================================
            
            # ---- 全局策略 (中央数据治理委员会制定) ----
            globalPolicies:
              naming:
                database: "dw_{domain}_{layer}"   # dw_order_ods, dw_user_dwd
                table: "{layer}_{domain}_{entity}" # ods_order_raw, dwd_user_profile
                column: snake_case                  # user_id, order_amount
                topic: "{source}.{domain}.{entity}" # mysql.order.orders
                
              classification:
                levels:
                  - public                          # 公开数据
                  - internal                        # 内部数据
                  - confidential                    # 机密数据
                  - restricted                      # 受限数据 (PII)
                piiFields:
                  autoDetect: true
                  patterns:
                    - "phone|mobile|手机"
                    - "email|邮箱"
                    - "idcard|身份证"
                    - "address|地址"
                    - "name|姓名"
                    
              quality:
                minimumChecks:
                  - row_count_gt_zero              # 非空
                  - primary_key_unique             # 主键唯一
                  - freshness_within_sla           # 时效性
                failurePolicy: block               # block | warn | log
                
              retention:
                default: 3y
                pii: 1y
                logs: 90d
                
              lineage:
                required: true                     # 必须有血缘
                provider: openlineage
                
            # ---- 领域策略 (各领域团队自定义) ----
            domainPolicies:
              order-domain:
                additionalChecks:
                  - "order_amount > 0"
                  - "user_id IS NOT NULL"
                customClassification:
                  - name: "transaction_data"
                    retention: 5y
                accessControl:
                  - role: order-analyst
                    tables: ["dwd_*", "dws_*"]
                    columns: ["*", "-phone", "-address"]  # 排除 PII
                  - role: order-admin
                    tables: ["*"]
                    columns: ["*"]
                    
              user-domain:
                additionalChecks:
                  - "age BETWEEN 0 AND 150"
                  - "register_date <= CURRENT_DATE"
                piiHandling:
                  encryption: AES-256
                  masking:
                    phone: "138****1234"
                    email: "u***@company.com"
                    
            # ---- 互操作性标准 ----
            interoperability:
              formats:
                preferred: avro                    # 统一序列化格式
                alternatives: [parquet, json]
              encoding: UTF-8
              timezone: UTC
              nullHandling: explicit               # 必须显式 null，禁止空字符串
              dateFormat: "yyyy-MM-dd"
              timestampFormat: "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
            """;
        System.out.println(federatedGovernance);
    }

    // ============================================================
    //  第3部分: Schema Registry 集中管理
    // ============================================================
    private static void demonstrateSchemaRegistry() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("📚 第3部分: Schema Registry 集中管理");
        System.out.println("=".repeat(60));

        // 3.1 Confluent Schema Registry 架构
        System.out.println("\n🏗️ 3.1 Schema Registry 架构:");
        String schemaRegistryArch = """
            ┌─────────────────────────────────────────────────────────────┐
            │                Confluent Schema Registry                     │
            ├─────────────────────────────────────────────────────────────┤
            │                                                             │
            │  Producer                                                   │
            │  ┌──────────┐                                               │
            │  │ Avro/    │ ──① Register Schema──→ ┌───────────────┐    │
            │  │ Protobuf │                        │ Schema        │    │
            │  │ JSON     │ ←─② Schema ID────────── │ Registry      │    │
            │  └────┬─────┘                        │               │    │
            │       │                               │ ┌───────────┐│    │
            │       │ ③ [SchemaID + Data]           │ │ Avro      ││    │
            │       ▼                               │ │ Protobuf  ││    │
            │  ┌──────────┐                        │ │ JSON Schema││    │
            │  │  Kafka   │                        │ └───────────┘│    │
            │  │  Broker  │                        │               │    │
            │  └────┬─────┘                        │ _schemas     │    │
            │       │                               │ (内部Topic)  │    │
            │       ▼                               └──────┬───────┘    │
            │  Consumer                                    │            │
            │  ┌──────────┐                                │            │
            │  │ Deser-   │ ──④ Get Schema by ID──────────→│            │
            │  │ ializer  │ ←─⑤ Schema Definition─────────│            │
            │  └──────────┘                                             │
            └─────────────────────────────────────────────────────────────┘
            
            # ============================================================
            # Schema Registry Docker 部署
            # ============================================================
            # docker-compose 配置:
            schema-registry:
              image: confluentinc/cp-schema-registry:7.5.0
              ports:
                - "8081:8081"
              environment:
                SCHEMA_REGISTRY_HOST_NAME: schema-registry
                SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka:29092
                SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081
                # 全局兼容性级别
                SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL: BACKWARD
              depends_on:
                - kafka
            """;
        System.out.println(schemaRegistryArch);

        // 3.2 Avro Schema 定义 & 演进
        System.out.println("\n📋 3.2 Avro Schema 定义 & 演进:");
        String avroSchema = """
            // ============================================================
            // Order Avro Schema v2 (向后兼容 v1)
            // 文件: schemas/avro/order-value.avsc
            // ============================================================
            {
              "type": "record",
              "name": "OrderValue",
              "namespace": "com.company.ecommerce.orders",
              "doc": "电商订单值对象 v2",
              "fields": [
                {
                  "name": "order_id",
                  "type": "string",
                  "doc": "订单唯一ID"
                },
                {
                  "name": "user_id",
                  "type": "string",
                  "doc": "用户ID"
                },
                {
                  "name": "order_amount",
                  "type": {
                    "type": "bytes",
                    "logicalType": "decimal",
                    "precision": 10,
                    "scale": 2
                  },
                  "doc": "订单金额"
                },
                {
                  "name": "order_status",
                  "type": {
                    "type": "enum",
                    "name": "OrderStatus",
                    "symbols": ["PENDING", "PAID", "SHIPPED", "DELIVERED",
                                "CANCELLED", "REFUNDED", "PARTIAL_REFUND"],
                    "default": "PENDING"
                  }
                },
                {
                  "name": "payment_method",
                  "type": ["null", "string"],
                  "default": null,
                  "doc": "支付方式 (v1 新增可选字段)"
                },
                {
                  "name": "coupon_code",
                  "type": ["null", "string"],
                  "default": null,
                  "doc": "优惠券码 (v2 新增可选字段，向后兼容)"
                },
                {
                  "name": "created_at",
                  "type": {
                    "type": "long",
                    "logicalType": "timestamp-millis"
                  }
                },
                {
                  "name": "updated_at",
                  "type": {
                    "type": "long",
                    "logicalType": "timestamp-millis"
                  }
                }
              ]
            }
            
            # Schema Registry REST API 操作:
            
            # 注册 Schema
            curl -X POST http://localhost:8081/subjects/orders-value/versions \\
              -H "Content-Type: application/vnd.schemaregistry.v1+json" \\
              -d '{"schema": "<avsc-json-escaped>"}'
            
            # 查看所有版本
            curl http://localhost:8081/subjects/orders-value/versions
            
            # 检查兼容性
            curl -X POST http://localhost:8081/compatibility/subjects/orders-value/versions/latest \\
              -H "Content-Type: application/vnd.schemaregistry.v1+json" \\
              -d '{"schema": "<new-schema>"}'
            # 返回: {"is_compatible": true}
            
            # 设置兼容性级别
            curl -X PUT http://localhost:8081/config/orders-value \\
              -H "Content-Type: application/vnd.schemaregistry.v1+json" \\
              -d '{"compatibility": "FULL"}'
            """;
        System.out.println(avroSchema);
    }

    // ============================================================
    //  第4部分: Data API 服务化
    // ============================================================
    private static void demonstrateDataAPI() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🌍 第4部分: Data API 服务化");
        System.out.println("=".repeat(60));

        // 4.1 GraphQL Data API
        System.out.println("\n📡 4.1 GraphQL Data API:");
        String graphqlApi = """
            # ============================================================
            # GraphQL Schema - 数据产品 API
            # 文件: api/schema.graphql
            # ============================================================
            
            type Query {
              # ---- 订单查询 ----
              order(id: ID!): Order
              orders(
                filter: OrderFilter
                sort: OrderSort
                pagination: PaginationInput
              ): OrderConnection!
              
              # ---- 订单指标 ----
              orderMetrics(
                dateRange: DateRange!
                granularity: Granularity!
                dimensions: [MetricDimension!]
              ): [OrderMetric!]!
              
              # ---- 数据质量 ----
              dataQuality(dataset: String!): DataQualityReport
              
              # ---- 血缘查询 ----
              lineage(
                dataset: String!
                direction: LineageDirection!
                depth: Int = 3
              ): LineageGraph
            }
            
            type Order {
              orderId: ID!
              userId: String!
              orderAmount: Decimal!
              orderStatus: OrderStatus!
              paymentMethod: PaymentMethod
              items: [OrderItem!]!
              createdAt: DateTime!
              updatedAt: DateTime!
              
              # 关联查询 (按需加载)
              user: User @resolve(from: "user-domain")
              payments: [Payment!] @resolve(from: "payment-domain")
              logistics: Logistics @resolve(from: "logistics-domain")
            }
            
            type OrderMetric {
              date: Date!
              totalOrders: Int!
              totalAmount: Decimal!
              avgOrderAmount: Decimal!
              paymentDistribution: [PaymentStat!]!
              statusDistribution: [StatusStat!]!
              topCategories: [CategoryStat!]!
            }
            
            type OrderConnection {
              edges: [OrderEdge!]!
              pageInfo: PageInfo!
              totalCount: Int!
            }
            
            type LineageGraph {
              nodes: [LineageNode!]!
              edges: [LineageEdge!]!
            }
            
            type LineageNode {
              id: ID!
              name: String!
              type: NodeType!          # DATASET | JOB
              namespace: String!
              domain: String
              owner: String
              quality: DataQualityScore
            }
            
            # ---- 查询示例 ----
            #
            # query OrderDashboard {
            #   orderMetrics(
            #     dateRange: { start: "2024-01-01", end: "2024-01-31" }
            #     granularity: DAILY
            #     dimensions: [PAYMENT_METHOD, CATEGORY]
            #   ) {
            #     date
            #     totalOrders
            #     totalAmount
            #     avgOrderAmount
            #     paymentDistribution { method count amount }
            #   }
            #   
            #   lineage(
            #     dataset: "dwd_order_wide"
            #     direction: UPSTREAM
            #     depth: 5
            #   ) {
            #     nodes { id name type domain }
            #     edges { source target }
            #   }
            # }
            """;
        System.out.println(graphqlApi);

        // 4.2 REST Data API (OpenAPI 3.0)
        System.out.println("\n📡 4.2 REST Data API (OpenAPI 3.0):");
        String restApi = """
            # ============================================================
            # OpenAPI 3.0 - 数据产品 REST API
            # 文件: api/openapi.yaml
            # ============================================================
            openapi: 3.0.3
            info:
              title: Order Data Product API
              version: 2.1.0
              description: "订单域数据产品 RESTful API"
              contact:
                email: order-data@company.com
            
            servers:
              - url: https://data-api.company.com/v2
                description: Production
              - url: https://data-api-staging.company.com/v2
                description: Staging
            
            paths:
              /orders:
                get:
                  summary: "查询订单列表"
                  tags: [Orders]
                  parameters:
                    - name: user_id
                      in: query
                      schema: { type: string }
                    - name: status
                      in: query
                      schema:
                        type: string
                        enum: [PENDING, PAID, SHIPPED, DELIVERED]
                    - name: date_from
                      in: query
                      schema: { type: string, format: date }
                    - name: page
                      in: query
                      schema: { type: integer, default: 1 }
                    - name: page_size
                      in: query
                      schema: { type: integer, default: 20, maximum: 100 }
                  responses:
                    '200':
                      description: 成功
                      headers:
                        X-Total-Count: { schema: { type: integer } }
                        X-Data-Contract-Version: { schema: { type: string } }
                        X-Data-Freshness: { schema: { type: string, format: duration } }
                      content:
                        application/json:
                          schema:
                            type: object
                            properties:
                              data: { type: array, items: { $ref: '#/components/schemas/Order' } }
                              meta:
                                type: object
                                properties:
                                  contractVersion: { type: string }
                                  freshness: { type: string }
                                  quality:
                                    type: object
                                    properties:
                                      score: { type: number }
                                      lastChecked: { type: string, format: date-time }
            
              /orders/metrics:
                get:
                  summary: "查询订单聚合指标"
                  parameters:
                    - name: date_from
                      in: query
                      required: true
                      schema: { type: string, format: date }
                    - name: date_to
                      in: query
                      required: true
                      schema: { type: string, format: date }
                    - name: granularity
                      in: query
                      schema: { type: string, enum: [hourly, daily, weekly, monthly] }
                  responses:
                    '200':
                      description: 指标数据
            
              /health:
                get:
                  summary: "数据产品健康检查"
                  responses:
                    '200':
                      content:
                        application/json:
                          schema:
                            type: object
                            properties:
                              status: { type: string, enum: [healthy, degraded, unhealthy] }
                              freshness: { type: string }
                              quality: { type: number }
                              lastUpdate: { type: string, format: date-time }
                              slaCompliance: { type: number }
                              
            # 响应示例:
            # GET /health
            # {
            #   "status": "healthy",
            #   "freshness": "PT2M30S",     (2分30秒)
            #   "quality": 0.997,
            #   "lastUpdate": "2024-01-15T10:30:00Z",
            #   "slaCompliance": 0.999
            # }
            """;
        System.out.println(restApi);

        // 4.3 gRPC Data API
        System.out.println("\n📡 4.3 gRPC Data API (高性能):");
        String grpcApi = """
            // ============================================================
            // gRPC Proto - 高性能数据查询
            // 文件: api/proto/order_data.proto
            // ============================================================
            syntax = "proto3";
            package com.company.data.order;
            
            service OrderDataService {
              // 单条查询
              rpc GetOrder(GetOrderRequest) returns (Order);
              
              // 流式查询 (大数据量)
              rpc StreamOrders(StreamOrdersRequest) returns (stream Order);
              
              // 实时订阅 (CDC 推送)
              rpc SubscribeOrderChanges(SubscribeRequest)
                  returns (stream OrderChangeEvent);
              
              // 指标查询
              rpc GetOrderMetrics(MetricsRequest) returns (OrderMetrics);
            }
            
            message Order {
              string order_id = 1;
              string user_id = 2;
              int64 order_amount_cents = 3;    // 分为单位，避免浮点精度
              OrderStatus status = 4;
              repeated OrderItem items = 5;
              google.protobuf.Timestamp created_at = 6;
              
              // 数据产品元数据
              DataProductMeta meta = 100;
            }
            
            message DataProductMeta {
              string contract_version = 1;
              google.protobuf.Duration freshness = 2;
              double quality_score = 3;
            }
            
            message OrderChangeEvent {
              string order_id = 1;
              ChangeType change_type = 2;       // INSERT | UPDATE | DELETE
              Order before = 3;                 // 变更前
              Order after = 4;                  // 变更后
              google.protobuf.Timestamp event_time = 5;
            }
            
            enum ChangeType {
              INSERT = 0;
              UPDATE = 1;
              DELETE = 2;
            }
            
            // 使用场景:
            // 1. StreamOrders: 大量导出 → 比 REST 快 10x (二进制+流式)
            // 2. SubscribeOrderChanges: 实时推送 → 替代轮询
            // 3. GetOrderMetrics: 低延迟指标 → P99 < 10ms
            """;
        System.out.println(grpcApi);
    }

    // ============================================================
    //  第5部分: 契约测试 & 合规验证
    // ============================================================
    private static void demonstrateContractTesting() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🧪 第5部分: 契约测试 & 合规验证");
        System.out.println("=".repeat(60));

        // 5.1 生产者驱动的契约测试
        System.out.println("\n✅ 5.1 契约测试矩阵:");
        String contractTesting = """
            ┌──────────────────────────────────────────────────────────────┐
            │                    契约测试矩阵                               │
            ├──────────────┬───────────────────────────────────────────────┤
            │ 测试类型       │ 说明                                         │
            ├──────────────┼───────────────────────────────────────────────┤
            │ Schema 测试   │ 字段类型、必填项、枚举值是否符合契约             │
            │ Quality 测试  │ SodaCL / Great Expectations 检查是否通过       │
            │ SLA 测试      │ 延迟、新鲜度、可用性是否达标                    │
            │ 兼容性测试     │ 新 Schema 是否向后/向前兼容                    │
            │ 语义测试       │ 业务规则是否正确 (如 amount > 0)              │
            │ 安全测试       │ PII 字段是否正确标记、加密、脱敏               │
            │ API 测试       │ 端点响应是否符合 OpenAPI/GraphQL Schema       │
            │ 性能测试       │ API QPS、延迟是否符合 SLA                     │
            └──────────────┴───────────────────────────────────────────────┘
            
            # ============================================================
            # CI/CD 契约测试 Pipeline
            # .github/workflows/contract-test.yaml
            # ============================================================
            name: Data Contract CI/CD
            on:
              pull_request:
                paths:
                  - 'datacontracts/**'
                  - 'schemas/**'
                  - 'dbt/**'
              schedule:
                - cron: '0 */6 * * *'            # 每 6 小时定期检查
            
            jobs:
              lint:
                name: "契约语法检查"
                runs-on: ubuntu-latest
                steps:
                  - uses: actions/checkout@v4
                  - run: pip install datacontract-cli
                  - run: datacontract lint datacontracts/*.yaml
            
              compatibility:
                name: "Schema 兼容性检查"
                runs-on: ubuntu-latest
                steps:
                  - uses: actions/checkout@v4
                    with: { fetch-depth: 0 }
                  - run: |
                      # 对比 main 分支和当前分支
                      git show origin/main:datacontracts/orders-contract.yaml > /tmp/old.yaml
                      datacontract diff --old /tmp/old.yaml --new datacontracts/orders-contract.yaml
                      # 如果有 BREAKING 变更，CI 失败
            
              quality:
                name: "数据质量检查"
                runs-on: ubuntu-latest
                steps:
                  - run: |
                      datacontract test datacontracts/orders-contract.yaml \\
                        --server warehouse \\
                        --publish-to-marquez http://marquez:5001
            
              notify:
                name: "通知下游消费者"
                needs: [compatibility]
                if: contains(github.event.pull_request.labels.*.name, 'breaking-change')
                steps:
                  - run: |
                      # 查询 Marquez 获取下游消费者列表
                      CONSUMERS=$(curl -s http://marquez:5001/api/v1/datasets/orders/consumers)
                      # 发送 Slack 通知
                      curl -X POST $SLACK_WEBHOOK -d "{
                        \\"text\\": \\"⚠️ 订单数据契约有破坏性变更，请相关消费者确认:\n$CONSUMERS\\"
                      }"
            """;
        System.out.println(contractTesting);

        // 5.2 Open Policy Agent (OPA) 合规
        System.out.println("\n🔒 5.2 OPA 策略引擎 (合规自动化):");
        String opaPolicy = """
            # ============================================================
            # OPA Rego 策略 - 数据契约合规
            # 文件: policies/data_contract.rego
            # ============================================================
            package data.contract.compliance
            
            # 规则1: PII 字段必须有加密标记
            deny[msg] {
                field := input.models[_].fields[name]
                field.pii == true
                not field.encryption
                msg := sprintf(
                    "PII 字段 '%s' 缺少 encryption 配置", [name]
                )
            }
            
            # 规则2: 必须定义数据保留期
            deny[msg] {
                not input.servicelevels.retention
                msg := "缺少数据保留期 (retention) 定义"
            }
            
            # 规则3: Gold 级别产品必须有 >= 99.9% 可用性
            deny[msg] {
                input.metadata.labels.tier == "gold"
                availability := to_number(trim_suffix(
                    input.servicelevels.availability.percentage, "%"))
                availability < 99.9
                msg := sprintf(
                    "Gold 级别产品可用性必须 >= 99.9%%, 当前: %v%%",
                    [availability]
                )
            }
            
            # 规则4: 必须有至少一个数据质量检查
            deny[msg] {
                not input.quality
                msg := "缺少数据质量 (quality) 定义"
            }
            
            # 规则5: Schema 变更必须保持向后兼容
            deny[msg] {
                old_field := data.previous_contract.models[model].fields[name]
                not input.models[model].fields[name]
                old_field.required == true
                msg := sprintf(
                    "破坏性变更: 必选字段 '%s.%s' 被删除", [model, name]
                )
            }
            
            # 使用方式:
            # opa eval -i contract.json -d policies/ "data.contract.compliance.deny"
            """;
        System.out.println(opaPolicy);
    }

    // ============================================================
    //  第6部分: Data Product Canvas
    // ============================================================
    private static void demonstrateDataProductCanvas() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🎨 第6部分: Data Product Canvas (数据产品画布)");
        System.out.println("=".repeat(60));

        String canvas = """
            ╔══════════════════════════════════════════════════════════════════════╗
            ║                     Data Product Canvas                              ║
            ║                     订单分析数据产品                                    ║
            ╠═══════════════════╦═══════════════════╦══════════════════════════════╣
            ║                   ║                   ║                              ║
            ║  📊 Domain        ║  👤 Consumers     ║  🎯 Use Cases               ║
            ║  电商·订单域       ║                   ║                              ║
            ║                   ║  • BI 分析师       ║  • 每日GMV看板               ║
            ║  👨‍💻 Owner         ║  • 数据科学家     ║  • 用户购买行为分析            ║
            ║  order-analytics  ║  • 风控团队        ║  • 异常订单检测               ║
            ║  -team            ║  • 营销团队        ║  • 营销 ROI 计算             ║
            ║                   ║  • 推荐算法组      ║  • 推荐模型特征               ║
            ║                   ║  • 财务团队        ║  • 月度营收对账               ║
            ╠═══════════════════╬═══════════════════╬══════════════════════════════╣
            ║                   ║                   ║                              ║
            ║  📥 Input Ports   ║  📤 Output Ports  ║  📜 Contract                ║
            ║                   ║                   ║                              ║
            ║  • MySQL CDC      ║  • Doris 宽表     ║  • Schema: Avro v2.1       ║
            ║    (orders)       ║  • REST API       ║  • SLA: 99.9% 可用         ║
            ║  • MySQL CDC      ║  • GraphQL        ║  • Freshness: 5min         ║
            ║    (payments)     ║  • Kafka Topic    ║  • Quality: >99.5%         ║
            ║  • User API       ║  • Feature Store  ║  • Retention: 3年          ║
            ║    (profiles)     ║  • Grafana 看板   ║  • PII: encrypted          ║
            ║                   ║                   ║                              ║
            ╠═══════════════════╬═══════════════════╬══════════════════════════════╣
            ║                   ║                   ║                              ║
            ║  ⚙️ Transform     ║  📈 Observability ║  💰 Cost & Value            ║
            ║                   ║                   ║                              ║
            ║  • Flink CDC      ║  • OpenLineage    ║  Cost:                      ║
            ║    (实时流)       ║    (血缘追踪)     ║  • 计算: $2,400/月          ║
            ║  • dbt            ║  • Marquez        ║  • 存储: $800/月            ║
            ║    (批处理)       ║    (可视化)       ║  • 运维: 1.5 人力           ║
            ║  • Spark          ║  • Prometheus     ║                              ║
            ║    (ML特征)       ║    (监控告警)     ║  Value:                     ║
            ║                   ║  • SodaCL         ║  • 节省分析师 40% 取数时间   ║
            ║                   ║    (质量检测)     ║  • 风控准确率提升 25%        ║
            ║                   ║                   ║  • 营销 ROI 可量化          ║
            ╚═══════════════════╩═══════════════════╩══════════════════════════════╝
            
            数据产品成熟度评估:
            ┌─────────────────┬─────┬─────────────────────────────────────┐
            │ 维度             │ 评分 │ 说明                               │
            ├─────────────────┼─────┼─────────────────────────────────────┤
            │ 可发现性         │ ⭐⭐⭐⭐ │ DataHub 注册 + API 文档完善       │
            │ 可寻址性         │ ⭐⭐⭐⭐⭐│ REST/GraphQL/gRPC 多协议暴露     │
            │ 可信赖性         │ ⭐⭐⭐⭐ │ SLA 99.9% + 质量检查 + 血缘      │
            │ 自描述性         │ ⭐⭐⭐⭐ │ Data Contract + 完整文档          │
            │ 互操作性         │ ⭐⭐⭐  │ Avro 标准格式，待补充 Protobuf    │
            │ 安全性           │ ⭐⭐⭐⭐⭐│ PII 加密 + RBAC + 脱敏           │
            └─────────────────┴─────┴─────────────────────────────────────┘
            """;
        System.out.println(canvas);
    }
}
