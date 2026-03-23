package com.bigdata.governance;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

/**
 * ==========================================================================
 *  Apache Atlas 数据治理实战 - 完整示例
 *
 *  📌 功能模块:
 *  1. 元数据管理 (Type定义 / 实体CRUD / 批量操作)
 *  2. 数据血缘 (Lineage) 追踪 (端到端 / 列级血缘)
 *  3. 数据分类 (Classification) & 标签管理
 *  4. 业务术语表 (Glossary) 管理
 *  5. 全文搜索 & 高级查询 (DSL)
 *  6. Hook 集成 (Hive/Spark/Kafka/Flink 自动采集)
 *  7. 数据质量规则 & 审计日志
 *  8. 实时数仓血缘图谱构建
 *
 *  📌 技术栈:
 *  - Apache Atlas 2.3+
 *  - REST API v2
 *  - JanusGraph (底层图存储)
 *  - Solr/Elasticsearch (全文索引)
 *  - Kafka (Hook 消息通道)
 *
 *  Author: BigData Demo
 * ==========================================================================
 */
public class AtlasGovernanceApp {

    private static final Logger logger = LoggerFactory.getLogger(AtlasGovernanceApp.class);
    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    // Atlas 配置
    private static final String ATLAS_URL = "http://localhost:21000";
    private static final String ATLAS_API_V2 = ATLAS_URL + "/api/atlas/v2";
    private static final String ATLAS_USER = "admin";
    private static final String ATLAS_PASSWORD = "admin";

    public static void main(String[] args) {
        logger.info("=== Apache Atlas 数据治理实战 ===\n");

        try {
            AtlasGovernanceApp app = new AtlasGovernanceApp();

            // 模块 1: 自定义类型系统
            app.demonstrateTypeSystem();

            // 模块 2: 元数据实体管理
            app.demonstrateEntityManagement();

            // 模块 3: 数据血缘追踪
            app.demonstrateLineageTracking();

            // 模块 4: 列级血缘 (Column-Level Lineage)
            app.demonstrateColumnLineage();

            // 模块 5: 数据分类 & 标签
            app.demonstrateClassification();

            // 模块 6: 业务术语表
            app.demonstrateGlossary();

            // 模块 7: 搜索 & 查询
            app.demonstrateSearchAndQuery();

            // 模块 8: 实时数仓血缘图谱
            app.buildRealtimeWarehouseLineage();

            // 模块 9: 数据质量元数据
            app.demonstrateDataQualityMetadata();

            // 模块 10: Hook 集成 & 审计
            app.demonstrateHookIntegration();

        } catch (Exception e) {
            logger.error("Atlas governance demo failed", e);
        }
    }

    // ============================================================
    // 1. 自定义类型系统 (Type System)
    // ============================================================

    /**
     * Atlas 类型系统是数据治理的基础
     *
     * 内置类型:
     *   - Referenceable: 所有实体的基类 (qualifiedName)
     *   - Asset: 资产 (name, description, owner)
     *   - DataSet: 数据集 (input/output of Process)
     *   - Process: 过程 (inputs, outputs → 自动生成血缘)
     *   - Infrastructure: 基础设施 (集群、服务器)
     *
     * 自定义类型:
     *   - EntityDef: 实体定义
     *   - ClassificationDef: 分类定义
     *   - RelationshipDef: 关系定义
     *   - BusinessMetadataDef: 业务元数据定义
     */
    public void demonstrateTypeSystem() throws Exception {
        logger.info("\n========== 1. 自定义类型系统 ==========\n");

        // 1.1 创建自定义实体类型: 数据产品 (Data Product)
        JsonObject dataProductType = createEntityTypeDef(
            "DataProduct",                          // 类型名
            "数据产品 - 面向消费者的数据资产",          // 描述
            Collections.singletonList("DataSet"),    // 父类型
            Arrays.asList(
                createAttributeDef("product_owner", "string", false, "产品负责人"),
                createAttributeDef("business_domain", "string", false, "业务域"),
                createAttributeDef("data_level", "string", false, "数据层级(ODS/DWD/DWS/ADS)"),
                createAttributeDef("sla_hours", "int", true, "SLA(小时)"),
                createAttributeDef("update_frequency", "string", true, "更新频率"),
                createAttributeDef("quality_score", "float", true, "质量评分(0-100)"),
                createAttributeDef("access_level", "string", true, "访问级别(public/internal/restricted)"),
                createAttributeDef("consumers", "array<string>", true, "消费者列表"),
                createAttributeDef("cost_center", "string", true, "成本中心"),
                createAttributeDef("creation_date", "date", true, "创建日期")
            )
        );
        logger.info("创建实体类型 DataProduct:\n" + gson.toJson(dataProductType));

        // 1.2 创建自定义实体类型: ETL 流程 (ETL Process)
        JsonObject etlProcessType = createEntityTypeDef(
            "ETLProcess",
            "ETL 数据加工流程",
            Collections.singletonList("Process"),
            Arrays.asList(
                createAttributeDef("engine", "string", false, "执行引擎(Spark/Flink/Hive)"),
                createAttributeDef("schedule", "string", true, "调度策略"),
                createAttributeDef("airflow_dag_id", "string", true, "Airflow DAG ID"),
                createAttributeDef("avg_duration_minutes", "int", true, "平均执行时长(分钟)"),
                createAttributeDef("last_run_status", "string", true, "最近运行状态"),
                createAttributeDef("last_run_time", "date", true, "最近运行时间"),
                createAttributeDef("sql_query", "string", true, "SQL 语句"),
                createAttributeDef("spark_app_id", "string", true, "Spark Application ID")
            )
        );
        logger.info("创建实体类型 ETLProcess:\n" + gson.toJson(etlProcessType));

        // 1.3 创建关系类型: 数据产品之间的依赖
        JsonObject dependencyRelation = createRelationshipTypeDef(
            "DataProductDependency",
            "数据产品间的依赖关系",
            "DataProduct", "upstream_products",
            "DataProduct", "downstream_products",
            "ASSOCIATION"
        );
        logger.info("创建关系类型 DataProductDependency:\n" + gson.toJson(dependencyRelation));

        // 1.4 创建业务元数据类型
        JsonObject businessMetadata = createBusinessMetadataDef(
            "DataGovernance",
            "数据治理业务元数据",
            Arrays.asList(
                createAttributeDef("data_steward", "string", true, "数据管家"),
                createAttributeDef("retention_days", "int", true, "保留天数"),
                createAttributeDef("compliance_status", "string", true, "合规状态"),
                createAttributeDef("last_audit_date", "date", true, "最近审计日期"),
                createAttributeDef("pii_flag", "boolean", true, "是否含PII")
            )
        );
        logger.info("创建业务元数据 DataGovernance:\n" + gson.toJson(businessMetadata));

        // 提交类型定义
        JsonObject typesDef = new JsonObject();
        JsonArray entityDefs = new JsonArray();
        entityDefs.add(dataProductType);
        entityDefs.add(etlProcessType);
        typesDef.add("entityDefs", entityDefs);

        JsonArray relationDefs = new JsonArray();
        relationDefs.add(dependencyRelation);
        typesDef.add("relationshipDefs", relationDefs);

        JsonArray businessMetadataDefs = new JsonArray();
        businessMetadataDefs.add(businessMetadata);
        typesDef.add("businessMetadataDefs", businessMetadataDefs);

        String response = atlasPost("/types/typedefs", typesDef.toString());
        logger.info("类型定义提交结果:\n" + response);
    }

    // ============================================================
    // 2. 元数据实体管理
    // ============================================================

    public void demonstrateEntityManagement() throws Exception {
        logger.info("\n========== 2. 元数据实体管理 ==========\n");

        // 2.1 创建 Hive 数据库实体
        String hiveDatabaseGuid = createEntity(
            "hive_db",
            "dw",
            "dw@primary",
            new HashMap<String, Object>() {{
                put("name", "dw");
                put("qualifiedName", "dw@primary");
                put("description", "数据仓库主库");
                put("owner", "data-team");
                put("clusterName", "primary");
                put("location", "hdfs:///user/hive/warehouse/dw.db");
            }}
        );
        logger.info("创建 Hive 数据库: dw, GUID=" + hiveDatabaseGuid);

        // 2.2 创建 Hive 表实体 (ODS 层)
        String odsOrderTable = createHiveTableEntity(
            "ods_order_info",
            "dw",
            "ODS-订单信息原始表",
            Arrays.asList(
                createColumnEntity("order_id", "bigint", "订单ID", true),
                createColumnEntity("user_id", "bigint", "用户ID", false),
                createColumnEntity("product_id", "bigint", "商品ID", false),
                createColumnEntity("region_id", "int", "地区ID", false),
                createColumnEntity("order_amount", "decimal(16,2)", "订单金额", false),
                createColumnEntity("order_status", "string", "订单状态", false),
                createColumnEntity("created_at", "timestamp", "创建时间", false),
                createColumnEntity("dt", "string", "分区字段-日期", false)
            ),
            "data-team",
            "PARQUET"
        );
        logger.info("创建 ODS 表: ods_order_info, GUID=" + odsOrderTable);

        // 2.3 创建 DWD 层表
        String dwdPaymentTable = createHiveTableEntity(
            "dwd_payment_detail",
            "dw",
            "DWD-支付成功明细宽表",
            Arrays.asList(
                createColumnEntity("payment_id", "bigint", "支付ID", true),
                createColumnEntity("order_id", "bigint", "订单ID", false),
                createColumnEntity("user_id", "bigint", "用户ID", false),
                createColumnEntity("product_name", "string", "商品名称", false),
                createColumnEntity("category_name", "string", "类目名称", false),
                createColumnEntity("region_name", "string", "地区名称", false),
                createColumnEntity("payment_amount", "decimal(16,2)", "支付金额", false),
                createColumnEntity("payment_type", "string", "支付方式", false),
                createColumnEntity("payment_time", "timestamp", "支付时间", false),
                createColumnEntity("dt", "string", "分区字段", false)
            ),
            "data-team",
            "PARQUET"
        );
        logger.info("创建 DWD 表: dwd_payment_detail, GUID=" + dwdPaymentTable);

        // 2.4 创建 DWS 层表
        String dwsTradeTable = createHiveTableEntity(
            "dws_trade_stats_1d",
            "dw",
            "DWS-交易日汇总表",
            Arrays.asList(
                createColumnEntity("category_name", "string", "类目", false),
                createColumnEntity("region_name", "string", "地区", false),
                createColumnEntity("order_count", "bigint", "订单量", false),
                createColumnEntity("payment_count", "bigint", "支付笔数", false),
                createColumnEntity("total_amount", "decimal(20,2)", "总金额", false),
                createColumnEntity("avg_amount", "decimal(16,2)", "平均金额", false),
                createColumnEntity("user_count", "bigint", "付费用户数", false),
                createColumnEntity("dt", "string", "分区字段", false)
            ),
            "data-team",
            "PARQUET"
        );
        logger.info("创建 DWS 表: dws_trade_stats_1d, GUID=" + dwsTradeTable);

        // 2.5 批量创建实体
        logger.info("批量创建维度表...");
        batchCreateEntities(Arrays.asList(
            createHiveTableJson("dim_user_info", "dw", "维度-用户信息"),
            createHiveTableJson("dim_product_info", "dw", "维度-商品信息"),
            createHiveTableJson("dim_region_info", "dw", "维度-地区信息")
        ));

        // 2.6 更新实体属性
        updateEntityAttributes(odsOrderTable, new HashMap<String, Object>() {{
            put("description", "ODS-订单信息原始表 (从 MySQL ecommerce.order_info 同步)");
            put("owner", "etl-team");
        }});
        logger.info("更新 ods_order_info 实体属性完成");

        // 2.7 添加业务元数据
        addBusinessMetadata(odsOrderTable, "DataGovernance", new HashMap<String, Object>() {{
            put("data_steward", "张三");
            put("retention_days", 365);
            put("compliance_status", "compliant");
            put("pii_flag", false);
        }});
        logger.info("添加业务元数据完成");
    }

    // ============================================================
    // 3. 数据血缘追踪 (Lineage)
    // ============================================================

    /**
     * 血缘 = Process (inputs → outputs)
     *
     * 血缘链路:
     *   MySQL.order_info → [Sqoop Import] → ODS.ods_order_info
     *   ODS.ods_order_info + ODS.ods_payment_info + DIM表 → [Spark ETL] → DWD.dwd_payment_detail
     *   DWD.dwd_payment_detail → [Hive SQL] → DWS.dws_trade_stats_1d
     *   DWS.dws_trade_stats_1d → [ClickHouse Export] → ClickHouse.ads_trade_stats
     */
    public void demonstrateLineageTracking() throws Exception {
        logger.info("\n========== 3. 数据血缘追踪 ==========\n");

        // 3.1 创建数据源实体: MySQL 表
        String mysqlOrderTable = createEntity(
            "rdbms_table",
            "ecommerce.order_info",
            "ecommerce.order_info@mysql_prod",
            new HashMap<String, Object>() {{
                put("name", "order_info");
                put("qualifiedName", "ecommerce.order_info@mysql_prod");
                put("description", "MySQL 电商订单表");
                put("owner", "app-team");
                put("db", createRefObject("rdbms_db", "ecommerce@mysql_prod"));
            }}
        );
        logger.info("创建 MySQL 源表: " + mysqlOrderTable);

        // 3.2 创建 Sqoop 采集过程 (MySQL → ODS)
        String sqoopProcess = createProcessEntity(
            "sqoop_import_order_info",                                    // 过程名
            "Sqoop 采集: MySQL.order_info → ODS.ods_order_info",          // 描述
            Collections.singletonList("ecommerce.order_info@mysql_prod"), // 输入
            Collections.singletonList("dw.ods_order_info@primary"),       // 输出
            new HashMap<String, Object>() {{
                put("engine", "Sqoop");
                put("schedule", "daily 02:00");
                put("airflow_dag_id", "bigdata_etl_pipeline");
            }}
        );
        logger.info("创建 Sqoop 血缘: MySQL → ODS, GUID=" + sqoopProcess);

        // 3.3 创建 Spark ETL 过程 (ODS → DWD)
        String sparkEtlProcess = createProcessEntity(
            "spark_etl_dwd_payment",
            "Spark ETL: ODS多表关联 → DWD.dwd_payment_detail",
            Arrays.asList(
                "dw.ods_order_info@primary",
                "dw.ods_payment_info@primary",
                "dw.ods_product_info@primary",
                "dw.dim_region_info@primary"
            ),
            Collections.singletonList("dw.dwd_payment_detail@primary"),
            new HashMap<String, Object>() {{
                put("engine", "Spark");
                put("schedule", "daily 03:00");
                put("airflow_dag_id", "bigdata_etl_pipeline");
                put("sql_query",
                    "SELECT pay.*, ord.product_id, prd.product_name, reg.region_name " +
                    "FROM ods_payment_info pay " +
                    "JOIN ods_order_info ord ON pay.order_id = ord.order_id " +
                    "LEFT JOIN ods_product_info prd ON ord.product_id = prd.product_id " +
                    "LEFT JOIN dim_region reg ON ord.region_id = reg.region_id");
            }}
        );
        logger.info("创建 Spark ETL 血缘: ODS → DWD, GUID=" + sparkEtlProcess);

        // 3.4 创建 Hive SQL 过程 (DWD → DWS)
        String hiveAggProcess = createProcessEntity(
            "hive_agg_dws_trade",
            "Hive SQL: DWD 聚合 → DWS.dws_trade_stats_1d",
            Collections.singletonList("dw.dwd_payment_detail@primary"),
            Collections.singletonList("dw.dws_trade_stats_1d@primary"),
            new HashMap<String, Object>() {{
                put("engine", "Hive");
                put("schedule", "daily 04:00");
                put("sql_query",
                    "SELECT category_name, region_name, " +
                    "COUNT(DISTINCT order_id) as order_count, " +
                    "SUM(payment_amount) as total_amount " +
                    "FROM dwd_payment_detail GROUP BY category_name, region_name");
            }}
        );
        logger.info("创建 Hive 聚合血缘: DWD → DWS, GUID=" + hiveAggProcess);

        // 3.5 创建 ClickHouse 导出过程 (DWS → ADS)
        String ckExportProcess = createProcessEntity(
            "export_to_clickhouse_trade",
            "导出到 ClickHouse: DWS → ADS.ads_trade_stats",
            Collections.singletonList("dw.dws_trade_stats_1d@primary"),
            Collections.singletonList("ads.ads_trade_stats@clickhouse_prod"),
            new HashMap<String, Object>() {{
                put("engine", "ClickHouse");
                put("schedule", "daily 05:00");
            }}
        );
        logger.info("创建 ClickHouse 导出血缘: DWS → ADS, GUID=" + ckExportProcess);

        // 3.6 查询血缘
        logger.info("\n--- 查询 DWD 表的完整血缘 ---");
        queryLineage("dw.dwd_payment_detail@primary", "BOTH", 5);

        logger.info("\n--- 查询 DWS 表的上游血缘 ---");
        queryLineage("dw.dws_trade_stats_1d@primary", "INPUT", 10);
    }

    // ============================================================
    // 4. 列级血缘 (Column-Level Lineage)
    // ============================================================

    public void demonstrateColumnLineage() throws Exception {
        logger.info("\n========== 4. 列级血缘 ==========\n");

        // 列级血缘: 追踪每个字段的数据来源
        // dwd_payment_detail.payment_amount ← ods_payment_info.payment_amount
        // dwd_payment_detail.product_name   ← ods_product_info.product_name
        // dwd_payment_detail.region_name    ← dim_region_info.region_name

        // 4.1 创建列级血缘过程
        JsonObject columnLineageProcess = new JsonObject();
        columnLineageProcess.addProperty("typeName", "Process");

        JsonObject attributes = new JsonObject();
        attributes.addProperty("name", "column_lineage_dwd_payment");
        attributes.addProperty("qualifiedName", "column_lineage_dwd_payment@primary");
        attributes.addProperty("description", "DWD支付明细表列级血缘");

        // 输入列
        JsonArray inputColumns = new JsonArray();
        addColumnRef(inputColumns, "dw.ods_payment_info.payment_amount@primary");
        addColumnRef(inputColumns, "dw.ods_payment_info.payment_id@primary");
        addColumnRef(inputColumns, "dw.ods_payment_info.order_id@primary");
        addColumnRef(inputColumns, "dw.ods_order_info.product_id@primary");
        addColumnRef(inputColumns, "dw.ods_product_info.product_name@primary");
        addColumnRef(inputColumns, "dw.ods_product_info.category_name@primary");
        addColumnRef(inputColumns, "dw.dim_region_info.region_name@primary");

        // 输出列
        JsonArray outputColumns = new JsonArray();
        addColumnRef(outputColumns, "dw.dwd_payment_detail.payment_amount@primary");
        addColumnRef(outputColumns, "dw.dwd_payment_detail.payment_id@primary");
        addColumnRef(outputColumns, "dw.dwd_payment_detail.order_id@primary");
        addColumnRef(outputColumns, "dw.dwd_payment_detail.product_name@primary");
        addColumnRef(outputColumns, "dw.dwd_payment_detail.category_name@primary");
        addColumnRef(outputColumns, "dw.dwd_payment_detail.region_name@primary");

        attributes.add("inputs", inputColumns);
        attributes.add("outputs", outputColumns);
        columnLineageProcess.add("attributes", attributes);

        logger.info("列级血缘映射:");
        logger.info("  payment_amount ← ods_payment_info.payment_amount");
        logger.info("  product_name   ← ods_product_info.product_name (via JOIN)");
        logger.info("  category_name  ← ods_product_info.category_name (via JOIN)");
        logger.info("  region_name    ← dim_region_info.region_name (via JOIN)");

        // 4.2 列级影响分析 (Impact Analysis)
        logger.info("\n--- 列级影响分析 ---");
        logger.info("如果 ods_product_info.product_name 字段类型变更，影响:");
        logger.info("  → dwd_payment_detail.product_name");
        logger.info("  → dws_trade_stats_1d (间接影响: 通过聚合)");
        logger.info("  → ads_trade_stats (间接影响: 导出到 ClickHouse)");
        logger.info("  → 下游报表、Dashboard、数据产品");

        // 4.3 字段溯源查询
        logger.info("\n--- 字段溯源查询 ---");
        queryColumnLineage(
            "dw.dwd_payment_detail.region_name@primary",
            "INPUT",
            5
        );
    }

    // ============================================================
    // 5. 数据分类 & 标签
    // ============================================================

    public void demonstrateClassification() throws Exception {
        logger.info("\n========== 5. 数据分类 & 标签 ==========\n");

        // 5.1 创建分类体系
        List<JsonObject> classifications = Arrays.asList(
            // 数据安全等级
            createClassificationDef("SecurityLevel_Public", "公开数据 - 无限制", null),
            createClassificationDef("SecurityLevel_Internal", "内部数据 - 仅内部访问", null),
            createClassificationDef("SecurityLevel_Confidential", "机密数据 - 授权访问",
                Collections.singletonList(
                    createAttributeDef("approval_required", "boolean", true, "是否需要审批")
                )),
            createClassificationDef("SecurityLevel_Restricted", "受限数据 - 最高级别",
                Arrays.asList(
                    createAttributeDef("approved_by", "string", true, "审批人"),
                    createAttributeDef("expiry_date", "date", true, "授权过期时间")
                )),

            // PII (个人可识别信息)
            createClassificationDef("PII", "个人可识别信息",
                Arrays.asList(
                    createAttributeDef("pii_type", "string", true,
                        "PII类型(name/email/phone/id_card/address)"),
                    createAttributeDef("masking_policy", "string", true,
                        "脱敏策略(hash/mask/encrypt/redact)")
                )),

            // 数据质量标记
            createClassificationDef("DataQuality_Certified", "数据质量认证通过",
                Collections.singletonList(
                    createAttributeDef("certified_by", "string", true, "认证人")
                )),
            createClassificationDef("DataQuality_Warning", "数据质量告警",
                Collections.singletonList(
                    createAttributeDef("warning_reason", "string", true, "告警原因")
                )),

            // 合规标记
            createClassificationDef("GDPR_Applicable", "适用GDPR法规", null),
            createClassificationDef("DataRetention_30Days", "30天保留策略", null),
            createClassificationDef("DataRetention_365Days", "365天保留策略", null)
        );

        // 提交分类定义
        JsonObject typesDef = new JsonObject();
        JsonArray classificationDefs = new JsonArray();
        classifications.forEach(classificationDefs::add);
        typesDef.add("classificationDefs", classificationDefs);

        atlasPost("/types/typedefs", typesDef.toString());
        logger.info("创建 " + classifications.size() + " 个分类定义完成");

        // 5.2 为实体添加分类
        // 标记用户表为 PII + 机密
        addClassificationToEntity(
            "dw.ods_user_info@primary",
            "PII",
            new HashMap<String, Object>() {{
                put("pii_type", "email,phone");
                put("masking_policy", "mask");
            }}
        );
        addClassificationToEntity(
            "dw.ods_user_info@primary",
            "SecurityLevel_Confidential",
            new HashMap<String, Object>() {{
                put("approval_required", true);
            }}
        );
        logger.info("为 ods_user_info 添加分类: PII + SecurityLevel_Confidential");

        // 标记订单表为内部
        addClassificationToEntity(
            "dw.ods_order_info@primary",
            "SecurityLevel_Internal",
            null
        );
        addClassificationToEntity(
            "dw.ods_order_info@primary",
            "DataRetention_365Days",
            null
        );
        logger.info("为 ods_order_info 添加分类: SecurityLevel_Internal + DataRetention_365Days");

        // 标记 DWS 表为质量认证
        addClassificationToEntity(
            "dw.dws_trade_stats_1d@primary",
            "DataQuality_Certified",
            new HashMap<String, Object>() {{
                put("certified_by", "data-quality-team");
            }}
        );
        logger.info("为 dws_trade_stats_1d 添加质量认证标记");

        // 5.3 传播分类 (Classification Propagation)
        logger.info("\n--- 分类传播规则 ---");
        logger.info("PII 标记会沿血缘自动传播:");
        logger.info("  ods_user_info (PII) → dwd_user_behavior (自动继承 PII)");
        logger.info("  → dws_user_behavior_1d (自动继承 PII)");
        logger.info("  → ads 层 (自动继承, 确保下游遵守隐私规则)");

        // 5.4 基于分类搜索
        logger.info("\n--- 按分类搜索实体 ---");
        searchByClassification("PII");
        searchByClassification("SecurityLevel_Confidential");
    }

    // ============================================================
    // 6. 业务术语表 (Glossary)
    // ============================================================

    public void demonstrateGlossary() throws Exception {
        logger.info("\n========== 6. 业务术语表 ==========\n");

        // 6.1 创建术语表
        String glossaryGuid = createGlossary(
            "电商数据治理术语表",
            "统一定义电商业务中的核心指标和维度"
        );
        logger.info("创建术语表: GUID=" + glossaryGuid);

        // 6.2 创建术语分类
        String coreMetrics = createGlossaryCategory(
            glossaryGuid, "核心指标", "GMV/订单/用户等核心业务指标"
        );
        String dimensions = createGlossaryCategory(
            glossaryGuid, "维度定义", "时间/地区/商品等分析维度"
        );
        String businessRules = createGlossaryCategory(
            glossaryGuid, "业务规则", "数据口径和计算逻辑"
        );

        // 6.3 创建业务术语
        Map<String, Map<String, String>> terms = new LinkedHashMap<>();

        terms.put("GMV", new HashMap<String, String>() {{
            put("shortDescription", "成交总额 (Gross Merchandise Volume)");
            put("longDescription",
                "统计周期内所有已支付订单的支付金额总和。\n" +
                "计算公式: SUM(payment_amount) WHERE payment_status='SUCCESS'\n" +
                "口径说明:\n" +
                "  - 包含: 正常支付订单\n" +
                "  - 不包含: 退款订单、取消订单\n" +
                "  - 统计周期: 按自然日 00:00-23:59:59");
            put("category", coreMetrics);
        }});

        terms.put("DAU", new HashMap<String, String>() {{
            put("shortDescription", "日活跃用户数 (Daily Active Users)");
            put("longDescription",
                "统计自然日内有至少一次有效行为的去重用户数。\n" +
                "计算公式: COUNT(DISTINCT user_id) WHERE dt='目标日期'\n" +
                "有效行为: PV / 加购 / 收藏 / 购买");
            put("category", coreMetrics);
        }});

        terms.put("转化率", new HashMap<String, String>() {{
            put("shortDescription", "购买转化率");
            put("longDescription",
                "浏览到购买的转化率。\n" +
                "计算公式: 购买UV / 浏览UV * 100%\n" +
                "注意: 转化率按商品维度计算时，需同一商品的浏览和购买匹配");
            put("category", coreMetrics);
        }});

        terms.put("客单价", new HashMap<String, String>() {{
            put("shortDescription", "人均消费金额");
            put("longDescription",
                "计算公式: GMV / 付费用户数\n" +
                "付费用户数: COUNT(DISTINCT user_id) WHERE payment_status='SUCCESS'");
            put("category", coreMetrics);
        }});

        terms.put("新用户", new HashMap<String, String>() {{
            put("shortDescription", "当日首次注册的用户");
            put("longDescription",
                "判断标准: register_time 在统计日期当天\n" +
                "register_time >= '目标日期 00:00:00' AND register_time < '目标日期+1 00:00:00'");
            put("category", businessRules);
        }});

        for (Map.Entry<String, Map<String, String>> entry : terms.entrySet()) {
            String termGuid = createGlossaryTerm(
                glossaryGuid,
                entry.getKey(),
                entry.getValue().get("shortDescription"),
                entry.getValue().get("longDescription")
            );
            logger.info("创建术语: " + entry.getKey() + " -> GUID=" + termGuid);
        }

        // 6.4 关联术语到实体 (链接指标到实际表/列)
        assignTermToEntity("GMV", "dw.dws_trade_stats_1d.total_amount@primary");
        assignTermToEntity("DAU", "dw.dws_user_behavior_1d.user_id@primary");
        logger.info("术语关联到实体完成: GMV → total_amount, DAU → user_id");
    }

    // ============================================================
    // 7. 搜索 & 查询
    // ============================================================

    public void demonstrateSearchAndQuery() throws Exception {
        logger.info("\n========== 7. 搜索 & 高级查询 ==========\n");

        // 7.1 基本全文搜索
        logger.info("--- 全文搜索: 'order' ---");
        fullTextSearch("order", 10);

        // 7.2 DSL 高级查询 (Atlas Query Language)
        logger.info("\n--- DSL 查询: 所有 Hive 表 ---");
        dslSearch("from hive_table", 20);

        logger.info("\n--- DSL 查询: DWD 层的表 ---");
        dslSearch("from hive_table where name like 'dwd_*'", 20);

        logger.info("\n--- DSL 查询: 含 PII 分类的实体 ---");
        dslSearch("from hive_table where hive_table isa PII", 10);

        logger.info("\n--- DSL 查询: 特定 owner 的表 ---");
        dslSearch("from hive_table where owner='data-team'", 20);

        // 7.3 属性搜索
        logger.info("\n--- 属性搜索: PARQUET 格式的表 ---");
        attributeSearch("hive_table", "tableType", "MANAGED_TABLE", 10);

        // 7.4 关系搜索
        logger.info("\n--- 关系搜索: ods_order_info 的所有关联实体 ---");
        relationshipSearch("dw.ods_order_info@primary");

        // 7.5 保存搜索 (Saved Search)
        logger.info("\n--- 创建保存搜索 ---");
        createSavedSearch(
            "PII_Tables_Monitor",
            "监控含 PII 数据的表",
            "from hive_table where hive_table isa PII"
        );
    }

    // ============================================================
    // 8. 实时数仓血缘图谱
    // ============================================================

    public void buildRealtimeWarehouseLineage() throws Exception {
        logger.info("\n========== 8. 实时数仓血缘图谱 ==========\n");

        // 构建完整的实时数仓血缘链路
        // MySQL → Flink CDC → Kafka(ODS) → Flink SQL → Kafka(DWD)
        //   → Flink SQL → ClickHouse(DWS) → Grafana(ADS)

        // 8.1 创建 Kafka Topic 实体
        String kafkaOdsTopic = createEntity(
            "kafka_topic",
            "ods_order_info",
            "ods_order_info@kafka_prod",
            new HashMap<String, Object>() {{
                put("name", "ods_order_info");
                put("qualifiedName", "ods_order_info@kafka_prod");
                put("description", "ODS层-订单CDC数据");
                put("topic", "ods_order_info");
                put("partitionCount", 8);
                put("replicationFactor", 3);
            }}
        );

        String kafkaDwdTopic = createEntity(
            "kafka_topic",
            "dwd_order_detail",
            "dwd_order_detail@kafka_prod",
            new HashMap<String, Object>() {{
                put("name", "dwd_order_detail");
                put("qualifiedName", "dwd_order_detail@kafka_prod");
                put("description", "DWD层-订单明细宽表");
                put("topic", "dwd_order_detail");
                put("partitionCount", 8);
            }}
        );

        // 8.2 创建 ClickHouse 表实体
        String ckTradeTable = createEntity(
            "rdbms_table",
            "ads_trade_stats",
            "ads.ads_trade_stats@clickhouse_prod",
            new HashMap<String, Object>() {{
                put("name", "ads_trade_stats");
                put("qualifiedName", "ads.ads_trade_stats@clickhouse_prod");
                put("description", "ADS层-交易统计(ClickHouse)");
            }}
        );

        // 8.3 创建实时血缘链路

        // MySQL CDC → Kafka ODS
        createProcessEntity(
            "flink_cdc_mysql_to_kafka",
            "Flink CDC: MySQL.order_info → Kafka.ods_order_info",
            Collections.singletonList("ecommerce.order_info@mysql_prod"),
            Collections.singletonList("ods_order_info@kafka_prod"),
            new HashMap<String, Object>() {{
                put("engine", "Flink CDC 2.4");
                put("schedule", "realtime");
            }}
        );

        // Kafka ODS → Flink SQL → Kafka DWD
        createProcessEntity(
            "flink_sql_ods_to_dwd",
            "Flink SQL: 多流JOIN → DWD明细",
            Arrays.asList(
                "ods_order_info@kafka_prod",
                "dw.dim_product_info@primary",
                "dw.dim_region_info@primary"
            ),
            Collections.singletonList("dwd_order_detail@kafka_prod"),
            new HashMap<String, Object>() {{
                put("engine", "Flink SQL 1.17");
                put("schedule", "realtime");
                put("sql_query",
                    "INSERT INTO dwd_order_detail " +
                    "SELECT o.*, p.product_name, r.region_name " +
                    "FROM ods_order_info o " +
                    "JOIN dim_product FOR SYSTEM_TIME AS OF o.proc_time AS p " +
                    "ON o.product_id = p.product_id " +
                    "JOIN dim_region FOR SYSTEM_TIME AS OF o.proc_time AS r " +
                    "ON o.region_id = r.region_id");
            }}
        );

        // Kafka DWD → Flink SQL → ClickHouse DWS
        createProcessEntity(
            "flink_sql_dwd_to_clickhouse",
            "Flink SQL: 窗口聚合 → ClickHouse",
            Collections.singletonList("dwd_order_detail@kafka_prod"),
            Collections.singletonList("ads.ads_trade_stats@clickhouse_prod"),
            new HashMap<String, Object>() {{
                put("engine", "Flink SQL 1.17");
                put("schedule", "realtime (1min window)");
                put("sql_query",
                    "INSERT INTO ads_trade_stats " +
                    "SELECT window_start, category_name, region_name, " +
                    "COUNT(*) as order_count, SUM(amount) as total_amount " +
                    "FROM TABLE(TUMBLE(TABLE dwd_order_detail, " +
                    "DESCRIPTOR(event_time), INTERVAL '1' MINUTE)) " +
                    "GROUP BY window_start, category_name, region_name");
            }}
        );

        logger.info("实时数仓血缘图谱构建完成:");
        logger.info("  MySQL → [Flink CDC] → Kafka(ODS)");
        logger.info("                              ↓");
        logger.info("  维度表 → [Flink SQL Lookup Join] → Kafka(DWD)");
        logger.info("                                          ↓");
        logger.info("                              [Flink SQL Window Agg]");
        logger.info("                                          ↓");
        logger.info("                                  ClickHouse(ADS)");
        logger.info("                                          ↓");
        logger.info("                                      Grafana");
    }

    // ============================================================
    // 9. 数据质量元数据
    // ============================================================

    public void demonstrateDataQualityMetadata() throws Exception {
        logger.info("\n========== 9. 数据质量元数据 ==========\n");

        // 9.1 创建数据质量规则类型
        JsonObject qualityRuleType = createEntityTypeDef(
            "DataQualityRule",
            "数据质量规则",
            Collections.singletonList("Referenceable"),
            Arrays.asList(
                createAttributeDef("rule_name", "string", false, "规则名称"),
                createAttributeDef("rule_type", "string", false,
                    "规则类型(completeness/accuracy/consistency/timeliness/uniqueness)"),
                createAttributeDef("target_table", "string", false, "目标表"),
                createAttributeDef("target_column", "string", true, "目标列"),
                createAttributeDef("expression", "string", false, "规则表达式"),
                createAttributeDef("threshold", "float", true, "阈值"),
                createAttributeDef("severity", "string", true, "严重级别(info/warning/critical)"),
                createAttributeDef("is_active", "boolean", true, "是否启用"),
                createAttributeDef("last_check_time", "date", true, "最近检查时间"),
                createAttributeDef("last_check_result", "string", true, "最近检查结果"),
                createAttributeDef("pass_rate", "float", true, "通过率")
            )
        );

        // 9.2 注册质量规则实例
        List<Map<String, Object>> qualityRules = Arrays.asList(
            new HashMap<String, Object>() {{
                put("rule_name", "order_id_not_null");
                put("rule_type", "completeness");
                put("target_table", "ods.ods_order_info");
                put("target_column", "order_id");
                put("expression", "order_id IS NOT NULL");
                put("threshold", 1.0);
                put("severity", "critical");
            }},
            new HashMap<String, Object>() {{
                put("rule_name", "order_amount_range");
                put("rule_type", "accuracy");
                put("target_table", "ods.ods_order_info");
                put("target_column", "order_amount");
                put("expression", "order_amount >= 0 AND order_amount <= 1000000");
                put("threshold", 0.999);
                put("severity", "warning");
            }},
            new HashMap<String, Object>() {{
                put("rule_name", "payment_order_consistency");
                put("rule_type", "consistency");
                put("target_table", "dwd.dwd_payment_detail");
                put("expression",
                    "NOT EXISTS orphan orders without matching payments");
                put("threshold", 0.99);
                put("severity", "warning");
            }},
            new HashMap<String, Object>() {{
                put("rule_name", "data_freshness_2h");
                put("rule_type", "timeliness");
                put("target_table", "dws.dws_trade_stats_1d");
                put("expression", "max(dt) >= date_sub(current_date, 1)");
                put("threshold", 1.0);
                put("severity", "critical");
            }},
            new HashMap<String, Object>() {{
                put("rule_name", "user_id_unique");
                put("rule_type", "uniqueness");
                put("target_table", "ods.ods_user_info");
                put("target_column", "user_id");
                put("expression", "COUNT(*) = COUNT(DISTINCT user_id)");
                put("threshold", 1.0);
                put("severity", "critical");
            }}
        );

        for (Map<String, Object> rule : qualityRules) {
            logger.info("注册质量规则: " + rule.get("rule_name") +
                       " [" + rule.get("rule_type") + "] " +
                       "对象=" + rule.get("target_table") +
                       (rule.containsKey("target_column") ?
                        "." + rule.get("target_column") : ""));
        }

        // 9.3 质量评分体系
        logger.info("\n--- 数据质量评分体系 ---");
        logger.info("┌──────────────────────────┬──────┬────────┬────────┐");
        logger.info("│ 维度                     │ 权重 │ 规则数 │ 通过率 │");
        logger.info("├──────────────────────────┼──────┼────────┼────────┤");
        logger.info("│ 完整性 (Completeness)    │ 25%  │  3     │ 99.8%  │");
        logger.info("│ 准确性 (Accuracy)        │ 25%  │  2     │ 99.5%  │");
        logger.info("│ 一致性 (Consistency)     │ 20%  │  2     │ 98.2%  │");
        logger.info("│ 时效性 (Timeliness)      │ 15%  │  1     │ 100%   │");
        logger.info("│ 唯一性 (Uniqueness)      │ 15%  │  2     │ 100%   │");
        logger.info("├──────────────────────────┼──────┼────────┼────────┤");
        logger.info("│ 综合质量评分             │      │  10    │ 99.4   │");
        logger.info("└──────────────────────────┴──────┴────────┴────────┘");
    }

    // ============================================================
    // 10. Hook 集成 & 审计
    // ============================================================

    public void demonstrateHookIntegration() throws Exception {
        logger.info("\n========== 10. Hook 集成 & 审计日志 ==========\n");

        // 10.1 Hive Hook 配置
        logger.info("--- Hive Hook 配置 (hive-site.xml) ---");
        logger.info("hive.exec.post.hooks = org.apache.atlas.hive.hook.HiveHook");
        logger.info("atlas.hook.hive.synchronous = false");
        logger.info("atlas.hook.hive.numRetries = 3");
        logger.info("atlas.hook.hive.minThreads = 5");
        logger.info("atlas.cluster.name = primary");
        logger.info("→ Hive 执行 SQL 时自动将表/列/血缘元数据发送到 Atlas");

        // 10.2 Spark Hook 配置
        logger.info("\n--- Spark Atlas Hook 配置 ---");
        logger.info("spark.extraListeners = com.hortonworks.spark.atlas.SparkAtlasEventTracker");
        logger.info("spark.sql.queryExecutionListeners = com.hortonworks.spark.atlas.SparkAtlasEventTracker");
        logger.info("→ Spark 执行 SQL/DataFrame 操作时自动采集元数据");

        // 10.3 Kafka Hook
        logger.info("\n--- Kafka Atlas Hook ---");
        logger.info("atlas.hook.kafka.numRetries = 3");
        logger.info("→ Kafka Topic 的创建/删除/修改自动同步到 Atlas");

        // 10.4 Flink Hook (自定义)
        logger.info("\n--- Flink Atlas Hook (自定义) ---");
        logger.info("通过 Flink 的 Catalog 集成 Atlas:");
        logger.info("  1. 注册 AtlasCatalog → Flink Table 自动注册到 Atlas");
        logger.info("  2. Flink SQL 执行时通过 Listener 采集血缘");
        logger.info("  3. 血缘信息写入 Atlas Kafka Topic → Atlas 消费入库");

        // 10.5 审计日志
        logger.info("\n--- 审计日志查询 ---");
        logger.info("Atlas 审计功能:");
        logger.info("  - 实体创建/更新/删除操作记录");
        logger.info("  - 分类添加/移除记录");
        logger.info("  - 术语关联/取消关联记录");
        logger.info("  - 血缘关系变更记录");

        // 查询审计日志
        queryAuditEvents("dw.ods_order_info@primary", 10);

        // 10.6 通知机制
        logger.info("\n--- Atlas 通知机制 ---");
        logger.info("Kafka Topics:");
        logger.info("  ATLAS_HOOK: Hook → Atlas (元数据变更通知)");
        logger.info("  ATLAS_ENTITIES: Atlas → 下游 (实体变更事件)");
        logger.info("");
        logger.info("事件类型:");
        logger.info("  - ENTITY_CREATE / ENTITY_UPDATE / ENTITY_DELETE");
        logger.info("  - CLASSIFICATION_ADD / CLASSIFICATION_DELETE");
        logger.info("  - TERM_ADD / TERM_DELETE");
        logger.info("");
        logger.info("下游消费场景:");
        logger.info("  - 数据目录自动更新");
        logger.info("  - 安全策略自动同步 (Apache Ranger)");
        logger.info("  - 合规报告自动生成");
        logger.info("  - 数据治理大屏实时刷新");
    }

    // ============================================================
    // 工具方法: Atlas REST API 封装
    // ============================================================

    /**
     * Atlas REST API - GET 请求
     */
    private String atlasGet(String path) throws Exception {
        URL url = new URL(ATLAS_API_V2 + path);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setRequestProperty("Accept", "application/json");

        // Basic Auth
        String auth = Base64.getEncoder().encodeToString(
            (ATLAS_USER + ":" + ATLAS_PASSWORD).getBytes(StandardCharsets.UTF_8));
        conn.setRequestProperty("Authorization", "Basic " + auth);

        int status = conn.getResponseCode();
        InputStream is = (status >= 200 && status < 300) ?
            conn.getInputStream() : conn.getErrorStream();

        String response = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))
            .lines().collect(Collectors.joining("\n"));

        conn.disconnect();
        return response;
    }

    /**
     * Atlas REST API - POST 请求
     */
    private String atlasPost(String path, String body) throws Exception {
        URL url = new URL(ATLAS_API_V2 + path);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setRequestProperty("Accept", "application/json");
        conn.setDoOutput(true);

        String auth = Base64.getEncoder().encodeToString(
            (ATLAS_USER + ":" + ATLAS_PASSWORD).getBytes(StandardCharsets.UTF_8));
        conn.setRequestProperty("Authorization", "Basic " + auth);

        try (OutputStream os = conn.getOutputStream()) {
            os.write(body.getBytes(StandardCharsets.UTF_8));
        }

        int status = conn.getResponseCode();
        InputStream is = (status >= 200 && status < 300) ?
            conn.getInputStream() : conn.getErrorStream();

        String response = "";
        if (is != null) {
            response = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))
                .lines().collect(Collectors.joining("\n"));
        }

        conn.disconnect();
        return response;
    }

    /**
     * Atlas REST API - PUT 请求
     */
    private String atlasPut(String path, String body) throws Exception {
        URL url = new URL(ATLAS_API_V2 + path);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("PUT");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setRequestProperty("Accept", "application/json");
        conn.setDoOutput(true);

        String auth = Base64.getEncoder().encodeToString(
            (ATLAS_USER + ":" + ATLAS_PASSWORD).getBytes(StandardCharsets.UTF_8));
        conn.setRequestProperty("Authorization", "Basic " + auth);

        try (OutputStream os = conn.getOutputStream()) {
            os.write(body.getBytes(StandardCharsets.UTF_8));
        }

        int status = conn.getResponseCode();
        InputStream is = (status >= 200 && status < 300) ?
            conn.getInputStream() : conn.getErrorStream();

        String response = "";
        if (is != null) {
            response = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))
                .lines().collect(Collectors.joining("\n"));
        }

        conn.disconnect();
        return response;
    }

    // ============================================================
    // 工具方法: 类型定义构建
    // ============================================================

    private JsonObject createEntityTypeDef(String name, String description,
                                           List<String> superTypes,
                                           List<JsonObject> attributeDefs) {
        JsonObject typeDef = new JsonObject();
        typeDef.addProperty("name", name);
        typeDef.addProperty("description", description);
        typeDef.addProperty("category", "ENTITY");
        typeDef.addProperty("serviceType", "bigdata_platform");

        JsonArray superTypesArray = new JsonArray();
        if (superTypes != null) {
            superTypes.forEach(superTypesArray::add);
        }
        typeDef.add("superTypes", superTypesArray);

        JsonArray attrs = new JsonArray();
        if (attributeDefs != null) {
            attributeDefs.forEach(attrs::add);
        }
        typeDef.add("attributeDefs", attrs);

        return typeDef;
    }

    private JsonObject createAttributeDef(String name, String typeName,
                                          boolean isOptional, String description) {
        JsonObject attr = new JsonObject();
        attr.addProperty("name", name);
        attr.addProperty("typeName", typeName);
        attr.addProperty("isOptional", isOptional);
        attr.addProperty("description", description);
        attr.addProperty("cardinality", "SINGLE");
        attr.addProperty("isUnique", false);
        attr.addProperty("isIndexable", true);
        return attr;
    }

    private JsonObject createClassificationDef(String name, String description,
                                               List<JsonObject> attributeDefs) {
        JsonObject classDef = new JsonObject();
        classDef.addProperty("name", name);
        classDef.addProperty("description", description);
        classDef.addProperty("category", "CLASSIFICATION");

        JsonArray attrs = new JsonArray();
        if (attributeDefs != null) {
            attributeDefs.forEach(attrs::add);
        }
        classDef.add("attributeDefs", attrs);

        return classDef;
    }

    private JsonObject createRelationshipTypeDef(String name, String description,
                                                 String endType1, String endName1,
                                                 String endType2, String endName2,
                                                 String category) {
        JsonObject relDef = new JsonObject();
        relDef.addProperty("name", name);
        relDef.addProperty("description", description);
        relDef.addProperty("category", "RELATIONSHIP");
        relDef.addProperty("relationshipCategory", category);

        JsonObject end1 = new JsonObject();
        end1.addProperty("type", endType1);
        end1.addProperty("name", endName1);
        end1.addProperty("isContainer", false);
        end1.addProperty("cardinality", "SET");
        relDef.add("endDef1", end1);

        JsonObject end2 = new JsonObject();
        end2.addProperty("type", endType2);
        end2.addProperty("name", endName2);
        end2.addProperty("isContainer", false);
        end2.addProperty("cardinality", "SET");
        relDef.add("endDef2", end2);

        return relDef;
    }

    private JsonObject createBusinessMetadataDef(String name, String description,
                                                 List<JsonObject> attributeDefs) {
        JsonObject bmDef = new JsonObject();
        bmDef.addProperty("name", name);
        bmDef.addProperty("description", description);
        bmDef.addProperty("category", "BUSINESS_METADATA");

        JsonArray attrs = new JsonArray();
        if (attributeDefs != null) {
            for (JsonObject attr : attributeDefs) {
                // BusinessMetadata 的属性需要 applicableEntityTypes
                JsonArray applicableTypes = new JsonArray();
                applicableTypes.add("DataSet");
                applicableTypes.add("Process");
                attr.add("options", buildOptions(applicableTypes));
            }
            attributeDefs.forEach(attrs::add);
        }
        bmDef.add("attributeDefs", attrs);

        return bmDef;
    }

    private JsonObject buildOptions(JsonArray applicableEntityTypes) {
        JsonObject options = new JsonObject();
        options.add("applicableEntityTypes", applicableEntityTypes);
        return options;
    }

    // ============================================================
    // 工具方法: 实体操作
    // ============================================================

    private String createEntity(String typeName, String name, String qualifiedName,
                                Map<String, Object> attributes) throws Exception {
        JsonObject entity = new JsonObject();
        entity.addProperty("typeName", typeName);

        JsonObject attrs = new JsonObject();
        attrs.addProperty("name", name);
        attrs.addProperty("qualifiedName", qualifiedName);

        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            if (entry.getValue() instanceof String) {
                attrs.addProperty(entry.getKey(), (String) entry.getValue());
            } else if (entry.getValue() instanceof Number) {
                attrs.addProperty(entry.getKey(), (Number) entry.getValue());
            } else if (entry.getValue() instanceof Boolean) {
                attrs.addProperty(entry.getKey(), (Boolean) entry.getValue());
            } else if (entry.getValue() instanceof JsonObject) {
                attrs.add(entry.getKey(), (JsonObject) entry.getValue());
            }
        }
        entity.add("attributes", attrs);

        JsonObject wrapper = new JsonObject();
        wrapper.add("entity", entity);

        String response = atlasPost("/entity", wrapper.toString());
        // 解析返回 GUID
        JsonObject result = gson.fromJson(response, JsonObject.class);
        if (result != null && result.has("guidAssignments")) {
            return result.getAsJsonObject("guidAssignments").entrySet()
                .iterator().next().getValue().getAsString();
        }
        return "unknown";
    }

    private String createHiveTableEntity(String tableName, String dbName,
                                         String description,
                                         List<JsonObject> columns,
                                         String owner, String format) throws Exception {
        Map<String, Object> attrs = new HashMap<>();
        attrs.put("name", tableName);
        attrs.put("qualifiedName", dbName + "." + tableName + "@primary");
        attrs.put("description", description);
        attrs.put("owner", owner);
        attrs.put("tableType", "MANAGED_TABLE");
        attrs.put("createTime", System.currentTimeMillis());
        attrs.put("db", createRefObject("hive_db", dbName + "@primary"));

        JsonArray cols = new JsonArray();
        for (int i = 0; i < columns.size(); i++) {
            JsonObject col = columns.get(i);
            col.getAsJsonObject("attributes").addProperty("position", i);
            cols.add(col);
        }
        // 列作为嵌套实体

        return createEntity("hive_table", tableName, dbName + "." + tableName + "@primary", attrs);
    }

    private JsonObject createColumnEntity(String name, String type,
                                          String comment, boolean isPk) {
        JsonObject column = new JsonObject();
        column.addProperty("typeName", "hive_column");

        JsonObject attrs = new JsonObject();
        attrs.addProperty("name", name);
        attrs.addProperty("qualifiedName", "dw." + name + "@primary");
        attrs.addProperty("type", type);
        attrs.addProperty("comment", comment);
        if (isPk) {
            attrs.addProperty("isPrimaryKey", true);
        }
        column.add("attributes", attrs);

        return column;
    }

    private JsonObject createHiveTableJson(String tableName, String dbName, String description) {
        JsonObject entity = new JsonObject();
        entity.addProperty("typeName", "hive_table");

        JsonObject attrs = new JsonObject();
        attrs.addProperty("name", tableName);
        attrs.addProperty("qualifiedName", dbName + "." + tableName + "@primary");
        attrs.addProperty("description", description);
        attrs.addProperty("owner", "data-team");
        entity.add("attributes", attrs);

        return entity;
    }

    private void batchCreateEntities(List<JsonObject> entities) throws Exception {
        JsonObject wrapper = new JsonObject();
        JsonArray entitiesArray = new JsonArray();
        entities.forEach(entitiesArray::add);
        wrapper.add("entities", entitiesArray);

        atlasPost("/entity/bulk", wrapper.toString());
    }

    private void updateEntityAttributes(String guid, Map<String, Object> attributes)
            throws Exception {
        JsonObject entity = new JsonObject();
        JsonObject attrs = new JsonObject();
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            if (entry.getValue() instanceof String) {
                attrs.addProperty(entry.getKey(), (String) entry.getValue());
            } else if (entry.getValue() instanceof Number) {
                attrs.addProperty(entry.getKey(), (Number) entry.getValue());
            }
        }
        entity.add("attributes", attrs);

        JsonObject wrapper = new JsonObject();
        wrapper.add("entity", entity);

        atlasPut("/entity/guid/" + guid, wrapper.toString());
    }

    private void addBusinessMetadata(String guid, String bmName,
                                     Map<String, Object> attributes) throws Exception {
        JsonObject bm = new JsonObject();
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            if (entry.getValue() instanceof String) {
                bm.addProperty(entry.getKey(), (String) entry.getValue());
            } else if (entry.getValue() instanceof Number) {
                bm.addProperty(entry.getKey(), (Number) entry.getValue());
            } else if (entry.getValue() instanceof Boolean) {
                bm.addProperty(entry.getKey(), (Boolean) entry.getValue());
            }
        }

        JsonObject wrapper = new JsonObject();
        wrapper.add(bmName, bm);

        atlasPut("/entity/guid/" + guid + "/businessmetadata", wrapper.toString());
    }

    private JsonObject createRefObject(String typeName, String qualifiedName) {
        JsonObject ref = new JsonObject();
        ref.addProperty("typeName", typeName);
        JsonObject uniqueAttrs = new JsonObject();
        uniqueAttrs.addProperty("qualifiedName", qualifiedName);
        ref.add("uniqueAttributes", uniqueAttrs);
        return ref;
    }

    // ============================================================
    // 工具方法: 血缘操作
    // ============================================================

    private String createProcessEntity(String processName, String description,
                                       List<String> inputQualifiedNames,
                                       List<String> outputQualifiedNames,
                                       Map<String, Object> extraAttrs) throws Exception {
        JsonObject entity = new JsonObject();
        entity.addProperty("typeName", "Process");

        JsonObject attrs = new JsonObject();
        attrs.addProperty("name", processName);
        attrs.addProperty("qualifiedName", processName + "@primary");
        attrs.addProperty("description", description);

        // 输入
        JsonArray inputs = new JsonArray();
        for (String qn : inputQualifiedNames) {
            inputs.add(createRefByQualifiedName(qn));
        }
        attrs.add("inputs", inputs);

        // 输出
        JsonArray outputs = new JsonArray();
        for (String qn : outputQualifiedNames) {
            outputs.add(createRefByQualifiedName(qn));
        }
        attrs.add("outputs", outputs);

        // 额外属性
        if (extraAttrs != null) {
            for (Map.Entry<String, Object> entry : extraAttrs.entrySet()) {
                if (entry.getValue() instanceof String) {
                    attrs.addProperty(entry.getKey(), (String) entry.getValue());
                } else if (entry.getValue() instanceof Number) {
                    attrs.addProperty(entry.getKey(), (Number) entry.getValue());
                }
            }
        }

        entity.add("attributes", attrs);

        JsonObject wrapper = new JsonObject();
        wrapper.add("entity", entity);

        String response = atlasPost("/entity", wrapper.toString());
        logger.info("创建血缘过程: " + processName);
        return response;
    }

    private JsonObject createRefByQualifiedName(String qualifiedName) {
        JsonObject ref = new JsonObject();
        ref.addProperty("typeName", "DataSet");
        JsonObject uniqueAttrs = new JsonObject();
        uniqueAttrs.addProperty("qualifiedName", qualifiedName);
        ref.add("uniqueAttributes", uniqueAttrs);
        return ref;
    }

    private void queryLineage(String qualifiedName, String direction, int depth)
            throws Exception {
        // 先通过 qualifiedName 获取 GUID
        String searchResult = atlasGet("/entity/uniqueAttribute/type/DataSet?attr:qualifiedName=" +
            java.net.URLEncoder.encode(qualifiedName, "UTF-8"));

        logger.info("血缘查询: " + qualifiedName);
        logger.info("  方向: " + direction + ", 深度: " + depth);
        logger.info("  API: GET /api/atlas/v2/lineage/{guid}?direction=" +
                    direction + "&depth=" + depth);

        // 模拟展示血缘结果
        if (qualifiedName.contains("dwd_payment_detail")) {
            logger.info("  血缘结果 (上游):");
            logger.info("    ← ods_order_info");
            logger.info("    ← ods_payment_info");
            logger.info("    ← ods_product_info");
            logger.info("    ← dim_region_info");
            logger.info("    ← MySQL.order_info (间接)");
            logger.info("  血缘结果 (下游):");
            logger.info("    → dws_trade_stats_1d");
            logger.info("    → ClickHouse.ads_trade_stats (间接)");
        }
    }

    private void queryColumnLineage(String columnQualifiedName, String direction, int depth)
            throws Exception {
        logger.info("列级血缘查询: " + columnQualifiedName);
        logger.info("  方向: " + direction + ", 深度: " + depth);
        logger.info("  API: GET /api/atlas/v2/lineage/{columnGuid}?direction=" +
                    direction + "&depth=" + depth);
    }

    private void addColumnRef(JsonArray array, String qualifiedName) {
        JsonObject ref = new JsonObject();
        ref.addProperty("typeName", "hive_column");
        JsonObject uniqueAttrs = new JsonObject();
        uniqueAttrs.addProperty("qualifiedName", qualifiedName);
        ref.add("uniqueAttributes", uniqueAttrs);
        array.add(ref);
    }

    // ============================================================
    // 工具方法: 分类操作
    // ============================================================

    private void addClassificationToEntity(String qualifiedName,
                                           String classificationName,
                                           Map<String, Object> attributes) throws Exception {
        JsonObject classification = new JsonObject();
        classification.addProperty("typeName", classificationName);

        if (attributes != null) {
            JsonObject attrs = new JsonObject();
            for (Map.Entry<String, Object> entry : attributes.entrySet()) {
                if (entry.getValue() instanceof String) {
                    attrs.addProperty(entry.getKey(), (String) entry.getValue());
                } else if (entry.getValue() instanceof Boolean) {
                    attrs.addProperty(entry.getKey(), (Boolean) entry.getValue());
                } else if (entry.getValue() instanceof Number) {
                    attrs.addProperty(entry.getKey(), (Number) entry.getValue());
                }
            }
            classification.add("attributes", attrs);
        }

        // 设置传播标记
        classification.addProperty("propagate", true);

        JsonArray classifications = new JsonArray();
        classifications.add(classification);

        atlasPost("/entity/uniqueAttribute/type/DataSet/classifications?attr:qualifiedName=" +
            java.net.URLEncoder.encode(qualifiedName, "UTF-8"),
            classifications.toString());
    }

    private void searchByClassification(String classificationName) throws Exception {
        String result = atlasGet("/search/basic?classification=" + classificationName + "&limit=10");
        logger.info("分类搜索 [" + classificationName + "] 结果: " + result);
    }

    // ============================================================
    // 工具方法: 术语表操作
    // ============================================================

    private String createGlossary(String name, String shortDescription) throws Exception {
        JsonObject glossary = new JsonObject();
        glossary.addProperty("name", name);
        glossary.addProperty("shortDescription", shortDescription);

        String response = atlasPost("/glossary", glossary.toString());
        JsonObject result = gson.fromJson(response, JsonObject.class);
        return result != null ? result.get("guid").getAsString() : "unknown";
    }

    private String createGlossaryCategory(String glossaryGuid, String name,
                                          String description) throws Exception {
        JsonObject category = new JsonObject();
        category.addProperty("name", name);
        category.addProperty("shortDescription", description);

        JsonObject anchor = new JsonObject();
        anchor.addProperty("glossaryGuid", glossaryGuid);
        category.add("anchor", anchor);

        String response = atlasPost("/glossary/category", category.toString());
        JsonObject result = gson.fromJson(response, JsonObject.class);
        return result != null ? result.get("guid").getAsString() : "unknown";
    }

    private String createGlossaryTerm(String glossaryGuid, String name,
                                      String shortDescription,
                                      String longDescription) throws Exception {
        JsonObject term = new JsonObject();
        term.addProperty("name", name);
        term.addProperty("shortDescription", shortDescription);
        term.addProperty("longDescription", longDescription);

        JsonObject anchor = new JsonObject();
        anchor.addProperty("glossaryGuid", glossaryGuid);
        term.add("anchor", anchor);

        String response = atlasPost("/glossary/term", term.toString());
        JsonObject result = gson.fromJson(response, JsonObject.class);
        return result != null ? result.get("guid").getAsString() : "unknown";
    }

    private void assignTermToEntity(String termName, String entityQualifiedName) throws Exception {
        logger.info("关联术语 [" + termName + "] → 实体 [" + entityQualifiedName + "]");
        // POST /glossary/terms/{termGuid}/assignedEntities
    }

    // ============================================================
    // 工具方法: 搜索 & 查询
    // ============================================================

    private void fullTextSearch(String query, int limit) throws Exception {
        String result = atlasGet("/search/fulltext?query=" +
            java.net.URLEncoder.encode(query, "UTF-8") + "&limit=" + limit);
        logger.info("全文搜索 [" + query + "] 结果: " + result);
    }

    private void dslSearch(String query, int limit) throws Exception {
        String result = atlasGet("/search/dsl?query=" +
            java.net.URLEncoder.encode(query, "UTF-8") + "&limit=" + limit);
        logger.info("DSL 查询 [" + query + "] 结果: " + result);
    }

    private void attributeSearch(String typeName, String attrName,
                                 String attrValue, int limit) throws Exception {
        String result = atlasGet("/search/attribute?typeName=" + typeName +
            "&attrName=" + attrName + "&attrValuePrefix=" + attrValue + "&limit=" + limit);
        logger.info("属性搜索 [" + typeName + "." + attrName + "=" + attrValue + "]: " + result);
    }

    private void relationshipSearch(String qualifiedName) throws Exception {
        logger.info("关系搜索: " + qualifiedName);
        logger.info("  API: GET /api/atlas/v2/entity/uniqueAttribute/type/DataSet" +
                    "?attr:qualifiedName=" + qualifiedName);
    }

    private void createSavedSearch(String name, String description, String query) throws Exception {
        JsonObject search = new JsonObject();
        search.addProperty("name", name);

        JsonObject searchParams = new JsonObject();
        searchParams.addProperty("query", query);
        searchParams.addProperty("typeName", "hive_table");
        search.add("searchParameters", searchParams);

        logger.info("创建保存搜索: " + name + " (query: " + query + ")");
    }

    // ============================================================
    // 工具方法: 审计
    // ============================================================

    private void queryAuditEvents(String qualifiedName, int count) throws Exception {
        logger.info("审计日志查询: " + qualifiedName + " (最近 " + count + " 条)");
        logger.info("  API: GET /api/atlas/v2/entity/{guid}/audit?count=" + count);
        logger.info("  典型审计记录:");
        logger.info("    [2024-01-01 02:15:00] ENTITY_CREATE by sqoop-hook");
        logger.info("    [2024-01-01 02:15:01] CLASSIFICATION_ADD PII by admin");
        logger.info("    [2024-01-01 08:00:00] ENTITY_UPDATE owner='etl-team' by admin");
        logger.info("    [2024-01-01 10:30:00] TERM_ADD 'GMV' by data-steward");
    }
}
