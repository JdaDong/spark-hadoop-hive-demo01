"""
==========================================================================
  Airflow + Atlas 集成 DAG - 自动化数据治理
  
  📌 功能:
  1. ETL 完成后自动注册血缘到 Atlas
  2. 数据分类自动传播 & 合规检查
  3. 元数据变更告警
  4. 数据资产盘点 & 健康度报告
  5. Atlas 元数据同步 & 一致性校验
  
  📌 触发方式:
  - ETL DAG 完成后自动触发
  - 每日定时巡检
  
  Author: BigData Demo
==========================================================================
"""

from datetime import datetime, timedelta
from typing import Dict, List, Any
import json
import logging

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup
from airflow.sensors.external_task import ExternalTaskSensor

logger = logging.getLogger(__name__)

# Atlas 配置
ATLAS_URL = Variable.get("atlas_url", default_var="http://atlas:21000")
ATLAS_USER = Variable.get("atlas_user", default_var="admin")
ATLAS_PASSWORD = Variable.get("atlas_password", default_var="admin")


# ============================================================
# 工具类: Atlas REST API Client
# ============================================================

class AtlasClient:
    """Airflow Atlas REST API 客户端"""
    
    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url.rstrip("/") + "/api/atlas/v2"
        self.auth = (username, password)
    
    def _request(self, method: str, path: str, **kwargs) -> dict:
        import requests
        url = f"{self.base_url}{path}"
        resp = requests.request(
            method, url,
            auth=self.auth,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            timeout=30,
            **kwargs
        )
        resp.raise_for_status()
        return resp.json() if resp.content else {}
    
    def create_entity(self, entity: dict) -> dict:
        return self._request("POST", "/entity", json={"entity": entity})
    
    def create_entities(self, entities: list) -> dict:
        return self._request("POST", "/entity/bulk", json={"entities": entities})
    
    def get_entity_by_qualified_name(self, type_name: str, qualified_name: str) -> dict:
        return self._request("GET",
            f"/entity/uniqueAttribute/type/{type_name}?attr:qualifiedName={qualified_name}")
    
    def search(self, query: str, type_name: str = None, limit: int = 25) -> dict:
        params = {"query": query, "limit": limit}
        if type_name:
            params["typeName"] = type_name
        return self._request("GET", "/search/basic", params=params)
    
    def get_lineage(self, guid: str, direction: str = "BOTH", depth: int = 5) -> dict:
        return self._request("GET",
            f"/lineage/{guid}?direction={direction}&depth={depth}")
    
    def add_classification(self, guid: str, classifications: list):
        return self._request("POST",
            f"/entity/guid/{guid}/classifications", json=classifications)
    
    def get_audit(self, guid: str, count: int = 10) -> list:
        return self._request("GET",
            f"/entity/{guid}/audit?count={count}")
    
    def get_metrics(self) -> dict:
        return self._request("GET", "/admin/metrics")


# ============================================================
# 1. DAG: ETL 血缘自动注册
# ============================================================

with DAG(
    dag_id="atlas_lineage_registration",
    default_args={
        "owner": "data-governance",
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
    },
    description="Atlas 血缘自动注册 - ETL 完成后自动更新数据血缘",
    schedule_interval=None,                    # 由 ETL DAG 触发
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["atlas", "lineage", "governance"],
    params={
        "source_tables": [],
        "target_table": "",
        "process_name": "",
        "process_type": "Spark",               # Spark / Hive / Flink
        "dag_id": "",
        "task_id": "",
        "sql_query": "",
    },
) as lineage_dag:
    
    @task(task_id="register_lineage")
    def register_lineage(**kwargs):
        """将 ETL 血缘信息注册到 Atlas"""
        params = kwargs["params"]
        ds = kwargs["ds"]
        
        atlas = AtlasClient(ATLAS_URL, ATLAS_USER, ATLAS_PASSWORD)
        
        source_tables = params.get("source_tables", [])
        target_table = params.get("target_table", "")
        process_name = params.get("process_name", "")
        process_type = params.get("process_type", "Spark")
        dag_id = params.get("dag_id", "")
        task_id = params.get("task_id", "")
        sql_query = params.get("sql_query", "")
        
        if not source_tables or not target_table:
            logger.warning("Missing source_tables or target_table, skipping")
            return
        
        # 构建 Process 实体 (自动生成血缘)
        process_entity = {
            "typeName": "Process",
            "attributes": {
                "name": process_name or f"etl_{target_table}",
                "qualifiedName": f"{process_name}@airflow_{ds}",
                "description": f"自动注册的 ETL 血缘 (by Airflow {dag_id}.{task_id})",
                "inputs": [
                    {
                        "typeName": "DataSet",
                        "uniqueAttributes": {"qualifiedName": t}
                    } for t in source_tables
                ],
                "outputs": [
                    {
                        "typeName": "DataSet",
                        "uniqueAttributes": {"qualifiedName": target_table}
                    }
                ],
            }
        }
        
        result = atlas.create_entity(process_entity)
        
        logger.info(f"✅ Lineage registered: {source_tables} → {target_table}")
        logger.info(f"   Process: {process_name}, Engine: {process_type}")
        logger.info(f"   Atlas result: {result}")
        
        return {
            "process_name": process_name,
            "source_tables": source_tables,
            "target_table": target_table,
            "status": "registered"
        }
    
    @task(task_id="update_process_metadata")
    def update_process_metadata(lineage_result: dict, **kwargs):
        """更新 Process 的执行元数据"""
        params = kwargs["params"]
        ds = kwargs["ds"]
        
        atlas = AtlasClient(ATLAS_URL, ATLAS_USER, ATLAS_PASSWORD)
        process_name = lineage_result.get("process_name")
        
        # 获取 Process 实体
        try:
            entity = atlas.get_entity_by_qualified_name(
                "Process", f"{process_name}@airflow_{ds}")
            guid = entity.get("entity", {}).get("guid")
            
            if guid:
                # 更新执行时间和状态
                update_entity = {
                    "typeName": "Process",
                    "attributes": {
                        "qualifiedName": f"{process_name}@airflow_{ds}",
                        "last_run_time": datetime.now().isoformat(),
                        "last_run_status": "SUCCESS",
                        "airflow_dag_id": params.get("dag_id", ""),
                    },
                    "guid": guid,
                }
                atlas.create_entity(update_entity)
                logger.info(f"✅ Process metadata updated: {process_name}")
        except Exception as e:
            logger.warning(f"Failed to update process metadata: {e}")
    
    lineage = register_lineage()
    update_process_metadata(lineage)


# ============================================================
# 2. DAG: 数据资产巡检 & 治理报告
# ============================================================

with DAG(
    dag_id="atlas_governance_inspection",
    default_args={
        "owner": "data-governance",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="数据治理巡检 - 元数据完整性/分类覆盖/血缘完整性检查",
    schedule_interval="0 9 * * *",             # 每天 9:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["atlas", "governance", "inspection"],
) as inspection_dag:
    
    # ----------------------------------------------------------
    # 巡检 1: 元数据完整性检查
    # ----------------------------------------------------------
    
    @task(task_id="check_metadata_completeness")
    def check_metadata_completeness():
        """检查元数据完整性 - owner/description/分类 是否齐全"""
        atlas = AtlasClient(ATLAS_URL, ATLAS_USER, ATLAS_PASSWORD)
        
        issues = []
        
        # 查询所有 Hive 表
        result = atlas.search("*", type_name="hive_table", limit=100)
        entities = result.get("entities", [])
        
        for entity in entities:
            attrs = entity.get("attributes", {})
            name = attrs.get("qualifiedName", "")
            
            # 检查 owner
            if not attrs.get("owner"):
                issues.append({
                    "entity": name,
                    "issue": "missing_owner",
                    "severity": "warning"
                })
            
            # 检查 description
            if not attrs.get("description"):
                issues.append({
                    "entity": name,
                    "issue": "missing_description",
                    "severity": "info"
                })
            
            # 检查分类
            classifications = entity.get("classificationNames", [])
            if not classifications:
                issues.append({
                    "entity": name,
                    "issue": "no_classification",
                    "severity": "warning"
                })
        
        logger.info(f"元数据完整性检查: {len(entities)} 个实体, {len(issues)} 个问题")
        for issue in issues[:20]:
            logger.warning(f"  [{issue['severity']}] {issue['entity']}: {issue['issue']}")
        
        return {
            "total_entities": len(entities),
            "total_issues": len(issues),
            "issues": issues
        }
    
    # ----------------------------------------------------------
    # 巡检 2: 数据分类覆盖率
    # ----------------------------------------------------------
    
    @task(task_id="check_classification_coverage")
    def check_classification_coverage():
        """检查数据分类覆盖率"""
        atlas = AtlasClient(ATLAS_URL, ATLAS_USER, ATLAS_PASSWORD)
        
        # 获取 Atlas 指标
        metrics = atlas.get_metrics()
        
        # 统计各分类的实体数量
        classification_stats = {}
        classifications_to_check = [
            "PII", "SecurityLevel_Public", "SecurityLevel_Internal",
            "SecurityLevel_Confidential", "SecurityLevel_Restricted",
            "DataQuality_Certified"
        ]
        
        for cls_name in classifications_to_check:
            try:
                result = atlas.search("*", type_name=None, limit=1)
                # 使用分类过滤
                classification_stats[cls_name] = result.get("approximateCount", 0)
            except Exception:
                classification_stats[cls_name] = 0
        
        # 统计所有表中有安全等级标记的比例
        all_tables = atlas.search("*", type_name="hive_table", limit=100)
        total = len(all_tables.get("entities", []))
        classified = sum(1 for e in all_tables.get("entities", [])
                        if e.get("classificationNames"))
        
        coverage = classified / total * 100 if total > 0 else 0
        
        report = {
            "total_tables": total,
            "classified_tables": classified,
            "coverage_rate": round(coverage, 1),
            "classification_stats": classification_stats,
            "target_coverage": 90,
            "is_compliant": coverage >= 90
        }
        
        logger.info(f"分类覆盖率: {coverage:.1f}% ({classified}/{total})")
        if coverage < 90:
            logger.warning(f"⚠️ 分类覆盖率低于目标 90%!")
        else:
            logger.info("✅ 分类覆盖率达标")
        
        return report
    
    # ----------------------------------------------------------
    # 巡检 3: 血缘完整性检查
    # ----------------------------------------------------------
    
    @task(task_id="check_lineage_completeness")
    def check_lineage_completeness():
        """检查血缘完整性 - 是否所有表都有血缘关系"""
        atlas = AtlasClient(ATLAS_URL, ATLAS_USER, ATLAS_PASSWORD)
        
        # 获取所有表
        result = atlas.search("*", type_name="hive_table", limit=100)
        entities = result.get("entities", [])
        
        orphan_tables = []  # 没有血缘的表
        broken_lineage = [] # 断裂的血缘
        
        for entity in entities:
            guid = entity.get("guid")
            name = entity.get("attributes", {}).get("qualifiedName", "")
            
            try:
                lineage = atlas.get_lineage(guid, "BOTH", 1)
                relations = lineage.get("relations", [])
                
                if not relations:
                    # ODS 表允许没有上游, ADS 表允许没有下游
                    if "ods_" in name:
                        # 检查是否有下游
                        downstream = atlas.get_lineage(guid, "OUTPUT", 1)
                        if not downstream.get("relations"):
                            orphan_tables.append({"entity": name, "issue": "no_downstream"})
                    elif "ads_" in name or "dws_" in name:
                        # 检查是否有上游
                        upstream = atlas.get_lineage(guid, "INPUT", 1)
                        if not upstream.get("relations"):
                            broken_lineage.append({"entity": name, "issue": "no_upstream"})
                    else:
                        orphan_tables.append({"entity": name, "issue": "no_lineage"})
                        
            except Exception as e:
                logger.warning(f"Failed to check lineage for {name}: {e}")
        
        report = {
            "total_tables": len(entities),
            "orphan_tables": orphan_tables,
            "broken_lineage": broken_lineage,
            "lineage_coverage": round(
                (len(entities) - len(orphan_tables)) / len(entities) * 100, 1
            ) if entities else 0
        }
        
        logger.info(f"血缘完整性: 覆盖率 {report['lineage_coverage']}%")
        if orphan_tables:
            logger.warning(f"  孤立表 ({len(orphan_tables)}):")
            for t in orphan_tables[:10]:
                logger.warning(f"    {t['entity']}: {t['issue']}")
        
        return report
    
    # ----------------------------------------------------------
    # 巡检 4: PII 合规检查
    # ----------------------------------------------------------
    
    @task(task_id="check_pii_compliance")
    def check_pii_compliance():
        """PII 数据合规检查"""
        atlas = AtlasClient(ATLAS_URL, ATLAS_USER, ATLAS_PASSWORD)
        
        # 查找标记为 PII 的实体
        pii_entities = atlas.search("*", type_name="hive_table", limit=100)
        
        compliance_issues = []
        
        for entity in pii_entities.get("entities", []):
            classifications = entity.get("classificationNames", [])
            if "PII" not in classifications:
                continue
            
            name = entity.get("attributes", {}).get("qualifiedName", "")
            
            # 检查 PII 表是否有安全等级标记
            has_security_level = any(
                c.startswith("SecurityLevel_") for c in classifications
            )
            if not has_security_level:
                compliance_issues.append({
                    "entity": name,
                    "issue": "PII without security level",
                    "severity": "critical"
                })
            
            # 检查 PII 表的下游是否也有 PII 标记 (传播检查)
            guid = entity.get("guid")
            try:
                lineage = atlas.get_lineage(guid, "OUTPUT", 3)
                downstream_guids = set()
                for rel in lineage.get("relations", []):
                    downstream_guids.add(rel.get("toEntityId"))
                
                # 检查下游实体是否继承了 PII 标记
                for downstream_guid in downstream_guids:
                    downstream_entity = lineage.get("guidEntityMap", {}).get(downstream_guid, {})
                    downstream_cls = downstream_entity.get("classificationNames", [])
                    if "PII" not in downstream_cls:
                        downstream_name = downstream_entity.get("attributes", {}).get(
                            "qualifiedName", downstream_guid)
                        compliance_issues.append({
                            "entity": downstream_name,
                            "issue": f"Downstream of PII table {name} without PII classification",
                            "severity": "warning"
                        })
            except Exception:
                pass
        
        logger.info(f"PII 合规检查: {len(compliance_issues)} 个问题")
        for issue in compliance_issues:
            level = "🚨" if issue["severity"] == "critical" else "⚠️"
            logger.warning(f"  {level} {issue['entity']}: {issue['issue']}")
        
        return {
            "total_issues": len(compliance_issues),
            "critical_issues": len([i for i in compliance_issues if i["severity"] == "critical"]),
            "issues": compliance_issues
        }
    
    # ----------------------------------------------------------
    # 巡检 5: 生成治理报告
    # ----------------------------------------------------------
    
    @task(task_id="generate_governance_report")
    def generate_governance_report(
        metadata_result: dict,
        classification_result: dict,
        lineage_result: dict,
        pii_result: dict,
        **kwargs
    ):
        """生成数据治理综合报告"""
        ds = kwargs["ds"]
        
        report = {
            "report_date": ds,
            "report_type": "daily_governance_inspection",
            "summary": {
                "total_assets": metadata_result["total_entities"],
                "metadata_issues": metadata_result["total_issues"],
                "classification_coverage": classification_result["coverage_rate"],
                "lineage_coverage": lineage_result["lineage_coverage"],
                "pii_compliance_issues": pii_result["total_issues"],
            },
            "scores": {},
            "details": {
                "metadata": metadata_result,
                "classification": classification_result,
                "lineage": lineage_result,
                "pii_compliance": pii_result,
            }
        }
        
        # 计算治理评分
        metadata_score = max(0, 100 - metadata_result["total_issues"] * 2)
        classification_score = classification_result["coverage_rate"]
        lineage_score = lineage_result["lineage_coverage"]
        pii_score = max(0, 100 - pii_result["total_issues"] * 10)
        
        total_score = (
            metadata_score * 0.25 +
            classification_score * 0.25 +
            lineage_score * 0.25 +
            pii_score * 0.25
        )
        
        report["scores"] = {
            "metadata_completeness": round(metadata_score, 1),
            "classification_coverage": round(classification_score, 1),
            "lineage_completeness": round(lineage_score, 1),
            "pii_compliance": round(pii_score, 1),
            "total_governance_score": round(total_score, 1),
        }
        
        logger.info("=" * 60)
        logger.info(f"📊 数据治理巡检报告 ({ds})")
        logger.info("=" * 60)
        logger.info(f"  资产总数:       {report['summary']['total_assets']}")
        logger.info(f"  元数据完整性:   {metadata_score:.0f}/100")
        logger.info(f"  分类覆盖率:     {classification_score:.1f}%")
        logger.info(f"  血缘覆盖率:     {lineage_score:.1f}%")
        logger.info(f"  PII合规:        {pii_score:.0f}/100")
        logger.info(f"  ────────────────────────────")
        logger.info(f"  🏆 综合治理评分: {total_score:.1f}/100")
        logger.info("=" * 60)
        
        # 评级
        if total_score >= 90:
            logger.info("  评级: 🟢 优秀 (A)")
        elif total_score >= 75:
            logger.info("  评级: 🟡 良好 (B)")
        elif total_score >= 60:
            logger.info("  评级: 🟠 合格 (C)")
        else:
            logger.info("  评级: 🔴 需改进 (D)")
        
        return report
    
    # 任务编排
    metadata = check_metadata_completeness()
    classification = check_classification_coverage()
    lineage = check_lineage_completeness()
    pii = check_pii_compliance()
    generate_governance_report(metadata, classification, lineage, pii)


# ============================================================
# 3. DAG: 元数据变更监控
# ============================================================

with DAG(
    dag_id="atlas_metadata_change_monitor",
    default_args={
        "owner": "data-governance",
        "retries": 1,
    },
    description="Atlas 元数据变更监控 - 检测异常变更并告警",
    schedule_interval="*/30 * * * *",          # 每 30 分钟
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["atlas", "monitor", "alert"],
) as change_monitor_dag:
    
    @task(task_id="detect_schema_changes")
    def detect_schema_changes(**kwargs):
        """检测 Schema 变更 (新增/删除/修改列)"""
        atlas = AtlasClient(ATLAS_URL, ATLAS_USER, ATLAS_PASSWORD)
        
        # 查询最近 30 分钟内有变更的实体
        # Atlas 2.x 支持通过审计日志查询
        changes = []
        
        # 获取所有关键表的审计日志
        critical_tables = [
            "dw.ods_order_info@primary",
            "dw.dwd_payment_detail@primary",
            "dw.dws_trade_stats_1d@primary",
        ]
        
        for table_qn in critical_tables:
            try:
                entity = atlas.get_entity_by_qualified_name("hive_table", table_qn)
                guid = entity.get("entity", {}).get("guid")
                
                if guid:
                    audits = atlas.get_audit(guid, 5)
                    for audit in audits:
                        action = audit.get("action", "")
                        timestamp = audit.get("timestamp", 0)
                        
                        # 检查是否在最近 30 分钟内
                        from time import time
                        if (time() * 1000 - timestamp) <= 30 * 60 * 1000:
                            changes.append({
                                "entity": table_qn,
                                "action": action,
                                "user": audit.get("user", ""),
                                "timestamp": timestamp,
                                "detail": audit.get("detail", ""),
                            })
            except Exception as e:
                logger.warning(f"Failed to check {table_qn}: {e}")
        
        if changes:
            logger.warning(f"⚠️ 检测到 {len(changes)} 个元数据变更:")
            for c in changes:
                logger.warning(f"  {c['action']} on {c['entity']} by {c['user']}")
        else:
            logger.info("✅ 最近 30 分钟无元数据变更")
        
        return changes
    
    @task(task_id="detect_lineage_changes")
    def detect_lineage_changes():
        """检测血缘变更 (新增/断裂)"""
        atlas = AtlasClient(ATLAS_URL, ATLAS_USER, ATLAS_PASSWORD)
        
        # 检查关键链路是否完整
        critical_paths = [
            {
                "name": "订单全链路",
                "path": [
                    "ecommerce.order_info@mysql_prod",
                    "dw.ods_order_info@primary",
                    "dw.dwd_payment_detail@primary",
                    "dw.dws_trade_stats_1d@primary",
                ]
            },
            {
                "name": "实时链路",
                "path": [
                    "ecommerce.order_info@mysql_prod",
                    "ods_order_info@kafka_prod",
                    "dwd_order_detail@kafka_prod",
                    "ads.ads_trade_stats@clickhouse_prod",
                ]
            }
        ]
        
        broken_paths = []
        for path_def in critical_paths:
            logger.info(f"检查关键链路: {path_def['name']}")
            
            for i in range(len(path_def["path"]) - 1):
                source = path_def["path"][i]
                target = path_def["path"][i + 1]
                
                # 验证 source → target 的血缘是否存在
                try:
                    entity = atlas.get_entity_by_qualified_name("DataSet", source)
                    guid = entity.get("entity", {}).get("guid")
                    
                    if guid:
                        lineage = atlas.get_lineage(guid, "OUTPUT", 1)
                        downstream = set()
                        for rel in lineage.get("relations", []):
                            to_guid = rel.get("toEntityId")
                            to_entity = lineage.get("guidEntityMap", {}).get(to_guid, {})
                            downstream.add(
                                to_entity.get("attributes", {}).get("qualifiedName", ""))
                        
                        if target not in downstream:
                            broken_paths.append({
                                "path_name": path_def["name"],
                                "from": source,
                                "to": target,
                                "issue": "lineage_missing"
                            })
                except Exception:
                    broken_paths.append({
                        "path_name": path_def["name"],
                        "from": source,
                        "to": target,
                        "issue": "entity_not_found"
                    })
        
        if broken_paths:
            logger.warning(f"⚠️ 发现 {len(broken_paths)} 条断裂血缘:")
            for bp in broken_paths:
                logger.warning(f"  {bp['path_name']}: {bp['from']} ✗→ {bp['to']} ({bp['issue']})")
        else:
            logger.info("✅ 所有关键血缘链路完整")
        
        return broken_paths
    
    @task(task_id="send_change_alerts")
    def send_change_alerts(schema_changes: list, lineage_changes: list):
        """发送变更告警"""
        total_alerts = len(schema_changes) + len(lineage_changes)
        
        if total_alerts == 0:
            logger.info("无告警需要发送")
            return
        
        alert = {
            "title": f"🔔 数据治理变更告警 ({total_alerts} 项)",
            "schema_changes": schema_changes[:10],
            "lineage_breaks": lineage_changes[:10],
            "timestamp": datetime.now().isoformat(),
        }
        
        logger.warning(f"发送告警: {json.dumps(alert, indent=2, ensure_ascii=False)}")
        
        # 实际发送: 企微/飞书/邮件
        # requests.post(Variable.get("alert_webhook"), json=alert)
    
    schema = detect_schema_changes()
    lineage = detect_lineage_changes()
    send_change_alerts(schema, lineage)


# ============================================================
# 4. DAG: 数据资产盘点
# ============================================================

with DAG(
    dag_id="atlas_asset_inventory",
    default_args={
        "owner": "data-governance",
        "retries": 1,
    },
    description="数据资产盘点 - 每周统计资产分布、使用情况",
    schedule_interval="0 10 * * 1",            # 每周一 10:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["atlas", "inventory", "weekly"],
) as inventory_dag:
    
    @task(task_id="inventory_by_layer")
    def inventory_by_layer():
        """按数仓分层盘点"""
        atlas = AtlasClient(ATLAS_URL, ATLAS_USER, ATLAS_PASSWORD)
        
        layers = {
            "ODS": "ods_",
            "DWD": "dwd_",
            "DWS": "dws_",
            "ADS": "ads_",
            "DIM": "dim_",
        }
        
        inventory = {}
        for layer, prefix in layers.items():
            result = atlas.search(f"{prefix}*", type_name="hive_table", limit=100)
            entities = result.get("entities", [])
            
            classified = sum(1 for e in entities if e.get("classificationNames"))
            with_owner = sum(1 for e in entities
                          if e.get("attributes", {}).get("owner"))
            with_desc = sum(1 for e in entities
                         if e.get("attributes", {}).get("description"))
            
            inventory[layer] = {
                "table_count": len(entities),
                "classified_count": classified,
                "with_owner": with_owner,
                "with_description": with_desc,
            }
        
        logger.info("=" * 60)
        logger.info("📦 数据资产盘点 - 按数仓分层")
        logger.info("=" * 60)
        logger.info(f"{'层级':<6} {'表数':<6} {'有分类':<8} {'有owner':<8} {'有描述':<8}")
        logger.info("-" * 60)
        for layer, stats in inventory.items():
            logger.info(
                f"{layer:<6} {stats['table_count']:<6} "
                f"{stats['classified_count']:<8} "
                f"{stats['with_owner']:<8} "
                f"{stats['with_description']:<8}"
            )
        
        return inventory
    
    @task(task_id="inventory_by_domain")
    def inventory_by_domain():
        """按业务域盘点"""
        atlas = AtlasClient(ATLAS_URL, ATLAS_USER, ATLAS_PASSWORD)
        
        domains = {
            "交易": ["order", "payment", "trade"],
            "用户": ["user", "member", "account"],
            "商品": ["product", "item", "category"],
            "营销": ["promotion", "coupon", "campaign"],
            "流量": ["behavior", "pv", "click", "session"],
        }
        
        domain_stats = {}
        for domain, keywords in domains.items():
            total = 0
            for kw in keywords:
                result = atlas.search(kw, type_name="hive_table", limit=50)
                total += len(result.get("entities", []))
            domain_stats[domain] = total
        
        logger.info("\n📊 数据资产盘点 - 按业务域")
        for domain, count in domain_stats.items():
            logger.info(f"  {domain}: {count} 张表")
        
        return domain_stats
    
    @task(task_id="inventory_summary")
    def inventory_summary(by_layer: dict, by_domain: dict, **kwargs):
        """生成盘点汇总"""
        ds = kwargs["ds"]
        
        total_tables = sum(stats["table_count"] for stats in by_layer.values())
        total_classified = sum(stats["classified_count"] for stats in by_layer.values())
        
        summary = {
            "date": ds,
            "total_assets": total_tables,
            "classification_rate": round(
                total_classified / total_tables * 100, 1) if total_tables else 0,
            "by_layer": by_layer,
            "by_domain": by_domain,
        }
        
        logger.info(f"\n📋 资产盘点汇总: 共 {total_tables} 张表, "
                    f"分类覆盖率 {summary['classification_rate']}%")
        
        return summary
    
    layer = inventory_by_layer()
    domain = inventory_by_domain()
    inventory_summary(layer, domain)
