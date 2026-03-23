"""
==========================================================================
  Airflow 大数据 ETL Pipeline DAG - 完整实战
  
  📌 功能:
  1. 数据采集层: MySQL CDC / 日志文件 / API 数据拉取
  2. 数据清洗层: Spark 数据清洗 & 质量校验
  3. 数据加工层: Hive 分层建模 (ODS → DWD → DWS → ADS)
  4. 数据服务层: ClickHouse 聚合查询 / 报表导出
  5. 监控告警: 数据质量 / SLA / 失败重试
  
  📌 调度策略:
  - 每天凌晨 2:00 触发 T+1 离线批处理
  - 支持数据回刷 (backfill)
  - 任务依赖拓扑: 采集 → 清洗 → ODS → DWD → DWS → ADS → 导出
  
  📌 技术栈:
  - Apache Airflow 2.7+
  - SparkSubmitOperator / BashOperator / PythonOperator
  - HiveOperator / ClickHouseOperator (自定义)
  - Sensor (文件/分区/外部任务)
  - XCom / Variable / Connection
  - SLA / 告警回调 / 重试策略
  
  Author: BigData Demo
==========================================================================
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowSkipException

import json
import logging

logger = logging.getLogger(__name__)

# ============================================================
# 1. 全局配置 & 默认参数
# ============================================================

# 从 Airflow Variable 获取配置（支持热更新）
SPARK_HOME = Variable.get("spark_home", default_var="/opt/spark")
HADOOP_HOME = Variable.get("hadoop_home", default_var="/opt/hadoop")
HIVE_HOME = Variable.get("hive_home", default_var="/opt/hive")
HDFS_BASE_PATH = Variable.get("hdfs_base_path", default_var="/data/warehouse")
PROJECT_JAR = Variable.get("project_jar", default_var="/opt/app/bigdata-demo-1.0.0.jar")
ALERT_EMAILS = Variable.get("alert_emails", default_var="data-team@company.com").split(",")
CLICKHOUSE_CONN_ID = "clickhouse_default"
MYSQL_CONN_ID = "mysql_default"
HIVE_CONN_ID = "hive_default"

# DAG 默认参数
default_args = {
    "owner": "data-platform",
    "depends_on_past": False,                   # 不依赖上次运行
    "email": ALERT_EMAILS,
    "email_on_failure": True,                   # 失败发邮件
    "email_on_retry": False,
    "retries": 3,                               # 重试 3 次
    "retry_delay": timedelta(minutes=5),        # 重试间隔 5 分钟
    "retry_exponential_backoff": True,          # 指数退避
    "max_retry_delay": timedelta(minutes=30),   # 最大重试间隔
    "execution_timeout": timedelta(hours=2),    # 单任务超时 2 小时
    "sla": timedelta(hours=4),                  # SLA 4 小时
    "on_failure_callback": None,                # 下面定义
    "on_retry_callback": None,
    "pool": "default_pool",
    "priority_weight": 10,
    "weight_rule": "downstream",                # 权重规则: 下游越多越优先
}


# ============================================================
# 2. 告警回调函数
# ============================================================

def on_failure_callback(context: Dict[str, Any]):
    """任务失败告警回调 - 发送飞书/钉钉/企业微信通知"""
    task_instance = context["task_instance"]
    dag_id = context["dag"].dag_id
    task_id = task_instance.task_id
    execution_date = context["execution_date"].strftime("%Y-%m-%d %H:%M:%S")
    exception = context.get("exception", "Unknown")
    log_url = task_instance.log_url

    alert_msg = {
        "msg_type": "interactive",
        "card": {
            "header": {"title": {"tag": "plain_text", "content": f"🚨 ETL任务失败告警"}},
            "elements": [
                {"tag": "div", "text": {"tag": "plain_text",
                    "content": f"DAG: {dag_id}\n"
                               f"Task: {task_id}\n"
                               f"执行时间: {execution_date}\n"
                               f"异常: {str(exception)[:200]}"}},
                {"tag": "action", "actions": [
                    {"tag": "button", "text": {"tag": "plain_text", "content": "查看日志"},
                     "url": log_url, "type": "primary"}
                ]}
            ]
        }
    }
    
    logger.error(f"Task failed: {dag_id}.{task_id} at {execution_date}")
    logger.error(f"Exception: {exception}")
    # 实际使用时通过 webhook 发送
    # requests.post(Variable.get("feishu_webhook"), json=alert_msg)
    

def on_sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """SLA 超时告警"""
    task_names = [t.task_id for t in task_list]
    logger.warning(f"SLA Miss! Tasks: {task_names}")
    # 发送 SLA 超时告警通知


def on_success_callback(context: Dict[str, Any]):
    """DAG 整体成功回调"""
    dag_id = context["dag"].dag_id
    execution_date = context["execution_date"].strftime("%Y-%m-%d")
    logger.info(f"✅ DAG {dag_id} completed successfully for {execution_date}")


# 更新默认参数的回调
default_args["on_failure_callback"] = on_failure_callback


# ============================================================
# 3. 自定义 Operator: ClickHouse
# ============================================================

from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook

class ClickHouseOperator(BaseOperator):
    """
    自定义 ClickHouse Operator - 执行 SQL 语句
    
    支持:
    - 单条/多条 SQL 执行
    - 参数化查询
    - 结果返回 (XCom push)
    """
    
    template_fields = ("sql", "parameters")
    template_ext = (".sql",)
    ui_color = "#FFCC00"
    
    def __init__(
        self,
        sql: str,
        clickhouse_conn_id: str = "clickhouse_default",
        parameters: Dict = None,
        return_results: bool = False,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.sql = sql
        self.clickhouse_conn_id = clickhouse_conn_id
        self.parameters = parameters or {}
        self.return_results = return_results
    
    def execute(self, context):
        """执行 ClickHouse SQL"""
        connection = BaseHook.get_connection(self.clickhouse_conn_id)
        
        # 使用 clickhouse-driver 连接
        from clickhouse_driver import Client
        client = Client(
            host=connection.host,
            port=connection.port or 9000,
            user=connection.login or "default",
            password=connection.password or "",
            database=connection.schema or "default"
        )
        
        logger.info(f"Executing ClickHouse SQL: {self.sql[:200]}...")
        
        if self.return_results:
            result = client.execute(self.sql, self.parameters)
            context["ti"].xcom_push(key="query_result", value=result)
            return result
        else:
            client.execute(self.sql, self.parameters)
            logger.info("ClickHouse SQL executed successfully")


# ============================================================
# 4. 自定义 Sensor: Hive 分区感知
# ============================================================

from airflow.sensors.base import BaseSensorOperator

class HivePartitionSensor(BaseSensorOperator):
    """
    Hive 分区就绪 Sensor
    
    等待指定分区数据就绪后才继续下游任务
    支持: 分区存在性检查 / 数据量阈值 / 文件大小阈值
    """
    
    template_fields = ("table", "partition")
    ui_color = "#C5CAE9"
    
    def __init__(
        self,
        table: str,
        partition: str,
        min_records: int = 0,
        hive_conn_id: str = "hive_default",
        **kwargs
    ):
        super().__init__(**kwargs)
        self.table = table
        self.partition = partition
        self.min_records = min_records
        self.hive_conn_id = hive_conn_id
    
    def poke(self, context) -> bool:
        """检查 Hive 分区是否就绪"""
        from airflow.providers.apache.hive.hooks.hive import HiveMetastoreHook
        hook = HiveMetastoreHook(metastore_conn_id=self.hive_conn_id)
        
        # 检查分区是否存在
        partitions = hook.get_partitions(
            schema="dw",
            table_name=self.table,
            filter=self.partition
        )
        
        if not partitions:
            logger.info(f"Partition {self.partition} not found in {self.table}")
            return False
        
        # 检查数据量阈值
        if self.min_records > 0:
            from airflow.providers.apache.hive.hooks.hive import HiveServer2Hook
            hs2_hook = HiveServer2Hook(hiveserver2_conn_id=self.hive_conn_id)
            result = hs2_hook.get_records(
                f"SELECT COUNT(*) FROM dw.{self.table} WHERE {self.partition}"
            )
            count = result[0][0] if result else 0
            if count < self.min_records:
                logger.info(f"Partition has {count} records, need {self.min_records}")
                return False
        
        logger.info(f"Partition {self.partition} is ready in {self.table}")
        return True


# ============================================================
# 5. 数据质量检查函数
# ============================================================

def check_data_quality(table: str, dt: str, checks: List[Dict], **kwargs):
    """
    通用数据质量检查函数
    
    支持检查类型:
    - row_count: 行数阈值
    - null_check: 空值率
    - unique_check: 唯一性
    - range_check: 范围校验
    - freshness: 数据新鲜度
    - cross_table: 跨表一致性
    """
    from airflow.providers.apache.hive.hooks.hive import HiveServer2Hook
    hook = HiveServer2Hook(hiveserver2_conn_id=HIVE_CONN_ID)
    
    results = []
    all_passed = True
    
    for check in checks:
        check_type = check["type"]
        check_name = check.get("name", check_type)
        
        if check_type == "row_count":
            # 行数检查
            sql = f"SELECT COUNT(*) FROM {table} WHERE dt='{dt}'"
            result = hook.get_records(sql)
            actual = result[0][0]
            expected_min = check.get("min", 0)
            expected_max = check.get("max", float("inf"))
            passed = expected_min <= actual <= expected_max
            results.append({
                "check": check_name, "passed": passed,
                "actual": actual, "threshold": f"[{expected_min}, {expected_max}]"
            })
            
        elif check_type == "null_check":
            # 空值率检查
            column = check["column"]
            sql = f"""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END) as null_count
                FROM {table} WHERE dt='{dt}'
            """
            result = hook.get_records(sql)
            total, null_count = result[0]
            null_rate = null_count / total if total > 0 else 0
            max_null_rate = check.get("max_null_rate", 0.01)
            passed = null_rate <= max_null_rate
            results.append({
                "check": f"null_check({column})", "passed": passed,
                "actual": f"{null_rate:.4f}", "threshold": f"<= {max_null_rate}"
            })
            
        elif check_type == "unique_check":
            # 唯一性检查
            column = check["column"]
            sql = f"""
                SELECT COUNT(*) - COUNT(DISTINCT {column}) as dup_count
                FROM {table} WHERE dt='{dt}'
            """
            result = hook.get_records(sql)
            dup_count = result[0][0]
            passed = dup_count == 0
            results.append({
                "check": f"unique({column})", "passed": passed,
                "actual": dup_count, "threshold": "0"
            })
            
        elif check_type == "range_check":
            # 范围检查
            column = check["column"]
            min_val = check.get("min")
            max_val = check.get("max")
            conditions = []
            if min_val is not None:
                conditions.append(f"{column} < {min_val}")
            if max_val is not None:
                conditions.append(f"{column} > {max_val}")
            where = " OR ".join(conditions)
            sql = f"""
                SELECT COUNT(*) FROM {table} 
                WHERE dt='{dt}' AND ({where})
            """
            result = hook.get_records(sql)
            violation_count = result[0][0]
            passed = violation_count == 0
            results.append({
                "check": f"range({column})", "passed": passed,
                "actual": violation_count, "threshold": "0 violations"
            })
            
        elif check_type == "cross_table":
            # 跨表一致性检查
            source_table = check["source_table"]
            join_key = check["join_key"]
            sql = f"""
                SELECT COUNT(*) FROM {table} a
                LEFT JOIN {source_table} b ON a.{join_key} = b.{join_key}
                WHERE a.dt='{dt}' AND b.{join_key} IS NULL
            """
            result = hook.get_records(sql)
            orphan_count = result[0][0]
            max_orphan = check.get("max_orphan", 0)
            passed = orphan_count <= max_orphan
            results.append({
                "check": f"cross_table({source_table})", "passed": passed,
                "actual": orphan_count, "threshold": f"<= {max_orphan}"
            })
        
        if not passed:
            all_passed = False
    
    # 输出质量报告
    logger.info("=" * 60)
    logger.info(f"📊 Data Quality Report: {table} (dt={dt})")
    logger.info("=" * 60)
    for r in results:
        status = "✅ PASS" if r["passed"] else "❌ FAIL"
        logger.info(f"  {status} | {r['check']}: actual={r['actual']}, threshold={r['threshold']}")
    logger.info("=" * 60)
    
    # 将结果推送到 XCom
    kwargs["ti"].xcom_push(key="quality_results", value=results)
    
    if not all_passed:
        failed_checks = [r for r in results if not r["passed"]]
        raise ValueError(f"Data quality check failed: {failed_checks}")
    
    return results


def decide_branch(dt: str, **kwargs) -> str:
    """
    分支判断: 根据数据日期决定执行全量还是增量
    - 每月1号执行全量
    - 其他日期执行增量
    """
    from datetime import datetime
    date = datetime.strptime(dt, "%Y-%m-%d")
    if date.day == 1:
        logger.info(f"Monthly full load for {dt}")
        return "full_load_branch"
    else:
        logger.info(f"Incremental load for {dt}")
        return "incremental_load_branch"


# ============================================================
# 6. 主 DAG 定义
# ============================================================

with DAG(
    dag_id="bigdata_etl_pipeline",
    default_args=default_args,
    description="大数据 ETL Pipeline - 全链路数据处理",
    schedule_interval="0 2 * * *",              # 每天凌晨 2:00
    start_date=datetime(2024, 1, 1),
    end_date=None,
    catchup=False,                              # 不回填历史
    max_active_runs=1,                          # 同时只运行一个实例
    max_active_tasks=8,                         # 最大并行任务数
    dagrun_timeout=timedelta(hours=6),          # DAG 运行超时
    tags=["etl", "data-warehouse", "production"],
    on_success_callback=on_success_callback,
    sla_miss_callback=on_sla_miss_callback,
    doc_md="""
    ## 大数据 ETL Pipeline
    
    ### 数据流向
    ```
    数据源(MySQL/日志/API) → 采集(Sqoop/Flume/CDC) → ODS → DWD → DWS → ADS → 导出(ClickHouse/ES/报表)
    ```
    
    ### 运行时间
    - **调度**: 每日凌晨 02:00
    - **SLA**: 06:00 前完成
    - **预计耗时**: 2-3 小时
    
    ### 数据分区
    - 使用 `{{ ds }}` (execution_date) 作为分区字段 `dt`
    """,
) as dag:
    
    # 模板变量
    DS = "{{ ds }}"                    # 执行日期 YYYY-MM-DD
    DS_NODASH = "{{ ds_nodash }}"      # 执行日期 YYYYMMDD
    PREV_DS = "{{ prev_ds }}"          # 前一天
    NEXT_DS = "{{ next_ds }}"          # 后一天
    
    # ----------------------------------------------------------
    # Stage 0: 准备阶段
    # ----------------------------------------------------------
    
    start = EmptyOperator(task_id="start")
    
    # 检查上游依赖 (等待数据源系统就绪)
    check_source_ready = HttpSensor(
        task_id="check_source_ready",
        http_conn_id="source_api",
        endpoint="/api/health",
        method="GET",
        response_check=lambda response: response.json().get("status") == "ok",
        poke_interval=60,           # 每分钟检查一次
        timeout=1800,               # 最多等 30 分钟
        mode="reschedule",          # 释放 worker slot
        soft_fail=True,             # 超时不失败,跳过
    )
    
    # ----------------------------------------------------------
    # Stage 1: 数据采集层 (TaskGroup)
    # ----------------------------------------------------------
    
    with TaskGroup(group_id="data_ingestion", tooltip="数据采集层") as data_ingestion:
        
        # 1.1 MySQL 全表同步 (Sqoop)
        ingest_mysql_users = BashOperator(
            task_id="ingest_mysql_users",
            bash_command=f"""
                sqoop import \\
                    --connect jdbc:mysql://mysql:3306/ecommerce \\
                    --username root --password root123 \\
                    --table user_info \\
                    --where "updated_at >= '{{{{ ds }}}}'" \\
                    --target-dir {HDFS_BASE_PATH}/ods/user_info/dt={{{{ ds }}}} \\
                    --delete-target-dir \\
                    --fields-terminated-by '\\t' \\
                    --null-string '\\\\N' \\
                    --null-non-string '\\\\N' \\
                    --num-mappers 4 \\
                    --compress \\
                    --compression-codec snappy \\
                    --as-parquetfile
            """,
            pool="sqoop_pool",
            priority_weight=20,
        )
        
        ingest_mysql_orders = BashOperator(
            task_id="ingest_mysql_orders",
            bash_command=f"""
                sqoop import \\
                    --connect jdbc:mysql://mysql:3306/ecommerce \\
                    --username root --password root123 \\
                    --table order_info \\
                    --where "created_at >= '{{{{ ds }}}}' AND created_at < '{{{{ next_ds }}}}'" \\
                    --target-dir {HDFS_BASE_PATH}/ods/order_info/dt={{{{ ds }}}} \\
                    --delete-target-dir \\
                    --fields-terminated-by '\\t' \\
                    --num-mappers 8 \\
                    --split-by order_id \\
                    --as-parquetfile
            """,
            pool="sqoop_pool",
        )
        
        ingest_mysql_products = BashOperator(
            task_id="ingest_mysql_products",
            bash_command=f"""
                sqoop import \\
                    --connect jdbc:mysql://mysql:3306/ecommerce \\
                    --username root --password root123 \\
                    --table product_info \\
                    --target-dir {HDFS_BASE_PATH}/ods/product_info/dt={{{{ ds }}}} \\
                    --delete-target-dir \\
                    --num-mappers 2 \\
                    --as-parquetfile
            """,
            pool="sqoop_pool",
        )
        
        ingest_mysql_payments = BashOperator(
            task_id="ingest_mysql_payments",
            bash_command=f"""
                sqoop import \\
                    --connect jdbc:mysql://mysql:3306/ecommerce \\
                    --username root --password root123 \\
                    --table payment_info \\
                    --where "payment_time >= '{{{{ ds }}}}' AND payment_time < '{{{{ next_ds }}}}'" \\
                    --target-dir {HDFS_BASE_PATH}/ods/payment_info/dt={{{{ ds }}}} \\
                    --delete-target-dir \\
                    --num-mappers 4 \\
                    --as-parquetfile
            """,
            pool="sqoop_pool",
        )
        
        # 1.2 日志文件采集 (检查 HDFS 上日志文件就绪)
        wait_log_file = FileSensor(
            task_id="wait_log_file",
            filepath=f"{HDFS_BASE_PATH}/raw/user_behavior_log/dt={DS}/",
            fs_conn_id="hdfs_default",
            poke_interval=120,
            timeout=3600,
            mode="reschedule",
        )
        
        # 1.3 API 数据拉取
        @task(task_id="ingest_api_data", retries=5, retry_delay=timedelta(minutes=2))
        def ingest_api_data(ds: str):
            """从外部 API 拉取数据写入 HDFS"""
            import requests
            from hdfs import InsecureClient
            
            # 拉取 API 数据
            api_url = Variable.get("external_api_url", "http://api.example.com")
            response = requests.get(
                f"{api_url}/data/export",
                params={"date": ds, "format": "json"},
                timeout=300
            )
            response.raise_for_status()
            data = response.json()
            
            # 写入 HDFS
            hdfs_client = InsecureClient("http://namenode:9870", user="hadoop")
            hdfs_path = f"{HDFS_BASE_PATH}/ods/api_data/dt={ds}/data.json"
            hdfs_client.write(hdfs_path, json.dumps(data), overwrite=True)
            
            logger.info(f"Ingested {len(data)} records from API for {ds}")
            return len(data)
        
        api_data = ingest_api_data(ds=DS)
        
        # 采集任务可并行执行
        [ingest_mysql_users, ingest_mysql_orders, ingest_mysql_products,
         ingest_mysql_payments, wait_log_file, api_data]
    
    # ----------------------------------------------------------
    # Stage 2: ODS 层 - 原始数据加载
    # ----------------------------------------------------------
    
    with TaskGroup(group_id="ods_layer", tooltip="ODS层 - 原始数据") as ods_layer:
        
        # 2.1 加载用户表到 Hive ODS
        load_ods_users = HiveOperator(
            task_id="load_ods_users",
            hql=f"""
                -- 创建 ODS 用户表 (如不存在)
                CREATE TABLE IF NOT EXISTS ods.ods_user_info (
                    user_id        BIGINT     COMMENT '用户ID',
                    username       STRING     COMMENT '用户名',
                    email          STRING     COMMENT '邮箱',
                    phone          STRING     COMMENT '手机号',
                    gender         STRING     COMMENT '性别',
                    age            INT        COMMENT '年龄',
                    city           STRING     COMMENT '城市',
                    register_time  TIMESTAMP  COMMENT '注册时间',
                    updated_at     TIMESTAMP  COMMENT '更新时间'
                )
                COMMENT 'ODS-用户信息表'
                PARTITIONED BY (dt STRING)
                STORED AS PARQUET
                TBLPROPERTIES ('parquet.compression'='SNAPPY');
                
                -- 加载数据
                ALTER TABLE ods.ods_user_info DROP IF EXISTS PARTITION (dt='{{{{ ds }}}}');
                
                LOAD DATA INPATH '{HDFS_BASE_PATH}/ods/user_info/dt={{{{ ds }}}}'
                INTO TABLE ods.ods_user_info PARTITION (dt='{{{{ ds }}}}');
                
                -- 统计 & 校验
                SELECT 'ods_user_info', COUNT(*), dt 
                FROM ods.ods_user_info WHERE dt='{{{{ ds }}}}' GROUP BY dt;
            """,
            hive_cli_conn_id=HIVE_CONN_ID,
            mapred_queue="etl",
            mapred_job_name="ods_user_info_{{ ds }}",
        )
        
        # 2.2 加载订单表
        load_ods_orders = HiveOperator(
            task_id="load_ods_orders",
            hql=f"""
                CREATE TABLE IF NOT EXISTS ods.ods_order_info (
                    order_id       BIGINT     COMMENT '订单ID',
                    user_id        BIGINT     COMMENT '用户ID',
                    product_id     BIGINT     COMMENT '商品ID',
                    region_id      INT        COMMENT '地区ID',
                    order_amount   DECIMAL(16,2) COMMENT '订单金额',
                    order_status   STRING     COMMENT '订单状态',
                    created_at     TIMESTAMP  COMMENT '创建时间',
                    updated_at     TIMESTAMP  COMMENT '更新时间'
                )
                COMMENT 'ODS-订单信息表'
                PARTITIONED BY (dt STRING)
                STORED AS PARQUET;
                
                ALTER TABLE ods.ods_order_info DROP IF EXISTS PARTITION (dt='{{{{ ds }}}}');
                LOAD DATA INPATH '{HDFS_BASE_PATH}/ods/order_info/dt={{{{ ds }}}}'
                INTO TABLE ods.ods_order_info PARTITION (dt='{{{{ ds }}}}');
            """,
            hive_cli_conn_id=HIVE_CONN_ID,
        )
        
        # 2.3 加载商品表
        load_ods_products = HiveOperator(
            task_id="load_ods_products",
            hql=f"""
                CREATE TABLE IF NOT EXISTS ods.ods_product_info (
                    product_id     BIGINT     COMMENT '商品ID',
                    product_name   STRING     COMMENT '商品名称',
                    category_id    INT        COMMENT '类目ID',
                    category_name  STRING     COMMENT '类目名称',
                    price          DECIMAL(16,2) COMMENT '价格',
                    brand          STRING     COMMENT '品牌'
                )
                COMMENT 'ODS-商品信息表'
                PARTITIONED BY (dt STRING)
                STORED AS PARQUET;
                
                ALTER TABLE ods.ods_product_info DROP IF EXISTS PARTITION (dt='{{{{ ds }}}}');
                LOAD DATA INPATH '{HDFS_BASE_PATH}/ods/product_info/dt={{{{ ds }}}}'
                INTO TABLE ods.ods_product_info PARTITION (dt='{{{{ ds }}}}');
            """,
            hive_cli_conn_id=HIVE_CONN_ID,
        )
        
        # 2.4 加载支付表
        load_ods_payments = HiveOperator(
            task_id="load_ods_payments",
            hql=f"""
                CREATE TABLE IF NOT EXISTS ods.ods_payment_info (
                    payment_id     BIGINT     COMMENT '支付ID',
                    order_id       BIGINT     COMMENT '订单ID',
                    user_id        BIGINT     COMMENT '用户ID',
                    payment_amount DECIMAL(16,2) COMMENT '支付金额',
                    payment_type   STRING     COMMENT '支付方式',
                    payment_status STRING     COMMENT '支付状态',
                    payment_time   TIMESTAMP  COMMENT '支付时间'
                )
                COMMENT 'ODS-支付信息表'
                PARTITIONED BY (dt STRING)
                STORED AS PARQUET;
                
                ALTER TABLE ods.ods_payment_info DROP IF EXISTS PARTITION (dt='{{{{ ds }}}}');
                LOAD DATA INPATH '{HDFS_BASE_PATH}/ods/payment_info/dt={{{{ ds }}}}'
                INTO TABLE ods.ods_payment_info PARTITION (dt='{{{{ ds }}}}');
            """,
            hive_cli_conn_id=HIVE_CONN_ID,
        )
        
        # 2.5 加载行为日志 (Spark 清洗)
        load_ods_behavior_log = SparkSubmitOperator(
            task_id="load_ods_behavior_log",
            application=PROJECT_JAR,
            java_class="com.bigdata.spark.SparkHiveJavaApp",
            application_args=["--task", "load_behavior_log", "--dt", DS],
            conf={
                "spark.executor.memory": "4g",
                "spark.executor.cores": "2",
                "spark.executor.instances": "4",
                "spark.sql.shuffle.partitions": "200",
                "spark.sql.adaptive.enabled": "true",
            },
            conn_id="spark_default",
            name="ods_behavior_log_{{ ds }}",
            verbose=True,
        )
        
        # ODS 层任务可以并行
        [load_ods_users, load_ods_orders, load_ods_products,
         load_ods_payments, load_ods_behavior_log]
    
    # ----------------------------------------------------------
    # Stage 2.5: ODS 数据质量检查
    # ----------------------------------------------------------
    
    with TaskGroup(group_id="ods_quality_check", tooltip="ODS层数据质量检查") as ods_quality_check:
        
        check_ods_users = PythonOperator(
            task_id="check_ods_users",
            python_callable=check_data_quality,
            op_kwargs={
                "table": "ods.ods_user_info",
                "dt": DS,
                "checks": [
                    {"type": "row_count", "min": 100, "name": "user_count"},
                    {"type": "null_check", "column": "user_id", "max_null_rate": 0},
                    {"type": "unique_check", "column": "user_id"},
                    {"type": "null_check", "column": "email", "max_null_rate": 0.05},
                ]
            },
        )
        
        check_ods_orders = PythonOperator(
            task_id="check_ods_orders",
            python_callable=check_data_quality,
            op_kwargs={
                "table": "ods.ods_order_info",
                "dt": DS,
                "checks": [
                    {"type": "row_count", "min": 1000, "name": "order_count"},
                    {"type": "null_check", "column": "order_id", "max_null_rate": 0},
                    {"type": "unique_check", "column": "order_id"},
                    {"type": "range_check", "column": "order_amount", "min": 0, "max": 1000000},
                    {"type": "cross_table", "source_table": "ods.ods_user_info",
                     "join_key": "user_id", "max_orphan": 10},
                ]
            },
        )
        
        [check_ods_users, check_ods_orders]
    
    # ----------------------------------------------------------
    # Stage 3: DWD 层 - 数据明细层
    # ----------------------------------------------------------
    
    with TaskGroup(group_id="dwd_layer", tooltip="DWD层 - 数据明细") as dwd_layer:
        
        # 3.1 DWD 订单明细宽表 (Spark SQL)
        dwd_order_detail = SparkSubmitOperator(
            task_id="dwd_order_detail",
            application=PROJECT_JAR,
            java_class="com.bigdata.spark.SparkHiveJavaApp",
            application_args=["--task", "dwd_order_detail", "--dt", DS],
            conf={
                "spark.executor.memory": "8g",
                "spark.executor.cores": "4",
                "spark.executor.instances": "8",
                "spark.sql.shuffle.partitions": "400",
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
                "spark.dynamicAllocation.enabled": "true",
                "spark.dynamicAllocation.minExecutors": "2",
                "spark.dynamicAllocation.maxExecutors": "16",
            },
            conn_id="spark_default",
            name="dwd_order_detail_{{ ds }}",
        )
        
        # 3.2 DWD 用户行为明细
        dwd_user_behavior = HiveOperator(
            task_id="dwd_user_behavior",
            hql="""
                CREATE TABLE IF NOT EXISTS dwd.dwd_user_behavior (
                    user_id         BIGINT     COMMENT '用户ID',
                    item_id         BIGINT     COMMENT '商品ID',
                    behavior_type   STRING     COMMENT '行为类型(pv/cart/fav/buy)',
                    category_id     INT        COMMENT '类目ID',
                    category_name   STRING     COMMENT '类目名称',
                    brand           STRING     COMMENT '品牌',
                    event_time      TIMESTAMP  COMMENT '事件时间',
                    session_id      STRING     COMMENT '会话ID',
                    platform        STRING     COMMENT '平台(iOS/Android/Web)',
                    city            STRING     COMMENT '城市'
                )
                COMMENT 'DWD-用户行为明细表'
                PARTITIONED BY (dt STRING)
                STORED AS PARQUET
                TBLPROPERTIES ('parquet.compression'='SNAPPY');
                
                INSERT OVERWRITE TABLE dwd.dwd_user_behavior PARTITION (dt='{{ ds }}')
                SELECT
                    b.user_id,
                    b.item_id,
                    b.behavior_type,
                    p.category_id,
                    p.category_name,
                    p.brand,
                    b.event_time,
                    b.session_id,
                    b.platform,
                    u.city
                FROM ods.ods_behavior_log b
                LEFT JOIN ods.ods_product_info p ON b.item_id = p.product_id AND p.dt='{{ ds }}'
                LEFT JOIN ods.ods_user_info u ON b.user_id = u.user_id AND u.dt='{{ ds }}'
                WHERE b.dt='{{ ds }}'
                  AND b.user_id IS NOT NULL
                  AND b.event_time IS NOT NULL;
            """,
            hive_cli_conn_id=HIVE_CONN_ID,
        )
        
        # 3.3 DWD 支付成功明细
        dwd_payment_success = HiveOperator(
            task_id="dwd_payment_success",
            hql="""
                CREATE TABLE IF NOT EXISTS dwd.dwd_payment_detail (
                    payment_id      BIGINT,
                    order_id        BIGINT,
                    user_id         BIGINT,
                    product_id      BIGINT,
                    product_name    STRING,
                    category_name   STRING,
                    region_name     STRING,
                    payment_amount  DECIMAL(16,2),
                    payment_type    STRING,
                    payment_time    TIMESTAMP
                )
                COMMENT 'DWD-支付成功明细表'
                PARTITIONED BY (dt STRING)
                STORED AS PARQUET;
                
                INSERT OVERWRITE TABLE dwd.dwd_payment_detail PARTITION (dt='{{ ds }}')
                SELECT
                    pay.payment_id,
                    pay.order_id,
                    pay.user_id,
                    ord.product_id,
                    prd.product_name,
                    prd.category_name,
                    reg.region_name,
                    pay.payment_amount,
                    pay.payment_type,
                    pay.payment_time
                FROM ods.ods_payment_info pay
                JOIN ods.ods_order_info ord ON pay.order_id = ord.order_id AND ord.dt='{{ ds }}'
                LEFT JOIN ods.ods_product_info prd ON ord.product_id = prd.product_id AND prd.dt='{{ ds }}'
                LEFT JOIN dim.dim_region reg ON ord.region_id = reg.region_id
                WHERE pay.dt='{{ ds }}'
                  AND pay.payment_status = 'SUCCESS';
            """,
            hive_cli_conn_id=HIVE_CONN_ID,
        )
        
        [dwd_order_detail, dwd_user_behavior, dwd_payment_success]
    
    # ----------------------------------------------------------
    # Stage 4: DWS 层 - 数据汇总层
    # ----------------------------------------------------------
    
    with TaskGroup(group_id="dws_layer", tooltip="DWS层 - 数据汇总") as dws_layer:
        
        # 4.1 用户行为日汇总
        dws_user_behavior_daily = HiveOperator(
            task_id="dws_user_behavior_daily",
            hql="""
                CREATE TABLE IF NOT EXISTS dws.dws_user_behavior_1d (
                    user_id         BIGINT     COMMENT '用户ID',
                    city            STRING     COMMENT '城市',
                    pv_count        BIGINT     COMMENT '浏览次数',
                    cart_count      BIGINT     COMMENT '加购次数',
                    fav_count       BIGINT     COMMENT '收藏次数',
                    buy_count       BIGINT     COMMENT '购买次数',
                    session_count   BIGINT     COMMENT '会话数',
                    browse_duration BIGINT     COMMENT '浏览时长(秒)',
                    distinct_items  BIGINT     COMMENT '浏览商品去重数',
                    distinct_cats   BIGINT     COMMENT '浏览类目去重数'
                )
                COMMENT 'DWS-用户行为日汇总表'
                PARTITIONED BY (dt STRING)
                STORED AS PARQUET;
                
                INSERT OVERWRITE TABLE dws.dws_user_behavior_1d PARTITION (dt='{{ ds }}')
                SELECT
                    user_id,
                    city,
                    SUM(IF(behavior_type='pv', 1, 0)) as pv_count,
                    SUM(IF(behavior_type='cart', 1, 0)) as cart_count,
                    SUM(IF(behavior_type='fav', 1, 0)) as fav_count,
                    SUM(IF(behavior_type='buy', 1, 0)) as buy_count,
                    COUNT(DISTINCT session_id) as session_count,
                    0 as browse_duration,
                    COUNT(DISTINCT item_id) as distinct_items,
                    COUNT(DISTINCT category_id) as distinct_cats
                FROM dwd.dwd_user_behavior
                WHERE dt='{{ ds }}'
                GROUP BY user_id, city;
            """,
            hive_cli_conn_id=HIVE_CONN_ID,
        )
        
        # 4.2 交易日汇总
        dws_trade_daily = HiveOperator(
            task_id="dws_trade_daily",
            hql="""
                CREATE TABLE IF NOT EXISTS dws.dws_trade_stats_1d (
                    category_name   STRING     COMMENT '类目',
                    region_name     STRING     COMMENT '地区',
                    order_count     BIGINT     COMMENT '订单量',
                    payment_count   BIGINT     COMMENT '支付笔数',
                    total_amount    DECIMAL(20,2) COMMENT '总金额',
                    avg_amount      DECIMAL(16,2) COMMENT '平均金额',
                    user_count      BIGINT     COMMENT '付费用户数',
                    new_user_count  BIGINT     COMMENT '新用户数'
                )
                COMMENT 'DWS-交易日汇总表'
                PARTITIONED BY (dt STRING)
                STORED AS PARQUET;
                
                INSERT OVERWRITE TABLE dws.dws_trade_stats_1d PARTITION (dt='{{ ds }}')
                SELECT
                    category_name,
                    region_name,
                    COUNT(DISTINCT order_id) as order_count,
                    COUNT(DISTINCT payment_id) as payment_count,
                    SUM(payment_amount) as total_amount,
                    AVG(payment_amount) as avg_amount,
                    COUNT(DISTINCT user_id) as user_count,
                    0 as new_user_count
                FROM dwd.dwd_payment_detail
                WHERE dt='{{ ds }}'
                GROUP BY category_name, region_name;
            """,
            hive_cli_conn_id=HIVE_CONN_ID,
        )
        
        # 4.3 商品排名日汇总
        dws_product_ranking = HiveOperator(
            task_id="dws_product_ranking",
            hql="""
                CREATE TABLE IF NOT EXISTS dws.dws_product_ranking_1d (
                    product_id      BIGINT,
                    product_name    STRING,
                    category_name   STRING,
                    sale_count      BIGINT     COMMENT '销量',
                    sale_amount     DECIMAL(20,2) COMMENT '销售额',
                    pv_count        BIGINT     COMMENT '浏览量',
                    cart_rate       DECIMAL(8,4) COMMENT '加购率',
                    buy_rate        DECIMAL(8,4) COMMENT '转化率',
                    ranking         INT        COMMENT '销售额排名'
                )
                COMMENT 'DWS-商品排名日汇总'
                PARTITIONED BY (dt STRING)
                STORED AS PARQUET;
                
                INSERT OVERWRITE TABLE dws.dws_product_ranking_1d PARTITION (dt='{{ ds }}')
                SELECT
                    p.product_id,
                    p.product_name,
                    p.category_name,
                    COALESCE(s.sale_count, 0),
                    COALESCE(s.sale_amount, 0),
                    COALESCE(b.pv_count, 0),
                    COALESCE(b.cart_count / b.pv_count, 0) as cart_rate,
                    COALESCE(s.sale_count / b.pv_count, 0) as buy_rate,
                    ROW_NUMBER() OVER (ORDER BY COALESCE(s.sale_amount, 0) DESC) as ranking
                FROM ods.ods_product_info p
                LEFT JOIN (
                    SELECT product_id,
                           COUNT(*) as sale_count,
                           SUM(payment_amount) as sale_amount
                    FROM dwd.dwd_payment_detail WHERE dt='{{ ds }}'
                    GROUP BY product_id
                ) s ON p.product_id = s.product_id
                LEFT JOIN (
                    SELECT item_id,
                           SUM(IF(behavior_type='pv', 1, 0)) as pv_count,
                           SUM(IF(behavior_type='cart', 1, 0)) as cart_count
                    FROM dwd.dwd_user_behavior WHERE dt='{{ ds }}'
                    GROUP BY item_id
                ) b ON p.product_id = b.item_id
                WHERE p.dt='{{ ds }}';
            """,
            hive_cli_conn_id=HIVE_CONN_ID,
        )
        
        [dws_user_behavior_daily, dws_trade_daily, dws_product_ranking]
    
    # ----------------------------------------------------------
    # Stage 5: ADS 层 - 数据应用层 → ClickHouse
    # ----------------------------------------------------------
    
    with TaskGroup(group_id="ads_layer", tooltip="ADS层 - 数据导出") as ads_layer:
        
        # 5.1 导出到 ClickHouse - 交易汇总
        export_trade_to_ck = ClickHouseOperator(
            task_id="export_trade_to_ck",
            sql="""
                INSERT INTO ads.ads_trade_stats
                SELECT
                    '{{ ds }}' as dt,
                    category_name,
                    region_name,
                    order_count,
                    payment_count,
                    total_amount,
                    avg_amount,
                    user_count,
                    now() as etl_time
                FROM (
                    -- 从 Hive 外部表读取 DWS 数据
                    SELECT * FROM hive_dws_trade_stats_1d 
                    WHERE dt = '{{ ds }}'
                )
            """,
            clickhouse_conn_id=CLICKHOUSE_CONN_ID,
        )
        
        # 5.2 导出到 ClickHouse - 用户行为
        export_behavior_to_ck = ClickHouseOperator(
            task_id="export_behavior_to_ck",
            sql="""
                INSERT INTO ads.ads_user_behavior
                SELECT
                    '{{ ds }}' as dt,
                    user_id,
                    city,
                    pv_count,
                    cart_count,
                    fav_count,
                    buy_count,
                    session_count,
                    distinct_items,
                    now() as etl_time
                FROM hive_dws_user_behavior_1d
                WHERE dt = '{{ ds }}'
            """,
            clickhouse_conn_id=CLICKHOUSE_CONN_ID,
        )
        
        # 5.3 导出报表数据 (CSV → OSS/S3)
        @task(task_id="export_daily_report")
        def export_daily_report(ds: str):
            """导出日报到对象存储"""
            from airflow.providers.apache.hive.hooks.hive import HiveServer2Hook
            hook = HiveServer2Hook(hiveserver2_conn_id=HIVE_CONN_ID)
            
            # 查询日报数据
            report_data = hook.get_pandas_df(f"""
                SELECT 
                    dt,
                    SUM(order_count) as total_orders,
                    SUM(total_amount) as total_gmv,
                    SUM(user_count) as total_users,
                    SUM(total_amount) / SUM(order_count) as avg_order_amount
                FROM dws.dws_trade_stats_1d
                WHERE dt='{ds}'
                GROUP BY dt
            """)
            
            # 写入 CSV
            csv_path = f"/tmp/daily_report_{ds}.csv"
            report_data.to_csv(csv_path, index=False, encoding="utf-8")
            
            # 上传到 HDFS/OSS
            logger.info(f"Daily report exported: {csv_path}")
            return csv_path
        
        daily_report = export_daily_report(ds=DS)
        
        [export_trade_to_ck, export_behavior_to_ck, daily_report]
    
    # ----------------------------------------------------------
    # Stage 6: 收尾
    # ----------------------------------------------------------
    
    # DWS 数据质量检查
    check_dws_quality = PythonOperator(
        task_id="check_dws_quality",
        python_callable=check_data_quality,
        op_kwargs={
            "table": "dws.dws_trade_stats_1d",
            "dt": DS,
            "checks": [
                {"type": "row_count", "min": 10, "name": "trade_stats_count"},
                {"type": "range_check", "column": "total_amount", "min": 0},
            ]
        },
    )
    
    # 标记分区完成
    mark_partition_done = BashOperator(
        task_id="mark_partition_done",
        bash_command=f"""
            hdfs dfs -touchz {HDFS_BASE_PATH}/dws/_SUCCESS/dt={{{{ ds }}}}
            echo "Partition {{{{ ds }}}} marked as done"
        """,
    )
    
    # 触发下游 DAG (如数据大屏刷新)
    trigger_dashboard_refresh = TriggerDagRunOperator(
        task_id="trigger_dashboard_refresh",
        trigger_dag_id="dashboard_refresh_dag",
        conf={"dt": DS},
        wait_for_completion=False,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )
    
    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    
    # ----------------------------------------------------------
    # 任务依赖关系
    # ----------------------------------------------------------
    
    start >> check_source_ready >> data_ingestion >> ods_layer >> ods_quality_check
    ods_quality_check >> dwd_layer >> dws_layer >> check_dws_quality
    check_dws_quality >> ads_layer >> mark_partition_done >> trigger_dashboard_refresh >> end


# ============================================================
# 7. 子 DAG: 数据回刷 (Backfill)
# ============================================================

with DAG(
    dag_id="bigdata_etl_backfill",
    default_args=default_args,
    description="数据回刷 DAG - 支持指定日期范围重跑",
    schedule_interval=None,                    # 手动触发
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=3,
    tags=["etl", "backfill", "manual"],
    params={
        "start_date": "2024-01-01",
        "end_date": "2024-01-01",
        "layers": "ods,dwd,dws,ads",           # 要回刷的层
        "tables": "",                           # 特定表, 空=全部
    },
    doc_md="""
    ## 数据回刷 DAG
    
    ### 参数说明
    - `start_date`: 回刷起始日期
    - `end_date`: 回刷结束日期
    - `layers`: 要回刷的数仓层次 (逗号分隔)
    - `tables`: 要回刷的特定表 (逗号分隔, 空=全部)
    
    ### 使用方式
    通过 Airflow UI 手动触发, 传入 conf 参数
    """,
) as backfill_dag:
    
    @task(task_id="parse_params")
    def parse_params(**kwargs):
        """解析回刷参数"""
        params = kwargs["params"]
        start_date = params.get("start_date")
        end_date = params.get("end_date")
        layers = params.get("layers", "ods,dwd,dws,ads").split(",")
        tables = [t for t in params.get("tables", "").split(",") if t]
        
        # 生成日期列表
        from datetime import datetime, timedelta
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        dates = []
        current = start
        while current <= end:
            dates.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)
        
        logger.info(f"Backfill: dates={dates}, layers={layers}, tables={tables}")
        return {"dates": dates, "layers": layers, "tables": tables}
    
    @task(task_id="execute_backfill")
    def execute_backfill(config: dict):
        """执行回刷"""
        dates = config["dates"]
        layers = config["layers"]
        
        for dt in dates:
            for layer in layers:
                logger.info(f"Backfilling {layer} for {dt}...")
                # 触发对应层的重跑
                # 实际实现中可以使用 TriggerDagRunOperator 或直接调用 Spark/Hive
        
        return f"Backfill completed: {len(dates)} dates, {len(layers)} layers"
    
    params = parse_params()
    execute_backfill(params)


# ============================================================
# 8. 子 DAG: 数据质量监控 DAG
# ============================================================

with DAG(
    dag_id="data_quality_monitor",
    default_args={**default_args, "retries": 1},
    description="数据质量监控 - 定时扫描数据异常",
    schedule_interval="0 8 * * *",             # 每天 8:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["quality", "monitor"],
) as quality_dag:
    
    @task(task_id="check_data_freshness")
    def check_data_freshness(ds: str):
        """检查数据新鲜度 - 各层最新分区"""
        from airflow.providers.apache.hive.hooks.hive import HiveServer2Hook
        hook = HiveServer2Hook(hiveserver2_conn_id=HIVE_CONN_ID)
        
        tables_to_check = [
            "ods.ods_order_info",
            "dwd.dwd_payment_detail",
            "dws.dws_trade_stats_1d",
        ]
        
        alerts = []
        for table in tables_to_check:
            result = hook.get_records(f"SHOW PARTITIONS {table}")
            if result:
                latest_partition = sorted(result)[-1][0]
                latest_dt = latest_partition.split("=")[1]
                
                from datetime import datetime, timedelta
                latest_date = datetime.strptime(latest_dt, "%Y-%m-%d")
                expected_date = datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=1)
                
                if latest_date < expected_date:
                    delay_days = (expected_date - latest_date).days
                    alerts.append({
                        "table": table,
                        "latest_dt": latest_dt,
                        "expected_dt": expected_date.strftime("%Y-%m-%d"),
                        "delay_days": delay_days
                    })
                    logger.warning(f"⚠️ {table} is {delay_days} days behind!")
        
        if alerts:
            logger.error(f"Data freshness alerts: {json.dumps(alerts, indent=2)}")
        else:
            logger.info("✅ All tables are up to date")
        
        return alerts
    
    @task(task_id="check_data_volume_anomaly")
    def check_data_volume_anomaly(ds: str):
        """数据量异常检测 - 与历史均值对比"""
        from airflow.providers.apache.hive.hooks.hive import HiveServer2Hook
        hook = HiveServer2Hook(hiveserver2_conn_id=HIVE_CONN_ID)
        
        tables = [
            ("ods.ods_order_info", 0.5, 2.0),    # (表名, 最低比例, 最高比例)
            ("dwd.dwd_payment_detail", 0.5, 2.0),
        ]
        
        alerts = []
        for table, min_ratio, max_ratio in tables:
            # 获取最近 7 天平均数据量
            result = hook.get_records(f"""
                SELECT dt, COUNT(*) as cnt
                FROM {table}
                WHERE dt >= date_sub('{ds}', 7) AND dt <= '{ds}'
                GROUP BY dt
                ORDER BY dt
            """)
            
            if len(result) >= 2:
                counts = [r[1] for r in result]
                today_count = counts[-1]
                avg_count = sum(counts[:-1]) / len(counts[:-1])
                ratio = today_count / avg_count if avg_count > 0 else 0
                
                if ratio < min_ratio or ratio > max_ratio:
                    alerts.append({
                        "table": table,
                        "today_count": today_count,
                        "avg_count": round(avg_count, 0),
                        "ratio": round(ratio, 2),
                    })
                    logger.warning(f"⚠️ {table}: ratio={ratio:.2f} (today={today_count}, avg={avg_count:.0f})")
        
        return alerts
    
    @task(task_id="generate_quality_report")
    def generate_quality_report(freshness_alerts, volume_alerts, ds: str):
        """生成数据质量日报"""
        report = {
            "date": ds,
            "freshness_alerts": freshness_alerts,
            "volume_alerts": volume_alerts,
            "total_alerts": len(freshness_alerts) + len(volume_alerts),
            "status": "HEALTHY" if not freshness_alerts and not volume_alerts else "ATTENTION"
        }
        
        logger.info(f"📊 Data Quality Report for {ds}:")
        logger.info(json.dumps(report, indent=2, ensure_ascii=False))
        
        # 发送到监控系统
        return report
    
    freshness = check_data_freshness(ds="{{ ds }}")
    volume = check_data_volume_anomaly(ds="{{ ds }}")
    generate_quality_report(freshness, volume, ds="{{ ds }}")
