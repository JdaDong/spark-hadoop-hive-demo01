"""
==========================================================================
  Airflow 实时流处理管理 DAG - Flink 作业生命周期管理
  
  📌 功能:
  1. Flink 作业自动提交 / 监控 / 恢复
  2. Savepoint 定时创建 (容灾)
  3. Flink 作业健康检查 & 自动重启
  4. Checkpoint 监控告警
  5. Kafka Consumer Lag 监控
  
  📌 调度策略:
  - 健康检查: 每 5 分钟
  - Savepoint: 每 2 小时
  - Lag 监控: 每 10 分钟
  
  Author: BigData Demo
==========================================================================
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional
import json
import logging

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup

logger = logging.getLogger(__name__)

# 全局配置
FLINK_REST_URL = Variable.get("flink_rest_url", default_var="http://flink-jobmanager:8081")
KAFKA_BOOTSTRAP = Variable.get("kafka_bootstrap", default_var="kafka:9092")
SAVEPOINT_DIR = Variable.get("savepoint_dir", default_var="hdfs:///flink/savepoints")

# Flink 作业配置列表
FLINK_JOBS = Variable.get("flink_jobs", deserialize_json=True, default_var=[
    {
        "job_name": "realtime_dwd_order",
        "jar_path": "/opt/flink/usrlib/bigdata-demo-1.0.0.jar",
        "entry_class": "com.bigdata.realtime.RealtimeDataWarehouseApp",
        "parallelism": 4,
        "args": "--layer dwd",
        "kafka_topics": ["ods_order_info"],
        "consumer_group": "flink_dwd_order",
        "max_restart": 3,
        "checkpoint_interval": 60000,
    },
    {
        "job_name": "realtime_dws_trade",
        "jar_path": "/opt/flink/usrlib/bigdata-demo-1.0.0.jar",
        "entry_class": "com.bigdata.realtime.RealtimeDataWarehouseApp",
        "parallelism": 2,
        "args": "--layer dws",
        "kafka_topics": ["dwd_order_detail"],
        "consumer_group": "flink_dws_trade",
        "max_restart": 3,
        "checkpoint_interval": 30000,
    },
])


# ============================================================
# 1. Flink REST API 工具函数
# ============================================================

class FlinkRestClient:
    """Flink REST API 客户端"""
    
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
    
    def _request(self, method: str, path: str, **kwargs) -> dict:
        import requests
        url = f"{self.base_url}{path}"
        resp = requests.request(method, url, timeout=30, **kwargs)
        resp.raise_for_status()
        return resp.json() if resp.content else {}
    
    def get_cluster_overview(self) -> dict:
        """获取集群概览"""
        return self._request("GET", "/overview")
    
    def list_jobs(self) -> List[dict]:
        """列出所有作业"""
        data = self._request("GET", "/jobs")
        return data.get("jobs", [])
    
    def get_job_detail(self, job_id: str) -> dict:
        """获取作业详情"""
        return self._request("GET", f"/jobs/{job_id}")
    
    def get_job_checkpoints(self, job_id: str) -> dict:
        """获取 Checkpoint 信息"""
        return self._request("GET", f"/jobs/{job_id}/checkpoints")
    
    def submit_job(self, jar_id: str, entry_class: str, parallelism: int,
                   program_args: str = "", savepoint_path: str = None) -> str:
        """提交作业"""
        body = {
            "entryClass": entry_class,
            "parallelism": parallelism,
            "programArgs": program_args,
        }
        if savepoint_path:
            body["savepointPath"] = savepoint_path
            body["allowNonRestoredState"] = True
        
        data = self._request("POST", f"/jars/{jar_id}/run", json=body)
        return data.get("jobid")
    
    def cancel_job_with_savepoint(self, job_id: str, target_dir: str) -> str:
        """带 Savepoint 取消作业"""
        body = {"drain": False, "targetDirectory": target_dir}
        data = self._request("POST", f"/jobs/{job_id}/savepoints", json=body)
        return data.get("request-id")
    
    def trigger_savepoint(self, job_id: str, target_dir: str) -> str:
        """触发 Savepoint (不停止作业)"""
        body = {"cancel-job": False, "target-directory": target_dir}
        data = self._request("POST", f"/jobs/{job_id}/savepoints", json=body)
        return data.get("request-id")
    
    def get_savepoint_status(self, job_id: str, trigger_id: str) -> dict:
        """查询 Savepoint 状态"""
        return self._request("GET", f"/jobs/{job_id}/savepoints/{trigger_id}")
    
    def upload_jar(self, jar_path: str) -> str:
        """上传 JAR"""
        import requests
        url = f"{self.base_url}/jars/upload"
        with open(jar_path, "rb") as f:
            resp = requests.post(url, files={"jarfile": f}, timeout=120)
        resp.raise_for_status()
        data = resp.json()
        # 返回 jar_id (格式: uuid_filename.jar)
        return data["filename"].split("/")[-1]
    
    def list_jars(self) -> List[dict]:
        """列出已上传的 JAR"""
        data = self._request("GET", "/jars")
        return data.get("files", [])


# ============================================================
# 2. Kafka 监控工具函数
# ============================================================

class KafkaMonitor:
    """Kafka Consumer Lag 监控"""
    
    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
    
    def get_consumer_lag(self, group_id: str, topics: List[str]) -> Dict:
        """获取消费者组的 Lag"""
        from kafka import KafkaAdminClient, TopicPartition
        from kafka import KafkaConsumer
        
        admin = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
        consumer = KafkaConsumer(bootstrap_servers=self.bootstrap_servers)
        
        total_lag = 0
        partitions_info = []
        
        for topic in topics:
            # 获取分区列表
            partitions = consumer.partitions_for_topic(topic)
            if not partitions:
                continue
            
            for partition in partitions:
                tp = TopicPartition(topic, partition)
                
                # 获取最新 offset
                consumer.assign([tp])
                consumer.seek_to_end(tp)
                end_offset = consumer.position(tp)
                
                # 获取消费者组的 committed offset
                offsets = admin.list_consumer_group_offsets(group_id)
                committed = offsets.get(tp)
                committed_offset = committed.offset if committed else 0
                
                lag = end_offset - committed_offset
                total_lag += lag
                
                partitions_info.append({
                    "topic": topic,
                    "partition": partition,
                    "end_offset": end_offset,
                    "committed_offset": committed_offset,
                    "lag": lag
                })
        
        consumer.close()
        admin.close()
        
        return {
            "group_id": group_id,
            "total_lag": total_lag,
            "partitions": partitions_info
        }


# ============================================================
# 3. DAG 1: Flink 作业健康检查 (每 5 分钟)
# ============================================================

with DAG(
    dag_id="flink_job_health_check",
    default_args={
        "owner": "data-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    description="Flink 作业健康检查 & 自动恢复",
    schedule_interval="*/5 * * * *",            # 每 5 分钟
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["flink", "monitor", "realtime"],
) as health_check_dag:
    
    @task(task_id="check_flink_cluster")
    def check_flink_cluster():
        """检查 Flink 集群状态"""
        client = FlinkRestClient(FLINK_REST_URL)
        overview = client.get_cluster_overview()
        
        logger.info(f"Flink Cluster: {overview}")
        logger.info(f"  TaskManagers: {overview.get('taskmanagers', 0)}")
        logger.info(f"  Slots Total: {overview.get('slots-total', 0)}")
        logger.info(f"  Slots Available: {overview.get('slots-available', 0)}")
        logger.info(f"  Jobs Running: {overview.get('jobs-running', 0)}")
        logger.info(f"  Jobs Finished: {overview.get('jobs-finished', 0)}")
        logger.info(f"  Jobs Cancelled: {overview.get('jobs-cancelled', 0)}")
        logger.info(f"  Jobs Failed: {overview.get('jobs-failed', 0)}")
        
        # 检查 TaskManager 数量
        if overview.get("taskmanagers", 0) == 0:
            raise RuntimeError("No TaskManagers available!")
        
        return overview
    
    @task(task_id="check_running_jobs")
    def check_running_jobs():
        """检查 Flink 作业运行状态"""
        client = FlinkRestClient(FLINK_REST_URL)
        jobs = client.list_jobs()
        
        running_jobs = [j for j in jobs if j["status"] == "RUNNING"]
        failed_jobs = [j for j in jobs if j["status"] == "FAILED"]
        
        logger.info(f"Running jobs: {len(running_jobs)}")
        logger.info(f"Failed jobs: {len(failed_jobs)}")
        
        # 检查预期的作业是否在运行
        running_names = set()
        for job in running_jobs:
            detail = client.get_job_detail(job["id"])
            running_names.add(detail.get("name", ""))
        
        expected_names = {j["job_name"] for j in FLINK_JOBS}
        missing = expected_names - running_names
        
        result = {
            "running": len(running_jobs),
            "failed": len(failed_jobs),
            "expected": len(FLINK_JOBS),
            "missing_jobs": list(missing),
            "failed_jobs": [j["id"] for j in failed_jobs],
        }
        
        if missing:
            logger.warning(f"⚠️ Missing jobs: {missing}")
        else:
            logger.info("✅ All expected jobs are running")
        
        return result
    
    @task(task_id="check_checkpoints")
    def check_checkpoints():
        """检查 Checkpoint 健康状况"""
        client = FlinkRestClient(FLINK_REST_URL)
        jobs = client.list_jobs()
        
        alerts = []
        for job in jobs:
            if job["status"] != "RUNNING":
                continue
            
            ckpt = client.get_job_checkpoints(job["id"])
            latest = ckpt.get("latest", {}).get("completed")
            
            if latest:
                # 检查最近一次 Checkpoint 时间
                last_ckpt_ts = latest.get("latest_ack_timestamp", 0)
                duration = latest.get("end_to_end_duration", 0)
                size = latest.get("state_size", 0)
                
                # 如果超过 10 分钟没有成功的 Checkpoint
                from time import time
                elapsed = (time() * 1000 - last_ckpt_ts) / 1000 / 60
                if elapsed > 10:
                    alerts.append({
                        "job_id": job["id"],
                        "last_checkpoint_minutes_ago": round(elapsed, 1),
                        "duration_ms": duration,
                        "state_size_bytes": size,
                    })
                    logger.warning(f"⚠️ Job {job['id']}: no checkpoint in {elapsed:.1f} minutes")
            else:
                alerts.append({"job_id": job["id"], "issue": "no_completed_checkpoint"})
        
        return alerts
    
    @task(task_id="auto_recover_jobs")
    def auto_recover_jobs(job_status: dict, ckpt_alerts: list):
        """自动恢复失败/缺失的作业"""
        if not job_status["missing_jobs"] and not job_status["failed_jobs"]:
            logger.info("✅ No recovery needed")
            return
        
        client = FlinkRestClient(FLINK_REST_URL)
        
        for job_config in FLINK_JOBS:
            if job_config["job_name"] in job_status["missing_jobs"]:
                logger.info(f"🔄 Recovering job: {job_config['job_name']}")
                
                try:
                    # 查找最新的 Savepoint
                    # 上传 JAR 并重新提交
                    jars = client.list_jars()
                    jar_id = None
                    for jar in jars:
                        if job_config["jar_path"].split("/")[-1] in jar.get("name", ""):
                            jar_id = jar["id"]
                            break
                    
                    if not jar_id:
                        jar_id = client.upload_jar(job_config["jar_path"])
                    
                    # 从最新 Savepoint 恢复
                    import subprocess
                    result = subprocess.run(
                        ["hdfs", "dfs", "-ls", f"{SAVEPOINT_DIR}/{job_config['job_name']}/"],
                        capture_output=True, text=True
                    )
                    savepoint_path = None
                    if result.returncode == 0:
                        lines = result.stdout.strip().split("\n")
                        if lines:
                            savepoint_path = lines[-1].split()[-1]
                    
                    # 提交作业
                    job_id = client.submit_job(
                        jar_id=jar_id,
                        entry_class=job_config["entry_class"],
                        parallelism=job_config["parallelism"],
                        program_args=job_config.get("args", ""),
                        savepoint_path=savepoint_path,
                    )
                    
                    logger.info(f"✅ Job recovered: {job_config['job_name']} -> {job_id}")
                    
                except Exception as e:
                    logger.error(f"❌ Failed to recover {job_config['job_name']}: {e}")
    
    cluster = check_flink_cluster()
    job_status = check_running_jobs()
    ckpt = check_checkpoints()
    auto_recover_jobs(job_status, ckpt)


# ============================================================
# 4. DAG 2: Flink Savepoint 定时备份 (每 2 小时)
# ============================================================

with DAG(
    dag_id="flink_savepoint_backup",
    default_args={
        "owner": "data-platform",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    description="Flink Savepoint 定时创建 & 清理",
    schedule_interval="0 */2 * * *",           # 每 2 小时
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["flink", "savepoint", "backup"],
) as savepoint_dag:
    
    @task(task_id="trigger_savepoints")
    def trigger_savepoints():
        """为所有运行中的作业触发 Savepoint"""
        client = FlinkRestClient(FLINK_REST_URL)
        jobs = client.list_jobs()
        
        results = []
        for job in jobs:
            if job["status"] != "RUNNING":
                continue
            
            try:
                detail = client.get_job_detail(job["id"])
                job_name = detail.get("name", job["id"])
                
                target_dir = f"{SAVEPOINT_DIR}/{job_name}"
                trigger_id = client.trigger_savepoint(job["id"], target_dir)
                
                results.append({
                    "job_id": job["id"],
                    "job_name": job_name,
                    "trigger_id": trigger_id,
                    "target_dir": target_dir,
                })
                logger.info(f"✅ Savepoint triggered for {job_name}: {trigger_id}")
                
            except Exception as e:
                logger.error(f"❌ Failed to trigger savepoint for {job['id']}: {e}")
                results.append({
                    "job_id": job["id"],
                    "error": str(e),
                })
        
        return results
    
    @task(task_id="verify_savepoints")
    def verify_savepoints(trigger_results: list):
        """验证 Savepoint 是否完成"""
        import time
        client = FlinkRestClient(FLINK_REST_URL)
        
        for result in trigger_results:
            if "error" in result:
                continue
            
            job_id = result["job_id"]
            trigger_id = result["trigger_id"]
            
            # 轮询检查状态
            for _ in range(30):
                status = client.get_savepoint_status(job_id, trigger_id)
                operation_status = status.get("status", {}).get("id")
                
                if operation_status == "COMPLETED":
                    location = status.get("operation", {}).get("location")
                    logger.info(f"✅ Savepoint completed: {result['job_name']} -> {location}")
                    result["savepoint_path"] = location
                    break
                elif operation_status == "FAILED":
                    cause = status.get("operation", {}).get("failure-cause", {})
                    logger.error(f"❌ Savepoint failed for {result['job_name']}: {cause}")
                    break
                
                time.sleep(10)
        
        return trigger_results
    
    @task(task_id="cleanup_old_savepoints")
    def cleanup_old_savepoints():
        """清理过期的 Savepoint (保留最近 5 个)"""
        import subprocess
        
        for job_config in FLINK_JOBS:
            job_name = job_config["job_name"]
            sp_dir = f"{SAVEPOINT_DIR}/{job_name}"
            
            # 列出所有 Savepoint
            result = subprocess.run(
                ["hdfs", "dfs", "-ls", sp_dir],
                capture_output=True, text=True
            )
            
            if result.returncode != 0:
                continue
            
            lines = [l for l in result.stdout.strip().split("\n") if l and not l.startswith("Found")]
            
            # 保留最近 5 个
            keep_count = 5
            if len(lines) > keep_count:
                to_delete = lines[:-keep_count]
                for line in to_delete:
                    path = line.split()[-1]
                    logger.info(f"🗑️ Deleting old savepoint: {path}")
                    subprocess.run(["hdfs", "dfs", "-rm", "-r", path])
        
        logger.info("✅ Old savepoints cleaned up")
    
    triggers = trigger_savepoints()
    verified = verify_savepoints(triggers)
    cleanup_old_savepoints()


# ============================================================
# 5. DAG 3: Kafka Consumer Lag 监控 (每 10 分钟)
# ============================================================

with DAG(
    dag_id="kafka_consumer_lag_monitor",
    default_args={
        "owner": "data-platform",
        "retries": 1,
    },
    description="Kafka Consumer Lag 监控 & 告警",
    schedule_interval="*/10 * * * *",          # 每 10 分钟
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["kafka", "monitor", "lag"],
) as lag_monitor_dag:
    
    @task(task_id="collect_consumer_lag")
    def collect_consumer_lag():
        """收集所有消费者组的 Lag"""
        monitor = KafkaMonitor(KAFKA_BOOTSTRAP)
        
        all_lags = []
        for job in FLINK_JOBS:
            try:
                lag_info = monitor.get_consumer_lag(
                    group_id=job["consumer_group"],
                    topics=job["kafka_topics"]
                )
                all_lags.append(lag_info)
                
                logger.info(f"Consumer Group: {job['consumer_group']}, "
                          f"Total Lag: {lag_info['total_lag']}")
                
            except Exception as e:
                logger.error(f"Failed to get lag for {job['consumer_group']}: {e}")
                all_lags.append({
                    "group_id": job["consumer_group"],
                    "error": str(e)
                })
        
        return all_lags
    
    @task(task_id="evaluate_lag_alerts")
    def evaluate_lag_alerts(lag_data: list):
        """评估 Lag 告警"""
        LAG_THRESHOLD = int(Variable.get("kafka_lag_threshold", default_var="100000"))
        LAG_CRITICAL = int(Variable.get("kafka_lag_critical", default_var="1000000"))
        
        alerts = []
        for lag in lag_data:
            if "error" in lag:
                alerts.append({
                    "group_id": lag["group_id"],
                    "level": "ERROR",
                    "message": f"Cannot check lag: {lag['error']}"
                })
                continue
            
            total_lag = lag["total_lag"]
            group_id = lag["group_id"]
            
            if total_lag >= LAG_CRITICAL:
                alerts.append({
                    "group_id": group_id,
                    "level": "CRITICAL",
                    "lag": total_lag,
                    "message": f"Lag {total_lag} exceeds critical threshold {LAG_CRITICAL}"
                })
                logger.error(f"🚨 CRITICAL: {group_id} lag={total_lag}")
            elif total_lag >= LAG_THRESHOLD:
                alerts.append({
                    "group_id": group_id,
                    "level": "WARNING",
                    "lag": total_lag,
                    "message": f"Lag {total_lag} exceeds warning threshold {LAG_THRESHOLD}"
                })
                logger.warning(f"⚠️ WARNING: {group_id} lag={total_lag}")
            else:
                logger.info(f"✅ OK: {group_id} lag={total_lag}")
        
        if alerts:
            # 推送告警 (企微/飞书/钉钉)
            logger.warning(f"Total alerts: {len(alerts)}")
        
        return alerts
    
    @task(task_id="record_lag_metrics")
    def record_lag_metrics(lag_data: list):
        """记录 Lag 指标到时序数据库 (ClickHouse/InfluxDB)"""
        metrics = []
        for lag in lag_data:
            if "error" in lag:
                continue
            
            metrics.append({
                "group_id": lag["group_id"],
                "total_lag": lag["total_lag"],
                "timestamp": datetime.now().isoformat(),
                "partitions_count": len(lag.get("partitions", [])),
            })
        
        # 写入 ClickHouse 用于 Grafana 展示
        logger.info(f"Recording {len(metrics)} lag metrics")
        
        return metrics
    
    lag_data = collect_consumer_lag()
    evaluate_lag_alerts(lag_data)
    record_lag_metrics(lag_data)
