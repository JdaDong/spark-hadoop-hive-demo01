package com.bigdata.serverless;

import java.util.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * ============================================================================
 * Serverless 计算平台 - Flink on Kubernetes 云原生架构
 * ============================================================================
 *
 * 一、Flink Kubernetes Operator
 *    1. Operator 架构 & CRD 设计
 *    2. FlinkDeployment (Application/Session 模式)
 *    3. FlinkSessionJob (会话作业提交)
 *    4. 自动伸缩 (Reactive/Kubernetes HPA)
 *    5. Savepoint & Checkpoint 管理
 *
 * 二、Kubernetes 资源编排
 *    1. Pod Template (Sidecar/InitContainer)
 *    2. Resource Quota & LimitRange
 *    3. RBAC & ServiceAccount
 *    4. ConfigMap & Secret 管理
 *    5. PVC 持久化存储
 *
 * 三、Serverless 弹性伸缩
 *    1. Flink Autoscaler (基于 Backpressure)
 *    2. Kubernetes HPA (基于 CPU/Memory/Custom Metrics)
 *    3. KEDA (Event-Driven Autoscaling, Kafka Lag)
 *    4. VPA (Vertical Pod Autoscaler)
 *    5. Spot Instance 混合调度
 *
 * 四、CI/CD & GitOps
 *    1. Docker 镜像构建 (多阶段)
 *    2. Helm Chart 管理
 *    3. ArgoCD GitOps 部署
 *    4. Canary & Blue-Green 发布
 *    5. Prometheus + Grafana 监控
 *
 * 五、多租户 & 资源隔离
 *    1. Namespace 隔离
 *    2. 资源配额管理
 *    3. 优先级 & 抢占
 *    4. 成本核算 & 计费
 *
 * 技术栈: Flink 1.17 + K8s Operator 1.7 + KEDA 2.12 + ArgoCD 2.9
 * ============================================================================
 */
public class FlinkServerlessApp {

    public static void main(String[] args) {
        System.out.println("╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║      Serverless 计算平台 - Flink on Kubernetes 云原生        ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝");

        // 一、Flink Kubernetes Operator
        demoFlinkOperator();
        demoFlinkDeployment();
        demoFlinkSessionJob();
        demoSavepointManagement();

        // 二、Kubernetes 资源编排
        demoPodTemplate();
        demoResourceManagement();
        demoRbacConfig();
        demoStorageConfig();

        // 三、Serverless 弹性伸缩
        demoFlinkAutoscaler();
        demoKedaScaling();
        demoHpaConfig();
        demoSpotInstance();

        // 四、CI/CD & GitOps
        demoDockerBuild();
        demoHelmChart();
        demoArgoCd();
        demoCanaryDeploy();

        // 五、多租户
        demoMultiTenancy();
        demoCostManagement();

        System.out.println("\n✅ Serverless 计算平台演示完成！");
    }

    // ====================== 一、Flink Kubernetes Operator ======================

    /**
     * 1.1 Flink Kubernetes Operator 架构
     */
    static void demoFlinkOperator() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🎯 1.1 Flink Kubernetes Operator 架构");
        System.out.println("=".repeat(60));

        String architecture = String.join("\n",
            "Flink on K8s 架构:",
            "",
            "┌─────────────────────────────────────────────────────────┐",
            "│                Kubernetes Cluster                       │",
            "│  ┌─────────────────────────────────────────────────┐   │",
            "│  │         Flink Kubernetes Operator                │   │",
            "│  │  ┌──────────────┐  ┌──────────────┐            │   │",
            "│  │  │ CRD Watcher  │  │ Reconciler   │            │   │",
            "│  │  │ (FlinkDeploy │  │ (状态协调)   │            │   │",
            "│  │  │  ment)       │  │              │            │   │",
            "│  │  └──────┬───────┘  └──────┬───────┘            │   │",
            "│  └─────────┼─────────────────┼────────────────────┘   │",
            "│            │                 │                         │",
            "│  ┌─────────▼─────────────────▼─────────────────────┐  │",
            "│  │              Flink Application                   │  │",
            "│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐     │  │",
            "│  │  │JobManager│  │TaskManager│  │TaskManager│     │  │",
            "│  │  │ (Pod)    │  │ (Pod)     │  │ (Pod)     │     │  │",
            "│  │  └──────────┘  └──────────┘  └──────────┘     │  │",
            "│  │        ↕ Checkpoint/Savepoint (S3/HDFS)        │  │",
            "│  └─────────────────────────────────────────────────┘  │",
            "│                                                        │",
            "│  ┌──────────┐  ┌──────────┐  ┌──────────┐            │",
            "│  │Prometheus│  │ Grafana  │  │  KEDA    │            │",
            "│  │(监控)    │  │(仪表盘)  │  │(弹性伸缩)│            │",
            "│  └──────────┘  └──────────┘  └──────────┘            │",
            "└─────────────────────────────────────────────────────────┘",
            "",
            "# 安装 Flink Operator:",
            "helm repo add flink-operator-repo \\",
            "  https://downloads.apache.org/flink/flink-kubernetes-operator-1.7.0/",
            "helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator \\",
            "  --namespace flink-system --create-namespace \\",
            "  --set webhook.create=true \\",
            "  --set metrics.port=9999"
        );
        System.out.println(architecture);
    }

    /**
     * 1.2 FlinkDeployment - Application 模式
     */
    static void demoFlinkDeployment() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🚀 1.2 FlinkDeployment - Application 模式");
        System.out.println("=".repeat(60));

        String appDeployment = String.join("\n",
            "# flink-deployments/ecommerce-streaming.yaml",
            "apiVersion: flink.apache.org/v1beta1",
            "kind: FlinkDeployment",
            "metadata:",
            "  name: ecommerce-streaming",
            "  namespace: flink-prod",
            "  labels:",
            "    app: ecommerce",
            "    team: data-platform",
            "    env: production",
            "spec:",
            "  image: registry.company.com/flink/ecommerce-streaming:v1.2.0",
            "  flinkVersion: v1_17",
            "  flinkConfiguration:",
            "    # ===== Checkpoint =====",
            "    execution.checkpointing.interval: \"60000\"        # 60s",
            "    execution.checkpointing.mode: EXACTLY_ONCE",
            "    execution.checkpointing.timeout: \"300000\"         # 5min",
            "    execution.checkpointing.min-pause: \"30000\"        # 30s",
            "    execution.checkpointing.max-concurrent-checkpoints: \"1\"",
            "    state.backend: rocksdb",
            "    state.backend.rocksdb.localdir: /tmp/rocksdb",
            "    state.backend.incremental: \"true\"",
            "    state.checkpoints.dir: s3://flink-checkpoints/ecommerce-streaming",
            "    state.savepoints.dir: s3://flink-savepoints/ecommerce-streaming",
            "    ",
            "    # ===== 重启策略 =====",
            "    restart-strategy: exponential-delay",
            "    restart-strategy.exponential-delay.initial-backoff: 1s",
            "    restart-strategy.exponential-delay.max-backoff: 60s",
            "    restart-strategy.exponential-delay.backoff-multiplier: \"2.0\"",
            "    restart-strategy.exponential-delay.reset-backoff-threshold: 10min",
            "    ",
            "    # ===== 网络 =====",
            "    taskmanager.numberOfTaskSlots: \"4\"",
            "    parallelism.default: \"8\"",
            "    taskmanager.memory.network.fraction: \"0.15\"",
            "    taskmanager.memory.network.max: 256mb",
            "    ",
            "    # ===== Kafka 配置 =====",
            "    kafka.bootstrap.servers: kafka:9092",
            "    kafka.group.id: ecommerce-streaming-prod",
            "    kafka.auto.offset.reset: latest",
            "    ",
            "    # ===== 高可用 =====",
            "    high-availability: kubernetes",
            "    high-availability.storageDir: s3://flink-ha/ecommerce-streaming",
            "    kubernetes.cluster-id: ecommerce-streaming",
            "    ",
            "    # ===== Metrics =====",
            "    metrics.reporters: prom",
            "    metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory",
            "    metrics.reporter.prom.port: \"9249\"",
            "  ",
            "  serviceAccount: flink-sa",
            "  ",
            "  # ===== JobManager =====",
            "  jobManager:",
            "    resource:",
            "      memory: \"2048m\"",
            "      cpu: 1",
            "    replicas: 1",
            "    podTemplate:",
            "      spec:",
            "        containers:",
            "          - name: flink-main-container",
            "            env:",
            "              - name: KAFKA_BOOTSTRAP_SERVERS",
            "                valueFrom:",
            "                  configMapKeyRef:",
            "                    name: flink-config",
            "                    key: kafka.bootstrap.servers",
            "              - name: DORIS_PASSWORD",
            "                valueFrom:",
            "                  secretKeyRef:",
            "                    name: flink-secrets",
            "                    key: doris-password",
            "  ",
            "  # ===== TaskManager =====",
            "  taskManager:",
            "    resource:",
            "      memory: \"4096m\"",
            "      cpu: 2",
            "    replicas: 2",
            "    podTemplate:",
            "      spec:",
            "        containers:",
            "          - name: flink-main-container",
            "            volumeMounts:",
            "              - name: rocksdb-vol",
            "                mountPath: /tmp/rocksdb",
            "        volumes:",
            "          - name: rocksdb-vol",
            "            emptyDir:",
            "              sizeLimit: 10Gi",
            "        # 反亲和: TM Pod 分散到不同节点",
            "        affinity:",
            "          podAntiAffinity:",
            "            preferredDuringSchedulingIgnoredDuringExecution:",
            "              - weight: 100",
            "                podAffinityTerm:",
            "                  labelSelector:",
            "                    matchLabels:",
            "                      app: ecommerce-streaming",
            "                      component: taskmanager",
            "                  topologyKey: kubernetes.io/hostname",
            "        # 容忍 Spot 节点",
            "        tolerations:",
            "          - key: \"kubernetes.azure.com/scalesetpriority\"",
            "            operator: \"Equal\"",
            "            value: \"spot\"",
            "            effect: \"NoSchedule\"",
            "  ",
            "  # ===== Job 配置 =====",
            "  job:",
            "    jarURI: local:///opt/flink/usrlib/ecommerce-streaming.jar",
            "    entryClass: com.bigdata.realtime.RealtimeDataWarehouseApp",
            "    args: [\"--env\", \"prod\", \"--parallelism\", \"8\"]",
            "    parallelism: 8",
            "    upgradeMode: savepoint      # stateless / savepoint / last-state",
            "    state: running",
            "    savepointTriggerNonce: 0     # 递增触发 savepoint",
            "    initialSavepointPath: s3://flink-savepoints/ecommerce/sp-20240323-001",
            "    allowNonRestoredState: false"
        );
        System.out.println(appDeployment);

        // Session 模式
        String sessionDeployment = String.join("\n",
            "",
            "# Session 模式 (多作业共享集群):",
            "apiVersion: flink.apache.org/v1beta1",
            "kind: FlinkDeployment",
            "metadata:",
            "  name: flink-session-cluster",
            "  namespace: flink-dev",
            "spec:",
            "  image: flink:1.17.1-scala_2.12-java11",
            "  flinkVersion: v1_17",
            "  flinkConfiguration:",
            "    taskmanager.numberOfTaskSlots: \"4\"",
            "  jobManager:",
            "    resource: { memory: \"2048m\", cpu: 1 }",
            "  taskManager:",
            "    resource: { memory: \"4096m\", cpu: 2 }",
            "    replicas: 3",
            "  mode: native    # native = 按需启动 TM"
        );
        System.out.println(sessionDeployment);
    }

    /**
     * 1.3 FlinkSessionJob - 会话作业
     */
    static void demoFlinkSessionJob() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("📋 1.3 FlinkSessionJob - 会话作业提交");
        System.out.println("=".repeat(60));

        String sessionJob = String.join("\n",
            "# 向 Session 集群提交作业:",
            "apiVersion: flink.apache.org/v1beta1",
            "kind: FlinkSessionJob",
            "metadata:",
            "  name: etl-orders-job",
            "  namespace: flink-dev",
            "spec:",
            "  deploymentName: flink-session-cluster    # 引用 Session 集群",
            "  job:",
            "    jarURI: https://artifacts.company.com/flink/etl-orders-1.0.jar",
            "    entryClass: com.bigdata.flink.FlinkStreamingJavaApp",
            "    args: [\"--topic\", \"orders\", \"--env\", \"dev\"]",
            "    parallelism: 4",
            "    upgradeMode: stateless",
            "",
            "# 优势对比:",
            "# Application 模式: 每个作业独立集群, 资源隔离好, 适合生产",
            "# Session 模式:    多作业共享集群, 启动快, 适合开发/小作业"
        );
        System.out.println(sessionJob);
    }

    /**
     * 1.4 Savepoint & Checkpoint 管理
     */
    static void demoSavepointManagement() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("💾 1.4 Savepoint & Checkpoint 管理");
        System.out.println("=".repeat(60));

        String savepoint = String.join("\n",
            "# Savepoint 触发 (修改 savepointTriggerNonce):",
            "kubectl patch flinkdeployment ecommerce-streaming -n flink-prod \\",
            "  --type=merge -p '{\"spec\":{\"job\":{\"savepointTriggerNonce\": 1}}}'",
            "",
            "# Savepoint 升级 (upgradeMode: savepoint):",
            "# 1. Operator 自动触发 savepoint",
            "# 2. 取消当前作业",
            "# 3. 从 savepoint 恢复新版本",
            "",
            "# Last-state 升级 (upgradeMode: last-state):",
            "# 使用最后一次 checkpoint 恢复 (更快, 但可能丢少量数据)",
            "",
            "# Checkpoint 清理策略:",
            "# state.checkpoints.num-retained: 3",
            "# state.checkpoints.dir: s3://flink-checkpoints/${app}/",
            "",
            "# 作业状态查看:",
            "kubectl get flinkdeployment ecommerce-streaming -n flink-prod -o yaml",
            "# status:",
            "#   jobStatus:",
            "#     state: RUNNING",
            "#     savepointInfo:",
            "#       lastSavepoint: s3://flink-savepoints/.../sp-001",
            "#       triggerTimestamp: 1711180800",
            "#   reconciliationStatus:",
            "#     state: DEPLOYED",
            "",
            "# 从指定 Savepoint 恢复:",
            "# spec.job.initialSavepointPath: s3://flink-savepoints/.../sp-001"
        );
        System.out.println(savepoint);
    }

    // ====================== 二、Kubernetes 资源编排 ======================

    /**
     * 2.1 Pod Template
     */
    static void demoPodTemplate() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🐳 2.1 Pod Template (Sidecar / InitContainer)");
        System.out.println("=".repeat(60));

        String podTemplate = String.join("\n",
            "# 高级 Pod Template 配置:",
            "taskManager:",
            "  podTemplate:",
            "    spec:",
            "      # InitContainer: 下载依赖 JAR",
            "      initContainers:",
            "        - name: download-connectors",
            "          image: busybox:1.36",
            "          command: ['sh', '-c']",
            "          args:",
            "            - |",
            "              wget -O /opt/flink/lib/flink-connector-kafka.jar \\",
            "                https://artifacts.company.com/flink/connectors/kafka-3.0.1.jar",
            "              wget -O /opt/flink/lib/flink-doris-connector.jar \\",
            "                https://artifacts.company.com/flink/connectors/doris-1.5.0.jar",
            "          volumeMounts:",
            "            - name: flink-lib",
            "              mountPath: /opt/flink/lib",
            "      ",
            "      containers:",
            "        - name: flink-main-container",
            "          resources:",
            "            requests: { cpu: \"2\", memory: \"4Gi\" }",
            "            limits:   { cpu: \"4\", memory: \"8Gi\" }",
            "          volumeMounts:",
            "            - name: flink-lib",
            "              mountPath: /opt/flink/lib",
            "            - name: rocksdb",
            "              mountPath: /tmp/rocksdb",
            "          ",
            "        # Sidecar: Fluent Bit 日志收集",
            "        - name: log-collector",
            "          image: fluent/fluent-bit:2.2",
            "          volumeMounts:",
            "            - name: flink-logs",
            "              mountPath: /flink-logs",
            "          env:",
            "            - name: ELASTICSEARCH_HOST",
            "              value: \"elasticsearch:9200\"",
            "      ",
            "      volumes:",
            "        - name: flink-lib",
            "          emptyDir: {}",
            "        - name: rocksdb",
            "          emptyDir: { sizeLimit: 10Gi }",
            "        - name: flink-logs",
            "          emptyDir: {}"
        );
        System.out.println(podTemplate);
    }

    /**
     * 2.2 Resource Management
     */
    static void demoResourceManagement() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("📊 2.2 Resource Quota & LimitRange");
        System.out.println("=".repeat(60));

        String resourceQuota = String.join("\n",
            "# ResourceQuota: 命名空间资源上限",
            "apiVersion: v1",
            "kind: ResourceQuota",
            "metadata:",
            "  name: flink-prod-quota",
            "  namespace: flink-prod",
            "spec:",
            "  hard:",
            "    requests.cpu: \"64\"",
            "    requests.memory: 256Gi",
            "    limits.cpu: \"128\"",
            "    limits.memory: 512Gi",
            "    pods: \"100\"",
            "    persistentvolumeclaims: \"20\"",
            "",
            "---",
            "# LimitRange: 单 Pod 资源限制",
            "apiVersion: v1",
            "kind: LimitRange",
            "metadata:",
            "  name: flink-pod-limits",
            "  namespace: flink-prod",
            "spec:",
            "  limits:",
            "    - type: Container",
            "      default:                  # 默认 limits",
            "        cpu: \"4\"",
            "        memory: 8Gi",
            "      defaultRequest:            # 默认 requests",
            "        cpu: \"1\"",
            "        memory: 2Gi",
            "      max:                       # 最大值",
            "        cpu: \"16\"",
            "        memory: 32Gi",
            "      min:                       # 最小值",
            "        cpu: \"0.5\"",
            "        memory: 512Mi"
        );
        System.out.println(resourceQuota);
    }

    /**
     * 2.3 RBAC
     */
    static void demoRbacConfig() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🔐 2.3 RBAC & ServiceAccount");
        System.out.println("=".repeat(60));

        String rbac = String.join("\n",
            "# ServiceAccount + ClusterRole + ClusterRoleBinding",
            "apiVersion: v1",
            "kind: ServiceAccount",
            "metadata:",
            "  name: flink-sa",
            "  namespace: flink-prod",
            "---",
            "apiVersion: rbac.authorization.k8s.io/v1",
            "kind: ClusterRole",
            "metadata:",
            "  name: flink-role",
            "rules:",
            "  - apiGroups: [\"\"]",
            "    resources: [pods, configmaps, services]",
            "    verbs: [get, list, watch, create, update, delete]",
            "  - apiGroups: [apps]",
            "    resources: [deployments, replicasets]",
            "    verbs: [get, list, watch, create, update, delete]",
            "  - apiGroups: [flink.apache.org]",
            "    resources: [flinkdeployments, flinksessionjobs]",
            "    verbs: ['*']",
            "---",
            "apiVersion: rbac.authorization.k8s.io/v1",
            "kind: ClusterRoleBinding",
            "metadata:",
            "  name: flink-role-binding",
            "subjects:",
            "  - kind: ServiceAccount",
            "    name: flink-sa",
            "    namespace: flink-prod",
            "roleRef:",
            "  kind: ClusterRole",
            "  name: flink-role",
            "  apiGroup: rbac.authorization.k8s.io"
        );
        System.out.println(rbac);
    }

    /**
     * 2.4 Storage Config
     */
    static void demoStorageConfig() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("💿 2.4 PVC 持久化存储");
        System.out.println("=".repeat(60));

        String storage = String.join("\n",
            "# S3 (MinIO) 存储 for Checkpoint/Savepoint:",
            "flinkConfiguration:",
            "  s3.endpoint: http://minio:9000",
            "  s3.access-key: ${MINIO_ACCESS_KEY}",
            "  s3.secret-key: ${MINIO_SECRET_KEY}",
            "  s3.path.style.access: \"true\"",
            "  state.checkpoints.dir: s3://flink-state/checkpoints/${app}",
            "  state.savepoints.dir: s3://flink-state/savepoints/${app}",
            "  high-availability.storageDir: s3://flink-state/ha/${app}",
            "",
            "# PVC for RocksDB (本地状态):",
            "apiVersion: v1",
            "kind: PersistentVolumeClaim",
            "metadata:",
            "  name: flink-rocksdb-pvc",
            "spec:",
            "  accessModes: [ReadWriteOnce]",
            "  storageClassName: ssd-csi",
            "  resources:",
            "    requests:",
            "      storage: 50Gi"
        );
        System.out.println(storage);
    }

    // ====================== 三、Serverless 弹性伸缩 ======================

    /**
     * 3.1 Flink Autoscaler
     */
    static void demoFlinkAutoscaler() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("📈 3.1 Flink Autoscaler (内置自动伸缩)");
        System.out.println("=".repeat(60));

        String autoscaler = String.join("\n",
            "# Flink Operator 内置 Autoscaler (1.6+)",
            "# 基于反压 (Backpressure) + 数据速率自动调整并行度",
            "",
            "apiVersion: flink.apache.org/v1beta1",
            "kind: FlinkDeployment",
            "metadata:",
            "  name: ecommerce-streaming-autoscale",
            "spec:",
            "  flinkConfiguration:",
            "    # ===== 自动伸缩配置 =====",
            "    job.autoscaler.enabled: \"true\"",
            "    job.autoscaler.stabilization.interval: 5m",
            "    job.autoscaler.metrics.window: 10m",
            "    ",
            "    # 并行度范围",
            "    job.autoscaler.scale-up.max-factor: \"2.0\"       # 最多扩容2倍",
            "    job.autoscaler.scale-down.max-factor: \"0.5\"     # 最多缩容50%",
            "    job.autoscaler.scale-up.grace-period: 5m        # 扩容冷却期",
            "    job.autoscaler.scale-down.grace-period: 15m     # 缩容冷却期",
            "    ",
            "    # 目标利用率",
            "    job.autoscaler.target.utilization: \"0.7\"        # 70% 利用率",
            "    job.autoscaler.target.utilization.boundary: \"0.3\" # 30% 容忍度",
            "    ",
            "    # 反压阈值",
            "    job.autoscaler.backpressure.propagation-delay: 30s",
            "    ",
            "    # 每个 Vertex 的并行度范围",
            "    pipeline.jobvertex-parallelism-overrides: \\",
            "      \"a1b2c3d4e5f6:1,16;f6e5d4c3b2a1:2,32\"  # vertexId:min,max",
            "",
            "  job:",
            "    parallelism: 4                    # 初始并行度",
            "    upgradeMode: last-state           # 伸缩时用 last-state 快速恢复",
            "",
            "# 自动伸缩决策流程:",
            "# 1. 采集指标: busyTime / backpressuredTime / idleTime",
            "# 2. 计算利用率: utilization = busyTime / (busyTime + idleTime)",
            "# 3. 计算目标并行度: target = current * (utilization / 0.7)",
            "# 4. 稳定性检查: 等待 stabilization.interval",
            "# 5. 触发伸缩: Savepoint → 取消 → 新并行度启动"
        );
        System.out.println(autoscaler);
    }

    /**
     * 3.2 KEDA 事件驱动伸缩
     */
    static void demoKedaScaling() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("⚡ 3.2 KEDA 事件驱动伸缩 (Kafka Lag)");
        System.out.println("=".repeat(60));

        String keda = String.join("\n",
            "# KEDA ScaledObject: 基于 Kafka Consumer Lag 伸缩 TM",
            "apiVersion: keda.sh/v1alpha1",
            "kind: ScaledObject",
            "metadata:",
            "  name: flink-tm-kafka-scaler",
            "  namespace: flink-prod",
            "spec:",
            "  scaleTargetRef:",
            "    apiVersion: flink.apache.org/v1beta1",
            "    kind: FlinkDeployment",
            "    name: ecommerce-streaming",
            "  pollingInterval: 30          # 30s 检查一次",
            "  cooldownPeriod: 300          # 5min 冷却期",
            "  minReplicaCount: 2           # 最少 2 个 TM",
            "  maxReplicaCount: 16          # 最多 16 个 TM",
            "  ",
            "  triggers:",
            "    # Kafka Consumer Lag 触发器",
            "    - type: kafka",
            "      metadata:",
            "        bootstrapServers: kafka:9092",
            "        consumerGroup: ecommerce-streaming-prod",
            "        topic: user_events",
            "        lagThreshold: \"5000\"    # Lag > 5000 → 扩容",
            "        activationLagThreshold: \"100\"  # Lag > 100 → 激活",
            "        offsetResetPolicy: latest",
            "    ",
            "    # Prometheus 自定义指标触发器",
            "    - type: prometheus",
            "      metadata:",
            "        serverAddress: http://prometheus:9090",
            "        metricName: flink_taskmanager_job_task_backPressuredTimeMsPerSecond",
            "        threshold: \"500\"        # 反压 > 500ms/s → 扩容",
            "        query: |",
            "          avg(flink_taskmanager_job_task_backPressuredTimeMsPerSecond{",
            "            job=\"ecommerce-streaming\"",
            "          })",
            "",
            "  advanced:",
            "    restoreToOriginalReplicaCount: false",
            "    horizontalPodAutoscalerConfig:",
            "      behavior:",
            "        scaleUp:",
            "          stabilizationWindowSeconds: 60",
            "          policies:",
            "            - type: Pods",
            "              value: 4",
            "              periodSeconds: 60",
            "        scaleDown:",
            "          stabilizationWindowSeconds: 300",
            "          policies:",
            "            - type: Percent",
            "              value: 25",
            "              periodSeconds: 120"
        );
        System.out.println(keda);
    }

    /**
     * 3.3 HPA 配置
     */
    static void demoHpaConfig() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("📊 3.3 Kubernetes HPA (CPU/Memory/Custom)");
        System.out.println("=".repeat(60));

        String hpa = String.join("\n",
            "# HPA: 基于 CPU + 自定义指标",
            "apiVersion: autoscaling/v2",
            "kind: HorizontalPodAutoscaler",
            "metadata:",
            "  name: flink-tm-hpa",
            "  namespace: flink-prod",
            "spec:",
            "  scaleTargetRef:",
            "    apiVersion: apps/v1",
            "    kind: Deployment",
            "    name: ecommerce-streaming-taskmanager",
            "  minReplicas: 2",
            "  maxReplicas: 16",
            "  metrics:",
            "    - type: Resource",
            "      resource:",
            "        name: cpu",
            "        target:",
            "          type: Utilization",
            "          averageUtilization: 70   # CPU > 70% → 扩容",
            "    - type: Resource",
            "      resource:",
            "        name: memory",
            "        target:",
            "          type: Utilization",
            "          averageUtilization: 80   # Memory > 80% → 扩容",
            "    - type: Pods",
            "      pods:",
            "        metric:",
            "          name: flink_taskmanager_numRecordsInPerSecond",
            "        target:",
            "          type: AverageValue",
            "          averageValue: \"10000\"   # 每秒 > 10000 条 → 扩容",
            "  behavior:",
            "    scaleUp:",
            "      stabilizationWindowSeconds: 120",
            "      policies:",
            "        - type: Pods",
            "          value: 2",
            "          periodSeconds: 120       # 每2分钟最多扩2个",
            "    scaleDown:",
            "      stabilizationWindowSeconds: 600",
            "      policies:",
            "        - type: Percent",
            "          value: 25",
            "          periodSeconds: 300       # 每5分钟最多缩25%"
        );
        System.out.println(hpa);
    }

    /**
     * 3.4 Spot Instance 混合调度
     */
    static void demoSpotInstance() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("💰 3.4 Spot Instance 混合调度 (降本)");
        System.out.println("=".repeat(60));

        String spot = String.join("\n",
            "# Spot 混合调度策略:",
            "# JM: 固定节点 (On-Demand) - 保证高可用",
            "# TM: Spot 节点 - 降低成本 60-90%",
            "",
            "# Node 标签:",
            "# kubectl label node worker-1 node-type=on-demand",
            "# kubectl label node worker-2 node-type=spot",
            "",
            "# JM Pod: 亲和 On-Demand 节点",
            "jobManager:",
            "  podTemplate:",
            "    spec:",
            "      nodeSelector:",
            "        node-type: on-demand",
            "",
            "# TM Pod: 优先 Spot, 容忍中断",
            "taskManager:",
            "  podTemplate:",
            "    spec:",
            "      affinity:",
            "        nodeAffinity:",
            "          preferredDuringSchedulingIgnoredDuringExecution:",
            "            - weight: 80",
            "              preference:",
            "                matchExpressions:",
            "                  - key: node-type",
            "                    operator: In",
            "                    values: [spot]",
            "            - weight: 20",
            "              preference:",
            "                matchExpressions:",
            "                  - key: node-type",
            "                    operator: In",
            "                    values: [on-demand]",
            "      tolerations:",
            "        - key: kubernetes.azure.com/scalesetpriority",
            "          operator: Equal",
            "          value: spot",
            "          effect: NoSchedule",
            "",
            "# 成本对比:",
            "# On-Demand (8C32G): ¥2.5/h × 4 TM = ¥10/h = ¥7200/月",
            "# Spot (8C32G):      ¥0.5/h × 4 TM = ¥2/h  = ¥1440/月 (节省80%!)",
            "# 混合 (2 On-Demand + 2 Spot): ¥6/h = ¥4320/月 (节省40%)"
        );
        System.out.println(spot);
    }

    // ====================== 四、CI/CD & GitOps ======================

    /**
     * 4.1 Docker 多阶段构建
     */
    static void demoDockerBuild() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🐳 4.1 Docker 多阶段构建");
        System.out.println("=".repeat(60));

        String dockerfile = String.join("\n",
            "# Dockerfile - Flink 应用多阶段构建",
            "",
            "# === Stage 1: 构建 ===",
            "FROM maven:3.9-eclipse-temurin-11 AS builder",
            "WORKDIR /build",
            "COPY pom.xml .",
            "RUN mvn dependency:go-offline -B          # 缓存依赖",
            "COPY src/ src/",
            "RUN mvn clean package -DskipTests -Pflink # 打包",
            "",
            "# === Stage 2: 运行 ===",
            "FROM flink:1.17.1-scala_2.12-java11",
            "",
            "# 安装连接器",
            "RUN wget -P /opt/flink/lib/ \\",
            "  https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.0.1-1.17/flink-connector-kafka-3.0.1-1.17.jar",
            "RUN wget -P /opt/flink/lib/ \\",
            "  https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/1.17.1/flink-s3-fs-hadoop-1.17.1.jar",
            "",
            "# 复制应用 JAR",
            "COPY --from=builder /build/target/*-jar-with-dependencies.jar \\",
            "  /opt/flink/usrlib/ecommerce-streaming.jar",
            "",
            "# 健康检查",
            "HEALTHCHECK --interval=30s --timeout=5s \\",
            "  CMD curl -f http://localhost:8081/overview || exit 1",
            "",
            "# 构建 & 推送:",
            "# docker build -t registry.company.com/flink/ecommerce-streaming:v1.2.0 .",
            "# docker push registry.company.com/flink/ecommerce-streaming:v1.2.0"
        );
        System.out.println(dockerfile);
    }

    /**
     * 4.2 Helm Chart
     */
    static void demoHelmChart() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("⎈ 4.2 Helm Chart 管理");
        System.out.println("=".repeat(60));

        String helmChart = String.join("\n",
            "# helm-charts/flink-app/Chart.yaml",
            "apiVersion: v2",
            "name: flink-ecommerce",
            "version: 1.2.0",
            "appVersion: \"1.17.1\"",
            "",
            "# helm-charts/flink-app/values.yaml",
            "replicaCount:",
            "  taskManager: 2",
            "",
            "image:",
            "  repository: registry.company.com/flink/ecommerce-streaming",
            "  tag: v1.2.0",
            "  pullPolicy: IfNotPresent",
            "",
            "resources:",
            "  jobManager:",
            "    cpu: 1",
            "    memory: 2048m",
            "  taskManager:",
            "    cpu: 2",
            "    memory: 4096m",
            "",
            "flink:",
            "  parallelism: 8",
            "  checkpointInterval: 60000",
            "  stateBackend: rocksdb",
            "  s3Endpoint: http://minio:9000",
            "",
            "autoscaler:",
            "  enabled: true",
            "  minReplicas: 2",
            "  maxReplicas: 16",
            "  targetUtilization: 70",
            "",
            "kafka:",
            "  bootstrapServers: kafka:9092",
            "  consumerGroup: ecommerce-streaming-prod",
            "",
            "# 部署命令:",
            "# helm install ecommerce-streaming ./helm-charts/flink-app \\",
            "#   --namespace flink-prod --create-namespace \\",
            "#   -f values-prod.yaml",
            "",
            "# values-prod.yaml (环境覆盖):",
            "replicaCount:",
            "  taskManager: 4",
            "resources:",
            "  taskManager:",
            "    cpu: 4",
            "    memory: 8192m",
            "flink:",
            "  parallelism: 16"
        );
        System.out.println(helmChart);
    }

    /**
     * 4.3 ArgoCD GitOps
     */
    static void demoArgoCd() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🔄 4.3 ArgoCD GitOps 部署");
        System.out.println("=".repeat(60));

        String argocd = String.join("\n",
            "# ArgoCD Application (GitOps 声明式部署):",
            "apiVersion: argoproj.io/v1alpha1",
            "kind: Application",
            "metadata:",
            "  name: ecommerce-streaming",
            "  namespace: argocd",
            "  finalizers:",
            "    - resources-finalizer.argocd.argoproj.io",
            "spec:",
            "  project: data-platform",
            "  source:",
            "    repoURL: https://git.company.com/data-platform/flink-apps.git",
            "    targetRevision: main",
            "    path: helm-charts/flink-app",
            "    helm:",
            "      valueFiles:",
            "        - values.yaml",
            "        - values-prod.yaml",
            "  destination:",
            "    server: https://kubernetes.default.svc",
            "    namespace: flink-prod",
            "  syncPolicy:",
            "    automated:",
            "      prune: true           # 自动删除多余资源",
            "      selfHeal: true        # 自动修复 drift",
            "    syncOptions:",
            "      - CreateNamespace=true",
            "      - ApplyOutOfSyncOnly=true",
            "    retry:",
            "      limit: 3",
            "      backoff:",
            "        duration: 5s",
            "        factor: 2",
            "        maxDuration: 3m",
            "",
            "# GitOps 工作流:",
            "# 1. 开发者提交代码 → CI 构建 Docker 镜像",
            "# 2. CI 更新 Helm values.yaml 中的 image.tag",
            "# 3. ArgoCD 检测到 Git 变更 → 自动同步到 K8s",
            "# 4. Flink Operator 执行 savepoint → 停止旧版 → 启动新版",
            "# 5. Prometheus + Grafana 监控新版本指标"
        );
        System.out.println(argocd);
    }

    /**
     * 4.4 Canary 发布
     */
    static void demoCanaryDeploy() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🐤 4.4 Canary & Blue-Green 发布");
        System.out.println("=".repeat(60));

        String canary = String.join("\n",
            "# Flink 作业 Canary 发布策略:",
            "",
            "# 方式1: 双作业并行 (推荐)",
            "# 1. 保持旧版本运行: ecommerce-streaming-v1",
            "# 2. 启动新版本作业: ecommerce-streaming-v2 (10% 流量)",
            "# 3. 对比指标: 延迟/错误率/吞吐量",
            "# 4. 逐步切换流量: 10% → 50% → 100%",
            "# 5. 停止旧版本",
            "",
            "# 流量分配 (Kafka Topic 分区):",
            "# 旧版本消费分区: 0-8  (90%)",
            "# 新版本消费分区: 9    (10%)",
            "",
            "# 方式2: Blue-Green (停机切换)",
            "# 1. Blue (当前版本) 运行中",
            "# 2. 部署 Green (新版本), 从 savepoint 启动",
            "# 3. 验证 Green 正常",
            "# 4. 切换: 停 Blue → Green 接管全部流量",
            "",
            "# 自动化 Canary 脚本:",
            "# deploy-canary.sh",
            "#!/bin/bash",
            "NEW_VERSION=$1",
            "# 1. 部署 canary",
            "kubectl apply -f flink-deployment-canary-${NEW_VERSION}.yaml",
            "# 2. 等待 5 分钟",
            "sleep 300",
            "# 3. 检查 Prometheus 指标",
            "ERROR_RATE=$(curl -s prometheus:9090/api/v1/query?query=...)",
            "if [ \"$ERROR_RATE\" -gt \"0.01\" ]; then",
            "  echo 'Canary 失败, 回滚'",
            "  kubectl delete flinkdeployment canary-${NEW_VERSION}",
            "  exit 1",
            "fi",
            "# 4. 全量发布",
            "echo 'Canary 成功, 全量发布'",
            "kubectl apply -f flink-deployment-prod-${NEW_VERSION}.yaml"
        );
        System.out.println(canary);
    }

    // ====================== 五、多租户 ======================

    /**
     * 5.1 多租户资源隔离
     */
    static void demoMultiTenancy() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("🏢 5.1 多租户资源隔离");
        System.out.println("=".repeat(60));

        String multiTenancy = String.join("\n",
            "多租户 Namespace 规划:",
            "┌──────────────────────┬──────────┬────────────┬──────────────┐",
            "│ Namespace            │ 团队     │ 资源配额   │ 用途         │",
            "├──────────────────────┼──────────┼────────────┼──────────────┤",
            "│ flink-prod-ecommerce │ 电商     │ 64C/256G   │ 生产流处理   │",
            "│ flink-prod-finance   │ 金融     │ 32C/128G   │ 风控实时计算 │",
            "│ flink-prod-marketing │ 营销     │ 16C/64G    │ 用户行为分析 │",
            "│ flink-dev            │ 全部     │ 8C/32G     │ 开发测试     │",
            "│ flink-system         │ 平台组   │ 4C/16G     │ Operator     │",
            "└──────────────────────┴──────────┴────────────┴──────────────┘",
            "",
            "# PriorityClass (优先级):",
            "apiVersion: scheduling.k8s.io/v1",
            "kind: PriorityClass",
            "metadata:",
            "  name: flink-critical",
            "value: 1000000          # 最高优先级 (金融风控)",
            "globalDefault: false",
            "description: 'Critical Flink jobs that must not be preempted'",
            "---",
            "kind: PriorityClass",
            "metadata:",
            "  name: flink-normal",
            "value: 100000           # 普通优先级 (电商)",
            "---",
            "kind: PriorityClass",
            "metadata:",
            "  name: flink-low",
            "value: 10000            # 低优先级 (营销, 可被抢占)",
            "preemptionPolicy: PreemptLowerPriority"
        );
        System.out.println(multiTenancy);
    }

    /**
     * 5.2 成本管理
     */
    static void demoCostManagement() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("💰 5.2 成本核算 & 计费");
        System.out.println("=".repeat(60));

        String cost = String.join("\n",
            "成本核算模型:",
            "",
            "# Kubecost / OpenCost 集成:",
            "# 按 Namespace → 团队 → 作业 三级核算",
            "",
            "# 成本分析仪表盘 (Grafana):",
            "┌──────────────────────────────────────────────────────┐",
            "│  Flink 成本仪表盘 (2024-03)                          │",
            "│                                                      │",
            "│  总成本: ¥25,680                                     │",
            "│  ┌─────────────┬──────────┬──────────┬─────────────┐│",
            "│  │ 团队        │ CPU 成本 │ 内存成本 │ 存储成本    ││",
            "│  ├─────────────┼──────────┼──────────┼─────────────┤│",
            "│  │ 电商        │ ¥8,200   │ ¥6,400   │ ¥1,200     ││",
            "│  │ 金融        │ ¥4,100   │ ¥3,200   │ ¥800       ││",
            "│  │ 营销        │ ¥1,000   │ ¥480     │ ¥300       ││",
            "│  └─────────────┴──────────┴──────────┴─────────────┘│",
            "│                                                      │",
            "│  优化建议:                                            │",
            "│  - 营销团队使用 Spot 实例可节省 ¥1,200/月           │",
            "│  - 电商团队非高峰缩容可节省 ¥3,500/月               │",
            "└──────────────────────────────────────────────────────┘",
            "",
            "# Prometheus 成本指标:",
            "# sum(container_cpu_usage_seconds_total{namespace='flink-prod-ecommerce'})",
            "# sum(container_memory_working_set_bytes{namespace='flink-prod-ecommerce'})"
        );
        System.out.println(cost);
    }
}
