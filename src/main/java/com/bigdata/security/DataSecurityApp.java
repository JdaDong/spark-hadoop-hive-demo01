package com.bigdata.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.FsAction;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * ========================================================================
 * 大数据安全体系实战 - Ranger + Kerberos + 数据脱敏 + 审计
 * ========================================================================
 *
 * 功能矩阵:
 * ┌─────────────────────────────────────────────────────────────────────┐
 * │  模块              │  功能                                          │
 * ├─────────────────────────────────────────────────────────────────────┤
 * │  1. Kerberos 认证  │  KDC/TGT/Service Ticket/Keytab/委托令牌        │
 * │  2. Ranger 策略    │  RBAC/ABAC/行级/列级/标签策略/动态策略引擎       │
 * │  3. 数据脱敏       │  手机/身份证/姓名/邮箱/银行卡/地址/自定义正则    │
 * │  4. 数据加密       │  AES-256-GCM/RSA/列级加密/透明加密 TDE          │
 * │  5. 审计日志       │  操作审计/访问审计/异常检测/合规报告             │
 * │  6. 数据分级分类   │  自动发现/敏感度标记/合规标签/数据目录集成       │
 * │  7. 访问控制       │  HDFS ACL/Hive 列级授权/HBase Cell 安全         │
 * │  8. 安全合规       │  GDPR/CCPA/等保三级/数据生命周期管理            │
 * └─────────────────────────────────────────────────────────────────────┘
 *
 * 安全架构:
 *
 *   ┌─────────┐     ┌──────────┐     ┌──────────────┐
 *   │  用户   │────→│ Kerberos │────→│ Ranger Admin │
 *   │  请求   │     │   KDC    │     │  策略引擎    │
 *   └─────────┘     └──────────┘     └──────┬───────┘
 *                                           │
 *          ┌────────────────────────────────┼────────────────┐
 *          ▼                ▼               ▼                ▼
 *   ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌────────────┐
 *   │ HDFS Plugin│  │ Hive Plugin│  │HBase Plugin│  │Kafka Plugin│
 *   │  访问控制  │  │ 列级授权   │  │ Cell安全   │  │ Topic ACL  │
 *   └────────────┘  └────────────┘  └────────────┘  └────────────┘
 *          │                │               │                │
 *          └────────────────┴───────┬───────┴────────────────┘
 *                                   ▼
 *                          ┌─────────────────┐
 *                          │   审计日志中心   │
 *                          │ (Solr/ES 存储)  │
 *                          └─────────────────┘
 */
public class DataSecurityApp {

    // ==================== 1. Kerberos 认证管理器 ====================

    /**
     * Kerberos 认证管理 - 企业级安全认证体系
     *
     * 认证流程:
     * 1. 客户端 → KDC (AS): 请求 TGT (Ticket Granting Ticket)
     * 2. KDC → 客户端: 返回 TGT (用客户端密钥加密)
     * 3. 客户端 → KDC (TGS): 使用 TGT 请求 Service Ticket
     * 4. KDC → 客户端: 返回 Service Ticket
     * 5. 客户端 → 服务端: 使用 Service Ticket 认证
     */
    static class KerberosAuthManager {

        private final Configuration hadoopConf;
        private final String realm;
        private final String kdcHost;
        private UserGroupInformation currentUser;

        // 令牌缓存 (避免重复认证)
        private final ConcurrentHashMap<String, DelegationTokenInfo> tokenCache = new ConcurrentHashMap<>();

        // 认证统计
        private long totalAuthAttempts = 0;
        private long successfulAuths = 0;
        private long failedAuths = 0;

        public KerberosAuthManager(String realm, String kdcHost) {
            this.realm = realm;
            this.kdcHost = kdcHost;
            this.hadoopConf = new Configuration();
            configureKerberos();
        }

        /**
         * 配置 Kerberos 认证参数
         */
        private void configureKerberos() {
            // 基础 Kerberos 配置
            System.setProperty("java.security.krb5.realm", realm);
            System.setProperty("java.security.krb5.kdc", kdcHost);
            System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");

            // Hadoop 安全配置
            hadoopConf.set("hadoop.security.authentication", "kerberos");
            hadoopConf.set("hadoop.security.authorization", "true");
            hadoopConf.set("hadoop.rpc.protection", "authentication");

            // HDFS 安全配置
            hadoopConf.set("dfs.namenode.kerberos.principal", "nn/_HOST@" + realm);
            hadoopConf.set("dfs.datanode.kerberos.principal", "dn/_HOST@" + realm);
            hadoopConf.set("dfs.web.authentication.kerberos.principal", "HTTP/_HOST@" + realm);

            // Hive 安全配置
            hadoopConf.set("hive.server2.authentication", "KERBEROS");
            hadoopConf.set("hive.server2.authentication.kerberos.principal", "hive/_HOST@" + realm);
            hadoopConf.set("hive.metastore.kerberos.principal", "hive/_HOST@" + realm);

            // HBase 安全配置
            hadoopConf.set("hbase.security.authentication", "kerberos");
            hadoopConf.set("hbase.master.kerberos.principal", "hbase/_HOST@" + realm);
            hadoopConf.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@" + realm);

            // Kafka 安全配置
            hadoopConf.set("kafka.security.protocol", "SASL_PLAINTEXT");
            hadoopConf.set("kafka.sasl.mechanism", "GSSAPI");
            hadoopConf.set("kafka.sasl.kerberos.service.name", "kafka");

            UserGroupInformation.setConfiguration(hadoopConf);

            System.out.println("✅ Kerberos 配置完成 - Realm: " + realm + ", KDC: " + kdcHost);
        }

        /**
         * 使用 Keytab 进行认证 (服务间认证)
         */
        public boolean authenticateWithKeytab(String principal, String keytabPath) {
            totalAuthAttempts++;
            try {
                System.out.println("🔐 Keytab 认证 - Principal: " + principal);
                UserGroupInformation.loginUserFromKeytab(principal, keytabPath);
                currentUser = UserGroupInformation.getLoginUser();
                successfulAuths++;
                System.out.println("  ✅ 认证成功! 用户: " + currentUser.getUserName());
                System.out.println("  📋 有效凭证: " + currentUser.getCredentials().numberOfTokens() + " 个令牌");
                return true;
            } catch (Exception e) {
                failedAuths++;
                System.out.println("  ❌ 认证失败: " + e.getMessage());
                return false;
            }
        }

        /**
         * 使用密码进行认证 (交互式认证)
         */
        public boolean authenticateWithPassword(String principal, String password) {
            totalAuthAttempts++;
            try {
                System.out.println("🔑 密码认证 - Principal: " + principal);
                // 在实际环境中使用 javax.security.auth.login 进行 JAAS 认证
                // LoginContext lc = new LoginContext("Client", new PasswordCallback(principal, password));
                // lc.login();
                successfulAuths++;
                System.out.println("  ✅ 密码认证成功");
                return true;
            } catch (Exception e) {
                failedAuths++;
                System.out.println("  ❌ 认证失败: " + e.getMessage());
                return false;
            }
        }

        /**
         * 获取委托令牌 (Delegation Token) - 用于长时间运行的作业
         */
        public String getDelegationToken(String service, String renewer) {
            try {
                String tokenId = UUID.randomUUID().toString().substring(0, 8);
                long expireTime = System.currentTimeMillis() + 24 * 3600 * 1000; // 24小时有效

                DelegationTokenInfo tokenInfo = new DelegationTokenInfo(
                        tokenId, service, renewer, System.currentTimeMillis(), expireTime
                );
                tokenCache.put(tokenId, tokenInfo);

                System.out.println("🎫 委托令牌已获取:");
                System.out.println("  Token ID: " + tokenId);
                System.out.println("  服务: " + service);
                System.out.println("  更新者: " + renewer);
                System.out.println("  过期时间: " + new Date(expireTime));

                return tokenId;
            } catch (Exception e) {
                System.out.println("❌ 获取委托令牌失败: " + e.getMessage());
                return null;
            }
        }

        /**
         * 刷新令牌 (TGT 续期)
         */
        public void renewToken(String tokenId) {
            DelegationTokenInfo token = tokenCache.get(tokenId);
            if (token != null) {
                long newExpiry = System.currentTimeMillis() + 24 * 3600 * 1000;
                token.expireTime = newExpiry;
                System.out.println("🔄 令牌已续期 - Token: " + tokenId + ", 新过期时间: " + new Date(newExpiry));
            }
        }

        /**
         * 检查认证状态
         */
        public Map<String, Object> getAuthStatus() {
            Map<String, Object> status = new LinkedHashMap<>();
            status.put("realm", realm);
            status.put("kdc", kdcHost);
            status.put("authenticated", currentUser != null);
            status.put("currentUser", currentUser != null ? currentUser.getUserName() : "N/A");
            status.put("totalAttempts", totalAuthAttempts);
            status.put("successRate", totalAuthAttempts > 0 ?
                    String.format("%.1f%%", (double) successfulAuths / totalAuthAttempts * 100) : "N/A");
            status.put("activeTokens", tokenCache.size());
            return status;
        }

        static class DelegationTokenInfo {
            String tokenId;
            String service;
            String renewer;
            long issueTime;
            long expireTime;

            DelegationTokenInfo(String tokenId, String service, String renewer, long issueTime, long expireTime) {
                this.tokenId = tokenId;
                this.service = service;
                this.renewer = renewer;
                this.issueTime = issueTime;
                this.expireTime = expireTime;
            }
        }
    }

    // ==================== 2. Ranger 策略管理引擎 ====================

    /**
     * Apache Ranger 策略管理 - 统一授权框架
     *
     * 策略类型:
     * 1. 基于资源的策略 (Resource-Based) - HDFS路径/Hive库表列/HBase表族
     * 2. 基于标签的策略 (Tag-Based) - 敏感标签/合规标签
     * 3. 行级过滤 (Row-Level Filter) - WHERE条件动态过滤
     * 4. 列级脱敏 (Column Masking) - 自动数据脱敏
     */
    static class RangerPolicyEngine {

        // 策略存储
        private final List<RangerPolicy> policies = new ArrayList<>();
        private final Map<String, List<String>> roleMapping = new HashMap<>();
        private final Map<String, Set<String>> tagPolicies = new HashMap<>();

        // 审计回调
        private final List<AuditCallback> auditCallbacks = new ArrayList<>();

        // 策略评估缓存
        private final ConcurrentHashMap<String, Boolean> policyCache = new ConcurrentHashMap<>();

        // 统计
        private long totalEvaluations = 0;
        private long allowedCount = 0;
        private long deniedCount = 0;
        private long cacheHits = 0;

        public RangerPolicyEngine() {
            initializeDefaultPolicies();
        }

        /**
         * 初始化默认安全策略
         */
        private void initializeDefaultPolicies() {
            System.out.println("📜 初始化 Ranger 安全策略...\n");

            // ============== HDFS 策略 ==============
            System.out.println("=== HDFS 访问策略 ===");

            addPolicy(RangerPolicy.builder()
                    .name("hdfs-data-warehouse-admin")
                    .service("hdfs")
                    .resourceType("path")
                    .resource("/data/warehouse/**")
                    .users(Arrays.asList("hdfs_admin", "hive"))
                    .groups(Arrays.asList("data_engineers"))
                    .permissions(Arrays.asList("read", "write", "execute"))
                    .effect("ALLOW")
                    .priority(100)
                    .build());

            addPolicy(RangerPolicy.builder()
                    .name("hdfs-data-warehouse-readonly")
                    .service("hdfs")
                    .resourceType("path")
                    .resource("/data/warehouse/**")
                    .users(Collections.emptyList())
                    .groups(Arrays.asList("data_analysts", "data_scientists"))
                    .permissions(Arrays.asList("read", "execute"))
                    .effect("ALLOW")
                    .priority(50)
                    .build());

            addPolicy(RangerPolicy.builder()
                    .name("hdfs-sensitive-data-deny")
                    .service("hdfs")
                    .resourceType("path")
                    .resource("/data/sensitive/**")
                    .users(Collections.emptyList())
                    .groups(Arrays.asList("data_analysts"))
                    .permissions(Arrays.asList("read", "write"))
                    .effect("DENY")
                    .priority(200)
                    .build());

            // ============== Hive 策略 ==============
            System.out.println("=== Hive 访问策略 ===");

            addPolicy(RangerPolicy.builder()
                    .name("hive-ods-full-access")
                    .service("hive")
                    .resourceType("database:table:column")
                    .resource("ods_db:*:*")
                    .users(Arrays.asList("etl_service"))
                    .groups(Arrays.asList("data_engineers"))
                    .permissions(Arrays.asList("select", "insert", "update", "delete", "create", "alter", "drop"))
                    .effect("ALLOW")
                    .priority(100)
                    .build());

            addPolicy(RangerPolicy.builder()
                    .name("hive-dw-select-only")
                    .service("hive")
                    .resourceType("database:table:column")
                    .resource("dw_db:*:*")
                    .users(Collections.emptyList())
                    .groups(Arrays.asList("data_analysts"))
                    .permissions(Arrays.asList("select"))
                    .effect("ALLOW")
                    .priority(50)
                    .build());

            // 列级权限 - 敏感字段禁止访问
            addPolicy(RangerPolicy.builder()
                    .name("hive-mask-sensitive-columns")
                    .service("hive")
                    .resourceType("database:table:column")
                    .resource("*:user_info:id_card,phone,bank_card")
                    .users(Collections.emptyList())
                    .groups(Arrays.asList("data_analysts", "report_viewers"))
                    .permissions(Arrays.asList("select"))
                    .effect("DENY")
                    .maskType("MASK_HASH")
                    .priority(300)
                    .build());

            // 行级过滤 - 只能看到自己部门的数据
            addPolicy(RangerPolicy.builder()
                    .name("hive-row-filter-by-dept")
                    .service("hive")
                    .resourceType("database:table")
                    .resource("dw_db:employee_info")
                    .users(Collections.emptyList())
                    .groups(Arrays.asList("dept_managers"))
                    .permissions(Arrays.asList("select"))
                    .effect("ALLOW")
                    .rowFilter("dept_id = {user.dept}")
                    .priority(150)
                    .build());

            // ============== HBase 策略 ==============
            System.out.println("=== HBase 访问策略 ===");

            addPolicy(RangerPolicy.builder()
                    .name("hbase-user-profile-access")
                    .service("hbase")
                    .resourceType("table:column_family:column")
                    .resource("user_profile:basic_info:*")
                    .users(Collections.emptyList())
                    .groups(Arrays.asList("app_services"))
                    .permissions(Arrays.asList("read", "write"))
                    .effect("ALLOW")
                    .priority(100)
                    .build());

            addPolicy(RangerPolicy.builder()
                    .name("hbase-user-profile-sensitive-deny")
                    .service("hbase")
                    .resourceType("table:column_family:column")
                    .resource("user_profile:sensitive_info:*")
                    .users(Collections.emptyList())
                    .groups(Arrays.asList("app_services"))
                    .permissions(Arrays.asList("read"))
                    .effect("DENY")
                    .priority(200)
                    .build());

            // ============== Kafka 策略 ==============
            System.out.println("=== Kafka 访问策略 ===");

            addPolicy(RangerPolicy.builder()
                    .name("kafka-producer-access")
                    .service("kafka")
                    .resourceType("topic")
                    .resource("user_events,order_events,page_views")
                    .users(Arrays.asList("producer_service"))
                    .groups(Arrays.asList("stream_producers"))
                    .permissions(Arrays.asList("publish"))
                    .effect("ALLOW")
                    .priority(100)
                    .build());

            addPolicy(RangerPolicy.builder()
                    .name("kafka-consumer-access")
                    .service("kafka")
                    .resourceType("topic")
                    .resource("user_events,order_events")
                    .users(Collections.emptyList())
                    .groups(Arrays.asList("stream_consumers", "flink_jobs"))
                    .permissions(Arrays.asList("consume"))
                    .effect("ALLOW")
                    .priority(100)
                    .build());

            // ============== 标签策略 ==============
            System.out.println("=== 标签安全策略 ===");

            addTagPolicy("PII", new HashSet<>(Arrays.asList("data_security_admin")),
                    "只有安全管理员可以访问 PII (个人身份信息) 标记的资源");
            addTagPolicy("FINANCE", new HashSet<>(Arrays.asList("finance_team", "cfo")),
                    "金融相关数据只有财务团队可以访问");
            addTagPolicy("PHI", new HashSet<>(Arrays.asList("medical_team")),
                    "健康医疗信息只有医疗团队可以访问");
            addTagPolicy("CLASSIFIED", new HashSet<>(Arrays.asList("security_admin")),
                    "机密数据只有安全管理员可以访问");

            // ============== 角色映射 ==============
            System.out.println("=== 角色体系 ===");

            roleMapping.put("SUPER_ADMIN", Arrays.asList("hdfs_admin", "hive_admin", "hbase_admin"));
            roleMapping.put("DATA_ENGINEER", Arrays.asList("etl_service", "spark_service"));
            roleMapping.put("DATA_ANALYST", Arrays.asList("analyst_wang", "analyst_li", "analyst_zhang"));
            roleMapping.put("DATA_SCIENTIST", Arrays.asList("ml_engineer_chen", "ml_engineer_zhao"));
            roleMapping.put("APP_DEVELOPER", Arrays.asList("app_service_01", "app_service_02"));
            roleMapping.put("REPORT_VIEWER", Arrays.asList("manager_liu", "director_wu"));

            roleMapping.forEach((role, users) ->
                    System.out.println("  👥 " + role + " → " + users));

            System.out.println("\n✅ 共加载 " + policies.size() + " 条策略, "
                    + tagPolicies.size() + " 条标签策略, "
                    + roleMapping.size() + " 个角色\n");
        }

        /**
         * 添加策略
         */
        public void addPolicy(RangerPolicy policy) {
            policies.add(policy);
            policyCache.clear(); // 清空缓存
            System.out.println("  📋 策略: " + policy.name
                    + " [" + policy.service + "] "
                    + policy.resource
                    + " → " + policy.effect
                    + " (" + String.join(",", policy.permissions) + ")");
        }

        /**
         * 添加标签策略
         */
        public void addTagPolicy(String tag, Set<String> allowedGroups, String description) {
            tagPolicies.put(tag, allowedGroups);
            System.out.println("  🏷️ 标签: " + tag + " → 允许: " + allowedGroups + " | " + description);
        }

        /**
         * 策略评估 - 核心鉴权逻辑
         *
         * 评估优先级:
         * 1. DENY 策略优先于 ALLOW 策略
         * 2. 高优先级数字优先于低优先级
         * 3. 精确匹配优先于通配符匹配
         * 4. 标签策略与资源策略并行检查
         */
        public AccessResult evaluateAccess(String user, String group, String service,
                                           String resource, String permission) {
            totalEvaluations++;
            long startTime = System.nanoTime();

            // 1. 检查缓存
            String cacheKey = user + "|" + group + "|" + service + "|" + resource + "|" + permission;
            Boolean cached = policyCache.get(cacheKey);
            if (cached != null) {
                cacheHits++;
                return new AccessResult(cached, cached ? "CACHE_HIT_ALLOW" : "CACHE_HIT_DENY");
            }

            // 2. 收集匹配的策略
            List<RangerPolicy> matchingPolicies = policies.stream()
                    .filter(p -> p.service.equals(service))
                    .filter(p -> matchResource(p.resource, resource))
                    .filter(p -> p.permissions.contains(permission) || p.permissions.contains("*"))
                    .filter(p -> p.users.contains(user) || p.groups.contains(group) || p.users.contains("*"))
                    .sorted((a, b) -> Integer.compare(b.priority, a.priority)) // 高优先级优先
                    .collect(Collectors.toList());

            // 3. 按优先级评估 (DENY 优先)
            for (RangerPolicy policy : matchingPolicies) {
                if ("DENY".equals(policy.effect)) {
                    policyCache.put(cacheKey, false);
                    deniedCount++;
                    long elapsed = (System.nanoTime() - startTime) / 1000;
                    fireAuditEvent(user, service, resource, permission, false, policy.name, elapsed);
                    return new AccessResult(false, "DENIED by policy: " + policy.name);
                }
            }

            for (RangerPolicy policy : matchingPolicies) {
                if ("ALLOW".equals(policy.effect)) {
                    policyCache.put(cacheKey, true);
                    allowedCount++;
                    long elapsed = (System.nanoTime() - startTime) / 1000;
                    fireAuditEvent(user, service, resource, permission, true, policy.name, elapsed);
                    return new AccessResult(true, "ALLOWED by policy: " + policy.name);
                }
            }

            // 4. 默认拒绝 (安全第一)
            policyCache.put(cacheKey, false);
            deniedCount++;
            long elapsed = (System.nanoTime() - startTime) / 1000;
            fireAuditEvent(user, service, resource, permission, false, "DEFAULT_DENY", elapsed);
            return new AccessResult(false, "DEFAULT DENY - no matching policy");
        }

        /**
         * 资源通配符匹配
         */
        private boolean matchResource(String pattern, String resource) {
            if (pattern.equals("*") || pattern.equals(resource)) return true;
            String regex = pattern.replace("**", ".*").replace("*", "[^/]*");
            return Pattern.matches(regex, resource);
        }

        /**
         * 获取行级过滤条件
         */
        public String getRowFilter(String user, String group, String service, String resource) {
            return policies.stream()
                    .filter(p -> p.service.equals(service))
                    .filter(p -> matchResource(p.resource, resource))
                    .filter(p -> p.rowFilter != null)
                    .filter(p -> p.groups.contains(group))
                    .map(p -> p.rowFilter)
                    .findFirst()
                    .orElse(null);
        }

        /**
         * 获取列脱敏类型
         */
        public String getColumnMaskType(String user, String group, String service, String resource) {
            return policies.stream()
                    .filter(p -> p.service.equals(service))
                    .filter(p -> matchResource(p.resource, resource))
                    .filter(p -> p.maskType != null)
                    .filter(p -> p.groups.contains(group) || p.users.contains(user))
                    .map(p -> p.maskType)
                    .findFirst()
                    .orElse(null);
        }

        /**
         * 触发审计事件
         */
        private void fireAuditEvent(String user, String service, String resource,
                                    String permission, boolean allowed, String policyName, long elapsedMicros) {
            AuditEvent event = new AuditEvent(user, service, resource, permission, allowed, policyName, elapsedMicros);
            auditCallbacks.forEach(cb -> cb.onAuditEvent(event));
        }

        /**
         * 注册审计回调
         */
        public void registerAuditCallback(AuditCallback callback) {
            auditCallbacks.add(callback);
        }

        /**
         * 策略统计
         */
        public Map<String, Object> getPolicyStats() {
            Map<String, Object> stats = new LinkedHashMap<>();
            stats.put("totalPolicies", policies.size());
            stats.put("tagPolicies", tagPolicies.size());
            stats.put("totalEvaluations", totalEvaluations);
            stats.put("allowed", allowedCount);
            stats.put("denied", deniedCount);
            stats.put("cacheHits", cacheHits);
            stats.put("cacheHitRate", totalEvaluations > 0 ?
                    String.format("%.1f%%", (double) cacheHits / totalEvaluations * 100) : "N/A");
            return stats;
        }

        // ---- 策略 POJO ----
        static class RangerPolicy {
            String name, service, resourceType, resource, effect;
            List<String> users, groups, permissions;
            String maskType, rowFilter;
            int priority;

            static RangerPolicyBuilder builder() { return new RangerPolicyBuilder(); }

            static class RangerPolicyBuilder {
                RangerPolicy p = new RangerPolicy();
                RangerPolicyBuilder name(String v) { p.name = v; return this; }
                RangerPolicyBuilder service(String v) { p.service = v; return this; }
                RangerPolicyBuilder resourceType(String v) { p.resourceType = v; return this; }
                RangerPolicyBuilder resource(String v) { p.resource = v; return this; }
                RangerPolicyBuilder users(List<String> v) { p.users = v; return this; }
                RangerPolicyBuilder groups(List<String> v) { p.groups = v; return this; }
                RangerPolicyBuilder permissions(List<String> v) { p.permissions = v; return this; }
                RangerPolicyBuilder effect(String v) { p.effect = v; return this; }
                RangerPolicyBuilder maskType(String v) { p.maskType = v; return this; }
                RangerPolicyBuilder rowFilter(String v) { p.rowFilter = v; return this; }
                RangerPolicyBuilder priority(int v) { p.priority = v; return this; }
                RangerPolicy build() { return p; }
            }
        }

        static class AccessResult {
            boolean allowed;
            String reason;
            AccessResult(boolean allowed, String reason) {
                this.allowed = allowed;
                this.reason = reason;
            }
        }

        static class AuditEvent {
            String user, service, resource, permission, policyName;
            boolean allowed;
            long elapsedMicros;
            long timestamp = System.currentTimeMillis();

            AuditEvent(String user, String service, String resource, String permission,
                       boolean allowed, String policyName, long elapsedMicros) {
                this.user = user;
                this.service = service;
                this.resource = resource;
                this.permission = permission;
                this.allowed = allowed;
                this.policyName = policyName;
                this.elapsedMicros = elapsedMicros;
            }
        }

        interface AuditCallback {
            void onAuditEvent(AuditEvent event);
        }
    }

    // ==================== 3. 数据脱敏引擎 ====================

    /**
     * 数据脱敏引擎 - 支持多种脱敏算法
     *
     * 脱敏级别:
     * - L1 (低): 部分遮盖 (如手机号 138****6789)
     * - L2 (中): 大部分遮盖 (如身份证 ****1990****1234)
     * - L3 (高): 完全哈希 (如 SHA-256)
     * - L4 (极高): 替换为虚假数据 (Faker)
     */
    static class DataMaskingEngine {

        private final Map<String, MaskingRule> rules = new LinkedHashMap<>();
        private long totalMasked = 0;

        public DataMaskingEngine() {
            initDefaultRules();
        }

        private void initDefaultRules() {
            System.out.println("🎭 初始化数据脱敏规则...\n");

            // 手机号脱敏: 138****6789
            addRule("PHONE", new MaskingRule("手机号", "1[3-9]\\d{9}",
                    input -> input.substring(0, 3) + "****" + input.substring(7), "L1"));

            // 身份证脱敏: ****19900101****
            addRule("ID_CARD", new MaskingRule("身份证号", "\\d{17}[\\dXx]",
                    input -> "****" + input.substring(4, 12) + "****" + input.substring(16), "L2"));

            // 姓名脱敏: 张*明 或 张**
            addRule("NAME", new MaskingRule("姓名", ".{2,5}",
                    input -> {
                        if (input.length() == 2) return input.charAt(0) + "*";
                        return input.charAt(0) + "*".repeat(input.length() - 2) + input.charAt(input.length() - 1);
                    }, "L1"));

            // 邮箱脱敏: z***g@example.com
            addRule("EMAIL", new MaskingRule("邮箱", "[\\w.]+@[\\w.]+",
                    input -> {
                        int at = input.indexOf('@');
                        if (at <= 2) return "***" + input.substring(at);
                        return input.charAt(0) + "***" + input.charAt(at - 1) + input.substring(at);
                    }, "L1"));

            // 银行卡脱敏: 6222 **** **** 1234
            addRule("BANK_CARD", new MaskingRule("银行卡号", "\\d{16,19}",
                    input -> input.substring(0, 4) + " **** **** " + input.substring(input.length() - 4), "L2"));

            // 地址脱敏: 北京市***
            addRule("ADDRESS", new MaskingRule("地址", ".{5,100}",
                    input -> input.substring(0, Math.min(3, input.length())) + "***", "L2"));

            // IP 脱敏: 192.168.*.*
            addRule("IP", new MaskingRule("IP地址", "\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}",
                    input -> {
                        String[] parts = input.split("\\.");
                        return parts[0] + "." + parts[1] + ".*.*";
                    }, "L1"));

            // SHA-256 哈希
            addRule("HASH", new MaskingRule("SHA-256哈希", ".*",
                    input -> {
                        try {
                            MessageDigest digest = MessageDigest.getInstance("SHA-256");
                            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
                            StringBuilder sb = new StringBuilder();
                            for (byte b : hash) sb.append(String.format("%02x", b));
                            return sb.toString().substring(0, 16) + "...";
                        } catch (Exception e) {
                            return "HASH_ERROR";
                        }
                    }, "L3"));

            System.out.println("✅ 共加载 " + rules.size() + " 条脱敏规则\n");
        }

        private void addRule(String type, MaskingRule rule) {
            rules.put(type, rule);
            System.out.println("  🎭 " + type + " (" + rule.name + ") - 级别: " + rule.level);
        }

        /**
         * 执行脱敏
         */
        public String mask(String type, String input) {
            if (input == null || input.isEmpty()) return input;
            MaskingRule rule = rules.get(type);
            if (rule == null) return input;
            totalMasked++;
            return rule.maskFunction.apply(input);
        }

        /**
         * 自动识别并脱敏
         */
        public String autoMask(String input) {
            if (input == null) return null;
            // 手机号
            if (input.matches("1[3-9]\\d{9}")) return mask("PHONE", input);
            // 身份证
            if (input.matches("\\d{17}[\\dXx]")) return mask("ID_CARD", input);
            // 邮箱
            if (input.matches("[\\w.]+@[\\w.]+\\.[\\w]+")) return mask("EMAIL", input);
            // 银行卡
            if (input.matches("\\d{16,19}")) return mask("BANK_CARD", input);
            // IP
            if (input.matches("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}")) return mask("IP", input);
            return input;
        }

        /**
         * 批量脱敏数据记录
         */
        public Map<String, String> maskRecord(Map<String, String> record, Map<String, String> fieldTypeMapping) {
            Map<String, String> masked = new LinkedHashMap<>();
            record.forEach((field, value) -> {
                String maskType = fieldTypeMapping.get(field);
                masked.put(field, maskType != null ? mask(maskType, value) : value);
            });
            return masked;
        }

        /**
         * 演示脱敏效果
         */
        public void demonstrateMasking() {
            System.out.println("=== 数据脱敏效果演示 ===\n");

            String[][] samples = {
                    {"PHONE", "13812345678"},
                    {"PHONE", "15998765432"},
                    {"ID_CARD", "110101199001011234"},
                    {"NAME", "张三"},
                    {"NAME", "欧阳明"},
                    {"EMAIL", "zhangsan@example.com"},
                    {"EMAIL", "ab@test.com"},
                    {"BANK_CARD", "6222021234567890123"},
                    {"ADDRESS", "北京市朝阳区建国路88号"},
                    {"IP", "192.168.1.100"},
                    {"HASH", "password123"},
            };

            System.out.printf("  %-10s %-28s → %-28s%n", "类型", "原始数据", "脱敏后");
            System.out.println("  " + "-".repeat(72));
            for (String[] s : samples) {
                System.out.printf("  %-10s %-28s → %-28s%n", s[0], s[1], mask(s[0], s[1]));
            }

            System.out.println("\n--- 自动识别脱敏 ---");
            String[] autoSamples = {"13812345678", "110101199001011234", "test@qq.com", "192.168.1.1", "普通文本"};
            for (String s : autoSamples) {
                System.out.println("  " + s + " → " + autoMask(s));
            }
        }

        static class MaskingRule {
            String name, pattern, level;
            java.util.function.Function<String, String> maskFunction;

            MaskingRule(String name, String pattern,
                        java.util.function.Function<String, String> maskFunction, String level) {
                this.name = name;
                this.pattern = pattern;
                this.maskFunction = maskFunction;
                this.level = level;
            }
        }
    }

    // ==================== 4. 数据加密引擎 ====================

    /**
     * 数据加密引擎 - AES-256-GCM / 列级加密 / TDE
     */
    static class DataEncryptionEngine {

        private SecretKey masterKey;
        private final Map<String, SecretKey> columnKeys = new HashMap<>();
        private static final int GCM_IV_LENGTH = 12;
        private static final int GCM_TAG_LENGTH = 128;

        public DataEncryptionEngine() {
            initializeKeys();
        }

        private void initializeKeys() {
            try {
                System.out.println("🔒 初始化加密引擎...\n");

                // 生成主密钥 (Master Key)
                KeyGenerator keyGen = KeyGenerator.getInstance("AES");
                keyGen.init(256);
                masterKey = keyGen.generateKey();
                System.out.println("  🔑 主密钥 (AES-256) 已生成");

                // 生成列级加密密钥
                String[] sensitiveColumns = {"phone", "id_card", "bank_card", "email", "salary"};
                for (String col : sensitiveColumns) {
                    columnKeys.put(col, keyGen.generateKey());
                    System.out.println("  🔑 列密钥: " + col);
                }

                System.out.println("\n✅ 加密引擎就绪 - " + columnKeys.size() + " 个列密钥\n");
            } catch (Exception e) {
                System.out.println("❌ 加密引擎初始化失败: " + e.getMessage());
            }
        }

        /**
         * AES-256-GCM 加密
         */
        public byte[] encrypt(String plaintext, SecretKey key) throws Exception {
            byte[] iv = new byte[GCM_IV_LENGTH];
            new SecureRandom().nextBytes(iv);

            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH, iv);
            cipher.init(Cipher.ENCRYPT_MODE, key, spec);

            byte[] ciphertext = cipher.doFinal(plaintext.getBytes(StandardCharsets.UTF_8));

            // IV + Ciphertext
            byte[] result = new byte[iv.length + ciphertext.length];
            System.arraycopy(iv, 0, result, 0, iv.length);
            System.arraycopy(ciphertext, 0, result, iv.length, ciphertext.length);
            return result;
        }

        /**
         * AES-256-GCM 解密
         */
        public String decrypt(byte[] encrypted, SecretKey key) throws Exception {
            byte[] iv = Arrays.copyOfRange(encrypted, 0, GCM_IV_LENGTH);
            byte[] ciphertext = Arrays.copyOfRange(encrypted, GCM_IV_LENGTH, encrypted.length);

            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH, iv);
            cipher.init(Cipher.DECRYPT_MODE, key, spec);

            return new String(cipher.doFinal(ciphertext), StandardCharsets.UTF_8);
        }

        /**
         * 列级加密
         */
        public byte[] encryptColumn(String columnName, String value) throws Exception {
            SecretKey key = columnKeys.getOrDefault(columnName, masterKey);
            return encrypt(value, key);
        }

        /**
         * 列级解密
         */
        public String decryptColumn(String columnName, byte[] encrypted) throws Exception {
            SecretKey key = columnKeys.getOrDefault(columnName, masterKey);
            return decrypt(encrypted, key);
        }

        /**
         * 演示加密解密
         */
        public void demonstrateEncryption() throws Exception {
            System.out.println("=== 数据加密演示 ===\n");

            String[][] samples = {
                    {"phone", "13812345678"},
                    {"id_card", "110101199001011234"},
                    {"bank_card", "6222021234567890123"},
                    {"salary", "50000.00"},
            };

            for (String[] sample : samples) {
                String col = sample[0], value = sample[1];
                byte[] encrypted = encryptColumn(col, value);
                String decrypted = decryptColumn(col, encrypted);

                System.out.println("  列: " + col);
                System.out.println("    原文: " + value);
                System.out.println("    密文: " + Base64.getEncoder().encodeToString(encrypted).substring(0, 32) + "...");
                System.out.println("    解密: " + decrypted);
                System.out.println("    验证: " + (value.equals(decrypted) ? "✅ 一致" : "❌ 不一致"));
                System.out.println();
            }
        }
    }

    // ==================== 5. 安全审计系统 ====================

    /**
     * 安全审计系统 - 操作日志 + 异常检测 + 合规报告
     */
    static class SecurityAuditSystem implements RangerPolicyEngine.AuditCallback {

        private final List<RangerPolicyEngine.AuditEvent> auditLog = new CopyOnWriteArrayList<>();
        private final Map<String, Integer> userAccessCount = new ConcurrentHashMap<>();
        private final Map<String, Integer> deniedAccessCount = new ConcurrentHashMap<>();

        // 异常检测阈值
        private static final int MAX_DENIED_PER_MINUTE = 10;
        private static final int MAX_ACCESS_PER_MINUTE = 100;

        @Override
        public void onAuditEvent(RangerPolicyEngine.AuditEvent event) {
            auditLog.add(event);

            // 统计用户访问
            userAccessCount.merge(event.user, 1, Integer::sum);
            if (!event.allowed) {
                deniedAccessCount.merge(event.user, 1, Integer::sum);
            }

            // 异常检测
            detectAnomalies(event);
        }

        /**
         * 异常行为检测
         */
        private void detectAnomalies(RangerPolicyEngine.AuditEvent event) {
            int deniedCount = deniedAccessCount.getOrDefault(event.user, 0);
            int totalCount = userAccessCount.getOrDefault(event.user, 0);

            if (deniedCount > MAX_DENIED_PER_MINUTE) {
                System.out.println("🚨 [安全告警] 用户 " + event.user
                        + " 短时间内被拒绝 " + deniedCount + " 次! 疑似暴力访问!");
            }

            if (totalCount > MAX_ACCESS_PER_MINUTE) {
                System.out.println("⚠️ [安全告警] 用户 " + event.user
                        + " 访问频率异常: " + totalCount + " 次/分钟");
            }
        }

        /**
         * 生成安全合规报告
         */
        public void generateComplianceReport() {
            System.out.println("\n" + "=".repeat(60));
            System.out.println("📊 安全合规审计报告");
            System.out.println("=".repeat(60));

            // 总览
            long total = auditLog.size();
            long allowed = auditLog.stream().filter(e -> e.allowed).count();
            long denied = total - allowed;

            System.out.println("\n📈 访问统计:");
            System.out.println("  总请求数: " + total);
            System.out.println("  允许: " + allowed + " (" + (total > 0 ? allowed * 100 / total : 0) + "%)");
            System.out.println("  拒绝: " + denied + " (" + (total > 0 ? denied * 100 / total : 0) + "%)");

            // 按服务统计
            System.out.println("\n📊 服务维度:");
            auditLog.stream()
                    .collect(Collectors.groupingBy(e -> e.service, Collectors.counting()))
                    .forEach((service, count) ->
                            System.out.println("  " + service + ": " + count + " 次"));

            // 被拒绝最多的用户
            System.out.println("\n🚫 拒绝访问 Top 用户:");
            deniedAccessCount.entrySet().stream()
                    .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                    .limit(5)
                    .forEach(e -> System.out.println("  " + e.getKey() + ": " + e.getValue() + " 次"));

            // 策略命中统计
            System.out.println("\n📋 策略命中统计:");
            auditLog.stream()
                    .collect(Collectors.groupingBy(e -> e.policyName, Collectors.counting()))
                    .entrySet().stream()
                    .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                    .limit(10)
                    .forEach(e -> System.out.println("  " + e.getKey() + ": " + e.getValue() + " 次"));

            // GDPR 合规检查
            System.out.println("\n🏛️ GDPR / 等保三级 合规检查:");
            System.out.println("  ✅ 访问控制: 已启用 RBAC + ABAC");
            System.out.println("  ✅ 数据脱敏: 已配置列级脱敏");
            System.out.println("  ✅ 审计日志: 完整记录所有访问");
            System.out.println("  ✅ 数据加密: AES-256-GCM 列级加密");
            System.out.println("  ✅ 最小权限: 默认拒绝策略");
            System.out.println("  " + (denied > total * 0.3 ? "⚠️" : "✅")
                    + " 拒绝率: " + (total > 0 ? denied * 100 / total : 0) + "% "
                    + (denied > total * 0.3 ? "(过高,请检查策略配置)" : "(正常范围)"));

            System.out.println("\n" + "=".repeat(60));
        }
    }

    // ==================== 6. 数据分级分类 ====================

    /**
     * 数据分级分类 - 自动发现敏感数据并标记
     */
    static class DataClassifier {

        enum SensitivityLevel {
            PUBLIC("公开", 0),
            INTERNAL("内部", 1),
            CONFIDENTIAL("机密", 2),
            SECRET("绝密", 3);

            String label;
            int level;
            SensitivityLevel(String label, int level) { this.label = label; this.level = level; }
        }

        // 敏感字段模式
        private final Map<String, SensitivityLevel> fieldPatterns = new LinkedHashMap<>();

        public DataClassifier() {
            // 绝密
            fieldPatterns.put("password|passwd|secret|private_key", SensitivityLevel.SECRET);
            fieldPatterns.put("id_card|ssn|social_security", SensitivityLevel.SECRET);

            // 机密
            fieldPatterns.put("phone|mobile|telephone", SensitivityLevel.CONFIDENTIAL);
            fieldPatterns.put("email|mail", SensitivityLevel.CONFIDENTIAL);
            fieldPatterns.put("bank_card|credit_card|account_no", SensitivityLevel.CONFIDENTIAL);
            fieldPatterns.put("salary|income|revenue", SensitivityLevel.CONFIDENTIAL);
            fieldPatterns.put("address|location|gps", SensitivityLevel.CONFIDENTIAL);

            // 内部
            fieldPatterns.put("name|user_name|real_name", SensitivityLevel.INTERNAL);
            fieldPatterns.put("age|gender|birthday|birth_date", SensitivityLevel.INTERNAL);
            fieldPatterns.put("dept|department|team", SensitivityLevel.INTERNAL);
        }

        /**
         * 自动分类表字段
         */
        public Map<String, SensitivityLevel> classifyTable(String tableName, List<String> columns) {
            System.out.println("🔍 扫描表: " + tableName + " (" + columns.size() + " 列)");
            Map<String, SensitivityLevel> result = new LinkedHashMap<>();

            for (String col : columns) {
                SensitivityLevel level = classifyColumn(col);
                result.put(col, level);
                String icon = level == SensitivityLevel.SECRET ? "🔴" :
                        level == SensitivityLevel.CONFIDENTIAL ? "🟡" :
                                level == SensitivityLevel.INTERNAL ? "🔵" : "⚪";
                System.out.println("  " + icon + " " + col + " → " + level.label + " (" + level.name() + ")");
            }

            long secretCount = result.values().stream().filter(v -> v == SensitivityLevel.SECRET).count();
            long confCount = result.values().stream().filter(v -> v == SensitivityLevel.CONFIDENTIAL).count();
            System.out.println("  📊 结果: " + secretCount + " 绝密, " + confCount + " 机密, "
                    + (columns.size() - secretCount - confCount) + " 其他\n");

            return result;
        }

        private SensitivityLevel classifyColumn(String columnName) {
            String lower = columnName.toLowerCase();
            for (Map.Entry<String, SensitivityLevel> entry : fieldPatterns.entrySet()) {
                if (Pattern.compile(entry.getKey()).matcher(lower).find()) {
                    return entry.getValue();
                }
            }
            return SensitivityLevel.PUBLIC;
        }
    }

    // ==================== Main 方法 ====================

    public static void main(String[] args) throws Exception {
        System.out.println("╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║     大数据安全体系实战 - Ranger + Kerberos + 脱敏 + 审计     ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝\n");

        // ===== 1. Kerberos 认证 =====
        System.out.println("━━━━━━ 1. Kerberos 认证管理 ━━━━━━\n");
        KerberosAuthManager authManager = new KerberosAuthManager("BIGDATA.COM", "kdc.bigdata.com");

        // 模拟 Keytab 认证
        authManager.authenticateWithKeytab("hdfs/namenode@BIGDATA.COM", "/etc/keytabs/hdfs.keytab");
        authManager.authenticateWithKeytab("hive/hiveserver@BIGDATA.COM", "/etc/keytabs/hive.keytab");

        // 获取委托令牌
        String token1 = authManager.getDelegationToken("hdfs://namenode:8020", "yarn");
        String token2 = authManager.getDelegationToken("hive://hiveserver2:10000", "spark");

        System.out.println("\n📊 认证状态: " + authManager.getAuthStatus());

        // ===== 2. Ranger 策略引擎 =====
        System.out.println("\n━━━━━━ 2. Ranger 策略评估 ━━━━━━\n");
        RangerPolicyEngine policyEngine = new RangerPolicyEngine();

        // 注册审计
        SecurityAuditSystem auditSystem = new SecurityAuditSystem();
        policyEngine.registerAuditCallback(auditSystem);

        // 模拟各种访问场景
        System.out.println("=== 访问控制测试 ===\n");
        String[][] accessTests = {
                {"hdfs_admin",      "data_engineers",   "hdfs", "/data/warehouse/ods/user", "read",   "管理员读仓库"},
                {"analyst_wang",    "data_analysts",    "hdfs", "/data/warehouse/dw/sales", "read",   "分析师读仓库"},
                {"analyst_wang",    "data_analysts",    "hdfs", "/data/sensitive/pii",      "read",   "分析师读敏感数据"},
                {"etl_service",     "data_engineers",   "hive", "ods_db:user_log:*",        "insert", "ETL写ODS"},
                {"analyst_li",      "data_analysts",    "hive", "dw_db:sales:*",            "select", "分析师查DW"},
                {"analyst_li",      "data_analysts",    "hive", "ods_db:raw_data:*",        "insert", "分析师写ODS"},
                {"analyst_zhang",   "data_analysts",    "hive", "*:user_info:phone",        "select", "分析师查手机号"},
                {"app_service_01",  "app_services",     "hbase","user_profile:basic_info:*", "read",  "应用读基本信息"},
                {"app_service_01",  "app_services",     "hbase","user_profile:sensitive_info:*","read","应用读敏感信息"},
                {"producer_service","stream_producers", "kafka","user_events",               "publish","生产者发消息"},
                {"unknown_user",    "unknown_group",    "hdfs", "/data/warehouse/ods",       "write", "未知用户写仓库"},
        };

        for (String[] test : accessTests) {
            RangerPolicyEngine.AccessResult result = policyEngine.evaluateAccess(
                    test[0], test[1], test[2], test[3], test[4]);
            System.out.printf("  %s %s | %-20s → %-30s [%s] %s%n",
                    result.allowed ? "✅" : "🚫",
                    test[5],
                    test[0],
                    test[2] + ":" + test[3],
                    test[4],
                    result.reason);
        }

        // ===== 3. 数据脱敏 =====
        System.out.println("\n━━━━━━ 3. 数据脱敏引擎 ━━━━━━\n");
        DataMaskingEngine maskingEngine = new DataMaskingEngine();
        maskingEngine.demonstrateMasking();

        // 批量记录脱敏
        System.out.println("\n--- 批量记录脱敏 ---");
        Map<String, String> record = new LinkedHashMap<>();
        record.put("user_id", "U10001");
        record.put("name", "张三丰");
        record.put("phone", "13912345678");
        record.put("id_card", "310101198512121234");
        record.put("email", "zhangsan@company.com");
        record.put("salary", "35000");

        Map<String, String> fieldTypes = new LinkedHashMap<>();
        fieldTypes.put("name", "NAME");
        fieldTypes.put("phone", "PHONE");
        fieldTypes.put("id_card", "ID_CARD");
        fieldTypes.put("email", "EMAIL");

        Map<String, String> maskedRecord = maskingEngine.maskRecord(record, fieldTypes);
        System.out.println("  原始: " + record);
        System.out.println("  脱敏: " + maskedRecord);

        // ===== 4. 数据加密 =====
        System.out.println("\n━━━━━━ 4. 数据加密引擎 ━━━━━━\n");
        DataEncryptionEngine encryptionEngine = new DataEncryptionEngine();
        encryptionEngine.demonstrateEncryption();

        // ===== 5. 数据分级分类 =====
        System.out.println("━━━━━━ 5. 数据分级分类 ━━━━━━\n");
        DataClassifier classifier = new DataClassifier();

        classifier.classifyTable("user_info", Arrays.asList(
                "user_id", "user_name", "real_name", "phone", "email",
                "id_card", "bank_card", "password", "age", "gender",
                "address", "salary", "dept", "create_time", "status"
        ));

        classifier.classifyTable("order_info", Arrays.asList(
                "order_id", "user_id", "product_name", "amount", "payment_time",
                "address", "phone", "status"
        ));

        // ===== 6. 安全审计报告 =====
        System.out.println("━━━━━━ 6. 安全审计报告 ━━━━━━");
        auditSystem.generateComplianceReport();

        // ===== 7. 策略统计 =====
        System.out.println("\n📊 Ranger 策略引擎统计: " + policyEngine.getPolicyStats());

        System.out.println("\n🎉 大数据安全体系演示完成!");
    }
}
