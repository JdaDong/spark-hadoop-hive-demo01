# HDFS 高级特性使用指南

本文档详细介绍 HDFS 高级特性的使用方法和最佳实践。

## 📑 目录

1. [快照管理](#快照管理)
2. [配额管理](#配额管理)
3. [文件压缩](#文件压缩)
4. [存储策略](#存储策略)
5. [ACL权限管理](#acl权限管理)
6. [缓存管理](#缓存管理)
7. [性能优化](#性能优化)

---

## 快照管理

### 概述

快照(Snapshot)是文件系统在某个时刻的只读副本。HDFS 快照使用写时复制(Copy-on-Write)技术,不会复制数据,只记录变更。

### 应用场景

- **数据备份**: 定期创建快照作为备份点
- **数据恢复**: 误删除或数据损坏时快速恢复
- **测试环境**: 在快照上进行测试,不影响生产数据
- **审计合规**: 保留历史数据状态用于审计

### 使用方法

#### 1. 启用快照

```bash
# 允许目录创建快照
hdfs dfsadmin -allowSnapshot /user/data

# 查看可快照目录
hdfs lsSnapshottableDir
```

#### 2. 创建快照

```bash
# 创建快照
hdfs dfs -createSnapshot /user/data snap_$(date +%Y%m%d)

# 创建带时间戳的快照
hdfs dfs -createSnapshot /user/data snap_$(date +%Y%m%d_%H%M%S)
```

#### 3. 查看快照

```bash
# 列出所有快照
hdfs dfs -ls /user/data/.snapshot

# 查看特定快照内容
hdfs dfs -ls /user/data/.snapshot/snap_20240101

# 读取快照中的文件
hdfs dfs -cat /user/data/.snapshot/snap_20240101/file.txt
```

#### 4. 重命名快照

```bash
hdfs dfs -renameSnapshot /user/data snap_20240101 snap_backup_v1
```

#### 5. 恢复数据

```bash
# 方式1: 复制快照中的文件
hdfs dfs -cp /user/data/.snapshot/snap_20240101/deleted_file.txt /user/data/

# 方式2: 使用 distcp 恢复整个目录
hadoop distcp /user/data/.snapshot/snap_20240101 /user/data_restore
```

#### 6. 删除快照

```bash
hdfs dfs -deleteSnapshot /user/data snap_20240101
```

#### 7. 禁用快照

```bash
# 必须先删除所有快照
hdfs dfsadmin -disallowSnapshot /user/data
```

### Java API 使用

```java
DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.get(conf);
Path dir = new Path("/user/data");

// 启用快照
dfs.allowSnapshot(dir);

// 创建快照
Path snapshot = dfs.createSnapshot(dir, "snap1");

// 列出快照
FileStatus[] snapshots = dfs.getFileStatus(dir).getSnapshots();

// 删除快照
dfs.deleteSnapshot(dir, "snap1");

// 禁用快照
dfs.disallowSnapshot(dir);
```

### 最佳实践

1. **定期清理**: 快照会占用额外空间,定期删除旧快照
2. **命名规范**: 使用有意义的名称,如 `daily_20240101`
3. **自动化**: 使用 cron 或 Oozie 自动创建快照
4. **监控空间**: 监控快照占用的空间
5. **保留策略**: 制定快照保留策略(如保留最近7天)

### 注意事项

- 快照是只读的,不能修改快照中的文件
- 删除快照目录中的文件会导致快照占用空间
- 快照不支持嵌套(不能对快照目录创建快照)
- HDFS 快照性能开销很小

---

## 配额管理

### 概述

配额(Quota)用于限制目录的资源使用,防止某个用户或应用占用过多资源。

### 配额类型

#### 1. 名称配额 (Name Quota)

限制目录下文件和目录的总数。

```bash
# 设置名称配额(最多1000个文件/目录)
hdfs dfsadmin -setQuota 1000 /user/test

# 清除名称配额
hdfs dfsadmin -clrQuota /user/test
```

#### 2. 空间配额 (Space Quota)

限制目录占用的存储空间。

```bash
# 设置空间配额
hdfs dfsadmin -setSpaceQuota 10g /user/test     # 10GB
hdfs dfsadmin -setSpaceQuota 1t /user/test      # 1TB
hdfs dfsadmin -setSpaceQuota 1073741824 /user/test  # 1GB (字节)

# 清除空间配额
hdfs dfsadmin -clrSpaceQuota /user/test
```

### 查看配额

```bash
# 查看配额信息
hdfs dfs -count -q /user/test

# 输出格式:
# QUOTA    REM_QUOTA  SPACE_QUOTA  REM_SPACE_QUOTA  DIR_COUNT  FILE_COUNT  CONTENT_SIZE  PATH
# 1000     950        10737418240  10000000000      5          50          737418240     /user/test
```

### Java API 使用

```java
DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.get(conf);
Path dir = new Path("/user/test");

// 设置名称配额和空间配额
dfs.setQuota(dir, 1000L, 10L * 1024 * 1024 * 1024); // 1000个文件, 10GB

// 查询配额信息
ContentSummary summary = dfs.getContentSummary(dir);
long quota = summary.getQuota();
long spaceQuota = summary.getSpaceQuota();
long spaceConsumed = summary.getSpaceConsumed();

// 清除配额
dfs.setQuota(dir, HdfsConstants.QUOTA_RESET, HdfsConstants.QUOTA_RESET);
```

### 应用场景

1. **多租户管理**: 为不同租户分配固定配额
2. **成本控制**: 限制存储成本
3. **防止滥用**: 防止某个应用占用过多资源
4. **容量规划**: 按项目分配存储资源

### 最佳实践

1. **分级配额**: 在不同层级设置配额
2. **监控告警**: 配额使用超过80%时告警
3. **弹性调整**: 根据业务需求动态调整配额
4. **文档记录**: 记录配额设置的原因和历史

---

## 文件压缩

### 压缩算法对比

| 算法 | 压缩率 | 压缩速度 | 解压速度 | 可分割 | CPU | 适用场景 |
|------|--------|----------|----------|--------|-----|----------|
| **Gzip** | 高 (20-50%) | 中等 | 中等 | 否 | 中等 | 归档存储、冷数据 |
| **Snappy** | 中等 (10-30%) | 快 | 最快 | 否 (文件级别) | 低 | 实时计算、热数据 |
| **LZ4** | 中等 (10-25%) | 最快 | 最快 | 否 | 最低 | 超低延迟场景 |
| **Bzip2** | 最高 (30-60%) | 慢 | 慢 | 是 | 高 | 长期归档 |
| **Zstd** | 高 (25-50%) | 快 | 快 | 否 | 中等 | 通用场景 |

### 使用方法

#### 1. Gzip 压缩

```bash
# 压缩文件
hadoop fs -text /input/file.txt | gzip > output.txt.gz
hdfs dfs -put output.txt.gz /output/

# 使用 Hadoop 压缩
hadoop fs -Dfs.output.compression=true \
  -Dfs.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec \
  -get /input/file.txt /output/file.txt.gz
```

#### 2. Snappy 压缩

```bash
# 配置 Hadoop 使用 Snappy
export HADOOP_CLASSPATH=$HADOOP_HOME/lib/native

# MapReduce 使用 Snappy
hadoop jar myapp.jar \
  -D mapreduce.output.fileoutputformat.compress=true \
  -D mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec
```

#### 3. Java API

```java
// Gzip 压缩
Configuration conf = new Configuration();
CompressionCodec codec = new GzipCodec();
codec.setConf(conf);

Path inputPath = new Path("/input/file.txt");
Path outputPath = new Path("/output/file.txt.gz");

try (FSDataInputStream in = fs.open(inputPath);
     FSDataOutputStream out = fs.create(outputPath);
     OutputStream compressedOut = codec.createOutputStream(out)) {
    IOUtils.copyBytes(in, compressedOut, conf);
}

// Snappy 压缩
CompressionCodec snappyCodec = new SnappyCodec();
// 使用方式同上

// 自动检测压缩格式
CompressionCodecFactory factory = new CompressionCodecFactory(conf);
CompressionCodec codec = factory.getCodec(new Path("/data/file.txt.gz"));
if (codec != null) {
    // 文件已压缩
    try (InputStream in = codec.createInputStream(fs.open(path))) {
        // 读取解压后的数据
    }
}
```

### Hive 中使用压缩

```sql
-- 设置中间结果压缩
SET hive.exec.compress.intermediate=true;
SET mapreduce.map.output.compress=true;
SET mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec;

-- 设置输出压缩
SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec;

-- ORC 表使用压缩
CREATE TABLE data STORED AS ORC TBLPROPERTIES ('orc.compress'='SNAPPY');

-- Parquet 表使用压缩
CREATE TABLE data STORED AS PARQUET TBLPROPERTIES ('parquet.compression'='SNAPPY');
```

### 压缩选择建议

**实时计算场景**:
- 使用 **Snappy** 或 **LZ4**
- 优点: 压缩/解压速度快,CPU 开销小
- 适合: Spark、Flink 实时处理

**批处理场景**:
- 使用 **Snappy** 或 **Zstd**
- 平衡压缩率和性能

**归档存储**:
- 使用 **Gzip** 或 **Bzip2**
- 优点: 高压缩率,节省存储
- 适合: 冷数据、长期归档

**日志文件**:
- 使用 **Gzip**
- 文本文件压缩率高

---

## 存储策略

### 概述

HDFS 支持异构存储,可以将不同热度的数据存储在不同类型的介质上。

### 存储类型

- **RAM_DISK**: 内存
- **SSD**: 固态硬盘
- **DISK**: 普通磁盘
- **ARCHIVE**: 归档存储(高密度磁盘)

### 存储策略

| 策略 | 副本分布 | 适用场景 |
|------|----------|----------|
| **HOT** | 全部在 DISK | 热数据,频繁访问 |
| **WARM** | 1份 DISK + 1份 ARCHIVE | 温数据,偶尔访问 |
| **COLD** | 全部在 ARCHIVE | 冷数据,很少访问 |
| **ALL_SSD** | 全部在 SSD | 超热数据,极高性能 |
| **ONE_SSD** | 1份 SSD + 其他 DISK | 读多写少 |
| **LAZY_PERSIST** | 1份 RAM_DISK + 其他 DISK | 临时数据,写密集 |

### 使用方法

```bash
# 查看所有存储策略
hdfs storagepolicies -listPolicies

# 设置存储策略
hdfs storagepolicies -setStoragePolicy -path /user/hot_data -policy HOT
hdfs storagepolicies -setStoragePolicy -path /user/archive -policy COLD

# 查看路径的存储策略
hdfs storagepolicies -getStoragePolicy -path /user/hot_data

# 取消存储策略(使用默认)
hdfs storagepolicies -unsetStoragePolicy -path /user/hot_data

# 使存储策略生效(移动数据)
hdfs mover -p /user/archive
```

### Java API

```java
DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.get(conf);
Path path = new Path("/user/data");

// 设置存储策略
dfs.setStoragePolicy(path, "COLD");

// 获取存储策略
BlockStoragePolicy policy = dfs.getStoragePolicy(path);
System.out.println("Policy: " + policy.getName());

// 取消存储策略
dfs.unsetStoragePolicy(path);

// 获取所有存储策略
BlockStoragePolicy[] policies = dfs.getStoragePolicies();
for (BlockStoragePolicy p : policies) {
    System.out.println(p.getName() + ": " + p);
}
```

### 实践建议

1. **数据生命周期管理**:
   ```
   新数据 -> HOT (30天) -> WARM (90天) -> COLD (永久)
   ```

2. **自动化迁移**:
   ```bash
   # 定期运行 mover
   0 2 * * * hdfs mover -p /user/data
   ```

3. **监控存储使用**:
   ```bash
   hdfs dfsadmin -report
   ```

---

## ACL权限管理

### 概述

ACL(Access Control List)提供比传统 UNIX 权限更细粒度的访问控制。

### ACL 类型

- **Access ACL**: 控制文件/目录的访问
- **Default ACL**: 控制新创建的子文件/目录的权限

### ACL 条目格式

```
[default:]type:name:permission
```

- **type**: user, group, other, mask
- **name**: 用户名或组名
- **permission**: r, w, x

### 使用方法

#### 1. 启用 ACL

```xml
<!-- hdfs-site.xml -->
<property>
  <name>dfs.namenode.acls.enabled</name>
  <value>true</value>
</property>
```

#### 2. 设置 ACL

```bash
# 添加用户 ACL
hdfs dfs -setfacl -m user:alice:rwx /user/data

# 添加组 ACL
hdfs dfs -setfacl -m group:developers:r-x /user/data

# 设置默认 ACL
hdfs dfs -setfacl -m default:user:alice:rwx /user/data

# 删除 ACL 条目
hdfs dfs -setfacl -x user:alice /user/data

# 完全删除所有 ACL
hdfs dfs -setfacl -b /user/data

# 递归设置 ACL
hdfs dfs -setfacl -R -m user:alice:rwx /user/data
```

#### 3. 查看 ACL

```bash
hdfs dfs -getfacl /user/data

# 输出示例:
# file: /user/data
# owner: hadoop
# group: hadoop
# user::rwx
# user:alice:rwx
# group::r-x
# group:developers:r-x
# mask::rwx
# other::r-x
```

### Java API

```java
DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.get(conf);
Path path = new Path("/user/data");

// 创建 ACL 条目
List<AclEntry> aclSpec = Lists.newArrayList(
    aclEntry(ACCESS, USER, "alice", READ_WRITE),
    aclEntry(ACCESS, GROUP, "developers", READ_EXECUTE)
);

// 设置 ACL
dfs.setAcl(path, aclSpec);

// 修改 ACL
dfs.modifyAclEntries(path, aclSpec);

// 删除 ACL 条目
dfs.removeAclEntries(path, aclSpec);

// 删除所有 ACL
dfs.removeAcl(path);

// 查看 ACL
AclStatus aclStatus = dfs.getAclStatus(path);
for (AclEntry entry : aclStatus.getEntries()) {
    System.out.println(entry);
}
```

---

## 性能优化

### 1. 短路读取 (Short-Circuit Read)

客户端与 DataNode 在同一节点时,直接读取本地文件。

```xml
<!-- hdfs-site.xml -->
<property>
  <name>dfs.client.read.shortcircuit</name>
  <value>true</value>
</property>
<property>
  <name>dfs.domain.socket.path</name>
  <value>/var/lib/hadoop-hdfs/dn_socket</value>
</property>
```

### 2. EC (Erasure Coding)

使用纠删码代替副本,降低存储开销。

```bash
# 启用 EC
hdfs ec -setPolicy -path /user/data -policy RS-6-3-1024k

# 查看 EC 策略
hdfs ec -getPolicy -path /user/data

# 禁用 EC
hdfs ec -unsetPolicy -path /user/data
```

**存储开销对比**:
- 3副本: 3x
- RS-6-3: 1.5x
- RS-10-4: 1.4x

### 3. 合并小文件

```bash
# 使用 getmerge
hdfs dfs -getmerge /input/small_files /output/merged_file

# 使用 Hadoop Archive
hadoop archive -archiveName files.har -p /input /output

# 使用 CombineInputFormat (MapReduce)
# 在代码中设置
CombineTextInputFormat.setMaxInputSplitSize(job, 134217728); // 128MB
```

### 4. 配置优化

```xml
<!-- 增大块大小 -->
<property>
  <name>dfs.blocksize</name>
  <value>268435456</value> <!-- 256MB -->
</property>

<!-- 增大 I/O 缓冲区 -->
<property>
  <name>io.file.buffer.size</name>
  <value>131072</value> <!-- 128KB -->
</property>

<!-- 启用数据完整性检查 -->
<property>
  <name>dfs.datanode.synconclose</name>
  <value>true</value>
</property>
```

### 5. JVM 优化

```bash
# 增大 NameNode 堆内存
export HADOOP_NAMENODE_OPTS="-Xms8g -Xmx8g -XX:+UseG1GC"

# 增大 DataNode 堆内存
export HADOOP_DATANODE_OPTS="-Xms4g -Xmx4g -XX:+UseG1GC"
```

### 6. 监控指标

```bash
# NameNode 指标
hdfs dfsadmin -report

# DataNode 状态
hdfs dfsadmin -printTopology

# 文件系统检查
hdfs fsck / -files -blocks -locations

# 查看慢节点
hdfs dfsadmin -report | grep -A 5 "Dead datanodes"
```

---

## 总结

HDFS 高级特性提供了强大的数据管理能力:

✅ **快照**: 数据备份和恢复
✅ **配额**: 资源管理和成本控制
✅ **压缩**: 节省存储和网络
✅ **存储策略**: 数据生命周期管理
✅ **ACL**: 细粒度权限控制
✅ **性能优化**: 提升系统性能

合理使用这些特性可以大大提升 HDFS 的可用性、可靠性和性能。
