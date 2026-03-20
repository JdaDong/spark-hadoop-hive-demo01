# HBase 协处理器使用指南

## 📖 概述

本文档介绍如何使用和部署 HBase 协处理器。

## 🔧 协处理器文件说明

### 1. AuditLogObserver.java
**功能**: Observer 协处理器,用于审计日志记录

**拦截的操作**:
- `prePut`: Put 操作前执行
- `postPut`: Put 操作后执行
- `preDelete`: Delete 操作前执行
- `postDelete`: Delete 操作后执行
- `preBatchMutate`: 批量操作前执行

**应用场景**:
- 记录所有数据变更日志
- 数据验证(如年龄必须大于0)
- 防止删除关键数据(如 protected_ 开头的行)
- 权限控制
- 自动备份

### 2. RowCountEndpoint.java
**功能**: Endpoint 协处理器,用于服务端聚合计算

**提供的方法**:
- `getRowCount()`: 统计行数
- `getSum()`: 对指定列求和
- `getAverage()`: 计算平均值
- `getMax()`: 查找最大值

**应用场景**:
- 避免全表扫描后客户端聚合
- 减少网络传输数据量
- 提高查询性能

### 3. CoprocessorDeployment.java
**功能**: 协处理器部署管理工具

**提供的方法**:
- `deployObserverCoprocessor()`: 部署 Observer
- `deployEndpointCoprocessor()`: 部署 Endpoint
- `deployFromHDFS()`: 从 HDFS 加载
- `removeCoprocessor()`: 移除协处理器

## 🚀 部署步骤

### 方式1: 动态部署(推荐)

```bash
# 1. 编译项目
mvn clean package

# 2. 运行部署工具
java -cp target/spark-hadoop-hive-demo-1.0.0-jar-with-dependencies.jar \
  com.bigdata.hbase.coprocessor.CoprocessorDeployment
```

### 方式2: 使用 HBase Shell

```bash
# 启动 HBase Shell
hbase shell

# 禁用表
disable 'user_table'

# 添加协处理器
alter 'user_table', 
  'coprocessor' => '|com.bigdata.hbase.coprocessor.AuditLogObserver|'

# 启用表
enable 'user_table'

# 查看表信息
describe 'user_table'
```

### 方式3: 从 HDFS 加载(生产环境推荐)

```bash
# 1. 上传 JAR 到 HDFS
hdfs dfs -mkdir -p /hbase/coprocessors
hdfs dfs -put target/spark-hadoop-hive-demo-1.0.0-jar-with-dependencies.jar \
  /hbase/coprocessors/

# 2. HBase Shell 配置
hbase shell

disable 'user_table'

alter 'user_table', METHOD => 'table_att', 
  'coprocessor' => 'hdfs://localhost:9000/hbase/coprocessors/spark-hadoop-hive-demo-1.0.0-jar-with-dependencies.jar|
                    com.bigdata.hbase.coprocessor.AuditLogObserver|1001'

enable 'user_table'
```

### 方式4: 静态部署(全局)

编辑 `hbase-site.xml`:

```xml
<configuration>
  <!-- Observer Coprocessor -->
  <property>
    <name>hbase.coprocessor.region.classes</name>
    <value>com.bigdata.hbase.coprocessor.AuditLogObserver</value>
  </property>
  
  <!-- Endpoint Coprocessor -->
  <property>
    <name>hbase.coprocessor.user.region.classes</name>
    <value>com.bigdata.hbase.coprocessor.RowCountEndpoint</value>
  </property>
</configuration>
```

重启 HBase:
```bash
stop-hbase.sh
start-hbase.sh
```

## 🧪 测试协处理器

### 测试 Observer

```bash
# HBase Shell
hbase shell

# 创建测试表
create 'test_observer', 'info'

# 插入数据 - 会触发审计日志
put 'test_observer', 'row1', 'info:name', 'John'
put 'test_observer', 'row1', 'info:age', '30'

# 尝试插入无效年龄 - 会被拦截
put 'test_observer', 'row1', 'info:age', '-5'

# 尝试删除受保护的行 - 会被拦截
put 'test_observer', 'protected_row1', 'info:data', 'important'
delete 'test_observer', 'protected_row1'
```

### 测试 Endpoint

```java
// 客户端代码示例
Configuration conf = HBaseConfiguration.create();
Connection conn = ConnectionFactory.createConnection(conf);
Table table = conn.getTable(TableName.valueOf("analytics_table"));

// 调用 Endpoint 协处理器
// 注意: 需要定义 protobuf 协议,这里是简化示例
Scan scan = new Scan();
long count = getRowCount(table, scan);
System.out.println("Total rows: " + count);
```

## 📊 查看协处理器

```bash
# HBase Shell
hbase shell

# 查看表的协处理器配置
describe 'user_table'

# 输出示例:
# COPROCESSOR => {'class' => 'com.bigdata.hbase.coprocessor.AuditLogObserver', 
#                 'priority' => 1001}
```

## ❌ 移除协处理器

### 使用 Java API
```java
admin.disableTable(tableName);
TableDescriptor descriptor = admin.getDescriptor(tableName);
TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(descriptor);
builder.removeCoprocessor("com.bigdata.hbase.coprocessor.AuditLogObserver");
admin.modifyTable(builder.build());
admin.enableTable(tableName);
```

### 使用 HBase Shell
```bash
disable 'user_table'
alter 'user_table', METHOD => 'table_att_unset', NAME => 'coprocessor$1'
enable 'user_table'
```

## ⚠️ 注意事项

1. **性能影响**
   - 协处理器会影响 HBase 性能
   - 避免在协处理器中执行耗时操作
   - 不要在协处理器中访问外部系统

2. **错误处理**
   - 协处理器抛出异常会导致操作失败
   - 做好异常处理和日志记录
   - 测试充分后再部署到生产

3. **版本兼容**
   - 确保协处理器版本与 HBase 版本兼容
   - 升级 HBase 时重新测试协处理器

4. **安全性**
   - 协处理器运行在 RegionServer 进程中
   - 有权访问所有数据和系统资源
   - 只部署可信的协处理器代码

## 📝 调试技巧

### 1. 查看日志
```bash
# RegionServer 日志
tail -f $HBASE_HOME/logs/hbase-*-regionserver-*.log

# 查找协处理器相关日志
grep "AuditLogObserver" $HBASE_HOME/logs/hbase-*-regionserver-*.log
```

### 2. 启用详细日志
在 `log4j.properties` 中添加:
```properties
log4j.logger.com.bigdata.hbase.coprocessor=DEBUG
```

### 3. 使用 JMX 监控
```bash
# 启动 HBase 时开启 JMX
export HBASE_JMX_BASE="-Dcom.sun.management.jmxremote.ssl=false \
  -Dcom.sun.management.jmxremote.authenticate=false \
  -Dcom.sun.management.jmxremote.port=10102"
```

## 🎯 最佳实践

1. ✅ 先在开发环境测试
2. ✅ 使用动态部署而非静态部署
3. ✅ 从 HDFS 加载 JAR(便于更新)
4. ✅ 做好异常处理和日志
5. ✅ 监控协处理器性能影响
6. ✅ 版本管理和灰度发布

7. ❌ 避免阻塞操作
8. ❌ 避免内存泄漏
9. ❌ 避免频繁的外部调用
10. ❌ 不要修改协处理器无关的数据

## 📚 参考资料

- [HBase Coprocessor 官方文档](https://hbase.apache.org/book.html#cp)
- [HBase Coprocessor 示例](https://github.com/apache/hbase/tree/master/hbase-examples)
