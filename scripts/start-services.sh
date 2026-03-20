#!/bin/bash

# 大数据环境启动脚本

echo "================================="
echo "启动大数据环境..."
echo "================================="

# 检查 HADOOP_HOME
if [ -z "$HADOOP_HOME" ]; then
    echo "❌ HADOOP_HOME 未设置!"
    exit 1
fi

# 检查 HBASE_HOME
if [ -z "$HBASE_HOME" ]; then
    echo "❌ HBASE_HOME 未设置!"
    exit 1
fi

# 启动 HDFS
echo "🚀 启动 HDFS..."
$HADOOP_HOME/sbin/start-dfs.sh

# 等待 HDFS 启动
sleep 5

# 检查 HDFS 是否启动成功
if jps | grep -q "NameNode"; then
    echo "✅ HDFS 启动成功"
else
    echo "❌ HDFS 启动失败"
    exit 1
fi

# 启动 YARN (可选)
echo "🚀 启动 YARN..."
$HADOOP_HOME/sbin/start-yarn.sh
sleep 3

# 启动 HBase
echo "🚀 启动 HBase..."
$HBASE_HOME/bin/start-hbase.sh
sleep 5

# 检查 HBase 是否启动成功
if jps | grep -q "HMaster"; then
    echo "✅ HBase 启动成功"
else
    echo "❌ HBase 启动失败"
fi

# 启动 Hive Metastore
if [ ! -z "$HIVE_HOME" ]; then
    echo "🚀 启动 Hive Metastore..."
    nohup $HIVE_HOME/bin/hive --service metastore > /tmp/hive-metastore.log 2>&1 &
    sleep 3
    echo "✅ Hive Metastore 启动成功"
fi

echo "================================="
echo "✅ 所有服务启动完成!"
echo "================================="
echo ""
echo "服务状态:"
jps

echo ""
echo "HDFS Web UI: http://localhost:9870"
echo "YARN Web UI: http://localhost:8088"
echo "HBase Web UI: http://localhost:16010"
