#!/bin/bash

# 大数据环境停止脚本

echo "================================="
echo "停止大数据环境..."
echo "================================="

# 停止 Hive Metastore
echo "🛑 停止 Hive Metastore..."
pkill -f "hive.*metastore"

# 停止 HBase
if [ ! -z "$HBASE_HOME" ]; then
    echo "🛑 停止 HBase..."
    $HBASE_HOME/bin/stop-hbase.sh
    sleep 3
fi

# 停止 YARN
if [ ! -z "$HADOOP_HOME" ]; then
    echo "🛑 停止 YARN..."
    $HADOOP_HOME/sbin/stop-yarn.sh
    sleep 2
fi

# 停止 HDFS
if [ ! -z "$HADOOP_HOME" ]; then
    echo "🛑 停止 HDFS..."
    $HADOOP_HOME/sbin/stop-dfs.sh
    sleep 2
fi

echo "================================="
echo "✅ 所有服务已停止!"
echo "================================="

# 显示剩余的 Java 进程
echo ""
echo "剩余 Java 进程:"
jps
