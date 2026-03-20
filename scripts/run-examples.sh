#!/bin/bash

# 运行所有示例的脚本

PROJECT_DIR=$(cd "$(dirname "$0")/.." && pwd)
JAR_FILE="$PROJECT_DIR/target/spark-hadoop-hive-demo-1.0.0-jar-with-dependencies.jar"

echo "================================="
echo "运行大数据示例程序"
echo "================================="

# 检查 JAR 文件是否存在
if [ ! -f "$JAR_FILE" ]; then
    echo "❌ JAR 文件不存在,请先运行: mvn clean package"
    exit 1
fi

# 运行 Spark + Hive 示例 (Scala)
echo ""
echo "📊 运行 Spark + Hive 示例 (Scala)..."
echo "================================="
spark-submit \
    --class com.bigdata.spark.SparkHiveApplication \
    --master local[*] \
    --driver-memory 1g \
    "$JAR_FILE"

read -p "按回车继续下一个示例..."

# 运行 HBase 示例 (Java)
echo ""
echo "📊 运行 HBase 示例 (Java)..."
echo "================================="
java -cp "$JAR_FILE" com.bigdata.hbase.HBaseJavaApp

read -p "按回车继续下一个示例..."

# 运行 HDFS 示例 (Java)
echo ""
echo "📊 运行 HDFS 示例 (Java)..."
echo "================================="
java -cp "$JAR_FILE" com.bigdata.hdfs.HDFSJavaApp

echo ""
echo "================================="
echo "✅ 所有示例运行完成!"
echo "================================="
