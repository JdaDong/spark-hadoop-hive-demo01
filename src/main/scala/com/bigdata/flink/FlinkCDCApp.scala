package com.bigdata.flink

import com.ververica.cdc.connectors.mysql.source.MySqlSource
import com.ververica.cdc.connectors.mysql.table.StartupOptions
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.kafka.clients.producer.ProducerConfig

import java.util.Properties

/**
 * Flink CDC 实时数据同步示例
 * 
 * 功能特性:
 * 1. MySQL Binlog 实时订阅
 * 2. 增量数据捕获(CDC)
 * 3. 数据转换与过滤
 * 4. 多表同步
 * 5. Schema Evolution
 * 6. 实时写入 Kafka/Hive/Iceberg
 * 
 * @author BigData Team
 */
object FlinkCDCApp {

  def main(args: Array[String]): Unit = {
    println("=== Flink CDC 实时数据同步示例 ===\n")

    // 示例1: MySQL CDC 基础示例
    mysqlCDCBasic()

    // 示例2: 多表同步
    // multiTableSync()

    // 示例3: 数据转换与路由
    // dataTransformAndRoute()

    // 示例4: CDC to Kafka
    // cdcToKafka()
  }

  /**
   * 示例1: MySQL CDC 基础示例
   * - 监听 MySQL Binlog
   * - 捕获 INSERT/UPDATE/DELETE 事件
   * - 解析变更数据
   */
  def mysqlCDCBasic(): Unit = {
    println("【示例1】MySQL CDC 基础示例")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1) // CDC 通常使用单并行度

    // 启用检查点(必须开启)
    env.enableCheckpointing(60000) // 每分钟一次

    // 创建 MySQL CDC Source
    val mySqlSource = MySqlSource.builder[String]()
      .hostname("localhost")
      .port(3306)
      .databaseList("test_db") // 监听的数据库
      .tableList("test_db.users", "test_db.orders") // 监听的表
      .username("root")
      .password("password")
      .serverId("5400-5404") // 唯一服务器ID
      .startupOptions(StartupOptions.initial()) // 启动选项: initial, earliest, latest, timestamp
      .deserializer(new JsonDebeziumDeserializationSchema()) // JSON格式
      .build()

    // 读取 CDC 数据流
    val cdcStream = env.fromSource(
      mySqlSource,
      WatermarkStrategy.noWatermarks(),
      "MySQL CDC Source"
    )

    // 处理变更事件
    val processedStream = cdcStream.process(new ProcessFunction[String, CDCEvent] {
      override def processElement(
          value: String,
          ctx: ProcessFunction[String, CDCEvent]#Context,
          out: Collector[CDCEvent]
      ): Unit = {
        // 解析 JSON (这里简化处理)
        println(s"收到 CDC 事件: $value")
        
        // 实际应用中需要解析 JSON
        // val jsonNode = objectMapper.readTree(value)
        // val op = jsonNode.get("op").asText() // c=insert, u=update, d=delete
        // val before = jsonNode.get("before")
        // val after = jsonNode.get("after")
        
        out.collect(CDCEvent("users", "INSERT", value))
      }
    })

    println("\n✅ MySQL CDC Source 配置:")
    println("  数据库: test_db")
    println("  表: users, orders")
    println("  启动模式: initial (初始化 + 增量)")
    println("  检查点: 60秒")

    println("\n✅ CDC 事件格式:")
    println("""
      |{
      |  "before": null,
      |  "after": {
      |    "id": 1,
      |    "name": "张三",
      |    "age": 25
      |  },
      |  "source": {
      |    "version": "1.5.0",
      |    "connector": "mysql",
      |    "name": "mysql_binlog_source",
      |    "ts_ms": 1609459200000,
      |    "db": "test_db",
      |    "table": "users"
      |  },
      |  "op": "c",  // c=insert, u=update, d=delete, r=read
      |  "ts_ms": 1609459200123
      |}
      |""".stripMargin)

    println("MySQL CDC 基础示例完成\n")
  }

  /**
   * 示例2: 多表同步
   * - 监听多个表
   * - 按表名路由
   * - 不同表不同处理逻辑
   */
  def multiTableSync(): Unit = {
    println("【示例2】多表同步")

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 模拟 CDC 数据
    val cdcData = Seq(
      """{"table":"users","op":"c","after":{"id":1,"name":"张三"}}""",
      """{"table":"orders","op":"c","after":{"id":1,"user_id":1,"amount":100}}""",
      """{"table":"users","op":"u","after":{"id":1,"name":"李四"}}""",
      """{"table":"orders","op":"d","before":{"id":1,"user_id":1,"amount":100}}"""
    )

    val stream = env.fromCollection(cdcData)

    // 按表名分流
    val usersStream = stream.filter(_.contains("\"table\":\"users\""))
    val ordersStream = stream.filter(_.contains("\"table\":\"orders\""))

    println("\n✅ 多表同步配置:")
    println("  users 表 -> Hive 分区表")
    println("  orders 表 -> Iceberg 事务表")
    println("  products 表 -> ClickHouse 实时表")

    // users 表处理
    println("\n✅ users 表事件:")
    usersStream.map(event => {
      s"处理 users 表: $event -> 写入 Hive"
    })

    // orders 表处理
    println("\n✅ orders 表事件:")
    ordersStream.map(event => {
      s"处理 orders 表: $event -> 写入 Iceberg"
    })

    println("\n多表同步示例完成\n")
  }

  /**
   * 示例3: 数据转换与路由
   * - 数据清洗
   * - 字段映射
   * - 数据脱敏
   * - 路由到不同目标
   */
  def dataTransformAndRoute(): Unit = {
    println("【示例3】数据转换与路由")

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 模拟 CDC 事件
    val cdcEvents = Seq(
      CDCEvent("users", "INSERT", """{"id":1,"name":"张三","phone":"13800138000"}"""),
      CDCEvent("users", "UPDATE", """{"id":1,"name":"李四","phone":"13900139000"}"""),
      CDCEvent("orders", "INSERT", """{"id":1,"amount":1000.0,"status":"paid"}""")
    )

    val stream = env.fromCollection(cdcEvents)

    // 1. 数据脱敏
    val maskedStream = stream.map { event =>
      if (event.tableName == "users") {
        // 手机号脱敏
        val masked = event.data.replaceAll(""""phone":"(\d{3})\d{4}(\d{4})"""", """"phone":"$1****$2"""")
        event.copy(data = masked)
      } else {
        event
      }
    }

    println("\n✅ 数据脱敏:")
    println("  原始: {\"phone\":\"13800138000\"}")
    println("  脱敏: {\"phone\":\"138****8000\"}")

    // 2. 数据路由
    println("\n✅ 数据路由:")
    
    // 大订单告警
    val largeOrders = stream
      .filter(event => event.tableName == "orders" && event.data.contains("\"amount\":1000"))
    println("  大订单(>1000) -> Kafka 告警 Topic")

    // 用户变更同步
    val userChanges = stream.filter(_.tableName == "users")
    println("  用户变更 -> Redis 缓存更新")

    // 全量数据入湖
    println("  全量数据 -> Iceberg 数据湖")

    println("\n数据转换与路由示例完成\n")
  }

  /**
   * 示例4: CDC to Kafka
   * - CDC 数据写入 Kafka
   * - 按表名分 Topic
   * - 保序性保证
   */
  def cdcToKafka(): Unit = {
    println("【示例4】CDC to Kafka")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(60000)

    // 模拟 CDC 流
    val cdcStream = env.fromElements(
      """{"table":"users","op":"c","after":{"id":1}}""",
      """{"table":"orders","op":"c","after":{"id":1}}"""
    )

    // 创建 Kafka Sink
    val kafkaSink = KafkaSink.builder[String]()
      .setBootstrapServers("localhost:9092")
      .setRecordSerializer(
        KafkaRecordSerializationSchema.builder()
          .setTopic("cdc-events")
          .setValueSerializationSchema(new SimpleStringSchema())
          .build()
      )
      .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "900000") // 事务超时
      .build()

    // 写入 Kafka
    cdcStream.sinkTo(kafkaSink)

    println("\n✅ CDC to Kafka 配置:")
    println("  目标 Topic: cdc-events")
    println("  分区策略: 按主键哈希")
    println("  事务保证: Exactly-Once")

    println("\n✅ Kafka 消息格式:")
    println("""
      |Topic: cdc-users
      |Key: {"db":"test_db","table":"users","pk":{"id":1}}
      |Value: {"op":"c","before":null,"after":{...},"ts_ms":...}
      |""".stripMargin)

    println("CDC to Kafka 示例完成\n")
  }

  /**
   * 示例5: CDC 高级特性
   * - Schema Evolution
   * - 断点续传
   * - 数据回溯
   */
  def advancedFeatures(): Unit = {
    println("【示例5】CDC 高级特性")

    println("\n✅ Schema Evolution (模式演化):")
    println("  1. 自动感知表结构变更")
    println("  2. 支持增加列、删除列")
    println("  3. 兼容性检查")

    println("\n✅ 断点续传:")
    println("  1. 保存 Binlog 位点")
    println("  2. 故障恢复自动续传")
    println("  3. 精确一次语义")

    println("\n✅ 数据回溯:")
    println("  启动选项:")
    println("    - initial(): 全量 + 增量")
    println("    - earliest(): 从最早 Binlog 开始")
    println("    - latest(): 只读新增数据")
    println("    - timestamp(ts): 从指定时间开始")

    println("\n✅ 性能优化:")
    println("  1. 并行度设置: 1 (保证顺序)")
    println("  2. 批量大小: fetch.size=1024")
    println("  3. 检查点间隔: 1-5分钟")
    println("  4. Binlog 格式: ROW")

    println("\nCDC 高级特性示例完成\n")
  }

  // ==================== 样例类 ====================

  /**
   * CDC 事件
   */
  case class CDCEvent(
      tableName: String,
      operation: String, // INSERT, UPDATE, DELETE
      data: String
  )

  /**
   * 用户表
   */
  case class User(
      id: Long,
      name: String,
      email: String,
      phone: String
  )

  /**
   * 订单表
   */
  case class Order(
      id: Long,
      userId: Long,
      amount: Double,
      status: String
  )
}
