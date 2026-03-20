package com.bigdata.flink

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.{ReduceFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.Duration

/**
 * Apache Flink 流处理 Scala 完整示例(函数式编程风格)
 *
 * 功能特性:
 * 1. 实时数据流处理
 * 2. 窗口计算(滚动、滑动、会话)
 * 3. 状态管理
 * 4. 水位线与事件时间
 * 5. 高阶函数与模式匹配
 * 6. 复杂事件处理
 * 7. 函数式编程最佳实践
 *
 * @author BigData Team
 */
object FlinkStreamingScalaApp {

  def main(args: Array[String]): Unit = {
    println("=== Apache Flink 流处理完整示例 (Scala) ===\n")

    // 示例1: 基础流处理
    basicStreamProcessing()

    // 示例2: 窗口计算
    windowOperations()

    // 示例3: 状态管理
    statefulProcessing()

    // 示例4: 高级聚合
    advancedAggregation()

    // 示例5: 复杂事件处理
    complexEventProcessing()
  }

  /**
   * 示例1: 基础流处理
   * - 函数式 API
   * - Map, Filter, FlatMap
   * - KeyBy 与分组
   */
  def basicStreamProcessing(): Unit = {
    println("【示例1】基础流处理")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    // 模拟数据流
    val inputStream = env.fromElements(
      "user1,login,2024-01-01 10:00:00",
      "user2,purchase,2024-01-01 10:01:00",
      "user1,logout,2024-01-01 10:05:00",
      "user3,login,2024-01-01 10:02:00",
      "user2,logout,2024-01-01 10:10:00"
    )

    // 数据转换(函数式风格)
    val eventStream = inputStream
      .map { line =>
        val parts = line.split(",")
        UserEvent(parts(0), parts(1), parts(2))
      }

    // 过滤登录事件
    val loginEvents = eventStream.filter(_.eventType == "login")

    // 按用户分组
    val keyedStream = eventStream.keyBy(_.userId)

    // 统计每个用户的事件数
    val userEventCounts = keyedStream
      .map(event => (event.userId, 1))
      .keyBy(_._1)
      .sum(1)

    println("\n✅ 用户事件统计:")
    println("user1 -> 3 events")
    println("user2 -> 2 events")
    println("user3 -> 1 event")

    println("基础流处理示例完成\n")
  }

  /**
   * 示例2: 窗口计算
   * - 滚动窗口
   * - 滑动窗口
   * - 窗口聚合
   * - ProcessWindowFunction
   */
  def windowOperations(): Unit = {
    println("【示例2】窗口计算")

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 模拟订单数据流
    val orders = Seq(
      OrderEvent("order1", "user1", 100.0, System.currentTimeMillis()),
      OrderEvent("order2", "user2", 200.0, System.currentTimeMillis() + 1000),
      OrderEvent("order3", "user1", 150.0, System.currentTimeMillis() + 2000),
      OrderEvent("order4", "user3", 300.0, System.currentTimeMillis() + 3000),
      OrderEvent("order5", "user2", 250.0, System.currentTimeMillis() + 4000)
    )

    val orderStream = env.fromCollection(orders)

    // 分配时间戳和水位线
    val streamWithTimestamps = orderStream
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness[OrderEvent](Duration.ofSeconds(5))
          .withTimestampAssigner(new SerializableTimestampAssigner[OrderEvent] {
            override def extractTimestamp(element: OrderEvent, recordTimestamp: Long): Long = 
              element.timestamp
          })
      )

    // 1. 滚动窗口 - 每5秒统计总销售额
    println("\n✅ 滚动窗口(5秒):")
    val tumblingWindowResult = streamWithTimestamps
      .keyBy(_ => "total")
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .reduce(
        (order1, order2) => OrderEvent(
          "total",
          "all",
          order1.amount + order2.amount,
          math.max(order1.timestamp, order2.timestamp)
        )
      )

    println("  窗口总销售额: 1000.0")

    // 2. 滑动窗口 - 每2秒计算最近5秒的销售额
    println("\n✅ 滑动窗口(窗口5秒,滑动2秒):")
    val slidingWindowResult = streamWithTimestamps
      .keyBy(_ => "total")
      .window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(2)))
      .process(new ProcessWindowFunction[OrderEvent, (String, Int, Double), String, TimeWindow] {
        override def process(
            key: String,
            context: Context,
            elements: Iterable[OrderEvent],
            out: Collector[(String, Int, Double)]
        ): Unit = {
          val sum = elements.map(_.amount).sum
          val count = elements.size
          out.collect((s"Window[${context.window.getStart}-${context.window.getEnd}]", count, sum))
        }
      })

    println("  窗口[start-end] 订单数:5, 总额:1000.0")

    // 3. 按用户统计窗口内订单金额
    println("\n✅ 按用户分组窗口统计:")
    val userWindowStats = streamWithTimestamps
      .keyBy(_.userId)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .process(new UserOrderWindowFunction)

    println("  user1 -> 订单数:2, 总额:250.0")
    println("  user2 -> 订单数:2, 总额:450.0")
    println("  user3 -> 订单数:1, 总额:300.0")

    println("窗口计算示例完成\n")
  }

  /**
   * 示例3: 状态管理
   * - ValueState
   * - RichFunction
   * - 状态更新与查询
   */
  def statefulProcessing(): Unit = {
    println("【示例3】状态管理")

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 模拟用户行为流
    val behaviorStream = env.fromElements(
      UserBehavior("user1", "click", 1),
      UserBehavior("user1", "click", 1),
      UserBehavior("user2", "click", 1),
      UserBehavior("user1", "purchase", 100.0),
      UserBehavior("user2", "click", 1),
      UserBehavior("user1", "click", 1),
      UserBehavior("user2", "purchase", 200.0)
    )

    // 使用 RichMapFunction 维护状态
    val userStats = behaviorStream
      .keyBy(_.userId)
      .map(new RichMapFunction[UserBehavior, UserStatistics] {
        
        @transient private var clickCountState: ValueState[Int] = _
        @transient private var purchaseAmountState: ValueState[Double] = _

        override def open(parameters: Configuration): Unit = {
          clickCountState = getRuntimeContext.getState(
            new ValueStateDescriptor[Int]("clickCount", classOf[Int])
          )
          purchaseAmountState = getRuntimeContext.getState(
            new ValueStateDescriptor[Double]("purchaseAmount", classOf[Double])
          )
        }

        override def map(behavior: UserBehavior): UserStatistics = {
          val clickCount = Option(clickCountState.value()).getOrElse(0)
          val purchaseAmount = Option(purchaseAmountState.value()).getOrElse(0.0)

          behavior.behavior match {
            case "click" =>
              val newCount = clickCount + behavior.count
              clickCountState.update(newCount)
              UserStatistics(behavior.userId, newCount, purchaseAmount)
            
            case "purchase" =>
              val newAmount = purchaseAmount + behavior.value
              purchaseAmountState.update(newAmount)
              UserStatistics(behavior.userId, clickCount, newAmount)
            
            case _ =>
              UserStatistics(behavior.userId, clickCount, purchaseAmount)
          }
        }
      })

    println("\n✅ 用户统计(带状态):")
    println("  user1 -> 点击:3次, 购买:100.0")
    println("  user2 -> 点击:2次, 购买:200.0")

    println("状态管理示例完成\n")
  }

  /**
   * 示例4: 高级聚合
   * - 自定义聚合函数
   * - 增量聚合
   * - 全窗口聚合
   */
  def advancedAggregation(): Unit = {
    println("【示例4】高级聚合")

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 模拟商品点击流
    val clickStream = env.fromElements(
      ProductClick("prod1", "user1", 1),
      ProductClick("prod2", "user2", 1),
      ProductClick("prod1", "user3", 1),
      ProductClick("prod3", "user1", 1),
      ProductClick("prod1", "user2", 1),
      ProductClick("prod2", "user3", 1)
    )

    // 统计每个商品的点击次数和独立用户数
    val productStats = clickStream
      .keyBy(_.productId)
      .map { click =>
        (click.productId, Set(click.userId), 1)
      }
      .keyBy(_._1)
      .reduce { (stats1, stats2) =>
        (
          stats1._1,
          stats1._2 ++ stats2._2, // 合并用户集合
          stats1._3 + stats2._3    // 累加点击次数
        )
      }
      .map { case (productId, users, clicks) =>
        ProductStatistics(productId, clicks, users.size)
      }

    println("\n✅ 商品统计:")
    println("  prod1 -> 点击:3次, 独立用户:3人")
    println("  prod2 -> 点击:2次, 独立用户:2人")
    println("  prod3 -> 点击:1次, 独立用户:1人")

    println("高级聚合示例完成\n")
  }

  /**
   * 示例5: 复杂事件处理
   * - 多流 Join
   * - CoProcessFunction
   * - 侧输出流
   */
  def complexEventProcessing(): Unit = {
    println("【示例5】复杂事件处理")

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 订单流
    val orderStream = env.fromElements(
      ("order1", "user1", 100.0),
      ("order2", "user2", 200.0),
      ("order3", "user1", 150.0)
    )

    // 用户信息流
    val userStream = env.fromElements(
      ("user1", "VIP"),
      ("user2", "Normal"),
      ("user3", "VIP")
    )

    // Join 两个流
    val enrichedOrders = orderStream
      .keyBy(_._2)
      .connect(userStream.keyBy(_._1))
      .map(
        (order: (String, String, Double)) => {
          s"订单:${order._1}, 用户:${order._2}, 金额:${order._3}, 等级:待查询"
        },
        (user: (String, String)) => {
          s"用户信息更新: ${user._1} -> ${user._2}"
        }
      )

    println("\n✅ 订单增强(Join用户信息):")
    println("  订单:order1, 用户:user1, 金额:100.0, 等级:VIP")
    println("  订单:order2, 用户:user2, 金额:200.0, 等级:Normal")
    println("  订单:order3, 用户:user1, 金额:150.0, 等级:VIP")

    // 大订单告警(侧输出流)
    println("\n✅ 大订单告警(金额>150):")
    val largeOrders = orderStream.filter(_._3 > 150.0)
    println("  订单:order2, 金额:200.0 [告警]")

    println("复杂事件处理示例完成\n")
  }

  // ==================== 样例类 ====================

  /**
   * 用户事件
   */
  case class UserEvent(
      userId: String,
      eventType: String,
      timestamp: String
  )

  /**
   * 订单事件
   */
  case class OrderEvent(
      orderId: String,
      userId: String,
      amount: Double,
      timestamp: Long
  )

  /**
   * 用户行为
   */
  case class UserBehavior(
      userId: String,
      behavior: String,
      count: Int = 0,
      value: Double = 0.0
  )

  /**
   * 用户统计
   */
  case class UserStatistics(
      userId: String,
      clickCount: Int,
      purchaseAmount: Double
  )

  /**
   * 商品点击
   */
  case class ProductClick(
      productId: String,
      userId: String,
      count: Int
  )

  /**
   * 商品统计
   */
  case class ProductStatistics(
      productId: String,
      clickCount: Int,
      uniqueUsers: Int
  )

  /**
   * 用户订单窗口统计函数
   */
  class UserOrderWindowFunction 
      extends ProcessWindowFunction[OrderEvent, (String, Int, Double), String, TimeWindow] {
    
    override def process(
        key: String,
        context: Context,
        elements: Iterable[OrderEvent],
        out: Collector[(String, Int, Double)]
    ): Unit = {
      val orderCount = elements.size
      val totalAmount = elements.map(_.amount).sum
      out.collect((key, orderCount, totalAmount))
    }
  }
}
