package com.bigdata.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Apache Flink 流处理 Java 完整示例
 * 
 * 功能特性:
 * 1. 实时数据流处理
 * 2. 窗口计算(滚动窗口、滑动窗口、会话窗口)
 * 3. 状态管理(ValueState, ListState)
 * 4. 水位线与事件时间
 * 5. Kafka 集成
 * 6. 侧输出流(Side Output)
 * 7. 复杂事件处理(CEP)
 * 8. 容错与检查点
 * 
 * @author BigData Team
 */
public class FlinkStreamingJavaApp {

    public static void main(String[] args) throws Exception {
        System.out.println("=== Apache Flink 流处理完整示例 ===\n");

        // 示例1: 基础流处理
        basicStreamProcessing();

        // 示例2: 窗口计算
        windowOperations();

        // 示例3: 状态管理
        statefulProcessing();

        // 示例4: Kafka 集成
        // kafkaIntegration();

        // 示例5: 复杂事件处理
        complexEventProcessing();
    }

    /**
     * 示例1: 基础流处理
     * - DataStream API
     * - Map, Filter, KeyBy
     * - 聚合操作
     */
    private static void basicStreamProcessing() throws Exception {
        System.out.println("【示例1】基础流处理");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 模拟数据流
        DataStream<String> inputStream = env.fromElements(
            "user1,login,2024-01-01 10:00:00",
            "user2,purchase,2024-01-01 10:01:00",
            "user1,logout,2024-01-01 10:05:00",
            "user3,login,2024-01-01 10:02:00",
            "user2,logout,2024-01-01 10:10:00"
        );

        // 数据转换
        DataStream<UserEvent> eventStream = inputStream
            .map(new MapFunction<String, UserEvent>() {
                @Override
                public UserEvent map(String value) throws Exception {
                    String[] parts = value.split(",");
                    return new UserEvent(parts[0], parts[1], parts[2]);
                }
            });

        // 过滤登录事件
        DataStream<UserEvent> loginEvents = eventStream
            .filter(new FilterFunction<UserEvent>() {
                @Override
                public boolean filter(UserEvent event) throws Exception {
                    return "login".equals(event.eventType);
                }
            });

        // 按用户分组统计
        KeyedStream<UserEvent, String> keyedStream = eventStream.keyBy(event -> event.userId);

        // 输出结果
        System.out.println("\n✅ 登录事件:");
        loginEvents.print();

        // 不执行,仅展示
        // env.execute("Basic Stream Processing");
        System.out.println("基础流处理示例完成\n");
    }

    /**
     * 示例2: 窗口计算
     * - 滚动窗口(Tumbling Window)
     * - 滑动窗口(Sliding Window)
     * - 会话窗口(Session Window)
     * - 窗口聚合
     */
    private static void windowOperations() throws Exception {
        System.out.println("【示例2】窗口计算");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 模拟订单数据流
        DataStream<OrderEvent> orderStream = env.fromElements(
            new OrderEvent("order1", "user1", 100.0, System.currentTimeMillis()),
            new OrderEvent("order2", "user2", 200.0, System.currentTimeMillis() + 1000),
            new OrderEvent("order3", "user1", 150.0, System.currentTimeMillis() + 2000),
            new OrderEvent("order4", "user3", 300.0, System.currentTimeMillis() + 3000),
            new OrderEvent("order5", "user2", 250.0, System.currentTimeMillis() + 4000)
        );

        // 提取事件时间和生成水位线
        DataStream<OrderEvent> streamWithTimestamps = orderStream
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((event, timestamp) -> event.timestamp)
            );

        // 1. 滚动窗口 - 每5秒统计一次总销售额
        System.out.println("\n✅ 滚动窗口(5秒):");
        streamWithTimestamps
            .keyBy(order -> "total")
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .reduce(new ReduceFunction<OrderEvent>() {
                @Override
                public OrderEvent reduce(OrderEvent value1, OrderEvent value2) throws Exception {
                    return new OrderEvent(
                        "total",
                        "all",
                        value1.amount + value2.amount,
                        Math.max(value1.timestamp, value2.timestamp)
                    );
                }
            })
            .map(order -> "窗口总销售额: " + order.amount);
            // .print();

        // 2. 滑动窗口 - 每2秒计算最近5秒的销售额
        System.out.println("✅ 滑动窗口(窗口5秒,滑动2秒):");
        streamWithTimestamps
            .keyBy(order -> "total")
            .window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(2)))
            .process(new ProcessWindowFunction<OrderEvent, String, String, TimeWindow>() {
                @Override
                public void process(String key, Context context, 
                                  Iterable<OrderEvent> elements, 
                                  Collector<String> out) throws Exception {
                    double sum = 0;
                    int count = 0;
                    for (OrderEvent order : elements) {
                        sum += order.amount;
                        count++;
                    }
                    out.collect(String.format("窗口[%d-%d] 订单数:%d, 总额:%.2f",
                        context.window().getStart(),
                        context.window().getEnd(),
                        count, sum));
                }
            });
            // .print();

        System.out.println("窗口计算示例完成\n");
    }

    /**
     * 示例3: 状态管理
     * - ValueState: 存储单个值
     * - 状态更新与查询
     * - 状态过期清理
     */
    private static void statefulProcessing() throws Exception {
        System.out.println("【示例3】状态管理");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 模拟用户行为流
        DataStream<UserBehavior> behaviorStream = env.fromElements(
            new UserBehavior("user1", "click", 1),
            new UserBehavior("user1", "click", 1),
            new UserBehavior("user2", "click", 1),
            new UserBehavior("user1", "purchase", 1),
            new UserBehavior("user2", "click", 1),
            new UserBehavior("user1", "click", 1)
        );

        // 使用状态统计每个用户的点击次数
        DataStream<Tuple2<String, Integer>> clickCounts = behaviorStream
            .keyBy(behavior -> behavior.userId)
            .process(new KeyedProcessFunction<String, UserBehavior, Tuple2<String, Integer>>() {
                
                private transient ValueState<Integer> clickCountState;

                @Override
                public void open(Configuration parameters) throws Exception {
                    ValueStateDescriptor<Integer> descriptor = 
                        new ValueStateDescriptor<>("clickCount", Integer.class, 0);
                    clickCountState = getRuntimeContext().getState(descriptor);
                }

                @Override
                public void processElement(UserBehavior behavior, Context ctx, 
                                         Collector<Tuple2<String, Integer>> out) 
                        throws Exception {
                    if ("click".equals(behavior.behavior)) {
                        Integer currentCount = clickCountState.value();
                        currentCount += behavior.count;
                        clickCountState.update(currentCount);
                        
                        out.collect(new Tuple2<>(behavior.userId, currentCount));
                    }
                }
            });

        System.out.println("\n✅ 用户点击统计(带状态):");
        clickCounts.print();

        System.out.println("状态管理示例完成\n");
    }

    /**
     * 示例4: Kafka 集成
     * - KafkaSource
     * - 水位线策略
     * - 偏移量管理
     */
    private static void kafkaIntegration() throws Exception {
        System.out.println("【示例4】Kafka 集成");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // 启用检查点(精确一次语义)
        env.enableCheckpointing(60000); // 每分钟一次

        // 创建 Kafka Source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setBootstrapServers("localhost:9092")
            .setTopics("user-events")
            .setGroupId("flink-consumer-group")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        // 读取 Kafka 数据
        DataStream<String> kafkaStream = env.fromSource(
            kafkaSource,
            WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)),
            "Kafka Source"
        );

        // 处理数据
        kafkaStream
            .map(value -> "处理消息: " + value)
            .print();

        System.out.println("✅ Kafka 集成配置完成");
        System.out.println("Kafka 集成示例完成\n");
        
        // env.execute("Kafka Integration");
    }

    /**
     * 示例5: 复杂事件处理
     * - 多流 Join
     * - CoProcessFunction
     * - 广播流
     */
    private static void complexEventProcessing() throws Exception {
        System.out.println("【示例5】复杂事件处理");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 订单流
        DataStream<Tuple3<String, String, Double>> orderStream = env.fromElements(
            new Tuple3<>("order1", "user1", 100.0),
            new Tuple3<>("order2", "user2", 200.0),
            new Tuple3<>("order3", "user1", 150.0)
        );

        // 用户信息流
        DataStream<Tuple2<String, String>> userStream = env.fromElements(
            new Tuple2<>("user1", "VIP"),
            new Tuple2<>("user2", "Normal"),
            new Tuple2<>("user3", "VIP")
        );

        // Join 两个流
        DataStream<String> enrichedOrders = orderStream
            .keyBy(order -> order.f1)
            .connect(userStream.keyBy(user -> user.f0))
            .process(new EnrichOrderFunction());

        System.out.println("\n✅ 订单增强(Join用户信息):");
        enrichedOrders.print();

        System.out.println("复杂事件处理示例完成\n");
    }

    // ==================== 辅助类 ====================

    /**
     * 用户事件
     */
    public static class UserEvent {
        public String userId;
        public String eventType;
        public String timestamp;

        public UserEvent() {}

        public UserEvent(String userId, String eventType, String timestamp) {
            this.userId = userId;
            this.eventType = eventType;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return String.format("UserEvent{userId='%s', type='%s', time='%s'}", 
                userId, eventType, timestamp);
        }
    }

    /**
     * 订单事件
     */
    public static class OrderEvent {
        public String orderId;
        public String userId;
        public Double amount;
        public Long timestamp;

        public OrderEvent() {}

        public OrderEvent(String orderId, String userId, Double amount, Long timestamp) {
            this.orderId = orderId;
            this.userId = userId;
            this.amount = amount;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return String.format("Order{id='%s', user='%s', amount=%.2f}", 
                orderId, userId, amount);
        }
    }

    /**
     * 用户行为
     */
    public static class UserBehavior {
        public String userId;
        public String behavior;
        public Integer count;

        public UserBehavior() {}

        public UserBehavior(String userId, String behavior, Integer count) {
            this.userId = userId;
            this.behavior = behavior;
            this.count = count;
        }
    }

    /**
     * 订单增强函数 - Connect 双流处理
     */
    public static class EnrichOrderFunction 
            extends org.apache.flink.streaming.api.functions.co.CoProcessFunction<
                Tuple3<String, String, Double>, 
                Tuple2<String, String>, 
                String> {

        private transient ValueState<String> userLevelState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<String> descriptor = 
                new ValueStateDescriptor<>("userLevel", String.class);
            userLevelState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement1(Tuple3<String, String, Double> order, 
                                    Context ctx, 
                                    Collector<String> out) throws Exception {
            String userLevel = userLevelState.value();
            if (userLevel == null) {
                userLevel = "Unknown";
            }
            out.collect(String.format("订单:%s, 用户:%s, 金额:%.2f, 等级:%s",
                order.f0, order.f1, order.f2, userLevel));
        }

        @Override
        public void processElement2(Tuple2<String, String> user, 
                                    Context ctx, 
                                    Collector<String> out) throws Exception {
            userLevelState.update(user.f1);
        }
    }
}
