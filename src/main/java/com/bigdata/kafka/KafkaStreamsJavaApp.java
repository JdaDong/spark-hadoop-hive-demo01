package com.bigdata.kafka;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.*;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Kafka Streams 流处理高级示例
 * 
 * 涵盖内容:
 * 1. Kafka Streams 基础配置
 * 2. KStream / KTable / GlobalKTable
 * 3. 无状态转换 (map, filter, flatMap, branch)
 * 4. 有状态转换 (groupBy, aggregate, reduce, count)
 * 5. 窗口操作 (Tumbling, Hopping, Sliding, Session)
 * 6. 连接操作 (KStream-KStream, KStream-KTable, KStream-GlobalKTable)
 * 7. 交互式查询 (State Store)
 * 8. 错误处理 & 重试
 * 9. 拓扑优化
 * 10. 实战: 实时词频统计
 * 11. 实战: 用户行为分析
 * 12. 实战: 实时欺诈检测
 */
public class KafkaStreamsJavaApp {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String APPLICATION_ID = "kafka-streams-demo";

    public static void main(String[] args) {
        System.out.println("=== Kafka Streams 流处理高级示例 ===\n");

        // 1. 基础配置
        basicConfigDemo();

        // 2. 无状态转换
        statelessTransformDemo();

        // 3. 有状态转换
        statefulTransformDemo();

        // 4. 窗口操作
        windowOperationsDemo();

        // 5. 连接操作
        joinOperationsDemo();

        // 6. 实战: 词频统计
        wordCountDemo();

        // 7. 实战: 用户行为分析
        userBehaviorAnalysisDemo();

        // 8. 实战: 欺诈检测
        fraudDetectionDemo();

        System.out.println("\n=== 所有 Kafka Streams 示例完成 ===");
    }

    // ==================== 1. 基础配置 ====================
    static void basicConfigDemo() {
        System.out.println("--- 1. Kafka Streams 基础配置 ---");

        Properties props = new Properties();

        // 必须配置
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // 性能优化配置
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);                        // 线程数
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);                     // 提交间隔
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024);  // 缓存大小
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");               // 状态存储目录
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);                         // 内部 topic 副本

        // 容错配置
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2); // 精确一次
        props.put(StreamsConfig.producerPrefix("acks"), "all");
        props.put(StreamsConfig.producerPrefix("retries"), 10);
        props.put(StreamsConfig.consumerPrefix("max.poll.records"), 500);
        props.put(StreamsConfig.consumerPrefix("session.timeout.ms"), 30000);

        // Timestamp 提取器
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
            "org.apache.kafka.streams.processor.WallclockTimestampExtractor");

        System.out.println("  基础配置:");
        System.out.println("  - APPLICATION_ID: " + APPLICATION_ID);
        System.out.println("  - BOOTSTRAP_SERVERS: " + BOOTSTRAP_SERVERS);
        System.out.println("  - NUM_STREAM_THREADS: 4");
        System.out.println("  - PROCESSING_GUARANTEE: EXACTLY_ONCE_V2");
        System.out.println("  - CACHE_MAX_BYTES: 10MB\n");
    }

    // ==================== 2. 无状态转换 ====================
    static void statelessTransformDemo() {
        System.out.println("--- 2. 无状态转换操作 ---");

        StreamsBuilder builder = new StreamsBuilder();

        // 创建 KStream
        KStream<String, String> sourceStream = builder.stream("input-topic",
            Consumed.with(Serdes.String(), Serdes.String())
                .withName("source-processor")
        );

        // ========== 2.1 filter - 过滤 ==========
        KStream<String, String> filtered = sourceStream
            .filter((key, value) -> value != null && value.length() > 5,
                Named.as("filter-short-values"));

        // ========== 2.2 map - 一对一转换 ==========
        KStream<String, String> mapped = sourceStream
            .map((key, value) -> KeyValue.pair(key.toUpperCase(), value.toLowerCase()),
                Named.as("map-case-transform"));

        // ========== 2.3 mapValues - 只转换值 ==========
        KStream<String, String> mappedValues = sourceStream
            .mapValues(value -> value.trim().toUpperCase(),
                Named.as("map-values-upper"));

        // ========== 2.4 flatMap - 一对多转换 ==========
        KStream<String, String> flatMapped = sourceStream
            .flatMap((key, value) -> {
                List<KeyValue<String, String>> result = new ArrayList<>();
                for (String word : value.split("\\s+")) {
                    result.add(KeyValue.pair(word, word));
                }
                return result;
            }, Named.as("flat-map-split-words"));

        // ========== 2.5 flatMapValues - 一对多值转换 ==========
        KStream<String, String> flatMappedValues = sourceStream
            .flatMapValues(value -> Arrays.asList(value.split("\\s+")),
                Named.as("flat-map-values-split"));

        // ========== 2.6 branch - 分支 (Flink侧输出的类似) ==========
        @SuppressWarnings("unchecked")
        KStream<String, String>[] branches = sourceStream.branch(
            (key, value) -> value.startsWith("ERROR"),    // 分支0: 错误日志
            (key, value) -> value.startsWith("WARN"),     // 分支1: 警告日志
            (key, value) -> true                          // 分支2: 其他日志
        );
        KStream<String, String> errorStream = branches[0];
        KStream<String, String> warnStream = branches[1];
        KStream<String, String> otherStream = branches[2];

        // ========== 2.7 selectKey - 重新选择 Key ==========
        KStream<String, String> rekeyed = sourceStream
            .selectKey((key, value) -> value.substring(0, Math.min(3, value.length())),
                Named.as("select-key-prefix"));

        // ========== 2.8 merge - 合并多个流 ==========
        KStream<String, String> merged = errorStream.merge(warnStream,
            Named.as("merge-error-warn"));

        // ========== 2.9 peek - 观察 (不改变流) ==========
        KStream<String, String> peeked = sourceStream
            .peek((key, value) -> System.out.println("Processing: " + key + " -> " + value),
                Named.as("peek-debug"));

        // ========== 2.10 through / repartition - 重分区 ==========
        KStream<String, String> repartitioned = sourceStream
            .repartition(Repartitioned.with(Serdes.String(), Serdes.String())
                .withName("repartition-by-key")
                .withNumberOfPartitions(10));

        // 写入输出 Topic
        filtered.to("filtered-output", Produced.with(Serdes.String(), Serdes.String()));
        errorStream.to("error-logs", Produced.with(Serdes.String(), Serdes.String()));
        warnStream.to("warn-logs", Produced.with(Serdes.String(), Serdes.String()));

        System.out.println("  无状态转换:");
        System.out.println("  - filter: 过滤长度>5的消息");
        System.out.println("  - map: key大写, value小写");
        System.out.println("  - mapValues: value大写");
        System.out.println("  - flatMap: 分词");
        System.out.println("  - branch: 按日志级别分流");
        System.out.println("  - selectKey: 按前缀重新选key");
        System.out.println("  - merge: 合并流");
        System.out.println("  - peek: 调试观察");
        System.out.println("  - repartition: 重分区\n");
    }

    // ==================== 3. 有状态转换 ====================
    static void statefulTransformDemo() {
        System.out.println("--- 3. 有状态转换操作 ---");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> sourceStream = builder.stream("orders-topic");

        // ========== 3.1 groupByKey + count ==========
        KTable<String, Long> orderCounts = sourceStream
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .count(Materialized.<String, Long, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("order-count-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long()));

        // ========== 3.2 groupBy + aggregate ==========
        // 自定义分组键并聚合
        KTable<String, String> userStats = sourceStream
            .groupBy(
                (key, value) -> extractUserId(value),
                Grouped.with(Serdes.String(), Serdes.String())
            )
            .aggregate(
                // 初始化器
                () -> "{\"count\":0,\"total\":0}",
                // 聚合器
                (key, value, aggregate) -> {
                    // 解析并更新统计
                    double amount = parseAmount(value);
                    int count = parseCount(aggregate) + 1;
                    double total = parseTotal(aggregate) + amount;
                    return String.format("{\"count\":%d,\"total\":%.2f,\"avg\":%.2f}",
                        count, total, total / count);
                },
                Materialized.<String, String, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("user-stats-store")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
            );

        // ========== 3.3 groupByKey + reduce ==========
        KTable<String, String> latestValues = sourceStream
            .groupByKey()
            .reduce(
                // 保留最新值
                (oldValue, newValue) -> newValue,
                Materialized.<String, String, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("latest-value-store")
            );

        // ========== 3.4 KTable 操作 ==========
        // KTable → KStream
        KStream<String, Long> countStream = orderCounts.toStream();

        // KTable filter
        KTable<String, Long> highCountOrders = orderCounts
            .filter((key, count) -> count > 10);

        // KTable mapValues
        KTable<String, String> countLabels = orderCounts
            .mapValues(count -> {
                if (count > 100) return "热门";
                else if (count > 10) return "普通";
                else return "冷门";
            });

        // 输出到 topic
        orderCounts.toStream().to("order-counts",
            Produced.with(Serdes.String(), Serdes.Long()));
        userStats.toStream().to("user-stats",
            Produced.with(Serdes.String(), Serdes.String()));

        System.out.println("  有状态转换:");
        System.out.println("  - groupByKey + count: 订单计数");
        System.out.println("  - groupBy + aggregate: 用户统计(count/total/avg)");
        System.out.println("  - groupByKey + reduce: 保留最新值");
        System.out.println("  - KTable filter/mapValues: 过滤和标签\n");
    }

    // ==================== 4. 窗口操作 ====================
    static void windowOperationsDemo() {
        System.out.println("--- 4. 窗口操作 ---");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> sourceStream = builder.stream("events-topic");

        // ========== 4.1 Tumbling Window (滚动窗口) ==========
        // 每5分钟统计一次
        KTable<Windowed<String>, Long> tumblingCounts = sourceStream
            .groupByKey()
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
            .count(Materialized.<String, Long, WindowStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("tumbling-count-store"));

        // ========== 4.2 Hopping Window (跳跃/滑动窗口) ==========
        // 窗口大小10分钟, 每2分钟滑动一次
        KTable<Windowed<String>, Long> hoppingCounts = sourceStream
            .groupByKey()
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(10))
                .advanceBy(Duration.ofMinutes(2)))
            .count(Materialized.as("hopping-count-store"));

        // ========== 4.3 Sliding Window (滑动窗口) ==========
        // 在10秒内相同key的事件合并
        KTable<Windowed<String>, Long> slidingCounts = sourceStream
            .groupByKey()
            .windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(10)))
            .count(Materialized.as("sliding-count-store"));

        // ========== 4.4 Session Window (会话窗口) ==========
        // 30秒无活动则关闭会话
        KTable<Windowed<String>, Long> sessionCounts = sourceStream
            .groupByKey()
            .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(30)))
            .count(Materialized.as("session-count-store"));

        // ========== 4.5 Suppress (窗口结果抑制) ==========
        // 只在窗口关闭时输出最终结果
        KTable<Windowed<String>, Long> suppressedCounts = sourceStream
            .groupByKey()
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
            .count(Materialized.as("suppressed-count-store"))
            .suppress(Suppressed.untilWindowCloses(
                Suppressed.BufferConfig.unbounded()
                    .shutDownWhenFull()
            ));

        // 输出窗口化结果
        tumblingCounts.toStream()
            .map((windowedKey, count) -> KeyValue.pair(
                windowedKey.key(),
                String.format("window=[%s ~ %s], count=%d",
                    Instant.ofEpochMilli(windowedKey.window().start()),
                    Instant.ofEpochMilli(windowedKey.window().end()),
                    count)
            ))
            .to("windowed-counts", Produced.with(Serdes.String(), Serdes.String()));

        System.out.println("  窗口操作:");
        System.out.println("  - Tumbling Window: 5分钟滚动");
        System.out.println("  - Hopping Window: 10分钟窗口/2分钟滑动");
        System.out.println("  - Sliding Window: 10秒内合并");
        System.out.println("  - Session Window: 30秒不活跃关闭");
        System.out.println("  - Suppress: 窗口关闭时才输出结果\n");
    }

    // ==================== 5. 连接操作 ====================
    static void joinOperationsDemo() {
        System.out.println("--- 5. 连接操作 ---");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> ordersStream = builder.stream("orders-topic");
        KStream<String, String> paymentsStream = builder.stream("payments-topic");
        KTable<String, String> usersTable = builder.table("users-topic",
            Materialized.as("users-store"));
        GlobalKTable<String, String> productsTable = builder.globalTable("products-topic",
            Materialized.as("products-store"));

        // ========== 5.1 KStream-KStream Join (窗口内连接) ==========
        // 订单和支付在5分钟内匹配
        KStream<String, String> orderPaymentJoin = ordersStream.join(
            paymentsStream,
            // ValueJoiner
            (orderValue, paymentValue) -> String.format(
                "{\"order\":%s,\"payment\":%s}", orderValue, paymentValue),
            // 窗口: 前后5分钟
            JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)),
            // StreamJoined 配置
            StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String())
                .withName("order-payment-join")
                .withStoreName("order-payment-join-store")
        );

        // ========== 5.2 KStream-KStream Left Join ==========
        KStream<String, String> orderPaymentLeftJoin = ordersStream.leftJoin(
            paymentsStream,
            (orderValue, paymentValue) -> {
                if (paymentValue == null) {
                    return String.format("{\"order\":%s,\"status\":\"未支付\"}", orderValue);
                }
                return String.format("{\"order\":%s,\"payment\":%s,\"status\":\"已支付\"}",
                    orderValue, paymentValue);
            },
            JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(30)),
            StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String())
        );

        // ========== 5.3 KStream-KTable Join ==========
        // 订单流与用户维度表关联
        KStream<String, String> enrichedOrders = ordersStream.join(
            usersTable,
            (orderValue, userValue) -> String.format(
                "{\"order\":%s,\"user\":%s}", orderValue, userValue),
            Joined.with(Serdes.String(), Serdes.String(), Serdes.String())
                .withName("order-user-join")
        );

        // ========== 5.4 KStream-GlobalKTable Join ==========
        // 全局维度表关联 (不需要相同分区)
        KStream<String, String> productEnrichedOrders = ordersStream.join(
            productsTable,
            // KeyValueMapper: 从订单提取 product_id 作为连接 key
            (orderKey, orderValue) -> extractProductId(orderValue),
            // ValueJoiner
            (orderValue, productValue) -> String.format(
                "{\"order\":%s,\"product\":%s}", orderValue, productValue)
        );

        // ========== 5.5 KTable-KTable Join ==========
        KTable<String, String> anotherTable = builder.table("another-topic");
        KTable<String, String> tableJoin = usersTable.join(
            anotherTable,
            (userValue, anotherValue) -> String.format(
                "{\"user\":%s,\"extra\":%s}", userValue, anotherValue)
        );

        // 输出结果
        orderPaymentJoin.to("enriched-orders");
        enrichedOrders.to("orders-with-users");

        System.out.println("  连接操作:");
        System.out.println("  - KStream-KStream Join: 订单-支付 (5分钟窗口)");
        System.out.println("  - KStream-KStream Left Join: 订单-支付 (未支付检测)");
        System.out.println("  - KStream-KTable Join: 订单-用户维表");
        System.out.println("  - KStream-GlobalKTable Join: 订单-产品 (全局)");
        System.out.println("  - KTable-KTable Join: 双表关联\n");
    }

    // ==================== 6. 实战: 词频统计 ====================
    static void wordCountDemo() {
        System.out.println("--- 6. 实战: 实时词频统计 ---");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // 读取输入
        KStream<String, String> textLines = builder.stream("text-input");

        // 词频统计管道
        KTable<String, Long> wordCounts = textLines
            // 拆分单词
            .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
            // 过滤空值和停用词
            .filter((key, word) -> word != null && !word.isEmpty() && word.length() > 2)
            // 按单词分组
            .groupBy((key, word) -> word, Grouped.with(Serdes.String(), Serdes.String()))
            // 计数
            .count(Materialized.<String, Long, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("word-count-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long()));

        // 输出到 topic
        wordCounts.toStream()
            .map((word, count) -> KeyValue.pair(word, word + ":" + count))
            .to("word-count-output", Produced.with(Serdes.String(), Serdes.String()));

        // 构建拓扑
        Topology topology = builder.build();
        System.out.println("  词频统计拓扑:");
        System.out.println(topology.describe());

        // 注意: 实际运行需要 Kafka 集群
        // KafkaStreams streams = new KafkaStreams(topology, props);
        // streams.start();

        System.out.println("  流程: text-input → split → filter → groupBy → count → word-count-output\n");
    }

    // ==================== 7. 实战: 用户行为分析 ====================
    static void userBehaviorAnalysisDemo() {
        System.out.println("--- 7. 实战: 用户行为分析 ---");

        StreamsBuilder builder = new StreamsBuilder();

        // 用户行为流: key=userId, value=JSON{action, timestamp, page, duration}
        KStream<String, String> behaviorStream = builder.stream("user-behaviors");

        // ========== 7.1 实时 PV/UV 统计 ==========
        // PV (页面浏览量) - 每分钟
        KTable<Windowed<String>, Long> pvCounts = behaviorStream
            .filter((userId, behavior) -> extractAction(behavior).equals("page_view"))
            .selectKey((userId, behavior) -> extractPage(behavior))
            .groupByKey()
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
            .count(Materialized.as("pv-count-store"));

        // UV (独立访客) - 使用近似算法
        // 注意: 精确UV需要HyperLogLog或其他去重方案

        // ========== 7.2 用户会话分析 ==========
        KTable<Windowed<String>, Long> sessionActivity = behaviorStream
            .groupByKey()
            .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMinutes(30)))
            .count(Materialized.as("session-activity-store"));

        // ========== 7.3 实时漏斗分析 ==========
        // 浏览 → 点击 → 加购 → 下单 → 支付
        @SuppressWarnings("unchecked")
        KStream<String, String>[] funnelBranches = behaviorStream.branch(
            (userId, behavior) -> extractAction(behavior).equals("view"),
            (userId, behavior) -> extractAction(behavior).equals("click"),
            (userId, behavior) -> extractAction(behavior).equals("cart"),
            (userId, behavior) -> extractAction(behavior).equals("order"),
            (userId, behavior) -> extractAction(behavior).equals("pay")
        );

        // 每个阶段的计数
        for (int i = 0; i < funnelBranches.length; i++) {
            String[] stages = {"view", "click", "cart", "order", "pay"};
            funnelBranches[i]
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofHours(1)))
                .count(Materialized.as("funnel-" + stages[i] + "-store"));
        }

        // ========== 7.4 用户行为路径分析 ==========
        KStream<String, String> userPaths = behaviorStream
            .groupByKey()
            .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMinutes(30)))
            .aggregate(
                () -> "",
                (userId, behavior, aggregate) -> {
                    String action = extractAction(behavior);
                    return aggregate.isEmpty() ? action : aggregate + " → " + action;
                },
                (key, left, right) -> left + " | " + right,
                Materialized.as("user-path-store")
            )
            .toStream()
            .map((windowedKey, path) -> KeyValue.pair(windowedKey.key(), path));

        userPaths.to("user-paths-output");

        System.out.println("  用户行为分析:");
        System.out.println("  - 实时 PV 统计 (1分钟窗口)");
        System.out.println("  - 用户会话分析 (30分钟超时)");
        System.out.println("  - 实时漏斗分析 (view→click→cart→order→pay)");
        System.out.println("  - 用户行为路径分析\n");
    }

    // ==================== 8. 实战: 欺诈检测 ====================
    static void fraudDetectionDemo() {
        System.out.println("--- 8. 实战: 实时欺诈检测 ---");

        StreamsBuilder builder = new StreamsBuilder();

        // 交易流: key=accountId, value=JSON{amount, merchant, location, timestamp}
        KStream<String, String> transactionStream = builder.stream("transactions");

        // ========== 8.1 大额交易检测 ==========
        KStream<String, String> highValueAlerts = transactionStream
            .filter((accountId, txn) -> parseAmount(txn) > 10000)
            .mapValues(txn -> String.format(
                "{\"alert\":\"HIGH_VALUE\",\"amount\":%.2f,\"txn\":%s}",
                parseAmount(txn), txn));

        // ========== 8.2 高频交易检测 ==========
        // 5分钟内交易超过10次
        KTable<Windowed<String>, Long> txnFrequency = transactionStream
            .groupByKey()
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
            .count(Materialized.as("txn-frequency-store"));

        KStream<String, String> frequencyAlerts = txnFrequency.toStream()
            .filter((windowedKey, count) -> count > 10)
            .map((windowedKey, count) -> KeyValue.pair(
                windowedKey.key(),
                String.format("{\"alert\":\"HIGH_FREQUENCY\",\"count\":%d,\"window\":\"%s\"}",
                    count, windowedKey.window())
            ));

        // ========== 8.3 异地交易检测 ==========
        // 使用状态存储记录用户最近交易位置
        KStream<String, String> locationAlerts = transactionStream
            .groupByKey()
            .aggregate(
                () -> "",
                (accountId, txn, lastLocation) -> {
                    String currentLocation = extractLocation(txn);
                    if (!lastLocation.isEmpty() && !lastLocation.equals(currentLocation)) {
                        // 位置变化,可能是异地交易
                        return "ALERT:" + currentLocation;
                    }
                    return currentLocation;
                },
                Materialized.<String, String, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("location-store")
            )
            .toStream()
            .filter((key, value) -> value.startsWith("ALERT:"))
            .mapValues(value -> String.format(
                "{\"alert\":\"LOCATION_CHANGE\",\"new_location\":\"%s\"}",
                value.substring(6)));

        // ========== 8.4 合并所有告警 ==========
        KStream<String, String> allAlerts = highValueAlerts
            .merge(frequencyAlerts)
            .merge(locationAlerts);

        // 输出告警
        allAlerts.to("fraud-alerts", Produced.with(Serdes.String(), Serdes.String()));

        // 打印告警到控制台
        allAlerts.peek((key, value) ->
            System.out.println("  [FRAUD ALERT] Account: " + key + " -> " + value));

        // 构建拓扑
        Topology topology = builder.build();

        System.out.println("  欺诈检测规则:");
        System.out.println("  - 大额交易: 单笔 > 10000");
        System.out.println("  - 高频交易: 5分钟内 > 10次");
        System.out.println("  - 异地交易: 位置突然变化");
        System.out.println("  - 合并输出: fraud-alerts topic\n");
    }

    // ==================== 辅助方法 ====================
    private static String extractUserId(String value) {
        // 简单解析, 实际生产中使用 JSON 库
        try {
            if (value.contains("\"user_id\":")) {
                int start = value.indexOf("\"user_id\":\"") + 11;
                int end = value.indexOf("\"", start);
                return value.substring(start, end);
            }
        } catch (Exception e) {
            // ignore
        }
        return "unknown";
    }

    private static String extractProductId(String value) {
        try {
            if (value.contains("\"product_id\":\"")) {
                int start = value.indexOf("\"product_id\":\"") + 14;
                int end = value.indexOf("\"", start);
                return value.substring(start, end);
            }
        } catch (Exception e) {
            // ignore
        }
        return "unknown";
    }

    private static String extractAction(String value) {
        try {
            if (value.contains("\"action\":\"")) {
                int start = value.indexOf("\"action\":\"") + 10;
                int end = value.indexOf("\"", start);
                return value.substring(start, end);
            }
        } catch (Exception e) {
            // ignore
        }
        return "unknown";
    }

    private static String extractPage(String value) {
        try {
            if (value.contains("\"page\":\"")) {
                int start = value.indexOf("\"page\":\"") + 8;
                int end = value.indexOf("\"", start);
                return value.substring(start, end);
            }
        } catch (Exception e) {
            // ignore
        }
        return "unknown";
    }

    private static String extractLocation(String value) {
        try {
            if (value.contains("\"location\":\"")) {
                int start = value.indexOf("\"location\":\"") + 12;
                int end = value.indexOf("\"", start);
                return value.substring(start, end);
            }
        } catch (Exception e) {
            // ignore
        }
        return "unknown";
    }

    private static double parseAmount(String value) {
        try {
            if (value.contains("\"amount\":")) {
                int start = value.indexOf("\"amount\":") + 9;
                int end = value.indexOf(",", start);
                if (end < 0) end = value.indexOf("}", start);
                return Double.parseDouble(value.substring(start, end).trim());
            }
        } catch (Exception e) {
            // ignore
        }
        return 0.0;
    }

    private static int parseCount(String aggregate) {
        try {
            if (aggregate.contains("\"count\":")) {
                int start = aggregate.indexOf("\"count\":") + 8;
                int end = aggregate.indexOf(",", start);
                return Integer.parseInt(aggregate.substring(start, end).trim());
            }
        } catch (Exception e) {
            // ignore
        }
        return 0;
    }

    private static double parseTotal(String aggregate) {
        try {
            if (aggregate.contains("\"total\":")) {
                int start = aggregate.indexOf("\"total\":") + 8;
                int end = aggregate.indexOf(",", start);
                if (end < 0) end = aggregate.indexOf("}", start);
                return Double.parseDouble(aggregate.substring(start, end).trim());
            }
        } catch (Exception e) {
            // ignore
        }
        return 0.0;
    }
}
