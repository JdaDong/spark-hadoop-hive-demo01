package com.bigdata.kafka;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Apache Kafka Java 完整示例
 * 
 * 功能特性:
 * 1. Topic 管理(创建、删除、查询)
 * 2. 生产者(同步/异步发送)
 * 3. 消费者(手动/自动提交偏移量)
 * 4. 消费者组管理
 * 5. 分区与副本管理
 * 6. 事务消息
 * 7. 拦截器
 * 8. 自定义序列化
 * 
 * @author BigData Team
 */
public class KafkaJavaApp {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC_NAME = "test-topic";

    public static void main(String[] args) {
        System.out.println("=== Apache Kafka 完整示例 ===\n");

        try {
            // 示例1: Topic 管理
            topicManagement();

            // 示例2: 生产者
            producerDemo();

            // 示例3: 消费者
            // consumerDemo();

            // 示例4: 事务消息
            transactionalProducer();

            // 示例5: 高级配置
            advancedConfiguration();

        } catch (Exception e) {
            System.err.println("执行失败: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 示例1: Topic 管理
     * - 创建 Topic
     * - 查询 Topic 列表
     * - 查询 Topic 详情
     * - 删除 Topic
     */
    private static void topicManagement() throws Exception {
        System.out.println("【示例1】Topic 管理");

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        try (AdminClient adminClient = AdminClient.create(props)) {
            
            // 1. 创建 Topic
            System.out.println("\n✅ 创建 Topic: " + TOPIC_NAME);
            NewTopic newTopic = new NewTopic(TOPIC_NAME, 3, (short) 1);
            
            // 设置 Topic 配置
            Map<String, String> configs = new HashMap<>();
            configs.put("retention.ms", "86400000"); // 保留1天
            configs.put("compression.type", "snappy");
            newTopic.configs(configs);

            CreateTopicsResult result = adminClient.createTopics(Collections.singleton(newTopic));
            System.out.println("  分区数: 3, 副本因子: 1");
            System.out.println("  压缩类型: snappy");

            // 2. 查询 Topic 列表
            System.out.println("\n✅ Topic 列表:");
            ListTopicsResult topics = adminClient.listTopics();
            Set<String> topicNames = topics.names().get();
            topicNames.forEach(name -> System.out.println("  - " + name));

            // 3. 查询 Topic 详情
            System.out.println("\n✅ Topic 详情: " + TOPIC_NAME);
            DescribeTopicsResult describeResult = 
                adminClient.describeTopics(Collections.singleton(TOPIC_NAME));
            
            Map<String, TopicDescription> descriptions = describeResult.all().get();
            descriptions.forEach((name, desc) -> {
                System.out.println("  Topic名称: " + name);
                System.out.println("  分区数量: " + desc.partitions().size());
                desc.partitions().forEach(partition -> {
                    System.out.println("    分区" + partition.partition() + 
                        " Leader: " + partition.leader().id());
                });
            });

            // 4. 修改 Topic 配置
            System.out.println("\n✅ 修改 Topic 配置");
            ConfigResource resource = new ConfigResource(
                ConfigResource.Type.TOPIC, TOPIC_NAME);
            
            Map<ConfigResource, Collection<AlterConfigOp>> updateConfigs = new HashMap<>();
            updateConfigs.put(resource, Arrays.asList(
                new AlterConfigOp(new ConfigEntry("retention.ms", "172800000"), 
                    AlterConfigOp.OpType.SET)
            ));
            
            adminClient.incrementalAlterConfigs(updateConfigs);
            System.out.println("  retention.ms 修改为 172800000 (2天)");

        }

        System.out.println("\nTopic 管理示例完成\n");
    }

    /**
     * 示例2: 生产者
     * - 同步发送
     * - 异步发送(带回调)
     * - 批量发送
     * - 自定义分区策略
     */
    private static void producerDemo() {
        System.out.println("【示例2】生产者");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // 性能优化配置
        props.put(ProducerConfig.ACKS_CONFIG, "1"); // 0: 不等待, 1: leader确认, all: 全部副本确认
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            
            // 1. 同步发送
            System.out.println("\n✅ 同步发送消息:");
            for (int i = 0; i < 5; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    TOPIC_NAME,
                    "key-" + i,
                    "message-" + i
                );
                
                try {
                    RecordMetadata metadata = producer.send(record).get();
                    System.out.println(String.format(
                        "  消息发送成功: topic=%s, partition=%d, offset=%d",
                        metadata.topic(), metadata.partition(), metadata.offset()
                    ));
                } catch (Exception e) {
                    System.err.println("  发送失败: " + e.getMessage());
                }
            }

            // 2. 异步发送(带回调)
            System.out.println("\n✅ 异步发送消息:");
            for (int i = 5; i < 10; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    TOPIC_NAME,
                    "key-" + i,
                    "async-message-" + i
                );

                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception == null) {
                            System.out.println(String.format(
                                "  异步发送成功: partition=%d, offset=%d",
                                metadata.partition(), metadata.offset()
                            ));
                        } else {
                            System.err.println("  异步发送失败: " + exception.getMessage());
                        }
                    }
                });
            }

            // 等待异步发送完成
            producer.flush();

            // 3. 指定分区发送
            System.out.println("\n✅ 指定分区发送:");
            ProducerRecord<String, String> partitionRecord = new ProducerRecord<>(
                TOPIC_NAME,
                0, // 指定分区0
                "partition-key",
                "partition-message"
            );
            RecordMetadata metadata = producer.send(partitionRecord).get();
            System.out.println("  发送到分区: " + metadata.partition());

            // 4. 带时间戳的消息
            System.out.println("\n✅ 带时间戳的消息:");
            ProducerRecord<String, String> timestampRecord = new ProducerRecord<>(
                TOPIC_NAME,
                null,
                System.currentTimeMillis(),
                "timestamp-key",
                "timestamp-message"
            );
            producer.send(timestampRecord).get();
            System.out.println("  带时间戳消息发送完成");

        } catch (Exception e) {
            System.err.println("生产者执行失败: " + e.getMessage());
        }

        System.out.println("\n生产者示例完成\n");
    }

    /**
     * 示例3: 消费者
     * - 订阅 Topic
     * - 拉取消息
     * - 手动提交偏移量
     * - 消费者组
     */
    private static void consumerDemo() {
        System.out.println("【示例3】消费者");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        
        // 消费者配置
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // 手动提交
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // earliest, latest, none
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            
            // 订阅 Topic
            consumer.subscribe(Collections.singletonList(TOPIC_NAME));
            System.out.println("\n✅ 订阅 Topic: " + TOPIC_NAME);

            // 拉取消息(仅拉取一次用于演示)
            System.out.println("\n✅ 拉取消息:");
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            
            System.out.println("  拉取到 " + records.count() + " 条消息");
            
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(String.format(
                    "  topic=%s, partition=%d, offset=%d, key=%s, value=%s",
                    record.topic(), record.partition(), record.offset(),
                    record.key(), record.value()
                ));
            }

            // 手动提交偏移量
            consumer.commitSync();
            System.out.println("\n✅ 手动提交偏移量完成");

            // 查询分区信息
            consumer.partitionsFor(TOPIC_NAME).forEach(partitionInfo -> {
                System.out.println(String.format(
                    "  分区: %d, Leader: %d, Replicas: %d",
                    partitionInfo.partition(),
                    partitionInfo.leader().id(),
                    partitionInfo.replicas().length
                ));
            });

        } catch (Exception e) {
            System.err.println("消费者执行失败: " + e.getMessage());
        }

        System.out.println("\n消费者示例完成\n");
    }

    /**
     * 示例4: 事务消息
     * - 精确一次语义(Exactly-Once)
     * - 事务性生产者
     */
    private static void transactionalProducer() {
        System.out.println("【示例4】事务消息");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // 事务配置
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my-transactional-id");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            
            // 初始化事务
            producer.initTransactions();
            System.out.println("\n✅ 初始化事务");

            try {
                // 开始事务
                producer.beginTransaction();
                System.out.println("✅ 开始事务");

                // 发送消息
                for (int i = 0; i < 3; i++) {
                    ProducerRecord<String, String> record = new ProducerRecord<>(
                        TOPIC_NAME,
                        "txn-key-" + i,
                        "txn-message-" + i
                    );
                    producer.send(record);
                }
                System.out.println("✅ 发送3条事务消息");

                // 提交事务
                producer.commitTransaction();
                System.out.println("✅ 提交事务成功");

            } catch (Exception e) {
                // 回滚事务
                producer.abortTransaction();
                System.err.println("✅ 事务回滚: " + e.getMessage());
            }

        } catch (Exception e) {
            System.err.println("事务消息执行失败: " + e.getMessage());
        }

        System.out.println("\n事务消息示例完成\n");
    }

    /**
     * 示例5: 高级配置
     * - 性能优化
     * - 可靠性配置
     * - 监控指标
     */
    private static void advancedConfiguration() {
        System.out.println("【示例5】高级配置");

        System.out.println("\n✅ 生产者性能优化配置:");
        System.out.println("  batch.size: 16384 (批量大小)");
        System.out.println("  linger.ms: 10 (延迟时间)");
        System.out.println("  compression.type: snappy (压缩类型)");
        System.out.println("  buffer.memory: 32MB (缓冲区大小)");
        System.out.println("  max.in.flight.requests.per.connection: 5");

        System.out.println("\n✅ 生产者可靠性配置:");
        System.out.println("  acks: all (等待所有副本确认)");
        System.out.println("  retries: 3 (重试次数)");
        System.out.println("  enable.idempotence: true (幂等性)");
        System.out.println("  transactional.id: xxx (事务ID)");

        System.out.println("\n✅ 消费者性能优化配置:");
        System.out.println("  fetch.min.bytes: 1024 (最小拉取字节)");
        System.out.println("  fetch.max.wait.ms: 500 (最大等待时间)");
        System.out.println("  max.poll.records: 500 (单次拉取记录数)");
        System.out.println("  max.partition.fetch.bytes: 1MB");

        System.out.println("\n✅ 消费者可靠性配置:");
        System.out.println("  enable.auto.commit: false (手动提交)");
        System.out.println("  isolation.level: read_committed (读已提交)");
        System.out.println("  auto.offset.reset: earliest");

        System.out.println("\n✅ Kafka 性能指标:");
        System.out.println("  吞吐量: 百万级 TPS");
        System.out.println("  延迟: 毫秒级");
        System.out.println("  存储: PB级数据");

        System.out.println("\n高级配置示例完成\n");
    }

    // ==================== 辅助类 ====================

    /**
     * 自定义分区器
     */
    public static class CustomPartitioner implements Partitioner {
        @Override
        public int partition(String topic, Object key, byte[] keyBytes,
                           Object value, byte[] valueBytes, Cluster cluster) {
            // 根据 key 的哈希值选择分区
            int numPartitions = cluster.partitionCountForTopic(topic);
            return Math.abs(key.hashCode()) % numPartitions;
        }

        @Override
        public void close() {}

        @Override
        public void configure(Map<String, ?> configs) {}
    }

    /**
     * 自定义拦截器
     */
    public static class CustomProducerInterceptor implements ProducerInterceptor<String, String> {
        
        @Override
        public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
            // 发送前处理
            System.out.println("拦截器: 发送消息 " + record.value());
            return record;
        }

        @Override
        public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
            // 发送后回调
            if (exception == null) {
                System.out.println("拦截器: 消息已确认");
            }
        }

        @Override
        public void close() {}

        @Override
        public void configure(Map<String, ?> configs) {}
    }
}
