package com.demo.kafka.common;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * 共享的 Kafka 连接属性工厂。
 * Producer 和 Consumer 各取所需的基础配置，再叠加各自的特定参数。
 */
public final class KafkaConfig {

    private KafkaConfig() {}

    /**
     * 构建 Producer 基础配置。
     */
    public static Properties producerBaseProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, TopicConstants.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        // 高吞吐优化：批量发送
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);          // 64KB 批次
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);               // 最多等 5ms 凑批
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864L);   // 64MB 发送缓冲
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");    // LZ4 压缩
        props.put(ProducerConfig.ACKS_CONFIG, "1");                  // Leader 确认即可
        return props;
    }

    /**
     * 构建 Consumer 基础配置。
     */
    public static Properties consumerBaseProps() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, TopicConstants.BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, TopicConstants.CONSUMER_GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        // 禁用自动提交 —— 手动在批次屏障完成后提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // 大批量拉取以提高吞吐
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5000);
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1048576);    // 1MB 最小拉取
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 52428800);   // 50MB 最大拉取
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 10485760); // 10MB/分区
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }
}
