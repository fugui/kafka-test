package com.demo.kafka.common;

/**
 * Topic 名称与分区数等常量定义。
 * Producer 和 Consumer 共同引用，保证一致性。
 */
public final class TopicConstants {

    private TopicConstants() {}

    /** Kafka Topic 名称 */
    public static final String TOPIC_NAME = "log-events";

    /** 分区数（本地演示环境） */
    public static final int PARTITION_COUNT = 8;

    /** 消费者组 ID */
    public static final String CONSUMER_GROUP_ID = "log-consumer-group";

    /** Kafka Broker 地址 */
    public static final String BOOTSTRAP_SERVERS = "localhost:9092";
}
