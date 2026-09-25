package com.demo.kafka.producer;

import com.demo.kafka.common.KafkaConfig;
import com.demo.kafka.common.TopicConstants;
import com.demo.kafka.common.proto.LogMessageProto.LogMessage;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Kafka 日志生产者。
 * 将 Protobuf 序列化的日志消息发送到 Kafka Topic，
 * 以 source（服务名）作为 partition key，保证同一服务的日志进入同一分区。
 */
public class LogProducer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(LogProducer.class);

    private final KafkaProducer<String, byte[]> producer;
    private final RandomLogGenerator generator;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicLong sentCount = new AtomicLong(0);
    private final AtomicLong errorCount = new AtomicLong(0);

    public LogProducer() {
        Properties props = KafkaConfig.producerBaseProps();
        this.producer = new KafkaProducer<>(props);
        this.generator = new RandomLogGenerator();
    }

    /**
     * 以指定速率持续发送日志消息。
     *
     * @param messagesPerSecond 每秒发送消息数（0 = 不限速，全速发送）
     */
    public void startProducing(int messagesPerSecond) {
        log.info("Starting log producer: target rate = {} msg/s, topic = {}",
                messagesPerSecond == 0 ? "UNLIMITED" : messagesPerSecond,
                TopicConstants.TOPIC_NAME);

        long intervalNanos = messagesPerSecond > 0 ? 1_000_000_000L / messagesPerSecond : 0;
        long lastLogTime = System.currentTimeMillis();
        long lastLogCount = 0;

        while (running.get()) {
            long cycleStart = System.nanoTime();

            LogMessage logMessage = generator.generate();
            byte[] payload = logMessage.toByteArray();

            // 用 source（服务名）做 partition key，同一服务的日志进入同一分区
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(
                    TopicConstants.TOPIC_NAME,
                    logMessage.getSource(),
                    payload
            );

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    errorCount.incrementAndGet();
                    if (errorCount.get() % 1000 == 1) { // 避免刷屏
                        log.error("Send failed: {}", exception.getMessage());
                    }
                } else {
                    sentCount.incrementAndGet();
                }
            });

            // 定期打印统计（每 5 秒）
            long now = System.currentTimeMillis();
            if (now - lastLogTime >= 5000) {
                long currentCount = sentCount.get();
                long delta = currentCount - lastLogCount;
                double rate = delta / ((now - lastLogTime) / 1000.0);
                log.info("Stats: sent={}, errors={}, rate={} msg/s, payload_size=~{}B",
                        currentCount, errorCount.get(), String.format("%.0f", rate), payload.length);
                lastLogTime = now;
                lastLogCount = currentCount;
            }

            // 速率控制
            if (intervalNanos > 0) {
                long elapsed = System.nanoTime() - cycleStart;
                long sleepNanos = intervalNanos - elapsed;
                if (sleepNanos > 0) {
                    try {
                        Thread.sleep(sleepNanos / 1_000_000, (int) (sleepNanos % 1_000_000));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }

        log.info("Producer stopping. Total sent: {}, errors: {}", sentCount.get(), errorCount.get());
    }

    /**
     * 发送指定数量的消息然后停止。
     *
     * @param totalMessages 要发送的消息总数
     * @param messagesPerSecond 每秒发送速率（0 = 不限速）
     */
    public void sendBatch(long totalMessages, int messagesPerSecond) {
        log.info("Sending batch: {} messages at {} msg/s to topic {}",
                totalMessages,
                messagesPerSecond == 0 ? "UNLIMITED" : messagesPerSecond,
                TopicConstants.TOPIC_NAME);

        long intervalNanos = messagesPerSecond > 0 ? 1_000_000_000L / messagesPerSecond : 0;
        long lastLogTime = System.currentTimeMillis();
        long lastLogCount = 0;

        for (long i = 0; i < totalMessages && running.get(); i++) {
            long cycleStart = System.nanoTime();

            LogMessage logMessage = generator.generate();
            byte[] payload = logMessage.toByteArray();

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(
                    TopicConstants.TOPIC_NAME,
                    logMessage.getSource(),
                    payload
            );

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    errorCount.incrementAndGet();
                } else {
                    sentCount.incrementAndGet();
                }
            });

            // 定期打印统计
            long now = System.currentTimeMillis();
            if (now - lastLogTime >= 5000) {
                long currentCount = sentCount.get();
                long delta = currentCount - lastLogCount;
                double rate = delta / ((now - lastLogTime) / 1000.0);
                double progress = (i + 1) * 100.0 / totalMessages;
                log.info("Progress: {}/{} ({}%), rate={} msg/s",
                        i + 1, totalMessages, String.format("%.1f", progress), String.format("%.0f", rate));
                lastLogTime = now;
                lastLogCount = currentCount;
            }

            // 速率控制
            if (intervalNanos > 0) {
                long elapsed = System.nanoTime() - cycleStart;
                long sleepNanos = intervalNanos - elapsed;
                if (sleepNanos > 0) {
                    try {
                        Thread.sleep(sleepNanos / 1_000_000, (int) (sleepNanos % 1_000_000));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }

        // 刷盘确保所有消息发送完成
        producer.flush();
        log.info("Batch complete. Total sent: {}, errors: {}", sentCount.get(), errorCount.get());
    }

    public void stop() {
        running.set(false);
    }

    @Override
    public void close() {
        running.set(false);
        producer.flush();
        producer.close();
        log.info("Producer closed.");
    }
}
