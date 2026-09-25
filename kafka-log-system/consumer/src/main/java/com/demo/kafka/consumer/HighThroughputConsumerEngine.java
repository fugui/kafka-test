package com.demo.kafka.consumer;

import com.demo.kafka.common.KafkaConfig;
import com.demo.kafka.common.TopicConstants;
import com.demo.kafka.common.proto.LogMessageProto.LogMessage;
import com.demo.kafka.consumer.dashboard.DashboardServer;
import com.demo.kafka.consumer.metrics.ConsumerMetricsCollector;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 高吞吐 Kafka 消费者引擎。
 * <p>
 * 架构设计：
 * - Fetcher 线程：单线程专职 poll，零业务逻辑
 * - 条带化路由：同一 Partition 的消息路由到同一 Worker 队列
 * - Worker 线程池：各 Worker 独占一个队列 + 一个 JSONL 写入器
 *   - 微批取出 → Protobuf 解码 → TimSort 按时间戳排序 → JSONL 追加写
 * - 双重背压：队列高水位 → pause；低水位 → resume
 * - 批次屏障位移提交：只有 Worker 全部处理完一批才提交 offset
 * - 两阶段优雅停机
 */
public class HighThroughputConsumerEngine {
    private static final Logger log = LoggerFactory.getLogger(HighThroughputConsumerEngine.class);

    // ========== 核心组件 ==========
    private final KafkaConsumer<String, byte[]> consumer;
    private final int workerCount;
    private final List<BlockingQueue<TaskWrapper>> workerQueues;
    private final List<Thread> workerThreads;
    private final List<RollingJsonWriter> writers;
    private final Thread fetcherThread;
    private final ProtobufDecoder decoder;
    private final ObjectMapper objectMapper;
    private final AtomicBoolean running = new AtomicBoolean(true);

    // ========== 监控与仪表盘 ==========
    private final ConsumerMetricsCollector metricsCollector;
    private final DashboardServer dashboardServer;
    private final boolean noDisk;

    // ========== 背压参数 ==========
    private final int queueCapacity;
    private final int highWatermark;
    private final int lowWatermark;
    private volatile boolean isPaused = false;

    // ========== 统计计数器 ==========
    private final AtomicLong totalConsumed = new AtomicLong(0);
    private final AtomicLong totalDecodeErrors = new AtomicLong(0);
    private final AtomicLong totalWritten = new AtomicLong(0);

    // ========== 内部记录类型 ==========
    record TaskWrapper(ConsumerRecord<String, byte[]> record, CountDownLatch latch) {}
    record DecodedRecordWrapper(LogMessage message, TaskWrapper taskWrapper) {}

    /**
     * 自定义线程工厂 —— 规范线程命名，便于 jstack 定位。
     */
    static class NamedThreadFactory implements ThreadFactory {
        private final String prefix;
        private final AtomicInteger counter = new AtomicInteger(0);

        public NamedThreadFactory(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, prefix + String.format("%02d", counter.getAndIncrement()));
            t.setDaemon(true);
            return t;
        }
    }

    /**
     * 构建消费者引擎（使用默认 8080 端口启动实时监控 Dashboard，默认落盘）。
     *
     * @param workerCount  Worker 线程数（建议等于 CPU 核数）
     * @param outputDir    JSONL 输出目录
     * @param queueCapacity 每个 Worker 队列的容量上限
     */
    public HighThroughputConsumerEngine(int workerCount, String outputDir, int queueCapacity) {
        this(workerCount, outputDir, queueCapacity, 8080, false);
    }

    /**
     * 构建消费者引擎（默认落盘）。
     *
     * @param workerCount   Worker 线程数（建议等于 CPU 核数）
     * @param outputDir     JSONL 输出目录
     * @param queueCapacity  每个 Worker 队列的容量上限
     * @param dashboardPort 实时监控 Web 端口（<=0 则不启动 Web）
     */
    public HighThroughputConsumerEngine(int workerCount, String outputDir, int queueCapacity, int dashboardPort) {
        this(workerCount, outputDir, queueCapacity, dashboardPort, false);
    }

    /**
     * 构建消费者引擎（支持配置 noDisk 纯内存模式）。
     *
     * @param workerCount   Worker 线程数（建议等于 CPU 核数）
     * @param outputDir     JSONL 输出目录
     * @param queueCapacity  每个 Worker 队列的容量上限
     * @param dashboardPort 实时监控 Web 端口（<=0 则不启动 Web）
     * @param noDisk        是否开启纯内存不落盘压测模式
     */
    public HighThroughputConsumerEngine(int workerCount, String outputDir, int queueCapacity, int dashboardPort, boolean noDisk) {
        this.workerCount = workerCount;
        this.queueCapacity = queueCapacity;
        this.highWatermark = (int) (queueCapacity * 0.8);
        this.lowWatermark = (int) (queueCapacity * 0.2);
        this.noDisk = noDisk;

        // Kafka Consumer
        Properties props = KafkaConfig.consumerBaseProps();
        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(List.of(TopicConstants.TOPIC_NAME));

        // 解码器 & JSON 序列化器
        this.decoder = new ProtobufDecoder();
        this.objectMapper = new ObjectMapper();

        // 初始化 Worker 队列、线程与写入器
        this.workerQueues = new ArrayList<>(workerCount);
        this.workerThreads = new ArrayList<>(workerCount);
        this.writers = new ArrayList<>(workerCount);

        for (int i = 0; i < workerCount; i++) {
            BlockingQueue<TaskWrapper> queue = new ArrayBlockingQueue<>(queueCapacity);
            workerQueues.add(queue);

            RollingJsonWriter writer = new RollingJsonWriter(outputDir, i, noDisk);
            writers.add(writer);

            final int workerId = i;
            Thread workerThread = new Thread(() -> workerLoop(workerId, queue, writer),
                    "kfk-worker-" + String.format("%02d", i));
            workerThread.setDaemon(true);
            workerThreads.add(workerThread);
        }

        this.fetcherThread = new Thread(this::fetchLoop, "kfk-fetcher-0");
        this.fetcherThread.setDaemon(true);

        // 初始化实时监控指标收集器与仪表盘
        this.metricsCollector = new ConsumerMetricsCollector(
                workerCount, queueCapacity, highWatermark, lowWatermark,
                workerQueues, writers, consumer, noDisk);

        if (dashboardPort > 0) {
            this.dashboardServer = new DashboardServer(dashboardPort, metricsCollector);
        } else {
            this.dashboardServer = null;
        }
    }

    /**
     * 启动引擎：先启动所有 Worker 线程，再启动 Fetcher 线程与监控看板。
     */
    public void start() {
        log.info("Starting HighThroughputConsumerEngine: {} Workers, queue_capacity={}, noDisk={}",
                workerCount, queueCapacity, noDisk);

        // 注册优雅停机钩子
        registerShutdownHook();

        // 启动 Worker 线程
        for (Thread t : workerThreads) {
            t.start();
        }

        // 启动 Fetcher 线程
        fetcherThread.start();

        // 启动 Dashboard Web 服务
        if (dashboardServer != null) {
            try {
                dashboardServer.start();
            } catch (Exception e) {
                log.error("Failed to start DashboardServer", e);
            }
        }

        log.info("Engine started: 1 Fetcher + {} Workers running.", workerCount);
    }

    /**
     * 阻塞等待引擎终止。
     */
    public void awaitTermination() throws InterruptedException {
        fetcherThread.join();
        for (Thread t : workerThreads) {
            t.join(15000);
        }
    }

    // ===========================
    // Fetcher 线程 — 纯净拉取逻辑
    // ===========================
    private void fetchLoop() {
        log.info("[Fetcher] started, polling topic: {}", TopicConstants.TOPIC_NAME);
        long lastStatsTime = System.currentTimeMillis();
        long lastConsumedCount = 0;

        try {
            while (running.get()) {
                // 1. 背压评估
                evaluateBackpressure();

                // 2. 拉取消息（即使 paused 也要 poll 以维持心跳）
                long pollStart = System.currentTimeMillis();
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));
                long pollDuration = System.currentTimeMillis() - pollStart;
                metricsCollector.updatePartitions(consumer.assignment());
                if (records.isEmpty()) continue;

                int batchSize = records.count();
                long batchBytes = 0;
                for (ConsumerRecord<String, byte[]> r : records) {
                    if (r.value() != null) {
                        batchBytes += r.value().length;
                    }
                }
                totalConsumed.addAndGet(batchSize);
                metricsCollector.recordFetch(batchSize, batchBytes, pollDuration);

                // 3. 创建批次屏障
                CountDownLatch batchLatch = new CountDownLatch(batchSize);

                // 4. 按 Partition 条带化路由到 Worker 队列
                for (ConsumerRecord<String, byte[]> record : records) {
                    int targetQueue = Math.abs(record.partition()) % workerCount;
                    try {
                        // 带超时的 put，避免无限阻塞
                        boolean offered = workerQueues.get(targetQueue).offer(
                                new TaskWrapper(record, batchLatch), 5, TimeUnit.SECONDS);
                        if (!offered) {
                            log.warn("[Fetcher] Queue {} full, dropped record at offset {}",
                                    targetQueue, record.offset());
                            batchLatch.countDown(); // 放行计数器
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }

                // 5. 等待批次屏障完成后提交 offset
                boolean completed = batchLatch.await(30, TimeUnit.SECONDS);
                if (completed) {
                    consumer.commitAsync((offsets, ex) -> {
                        if (ex != null) {
                            log.warn("[Fetcher] Offset commit failed: {}", ex.getMessage());
                        }
                    });
                } else {
                    log.error("[Fetcher] Batch barrier timeout! {} records in batch", batchSize);
                }

                // 6. 定期打印统计（每 10 秒）
                long now = System.currentTimeMillis();
                if (now - lastStatsTime >= 10000) {
                    long currentCount = totalConsumed.get();
                    long delta = currentCount - lastConsumedCount;
                    double rate = delta / ((now - lastStatsTime) / 1000.0);
                    int maxQueueDepth = workerQueues.stream()
                            .mapToInt(BlockingQueue::size).max().orElse(0);
                    log.info("[Stats] consumed={}, written={}, decode_errors={}, " +
                                    "rate={} msg/s, max_queue_depth={}/{}",
                            currentCount, totalWritten.get(), totalDecodeErrors.get(),
                            String.format("%.0f", rate), maxQueueDepth, queueCapacity);
                    lastStatsTime = now;
                    lastConsumedCount = currentCount;
                }

            }
        } catch (InterruptedException e) {
            log.info("[Fetcher] interrupted, shutting down.");
        } catch (Exception e) {
            log.error("[Fetcher] fatal error", e);
        }
        log.info("[Fetcher] exited.");
    }

    // ===========================
    // Worker 线程 — 解码/排序/落盘
    // ===========================
    private void workerLoop(int workerId, BlockingQueue<TaskWrapper> queue, RollingJsonWriter writer) {
        log.info("[Worker-{}] started.", workerId);
        List<TaskWrapper> miniBatch = new ArrayList<>(1000);

        while (running.get() || !queue.isEmpty()) {
            try {
                // 1. 微批批量抽取（最长等待 10ms，最多 1000 条）
                miniBatch.clear();
                TaskWrapper first = queue.poll(10, TimeUnit.MILLISECONDS);
                if (first != null) {
                    miniBatch.add(first);
                    queue.drainTo(miniBatch, 999);
                }
                if (miniBatch.isEmpty()) continue;

                // 2. 解码阶段：Protobuf byte[] → LogMessage
                List<DecodedRecordWrapper> decodedList = new ArrayList<>(miniBatch.size());
                int decodedCount = 0;
                int errorCount = 0;
                for (TaskWrapper tw : miniBatch) {
                    try {
                        LogMessage msg = decoder.decode(tw.record().value());
                        decodedList.add(new DecodedRecordWrapper(msg, tw));
                        decodedCount++;
                    } catch (Throwable t) {
                        // 毒丸消息隔离：记录错误，放行计数器
                        totalDecodeErrors.incrementAndGet();
                        errorCount++;
                        if (totalDecodeErrors.get() % 100 == 1) {
                            log.error("[Worker-{}] Poison pill at offset {}: {}",
                                    workerId, tw.record().offset(), t.getMessage());
                        }
                        tw.latch().countDown();
                    }
                }
                metricsCollector.recordDecode(decodedCount, errorCount);

                // 3. 微批排序：按业务时间戳升序 (TimSort: O(K log K))
                decodedList.sort(Comparator.comparingLong(d -> d.message().getTimestamp()));

                // 4. 序列化为 JSON 并追加写入 JSONL 文件（若 noDisk 则跳过序列化与物理写盘）
                int writtenCount = 0;
                for (DecodedRecordWrapper dw : decodedList) {
                    try {
                        if (!noDisk) {
                            String jsonLine = toJsonLine(dw.message());
                            writer.writeLine(jsonLine);
                        } else {
                            // 纯内存压测模式：跳过 toJsonLine 序列化与系统磁盘 I/O，最大化吞吐
                            writer.writeLine(null);
                        }
                        totalWritten.incrementAndGet();
                        writtenCount++;
                    } catch (Exception e) {
                        log.error("[Worker-{}] Write error: {}", workerId, e.getMessage());
                    } finally {
                        dw.taskWrapper().latch().countDown();
                    }
                }
                metricsCollector.recordWritten(writtenCount);

            } catch (InterruptedException ignored) {
                // 正常中断退出
            } catch (Exception e) {
                log.error("[Worker-{}] processing error", workerId, e);
            }
        }

        log.info("[Worker-{}] exited. Total lines written by this worker: {}",
                workerId, writer.getTotalLinesWritten());
    }

    // ===========================
    // JSON 序列化
    // ===========================
    private String toJsonLine(LogMessage msg) {
        ObjectNode node = objectMapper.createObjectNode();
        node.put("timestamp", msg.getTimestamp());
        node.put("log_id", msg.getLogId());
        node.put("level", msg.getLevel().name());
        node.put("source", msg.getSource());
        node.put("thread_name", msg.getThreadName());
        node.put("message", msg.getMessage());
        node.put("host_ip", msg.getHostIp());
        node.put("trace_id", msg.getTraceId());

        if (msg.getExtraFieldsCount() > 0) {
            ObjectNode extras = objectMapper.createObjectNode();
            msg.getExtraFieldsMap().forEach(extras::put);
            node.set("extra_fields", extras);
        }

        return node.toString();
    }

    // ===========================
    // 双重背压机制
    // ===========================
    private void evaluateBackpressure() {
        Set<TopicPartition> assignment = consumer.assignment();
        if (assignment.isEmpty()) return;

        int maxQueueDepth = workerQueues.stream()
                .mapToInt(BlockingQueue::size).max().orElse(0);

        if (!isPaused && maxQueueDepth >= highWatermark) {
            consumer.pause(assignment);
            isPaused = true;
            metricsCollector.setBackpressure(true);
            log.warn("[Backpressure] PAUSED — max queue depth: {}/{}", maxQueueDepth, queueCapacity);
        } else if (isPaused && maxQueueDepth <= lowWatermark) {
            consumer.resume(assignment);
            isPaused = false;
            metricsCollector.setBackpressure(false);
            log.info("[Backpressure] RESUMED — max queue depth: {}/{}", maxQueueDepth, queueCapacity);
        }
    }

    // ===========================
    // 两阶段优雅停机
    // ===========================
    private void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("=== Graceful shutdown initiating ===");
            running.set(false);

            try {
                // 阶段一：等待 Fetcher 停止，排空 Worker 队列
                log.info("[Phase 1] Waiting for Fetcher to stop...");
                fetcherThread.join(5000);

                log.info("[Phase 1] Waiting for Workers to drain queues...");
                for (Thread t : workerThreads) {
                    t.join(15000);
                }

                // 阶段二：强制刷盘并关闭文件句柄
                log.info("[Phase 2] Flushing all JSONL writers to disk...");
                for (RollingJsonWriter w : writers) {
                    w.flushAndClose();
                }

                // 安全关闭 Consumer（提交最终 offset）
                log.info("[Phase 2] Closing Kafka consumer...");
                consumer.close(Duration.ofSeconds(5));

                // 关闭 Dashboard 与指标收集器
                if (dashboardServer != null) {
                    dashboardServer.stop();
                }
                if (metricsCollector != null) {
                    metricsCollector.stop();
                }

                log.info("=== Shutdown completed. Total consumed: {}, written: {}, errors: {} ===",
                        totalConsumed.get(), totalWritten.get(), totalDecodeErrors.get());

            } catch (Exception e) {
                log.error("Shutdown error", e);
            }
        }, "kfk-shutdown-hook"));
    }

    public void stop() {
        running.set(false);
    }
}
