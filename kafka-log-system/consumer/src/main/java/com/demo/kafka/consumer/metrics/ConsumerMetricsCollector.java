package com.demo.kafka.consumer.metrics;

import com.demo.kafka.consumer.RollingJsonWriter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * 消费者高性能实时指标采集器。
 * <p>
 * 设计原则：
 * 1. 极低开销：使用 LongAdder 统计计数，零锁竞争，支持百万 TPS
 * 2. 环形滑动窗口：每 1 秒在独立后台线程快照一次，缓存最近 60 秒的瞬时吞吐与队列深度时序数据
 * 3. 兼容 SSE 实时推送与 REST API 轮询
 */
public class ConsumerMetricsCollector {
    private static final int HISTORY_WINDOW_SECONDS = 60;

    // ========== 基础计数器 ==========
    private final LongAdder totalRecordsConsumed = new LongAdder();
    private final LongAdder totalBytesConsumed = new LongAdder();
    private final LongAdder totalRecordsDecoded = new LongAdder();
    private final LongAdder totalDecodeErrors = new LongAdder();
    private final LongAdder totalRecordsWritten = new LongAdder();
    private final LongAdder backpressureCount = new LongAdder();
    private final LongAdder totalBackpressureTimeMs = new LongAdder();

    // ========== 瞬时状态 ==========
    private volatile boolean backpressurePaused = false;
    private volatile long backpressureStartTime = 0;
    private volatile int lastPollBatchSize = 0;
    private volatile long lastPollDurationMs = 0;

    // ========== 引擎依赖 ==========
    private final int workerCount;
    private final int queueCapacity;
    private final int highWatermark;
    private final int lowWatermark;
    private final List<? extends BlockingQueue<?>> workerQueues;
    private final List<RollingJsonWriter> writers;
    private final KafkaConsumer<?, ?> consumer;
    private final long startTimeMs = System.currentTimeMillis();

    // ========== 滑动窗口历史数据 ==========
    public record TimePoint(
            long timestamp,
            double ingressTps,
            double ingressMBps,
            double writeTps,
            double writeMBps,
            int maxQueueDepth,
            boolean isPaused
    ) {}

    private final TimePoint[] history = new TimePoint[HISTORY_WINDOW_SECONDS];
    private int historyIndex = 0;
    private int historyCount = 0;
    private final Object historyLock = new Object();

    // 上一秒基准
    private long lastSnapshotTime = System.currentTimeMillis();
    private long lastRecordsConsumed = 0;
    private long lastBytesConsumed = 0;
    private long lastRecordsWritten = 0;
    private long lastBytesWritten = 0;

    // 瞬时速率缓存
    private volatile double currentIngressTps = 0;
    private volatile double currentIngressMBps = 0;
    private volatile double currentWriteTps = 0;
    private volatile double currentWriteMBps = 0;
    private volatile double currentExpansionRatio = 0;

    private final ScheduledExecutorService scheduler;
    private final ObjectMapper mapper = new ObjectMapper();
    private final MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();

    public ConsumerMetricsCollector(
            int workerCount,
            int queueCapacity,
            int highWatermark,
            int lowWatermark,
            List<? extends BlockingQueue<?>> workerQueues,
            List<RollingJsonWriter> writers,
            KafkaConsumer<?, ?> consumer) {
        this.workerCount = workerCount;
        this.queueCapacity = queueCapacity;
        this.highWatermark = highWatermark;
        this.lowWatermark = lowWatermark;
        this.workerQueues = workerQueues;
        this.writers = writers;
        this.consumer = consumer;

        // 初始化滑动窗口计算器（每秒调度一次）
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "kfk-metrics-sampler");
            t.setDaemon(true);
            return t;
        });
        this.scheduler.scheduleAtFixedRate(this::sampleSnapshot, 1, 1, TimeUnit.SECONDS);
    }

    // ================= 打点方法（高吞吐批次更新） =================
    public void recordFetch(int recordCount, long bytes, long durationMs) {
        totalRecordsConsumed.add(recordCount);
        totalBytesConsumed.add(bytes);
        this.lastPollBatchSize = recordCount;
        this.lastPollDurationMs = durationMs;
    }

    public void recordDecode(int count, int errors) {
        totalRecordsDecoded.add(count);
        if (errors > 0) {
            totalDecodeErrors.add(errors);
        }
    }

    public void recordWritten(int count) {
        totalRecordsWritten.add(count);
    }

    public void setBackpressure(boolean paused) {
        if (this.backpressurePaused != paused) {
            this.backpressurePaused = paused;
            if (paused) {
                backpressureCount.increment();
                backpressureStartTime = System.currentTimeMillis();
            } else {
                if (backpressureStartTime > 0) {
                    totalBackpressureTimeMs.add(System.currentTimeMillis() - backpressureStartTime);
                    backpressureStartTime = 0;
                }
            }
        }
    }

    // ================= 每秒采样与速率计算 =================
    private void sampleSnapshot() {
        long now = System.currentTimeMillis();
        long elapsedMs = Math.max(1, now - lastSnapshotTime);
        double elapsedSec = elapsedMs / 1000.0;

        long currentConsumedRecords = totalRecordsConsumed.sum();
        long currentConsumedBytes = totalBytesConsumed.sum();
        long currentWrittenRecords = totalRecordsWritten.sum();
        long currentWrittenBytes = getTotalBytesWrittenFromWriters();

        long deltaConsumedRecords = Math.max(0, currentConsumedRecords - lastRecordsConsumed);
        long deltaConsumedBytes = Math.max(0, currentConsumedBytes - lastBytesConsumed);
        long deltaWrittenRecords = Math.max(0, currentWrittenRecords - lastRecordsWritten);
        long deltaWrittenBytes = Math.max(0, currentWrittenBytes - lastBytesWritten);

        double inTps = deltaConsumedRecords / elapsedSec;
        double inMBps = (deltaConsumedBytes / (1024.0 * 1024.0)) / elapsedSec;
        double outTps = deltaWrittenRecords / elapsedSec;
        double outMBps = (deltaWrittenBytes / (1024.0 * 1024.0)) / elapsedSec;
        double expansion = inMBps > 0.001 ? (outMBps / inMBps) : 0.0;

        int maxDepth = 0;
        for (BlockingQueue<?> q : workerQueues) {
            int size = q.size();
            if (size > maxDepth) maxDepth = size;
        }

        this.currentIngressTps = inTps;
        this.currentIngressMBps = inMBps;
        this.currentWriteTps = outTps;
        this.currentWriteMBps = outMBps;
        this.currentExpansionRatio = expansion;

        TimePoint point = new TimePoint(now, inTps, inMBps, outTps, outMBps, maxDepth, backpressurePaused);

        synchronized (historyLock) {
            history[historyIndex] = point;
            historyIndex = (historyIndex + 1) % HISTORY_WINDOW_SECONDS;
            if (historyCount < HISTORY_WINDOW_SECONDS) {
                historyCount++;
            }
        }

        lastSnapshotTime = now;
        lastRecordsConsumed = currentConsumedRecords;
        lastBytesConsumed = currentConsumedBytes;
        lastRecordsWritten = currentWrittenRecords;
        lastBytesWritten = currentWrittenBytes;
    }

    private long getTotalBytesWrittenFromWriters() {
        long sum = 0;
        for (RollingJsonWriter w : writers) {
            sum += w.getTotalBytesWritten();
        }
        return sum;
    }

    // ================= 格式化导出 JSON 快照 =================
    public String buildMetricsJson() {
        ObjectNode root = mapper.createObjectNode();

        // 1. 系统与概要
        root.put("uptimeSeconds", (System.currentTimeMillis() - startTimeMs) / 1000);
        root.put("workerCount", workerCount);
        root.put("queueCapacity", queueCapacity);
        root.put("highWatermark", highWatermark);
        root.put("lowWatermark", lowWatermark);
        root.put("timestamp", System.currentTimeMillis());

        // 2. 实时速率 (1s 瞬时)
        ObjectNode rates = root.putObject("rates");
        rates.put("ingressTps", Math.round(currentIngressTps));
        rates.put("ingressMBps", Math.round(currentIngressMBps * 100.0) / 100.0);
        rates.put("writeTps", Math.round(currentWriteTps));
        rates.put("writeMBps", Math.round(currentWriteMBps * 100.0) / 100.0);
        rates.put("expansionRatio", Math.round(currentExpansionRatio * 100.0) / 100.0);

        // 3. 累计量 (Totals)
        ObjectNode totals = root.putObject("totals");
        totals.put("consumedRecords", totalRecordsConsumed.sum());
        totals.put("consumedBytes", totalBytesConsumed.sum());
        totals.put("decodedRecords", totalRecordsDecoded.sum());
        totals.put("decodeErrors", totalDecodeErrors.sum());
        totals.put("writtenRecords", totalRecordsWritten.sum());
        totals.put("writtenBytes", getTotalBytesWrittenFromWriters());

        // 4. 背压监控
        ObjectNode backpressure = root.putObject("backpressure");
        backpressure.put("isPaused", backpressurePaused);
        backpressure.put("triggerCount", backpressureCount.sum());
        long currentPauseDuration = 0;
        if (backpressurePaused && backpressureStartTime > 0) {
            currentPauseDuration = System.currentTimeMillis() - backpressureStartTime;
        }
        backpressure.put("totalPausedTimeMs", totalBackpressureTimeMs.sum() + currentPauseDuration);

        // 5. Worker 条带化队列详情
        ArrayNode workerArray = root.putArray("workers");
        int maxDepth = 0;
        for (int i = 0; i < workerCount; i++) {
            ObjectNode wNode = workerArray.addObject();
            int qSize = workerQueues.get(i).size();
            if (qSize > maxDepth) maxDepth = qSize;
            RollingJsonWriter writer = writers.get(i);

            wNode.put("workerId", i);
            wNode.put("queueDepth", qSize);
            wNode.put("queueUtilization", Math.round((qSize * 100.0 / queueCapacity) * 10.0) / 10.0);
            wNode.put("linesWritten", writer.getTotalLinesWritten());
            wNode.put("bytesWritten", writer.getTotalBytesWritten());
            wNode.put("currentFile", writer.getCurrentFileName());
            wNode.put("currentFileSizeKB", writer.getCurrentFileSize() / 1024);
        }
        root.put("maxQueueDepth", maxDepth);

        // 6. Kafka 分区分配
        ArrayNode partitionsArray = root.putArray("partitions");
        try {
            Set<TopicPartition> assignment = consumer.assignment();
            for (TopicPartition tp : assignment) {
                ObjectNode pNode = partitionsArray.addObject();
                pNode.put("topic", tp.topic());
                pNode.put("partition", tp.partition());
            }
        } catch (Exception ignored) {}

        // 7. JVM 内存与线程
        ObjectNode jvm = root.putObject("jvm");
        MemoryUsage heapUsage = memoryBean.getHeapMemoryUsage();
        jvm.put("heapUsedMB", heapUsage.getUsed() / (1024 * 1024));
        jvm.put("heapCommittedMB", heapUsage.getCommitted() / (1024 * 1024));
        jvm.put("heapMaxMB", heapUsage.getMax() / (1024 * 1024));
        jvm.put("threadCount", Thread.activeCount());

        // 8. 60秒滑动时序历史数据
        ArrayNode historyArray = root.putArray("history");
        synchronized (historyLock) {
            int start = (historyCount == HISTORY_WINDOW_SECONDS) ? historyIndex : 0;
            for (int i = 0; i < historyCount; i++) {
                int idx = (start + i) % HISTORY_WINDOW_SECONDS;
                TimePoint pt = history[idx];
                if (pt != null) {
                    ObjectNode ptNode = historyArray.addObject();
                    ptNode.put("t", pt.timestamp());
                    ptNode.put("inTps", Math.round(pt.ingressTps()));
                    ptNode.put("inMBps", Math.round(pt.ingressMBps() * 10.0) / 10.0);
                    ptNode.put("outTps", Math.round(pt.writeTps()));
                    ptNode.put("outMBps", Math.round(pt.writeMBps() * 10.0) / 10.0);
                    ptNode.put("maxQueue", pt.maxQueueDepth());
                    ptNode.put("paused", pt.isPaused());
                }
            }
        }

        return root.toString();
    }

    public void stop() {
        scheduler.shutdown();
    }
}
