package com.demo.kafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 消费者应用入口。
 *
 * 用法:
 *   java -jar consumer.jar                              # 默认参数启动
 *   java -jar consumer.jar --workers 8 --output /data/jsonl  # 自定义 Worker 数和输出目录
 *   java -jar consumer.jar --queue-size 10000            # 自定义队列容量
 */
public class ConsumerApp {
    private static final Logger log = LoggerFactory.getLogger(ConsumerApp.class);

    public static void main(String[] args) {
        // 默认 Worker 数 = CPU 核数
        int workers = Runtime.getRuntime().availableProcessors();
        String outputDir = "data/jsonl";
        int queueSize = 5000;
        int port = 8080;
        boolean noDisk = false;

        // 解析命令行参数
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--workers", "-w" -> {
                    if (i + 1 < args.length) workers = Integer.parseInt(args[++i]);
                }
                case "--output", "-o" -> {
                    if (i + 1 < args.length) outputDir = args[++i];
                }
                case "--queue-size", "-q" -> {
                    if (i + 1 < args.length) queueSize = Integer.parseInt(args[++i]);
                }
                case "--port", "-p" -> {
                    if (i + 1 < args.length) port = Integer.parseInt(args[++i]);
                }
                case "--no-disk", "--dry-run", "-n" -> {
                    noDisk = true;
                }
                case "--help", "-h" -> {
                    printUsage();
                    return;
                }
            }
        }

        log.info("=== Kafka High-Throughput Consumer Starting ===");
        log.info("Workers: {}, Output: {}, Queue size: {}, Dashboard Port: {}, NoDisk: {}",
                workers, noDisk ? "(disabled / in-memory)" : outputDir, queueSize, port, noDisk);
        log.info("CPU cores detected: {}", Runtime.getRuntime().availableProcessors());

        HighThroughputConsumerEngine engine = new HighThroughputConsumerEngine(
                workers, outputDir, queueSize, port, noDisk);
        engine.start();

        try {
            engine.awaitTermination();
        } catch (InterruptedException e) {
            log.info("Main thread interrupted.");
        }

        log.info("=== Kafka High-Throughput Consumer Stopped ===");
    }

    private static void printUsage() {
        System.out.println("""
                Kafka High-Throughput Consumer — Protobuf Decode & Rolling JSONL
                
                Usage: java -jar consumer.jar [OPTIONS]
                
                Options:
                  --workers, -w <N>      Worker thread count (default: CPU cores)
                  --output, -o <DIR>     JSONL output directory (default: data/jsonl)
                  --queue-size, -q <N>   Per-worker queue capacity (default: 5000)
                  --port, -p <PORT>      Embedded Dashboard HTTP port (default: 8080, 0 to disable)
                  --no-disk, -n, --dry-run Disable disk writing (pure in-memory benchmarking)
                  --help, -h             Show this help
                
                Examples:
                  java -jar consumer.jar                          # Auto-detect workers, dashboard on :8080
                  java -jar consumer.jar --no-disk                # Benchmark mode without disk write
                  java -jar consumer.jar -w 4 -o /tmp/output      # 4 workers, custom dir
                  java -jar consumer.jar -p 9090                  # Custom dashboard port
                """);
    }
}
