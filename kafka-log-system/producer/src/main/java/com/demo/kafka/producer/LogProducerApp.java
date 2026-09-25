package com.demo.kafka.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 日志生产者应用入口。
 *
 * 用法:
 *   java -jar producer.jar                    # 持续发送, 1000 msg/s
 *   java -jar producer.jar --rate 5000        # 持续发送, 5000 msg/s
 *   java -jar producer.jar --rate 0           # 持续发送, 不限速(全速)
 *   java -jar producer.jar --count 100000     # 发送 10 万条后停止
 *   java -jar producer.jar --count 100000 --rate 2000  # 发送 10 万条, 2000 msg/s
 */
public class LogProducerApp {
    private static final Logger log = LoggerFactory.getLogger(LogProducerApp.class);

    public static void main(String[] args) {
        int rate = 1000;          // 默认每秒 1000 条
        long count = -1;          // 默认持续发送 (-1 = 无限)

        // 解析命令行参数
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--rate", "-r" -> {
                    if (i + 1 < args.length) rate = Integer.parseInt(args[++i]);
                }
                case "--count", "-c" -> {
                    if (i + 1 < args.length) count = Long.parseLong(args[++i]);
                }
                case "--help", "-h" -> {
                    printUsage();
                    return;
                }
            }
        }

        log.info("=== Kafka Log Producer Starting ===");
        log.info("Mode: {}, Rate: {} msg/s",
                count > 0 ? "Batch (" + count + " messages)" : "Continuous",
                rate == 0 ? "UNLIMITED" : rate);

        try (LogProducer producer = new LogProducer()) {
            // 注册 Shutdown Hook，Ctrl+C 优雅停止
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutdown signal received, stopping producer...");
                producer.stop();
            }, "producer-shutdown-hook"));

            if (count > 0) {
                producer.sendBatch(count, rate);
            } else {
                producer.startProducing(rate);
            }
        }

        log.info("=== Kafka Log Producer Stopped ===");
    }

    private static void printUsage() {
        System.out.println("""
                Kafka Log Producer — Random Log Event Generator
                
                Usage: java -jar producer.jar [OPTIONS]
                
                Options:
                  --rate, -r <N>    Messages per second (default: 1000, 0 = unlimited)
                  --count, -c <N>   Total messages to send (default: continuous)
                  --help, -h        Show this help
                
                Examples:
                  java -jar producer.jar                      # 1000 msg/s, continuous
                  java -jar producer.jar --rate 5000           # 5000 msg/s, continuous
                  java -jar producer.jar --count 100000        # 100K messages then stop
                  java -jar producer.jar -c 50000 -r 2000      # 50K messages at 2000/s
                """);
    }
}
