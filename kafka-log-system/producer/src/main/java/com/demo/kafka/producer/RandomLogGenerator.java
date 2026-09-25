package com.demo.kafka.producer;

import com.demo.kafka.common.proto.LogMessageProto.LogMessage;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 随机日志数据生成器。
 * 模拟多个微服务产生的各级别日志，数据字段力求逼真。
 */
public class RandomLogGenerator {

    // 模拟的微服务名称列表
    private static final String[] SERVICES = {
            "order-service", "payment-service", "user-service",
            "inventory-service", "notification-service", "gateway-service",
            "auth-service", "search-service"
    };

    // 模拟的线程名前缀
    private static final String[] THREAD_PREFIXES = {
            "http-nio-8080-exec-", "scheduling-", "async-task-",
            "kafka-listener-", "pool-worker-", "reactor-http-nio-"
    };

    // 各级别日志消息模板
    private static final String[] DEBUG_MESSAGES = {
            "Entering method processOrder with orderId={}",
            "Cache hit for key: user_session_{}",
            "SQL query executed in {}ms: SELECT * FROM orders WHERE id = ?",
            "Request headers: Content-Type=application/json, Accept=*/*",
            "Connection pool stats: active={}, idle={}, total={}"
    };

    private static final String[] INFO_MESSAGES = {
            "Order {} created successfully, total amount: ${}",
            "User {} logged in from IP {}",
            "Payment processed: txnId={}, amount=${}, currency=USD",
            "Inventory updated for SKU {}: quantity {} -> {}",
            "Email notification sent to {} for order {}",
            "Service started on port {} in {}ms",
            "Health check passed: database=UP, redis=UP, kafka=UP",
            "Batch job completed: processed {} records in {}ms"
    };

    private static final String[] WARN_MESSAGES = {
            "Slow query detected ({}ms): SELECT * FROM products WHERE category = ?",
            "Connection pool near capacity: {}/{} connections in use",
            "Retry attempt {}/3 for external API call to {}",
            "Request rate approaching limit: {}/1000 requests per minute",
            "Deprecated API endpoint called: GET /api/v1/users",
            "Memory usage above 80%: heap={}MB/{}MB"
    };

    private static final String[] ERROR_MESSAGES = {
            "Failed to process order {}: insufficient inventory for SKU {}",
            "Payment gateway timeout after 30s for txnId={}",
            "Database connection failed: Connection refused to {}:3306",
            "NullPointerException in UserService.getProfile() at line 142",
            "Circuit breaker OPEN for service: {}. Fallback activated.",
            "Failed to send notification email: SMTP connection refused"
    };

    private static final String[] FATAL_MESSAGES = {
            "CRITICAL: Database master node unreachable, all write operations suspended",
            "CRITICAL: Kafka broker connection lost, message queue backed up {} messages",
            "CRITICAL: Out of disk space on /data partition, service shutting down"
    };

    // 模拟的主机 IP 段
    private static final String[] HOST_IPS;

    static {
        HOST_IPS = new String[8];
        for (int i = 0; i < 8; i++) {
            HOST_IPS[i] = "10.0." + (i / 4) + "." + (100 + i);
        }
    }

    private final String localHostIp;

    public RandomLogGenerator() {
        String ip;
        try {
            ip = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            ip = "127.0.0.1";
        }
        this.localHostIp = ip;
    }

    /**
     * 生成一条随机日志消息。
     */
    public LogMessage generate() {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        // 按权重随机选择日志级别 (DEBUG:10%, INFO:50%, WARN:25%, ERROR:12%, FATAL:3%)
        LogMessage.Level level = randomLevel(rnd);
        String message = randomMessage(level, rnd);
        String source = SERVICES[rnd.nextInt(SERVICES.length)];
        String threadName = THREAD_PREFIXES[rnd.nextInt(THREAD_PREFIXES.length)] + rnd.nextInt(1, 20);

        LogMessage.Builder builder = LogMessage.newBuilder()
                .setTimestamp(System.currentTimeMillis())
                .setLogId(UUID.randomUUID().toString())
                .setLevel(level)
                .setSource(source)
                .setThreadName(threadName)
                .setMessage(message)
                .setHostIp(HOST_IPS[rnd.nextInt(HOST_IPS.length)])
                .setTraceId(generateTraceId(rnd));

        // 随机添加扩展字段
        if (rnd.nextInt(100) < 30) { // 30% 概率带扩展字段
            builder.putExtraFields("request_id", "req-" + rnd.nextLong(100000, 999999));
            builder.putExtraFields("user_agent", "Mozilla/5.0 (compatible; ServiceClient/2.0)");
        }
        if (level == LogMessage.Level.ERROR || level == LogMessage.Level.FATAL) {
            builder.putExtraFields("stack_trace_hash", "STH-" + Integer.toHexString(rnd.nextInt()));
            builder.putExtraFields("alert_channel", "pagerduty");
        }

        return builder.build();
    }

    private LogMessage.Level randomLevel(ThreadLocalRandom rnd) {
        int roll = rnd.nextInt(100);
        if (roll < 10) return LogMessage.Level.DEBUG;
        if (roll < 60) return LogMessage.Level.INFO;
        if (roll < 85) return LogMessage.Level.WARN;
        if (roll < 97) return LogMessage.Level.ERROR;
        return LogMessage.Level.FATAL;
    }

    private String randomMessage(LogMessage.Level level, ThreadLocalRandom rnd) {
        String[] templates = switch (level) {
            case DEBUG -> DEBUG_MESSAGES;
            case INFO -> INFO_MESSAGES;
            case WARN -> WARN_MESSAGES;
            case ERROR -> ERROR_MESSAGES;
            case FATAL -> FATAL_MESSAGES;
            default -> INFO_MESSAGES;
        };

        String template = templates[rnd.nextInt(templates.length)];
        // 替换模板中的 {} 占位符为随机值
        return fillPlaceholders(template, rnd);
    }

    private String fillPlaceholders(String template, ThreadLocalRandom rnd) {
        StringBuilder sb = new StringBuilder(template.length() + 32);
        int i = 0;
        while (i < template.length()) {
            if (i + 1 < template.length() && template.charAt(i) == '{' && template.charAt(i + 1) == '}') {
                sb.append(rnd.nextInt(1, 99999));
                i += 2;
            } else {
                sb.append(template.charAt(i));
                i++;
            }
        }
        return sb.toString();
    }

    private String generateTraceId(ThreadLocalRandom rnd) {
        // 模拟 32 位十六进制 trace ID (类似 OpenTelemetry 格式)
        return String.format("%016x%016x", rnd.nextLong(), rnd.nextLong());
    }
}
