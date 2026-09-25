package com.demo.kafka.consumer.dashboard;

import com.demo.kafka.consumer.metrics.ConsumerMetricsCollector;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 内嵌轻量 Web 监控服务器（基于 JDK 内置 HttpServer）。
 * <p>
 * 特点：
 * 1. 零外部依赖：不依赖 Tomcat / Jetty / Spring Boot，不增加进程负担
 * 2. 守护线程执行：不干扰消费者优雅停机流程
 * 3. 支持 Server-Sent Events (SSE) 每秒主动推流与 REST API 查询
 */
public class DashboardServer {
    private static final Logger log = LoggerFactory.getLogger(DashboardServer.class);

    private final int port;
    private final ConsumerMetricsCollector metricsCollector;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private HttpServer server;
    private ExecutorService executor;
    private byte[] cachedIndexHtml;

    public DashboardServer(int port, ConsumerMetricsCollector metricsCollector) {
        this.port = port;
        this.metricsCollector = metricsCollector;
        loadIndexHtml();
    }

    private void loadIndexHtml() {
        try (InputStream is = getClass().getResourceAsStream("/static/index.html")) {
            if (is != null) {
                cachedIndexHtml = is.readAllBytes();
            } else {
                log.warn("static/index.html not found in classpath, using fallback.");
                cachedIndexHtml = "<html><body><h1>Dashboard template not found</h1></body></html>"
                        .getBytes(StandardCharsets.UTF_8);
            }
        } catch (Exception e) {
            log.error("Failed to load static/index.html", e);
            cachedIndexHtml = "<html><body><h1>Error loading dashboard</h1></body></html>"
                    .getBytes(StandardCharsets.UTF_8);
        }
    }

    public synchronized void start() throws IOException {
        if (running.get()) return;

        server = HttpServer.create(new InetSocketAddress(port), 0);

        // 首页 HTML
        server.createContext("/", new IndexHandler());

        // 实时指标 JSON 快照
        server.createContext("/api/metrics", new MetricsJsonHandler());

        // 实时指标 SSE 流 (Server-Sent Events)
        server.createContext("/api/stream", new SseStreamHandler());

        // 使用轻量虚拟线程或守护线程池
        executor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "kfk-dashboard-http");
            t.setDaemon(true);
            return t;
        });
        server.setExecutor(executor);

        server.start();
        running.set(true);

        log.info("==========================================================");
        log.info("⚡ Kafka Consumer Live Dashboard ready at: http://localhost:{}/", port);
        log.info("==========================================================");
    }

    public synchronized void stop() {
        if (!running.get()) return;
        running.set(false);
        try {
            if (server != null) {
                server.stop(1);
            }
            if (executor != null) {
                executor.shutdownNow();
            }
            log.info("DashboardServer stopped.");
        } catch (Exception e) {
            log.warn("Error stopping DashboardServer: {}", e.getMessage());
        }
    }

    /**
     * 首页静态文件 Handler
     */
    private class IndexHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }

            exchange.getResponseHeaders().set("Content-Type", "text/html; charset=UTF-8");
            exchange.getResponseHeaders().set("Cache-Control", "no-cache");
            exchange.sendResponseHeaders(200, cachedIndexHtml.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(cachedIndexHtml);
            }
        }
    }

    /**
     * JSON 快照 Handler
     */
    private class MetricsJsonHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }

            byte[] jsonBytes = metricsCollector.buildMetricsJson().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
            exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
            exchange.getResponseHeaders().set("Cache-Control", "no-cache");
            exchange.sendResponseHeaders(200, jsonBytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(jsonBytes);
            }
        }
    }

    /**
     * SSE 实时推流 Handler
     */
    private class SseStreamHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }

            exchange.getResponseHeaders().set("Content-Type", "text/event-stream; charset=UTF-8");
            exchange.getResponseHeaders().set("Cache-Control", "no-cache");
            exchange.getResponseHeaders().set("Connection", "keep-alive");
            exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");

            // 0 表示 Chunked 无限流响应
            exchange.sendResponseHeaders(200, 0);

            try (OutputStream os = exchange.getResponseBody()) {
                while (running.get()) {
                    String json = metricsCollector.buildMetricsJson();
                    String sseMsg = "data: " + json + "\n\n";
                    os.write(sseMsg.getBytes(StandardCharsets.UTF_8));
                    os.flush();

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            } catch (IOException e) {
                // 客户端浏览器刷新或关闭页面属于正常断连
            }
        }
    }
}
