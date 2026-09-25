package com.demo.kafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * 滚动 JSON Lines 文件写入器。
 * <p>
 * 每个 Worker 独占一个写入器实例，支持：
 * - 按文件大小滚动（默认 50MB）
 * - 按时间滚动（默认 2 分钟）
 * - 1MB BufferedWriter 缓冲区提升写盘吞吐
 * <p>
 * 文件命名格式: data_worker{workerId}_{yyyyMMdd_HHmmss}_{index}.jsonl
 */
public class RollingJsonWriter {
    private static final Logger log = LoggerFactory.getLogger(RollingJsonWriter.class);

    private final String dirPath;
    private final int workerId;
    private final long maxFileSize;
    private final long maxFileAgeMs;
    private final boolean noDisk;

    private BufferedWriter writer;
    private long currentFileSize = 0;
    private long fileOpenTime = 0;
    private int fileIndex = 0;
    private volatile long totalBytesWritten = 0;
    private long totalLinesWritten = 0;
    private long linesSinceLastFlush = 0;
    private long lastFlushTime = 0;
    private String currentFileName;

    private static final int FLUSH_INTERVAL_LINES = 1000;
    private static final long FLUSH_INTERVAL_MS = 2000;

    private static final DateTimeFormatter TS_FMT = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");

    /**
     * 创建滚动写入器。
     *
     * @param dirPath     输出目录路径
     * @param workerId    Worker 编号（用于文件名区分）
     * @param maxFileSizeMB 单文件最大 MB（达到后滚动）
     * @param maxFileAgeMinutes 单文件最大存活分钟数（达到后滚动）
     * @param noDisk      是否开启纯内存不落盘模式
     */
    public RollingJsonWriter(String dirPath, int workerId, int maxFileSizeMB, int maxFileAgeMinutes, boolean noDisk) {
        this.dirPath = dirPath;
        this.workerId = workerId;
        this.maxFileSize = (long) maxFileSizeMB * 1024 * 1024;
        this.maxFileAgeMs = (long) maxFileAgeMinutes * 60 * 1000;
        this.noDisk = noDisk;
        if (!noDisk) {
            new File(dirPath).mkdirs();
            rollFile();
        } else {
            this.currentFileName = "(in-memory / no-disk)";
            log.info("Worker {} initialized in NO-DISK mode (disk write disabled).", workerId);
        }
    }

    /**
     * 支持指定 noDisk 开关的构造器。
     */
    public RollingJsonWriter(String dirPath, int workerId, boolean noDisk) {
        this(dirPath, workerId, 50, 2, noDisk);
    }

    /**
     * 使用默认参数创建（50MB / 2 分钟滚动，默认落盘）。
     */
    public RollingJsonWriter(String dirPath, int workerId) {
        this(dirPath, workerId, 50, 2, false);
    }

    /**
     * 写入一行 JSON。线程安全（由 Worker 单线程独占调用，无需 synchronized）。
     */
    public void writeLine(String jsonLine) throws IOException {
        if (noDisk) {
            totalLinesWritten++;
            if (jsonLine != null) {
                totalBytesWritten += (jsonLine.length() + 1);
            }
            return;
        }

        long now = System.currentTimeMillis();
        if (currentFileSize >= maxFileSize || (now - fileOpenTime) >= maxFileAgeMs) {
            rollFile();
        }
        writer.write(jsonLine);
        writer.newLine();
        // 近似计算字节数（避免每行都创建 byte[] 的 GC 开销）
        long lineBytes = jsonLine.length() + 1;
        currentFileSize += lineBytes;
        totalBytesWritten += lineBytes;
        totalLinesWritten++;
        linesSinceLastFlush++;

        // 定期 flush：每 1000 行或每 2 秒，确保数据及时落盘
        if (linesSinceLastFlush >= FLUSH_INTERVAL_LINES || (now - lastFlushTime) >= FLUSH_INTERVAL_MS) {
            writer.flush();
            linesSinceLastFlush = 0;
            lastFlushTime = now;
        }
    }

    /**
     * 滚动到新文件。
     */
    private void rollFile() {
        try {
            if (writer != null) {
                writer.flush();
                writer.close();
                log.info("Closed JSONL file: {} (size: {} KB, lines: {})",
                        currentFileName,
                        currentFileSize / 1024,
                        totalLinesWritten);
            }

            String timestamp = LocalDateTime.now().format(TS_FMT);
            currentFileName = String.format("data_worker%02d_%s_%04d.jsonl",
                    workerId, timestamp, ++fileIndex);
            File file = new File(dirPath, currentFileName);

            // 1MB 缓冲区 —— 减少系统调用次数，提升顺序写吞吐
            this.writer = new BufferedWriter(new FileWriter(file, StandardCharsets.UTF_8, true), 1024 * 1024);
            this.currentFileSize = 0;
            this.fileOpenTime = System.currentTimeMillis();
            this.lastFlushTime = this.fileOpenTime;
            this.linesSinceLastFlush = 0;

            log.info("Rolled new JSONL file: {}", file.getAbsolutePath());
        } catch (IOException e) {
            log.error("Failed to roll JSONL file for worker {}", workerId, e);
            throw new RuntimeException("Cannot create output file", e);
        }
    }

    /**
     * 强制刷盘并关闭文件句柄。用于优雅停机阶段。
     */
    public void flushAndClose() {
        if (noDisk) {
            log.info("No-disk mode: worker {} processed total_lines={}", workerId, totalLinesWritten);
            return;
        }
        try {
            if (writer != null) {
                writer.flush();
                writer.close();
                log.info("Final flush for worker {}: file={}, total_lines={}",
                        workerId, currentFileName, totalLinesWritten);
            }
        } catch (IOException e) {
            log.error("Error flushing writer for worker {}", workerId, e);
        }
    }

    public boolean isNoDisk() {
        return noDisk;
    }

    public long getTotalLinesWritten() {
        return totalLinesWritten;
    }

    public long getTotalBytesWritten() {
        return totalBytesWritten;
    }

    public String getCurrentFileName() {
        return currentFileName;
    }

    public long getCurrentFileSize() {
        return currentFileSize;
    }

    public int getFileIndex() {
        return fileIndex;
    }
}
