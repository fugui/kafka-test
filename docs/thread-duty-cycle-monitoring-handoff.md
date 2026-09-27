# 核心业务线程忙闲监控实施任务交接文件 (Handoff Prompt)

> **文档路径**：`docs/thread-duty-cycle-monitoring-handoff.md`  
> **创建时间**：2026-09-27  
> **任务目标**：基于“业务层占空比统计（Duty Cycle / Utilization Monitoring）”方案，实现对 Kafka Consumer 核心流水线线程（Fetcher 线程与所有 Worker 线程）的忙闲监控与细分耗时画像透视，集成至后端指标收集器、REST/SSE API 与前端 Dashboard 看板中。

---

## 1. 任务背景与核心目标

当前系统中，`HighThroughputConsumerEngine` 使用了 1 个 Fetcher 线程拉取消息并条带化路由到 N 个 Worker 独占队列，由各 Worker 线程进行 Protobuf 解码、微批排序与 JSONL 滚动落盘。

**核心痛点**：
仅凭 JVM 线程状态（`RUNNABLE`、`TIMED_WAITING`）无法真实反映业务忙闲（如 I/O 阻塞或等待网络仍显示为 `RUNNABLE`）。当系统吞吐未达预期时，运维人员无法直接判断瓶颈是在：
1. **上游 Kafka / 网络拉取**（Fetcher 饥饿等待消息）；
2. **下游 Worker 磁盘落盘阻塞**（Worker 忙于文件追加写）；
3. **Protobuf 反序列化 CPU 瓶颈**（Worker 忙于解码计算）；
4. **批次同步屏障**（Fetcher 等待下游 Worker 完成落盘）。

**交付目标**：
实现一套 **Zero-GC（零垃圾对象分配）、Lock-Free（无锁竞争）、纳秒级高精度** 的业务线程忙闲监控体系，提供秒级滑动窗口忙闲率及细分耗时画像。

---

## 2. 涉及的关键文件与模块

| 模块 / 文件路径 | 变更类型 | 说明 |
| :--- | :--- | :--- |
| `consumer/src/main/java/com/demo/kafka/consumer/metrics/ThreadDutyTracker.java` | **新建** | 线程本地轻量级状态追踪器与差值采样计算器 |
| `consumer/src/main/java/com/demo/kafka/consumer/HighThroughputConsumerEngine.java` | **修改** | 为 Fetcher 与各 Worker 绑定 Tracker，在核心循环中埋点 |
| `consumer/src/main/java/com/demo/kafka/consumer/metrics/ConsumerMetricsCollector.java` | **修改** | 接入各 Tracker，在 1 秒定时采样中计算增量利用率并输出 JSON |
| `docs/metrics-api-specification.md` | **修改** | 同步更新 REST `/api/metrics` 与 SSE `/api/stream` 的 JSON 契约 |
| `consumer/src/main/resources/static/index.html` | **修改** | 看板中展示 Worker 负荷进度条与阶段耗时 Hover 气泡 |

---

## 3. 详细设计与实施步骤

### 步骤一：新建 `ThreadDutyTracker.java`
- **包路径**：`com.demo.kafka.consumer.metrics`
- **核心要求**：
  1. **单写多读模型**：业务线程作为唯一写入者更新状态；采样线程每秒只读读取并计算差值。
  2. **状态定义**：
     - `IDLE`：等待中（Worker 等队列，或 Fetcher 等待 Kafka/下游屏障）。
     - `BUSY_FETCH_POLL` / `BUSY_FETCH_DISPATCH`：Fetcher 的数据分发与处理。
     - `BUSY_DECODE`：Worker Protobuf 解码（CPU 密集）。
     - `BUSY_SORT`：Worker 微批时间戳排序（CPU 密集）。
     - `BUSY_WRITE`：Worker JSON 序列化与磁盘写入（I/O 密集）。
  3. **数据结构**：使用连续 `long[]` 数组记录各状态累计纳秒耗时，避免对象创建。
  4. **增量采样方法**：提供 `sampleDelta()` 方法，由采样线程调用，计算与上次采样的纳秒差值，输出当前秒的：
     - `busyPercent`（0.0% ~ 100.0%）
     - `idlePercent`（0.0% ~ 100.0%）
     - `breakdown`（各细分状态在总时间中的占比）

### 步骤二：改造 `HighThroughputConsumerEngine.java` 循环埋点
1. **初始化**：
   - 初始化 1 个 Fetcher Tracker 和 `workerCount` 个 Worker Tracker，并注入到 `ConsumerMetricsCollector`。
2. **Worker 循环埋点 (`workerLoop`)**：
   ```java
   // 1. 等待队列 -> IDLE
   tracker.transitionTo(ThreadDutyTracker.State.IDLE);
   TaskWrapper first = queue.poll(10, TimeUnit.MILLISECONDS);
   if (first != null) {
       miniBatch.add(first);
       queue.drainTo(miniBatch, 999);
   }
   if (miniBatch.isEmpty()) continue;

   // 2. 解码 -> BUSY_DECODE
   tracker.transitionTo(ThreadDutyTracker.State.BUSY_DECODE);
   // ... Protobuf 解码 ...

   // 3. 排序 -> BUSY_SORT
   tracker.transitionTo(ThreadDutyTracker.State.BUSY_SORT);
   // ... TimSort 排序 ...

   // 4. 落盘 -> BUSY_WRITE
   tracker.transitionTo(ThreadDutyTracker.State.BUSY_WRITE);
   // ... 写盘与 latch.countDown() ...
   ```
3. **Fetcher 循环埋点 (`fetchLoop`)**：
   - `consumer.poll(...)` 无数据时归为 `IDLE`；有数据时计为有效拉取；
   - 路由分发循环标记为 `BUSY_DISPATCH`；
   - `batchLatch.await(...)` 标记为 `WAIT_BATCH`。

### 步骤三：改造 `ConsumerMetricsCollector.java` 采样与 JSON 导出
1. 在 `sampleSnapshot()` 中，遍历各 Worker Tracker 与 Fetcher Tracker 调用 `sampleDelta()`。
2. 在 `buildMetricsJson()` 的 `workers` 数组项中输出 `dutyCycle` 对象：
   ```json
   {
     "workerId": 0,
     "queueDepth": 12,
     "queueUtilization": 0.2,
     "dutyCycle": {
       "busyPercent": 78.5,
       "idlePercent": 21.5,
       "status": "BUSY",
       "breakdown": {
         "decodePercent": 25.1,
         "sortPercent": 3.2,
         "writePercent": 50.2
       }
     }
   }
   ```
3. 在 `threads.groups` 的 `pipeline` 线程中，注入对应的 `busyPercent`。

### 步骤四：更新接口规范文档 `docs/metrics-api-specification.md`
在 `2.5 workers` 和 `2.8 threads` 章节中补充 `dutyCycle` 字段的契约说明和状态判定阈值。

### 步骤五：前端看板 `index.html` 渲染与样式升级
1. **Worker 队列卡片**：
   - 在各 Worker 卡片内添加负荷率进度条（Duty Cycle），采用颜色预警（绿 `<50%`、蓝 `50-80%`、橙 `80-95%`、红 `>95%`）。
   - 添加鼠标悬浮气泡，展示 `解码: 25% | 排序: 3% | 写盘: 50% | 空闲: 22%`。
2. **系统线程组表格**：
   - Pipeline 核心线程在表格中增加一列或在名称旁显示实时忙碌百分比标签。

---

## 4. 验证与验收标准

1. **编译构建**：
   在 `kafka-log-system` 根目录下执行：
   ```bash
   mvn clean compile
   ```
   必须 0 警告、0 报错通过。
2. **高吞吐压测无 GC 劣化**：
   高频状态切换必须在单次 20ns 以内完成，不得在循环中 `new` 任何堆对象。
3. **接口契约校验**：
   访问 `http://localhost:8080/api/metrics`，验证 `workers[].dutyCycle` 结构是否符合规范且百分比之和收敛于 100%。
4. **前端看板展示**：
   打开 Web Dashboard，确认 Worker 卡片能够平滑展示忙闲指示条及细分比例悬浮窗。

---

## 5. 新会话实施直接指令 (Prompt to Copy)

> 如果你在新会话中启动任务，请直接将以下提示语发送给 Agent：

```text
请阅读 docs/thread-duty-cycle-monitoring-handoff.md 中的架构设计与实施规范。
请帮我完整实施核心业务线程（Fetcher 与 Worker）的业务占空比（Duty Cycle）忙闲监控功能。

实施步骤：
1. 创建 com.demo.kafka.consumer.metrics.ThreadDutyTracker 工具类；
2. 修改 HighThroughputConsumerEngine，接入 Tracker 并对 WorkerLoop 与 FetchLoop 进行低开销埋点；
3. 修改 ConsumerMetricsCollector，完成每秒采样、差值计算与 JSON 格式化；
4. 更新 docs/metrics-api-specification.md 接口文档；
5. 更新 consumer/src/main/resources/static/index.html 看板界面，展示 Worker 负荷率与细分耗时画像；
6. 执行 mvn clean compile 确保编译通过。
```
