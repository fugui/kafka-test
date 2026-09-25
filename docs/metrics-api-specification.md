# Kafka Consumer 实时指标监控接口规范与对接指南

> **版本**：v1.1.0  
> **服务组件**：Kafka High-Throughput Consumer Engine - Dashboard Server  
> **默认服务端口**：`8080` (支持配置指定)  
> **传输格式**：JSON (`application/json; charset=UTF-8`)  
> **跨域支持**：默认开启 CORS (`Access-Control-Allow-Origin: *`)

---

## 1. 概述与对接模式

本服务内嵌轻量级零依赖监控服务器，用于暴露消费者端的高吞吐流量、批处理状态、条带化队列深度、JVM 资源以及底层线程组健康度。外部系统（如内部监控大盘、告警组件、自动化调度平台、Prometheus Exporter 等）可通过以下两种模式与本系统对接：

```
                    ┌────────────────────────┐
                    │ Kafka Consumer Engine  │
                    │   (DashboardServer)    │
                    └───────────┬────────────┘
                                │
        ┌───────────────────────┴───────────────────────┐
        │                                               │
   [模式一: 轮询]                                  [模式二: 实时流]
 GET /api/metrics                                 GET /api/stream
  (REST JSON 快照)                                (SSE 持续推送, 1Hz)
        │                                               │
        ▼                                               ▼
┌──────────────┐                                ┌──────────────┐
│ 第三方采集器  │                                │ 实时监控前端  │
│ 监控告警系统  │                                │ 流式分析服务  │
└──────────────┘                                └──────────────┘
```

### 1.1 模式一：REST API 单次快照（Pull 模式）
* **接口路径**：`GET /api/metrics`
* **适用场景**：第三方监控系统定时抓取（如每 5s/10s 抓取一次）、运维巡检探针、健康度检查。
* **特性**：轻量无状态，毫秒级响应。

### 1.2 模式二：Server-Sent Events 实时事件流（Push 模式）
* **接口路径**：`GET /api/stream`
* **协议标准**：W3C Server-Sent Events (SSE)，HTTP 1.1 Chunked Transfer。
* **适用场景**：自定义前端实时面板、低延迟流式分析、告警监听。
* **帧格式**：
  ```http
  HTTP/1.1 200 OK
  Content-Type: text/event-stream; charset=UTF-8
  Cache-Control: no-cache
  Connection: keep-alive
  Access-Control-Allow-Origin: *

  data: {"uptimeSeconds":120,"rates":{...},"threads":{...}}

  data: {"uptimeSeconds":121,"rates":{...},"threads":{...}}
  ```
* **推送频率**：固定 1 秒 / 次（服务端心跳推送）。

---

## 2. 接口详细契约规范

### 2.1 顶级字段定义 (Root Object)

| 字段名 | 类型 | 说明 | 示例 |
| :--- | :--- | :--- | :--- |
| `uptimeSeconds` | Long | 消费者进程已持续运行的秒数 | `3600` |
| `noDisk` | Boolean | 是否开启纯内存不落盘压测模式 | `false` |
| `workerCount` | Integer | 条带化 Worker 线程总数 | `4` |
| `queueCapacity` | Integer | 每个 Worker 独占任务队列的容量上限 | `5000` |
| `highWatermark` | Integer | 触发背压的高水位阈值（默认 80% 容量） | `4000` |
| `lowWatermark` | Integer | 解除背压的低水位阈值（默认 20% 容量） | `1000` |
| `maxQueueDepth` | Integer | 当前所有 Worker 队列中的最大堆积深度 | `128` |
| `timestamp` | Long | 服务端快照生成时的 Unix 毫秒时间戳 | `1790337296000` |
| `rates` | Object | 瞬时（最近 1 秒）吞吐率指标 | 见 [2.2](#22-rates-瞬时吞吐速率对象) |
| `totals` | Object | 进程启动以来的全量累计计数 | 见 [2.3](#23-totals-累计总量对象) |
| `backpressure`| Object | 当前背压状态与历史触发统计 | 见 [2.4](#24-backpressure-背压状态对象) |
| `workers` | Array | 各条带化 Worker 详细状态列表 | 见 [2.5](#25-workers-worker-流水线数组) |
| `partitions` | Array | 当前消费者实例实际分配到的 Kafka 分区列表 | 见 [2.6](#26-partitions-分区分配数组) |
| `jvm` | Object | JVM 基础堆内存与活动线程摘要 | 见 [2.7](#27-jvm-资源对象) |
| `threads` | Object | **系统线程组与线程运行状态深度监控** | 见 [2.8](#28-threads-系统线程组与运行状态对象) |
| `history` | Array | 最近 60 秒的滑动窗口时序序列（长度 ≤ 60） | 见 [2.9](#29-history-60秒滑动时序数组) |

---

### 2.2 `rates` (瞬时吞吐速率对象)

| 字段名 | 类型 | 单位 | 说明 | 告警/监控建议 |
| :--- | :--- | :--- | :--- | :--- |
| `ingressTps` | Long | msg/s | Kafka 拉取消费速率（条/秒） | 过低可能代表上游断流或 Fetcher 挂起 |
| `ingressMBps` | Double | MB/s | Kafka 拉取网络吞吐（兆字节/秒） | 核心拉取带宽指标 |
| `writeTps` | Long | lines/s| 磁盘 JSONL 实际落盘速率（行/秒） | 正常情况下应与 `ingressTps` 保持一致 |
| `writeMBps` | Double | MB/s | 磁盘写入带宽（兆字节/秒） | 监控磁盘 I/O 压力 |
| `expansionRatio`| Double | 倍率 | 数据膨胀比（落盘体积 / 拉取体积） | Protobuf 转 JSON 典型膨胀约 1.2~1.8 倍 |

---

### 2.3 `totals` (累计总量对象)

| 字段名 | 类型 | 说明 |
| :--- | :--- | :--- |
| `consumedRecords` | Long | 累计从 Kafka 拉取的有效消息条数 |
| `consumedBytes` | Long | 累计拉取的 Kafka 原始网络字节数 |
| `decodedRecords` | Long | 累计成功解码的 Protobuf 消息条数 |
| `decodeErrors` | Long | 累计解码异常/毒丸（Poison Pill）消息条数（**>0 需告警**） |
| `writtenRecords` | Long | 累计落盘写入的 JSONL 数据行数 |
| `writtenBytes` | Long | 累计落盘写入的磁盘字节数 |

---

### 2.4 `backpressure` (背压状态对象)

| 字段名 | 类型 | 说明 | 告警/监控建议 |
| :--- | :--- | :--- | :--- |
| `isPaused` | Boolean | 当前是否处于暂停拉取（背压中） | 为 `true` 说明下游处理或磁盘 I/O 阻塞 |
| `triggerCount` | Long | 进程启动至今累计触发背压的次数 | 频繁触发说明 Worker 数量不足或磁盘瓶颈 |
| `totalPausedTimeMs`| Long | 累计处于暂停拉取状态的总毫秒数 | 评估背压持续时间对业务延迟的影响 |

---

### 2.5 `workers` (Worker 流水线数组)

数组中每个元素对应一个工作线程（Worker）的状态：

```json
{
  "workerId": 0,
  "queueDepth": 12,
  "queueUtilization": 0.2,
  "linesWritten": 125430,
  "bytesWritten": 23412000,
  "currentFile": "/data/jsonl/worker-00-20260925-185000.jsonl",
  "currentFileSizeKB": 22863
}
```

| 字段名 | 类型 | 说明 |
| :--- | :--- | :--- |
| `workerId` | Integer | Worker 编号（从 `0` 到 `workerCount - 1`） |
| `queueDepth` | Integer | 当前该 Worker 任务队列积压的消息批次数 |
| `queueUtilization` | Double | 队列使用率百分比（`queueDepth / queueCapacity * 100`） |
| `linesWritten` | Long | 该 Worker 累计写入的记录行数 |
| `bytesWritten` | Long | 该 Worker 累计写入磁盘的文件字节数 |
| `currentFile` | String | 该 Worker 当前正在追加写入的滚动文件绝对路径 |
| `currentFileSizeKB`| Long | 当前活跃滚动文件已写入的体积（KB） |

---

### 2.6 `partitions` (分区分配数组)

反映当前消费组分配给本实例的分区清单：

```json
[
  { "topic": "log-events", "partition": 0 },
  { "topic": "log-events", "partition": 1 }
]
```

---

### 2.7 `jvm` (资源对象)

| 字段名 | 类型 | 单位 | 说明 |
| :--- | :--- | :--- | :--- |
| `heapUsedMB` | Long | MB | JVM 堆内存当前已使用量 |
| `heapCommittedMB` | Long | MB | JVM 堆内存已申请提交量 |
| `heapMaxMB` | Long | MB | JVM 最大可用堆内存上限（-Xmx） |
| `threadCount` | Integer | 个 | JVM 当前存活的活动线程总数 |

---

### 2.8 `threads` (系统线程组与运行状态对象)

本节为系统线程组与各线程健康度的核心契约，支持快速排查 **Worker 饥饿、心跳挂起、死锁及阻塞**。

#### 2.8.1 `threads` 结构概览
```json
{
  "totalLive": 14,
  "peakCount": 14,
  "daemonCount": 12,
  "totalStarted": 14,
  "deadlockedCount": 0,
  "states": {
    "RUNNABLE": 6,
    "TIMED_WAITING": 5,
    "WAITING": 3,
    "BLOCKED": 0
  },
  "groups": [ ... ]
}
```

#### 2.8.2 `threads` 字段明细

| 字段名 | 类型 | 说明 | 诊断标准 |
| :--- | :--- | :--- | :--- |
| `totalLive` | Integer | 当前 JVM 活跃存活线程总数 | 出现非预期持续上升代表线程泄露 |
| `peakCount` | Integer | JVM 历史峰值线程数 | 用于容量规划与线程池规格校准 |
| `daemonCount` | Integer | 守护线程数量 | 核心 Worker 与采样器均为守护线程 |
| `totalStarted` | Long | 自 JVM 启动以来已创建的总线程数 | 评估线程创建/销毁的流动性 |
| `deadlockedCount` | Integer | **当前探测到的死锁线程数量** | **🚨 必须 = 0；若 > 0 必须立刻触发 P0 级严重告警！** |
| `states` | Object | 各运行状态的线程数量汇总 | 见下表 |

* **`states` 状态分布映射**：
  * `RUNNABLE`: 正在执行或就绪等待 CPU 调度的线程数。
  * `TIMED_WAITING`: 处于带超时挂起的线程数（如 Worker 等待队列、Kafka Poll 轮询）。
  * `WAITING`: 无超时挂起的线程数（等待锁通知、`CountDownLatch`、Condition）。
  * `BLOCKED`: **处于监视器锁冲突被挂起的线程数**（正常应为 `0`；若持续 > 0 说明存在严重互斥锁争用）。

#### 2.8.3 `groups` 业务分组结构
数组中的每个分组代表一类具有相似业务职能的线程集：

| 分组 `key` | 分组名称 | 包含线程前缀 | 业务职责 |
| :--- | :--- | :--- | :--- |
| `pipeline` | Pipeline 核心消费组 | `kfk-fetcher-*`, `kfk-worker-*` | 核心拉取线程与条带化消费落盘工作线程 |
| `kafka` | Kafka 客户端组 | `kafka-coordinator-*`, `consumer-*` | Kafka Broker 网络通信、心跳保活及协调者 |
| `dashboard` | 监控推流组 | `kfk-dashboard-*`, `kfk-metrics-*` | HTTP 服务请求处理、SSE 推流及 1s 采样器 |
| `system` | JVM 系统底层组 | `main`, `Common-Cleaner`, `Signal` 等 | JVM 运行时基础设施、垃圾回收及清理守护 |

#### 2.8.4 单个线程对象（`groups[].threads[]`）

```json
{
  "id": 20,
  "name": "kfk-worker-00",
  "state": "TIMED_WAITING",
  "daemon": true,
  "priority": 5,
  "lockName": "java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject@37d12375",
  "lockOwner": ""
}
```

| 字段名 | 类型 | 说明 |
| :--- | :--- | :--- |
| `id` | Long | 线程唯一标识符（Thread ID，对应 jstack 中的 nid） |
| `name` | String | 线程名称（规范化前缀，便于排查定位） |
| `state` | String | 线程状态：`RUNNABLE` \| `TIMED_WAITING` \| `WAITING` \| `BLOCKED` |
| `daemon` | Boolean | 是否为守护线程（`true` / `false`） |
| `priority` | Integer | 线程优先级（通常为 1~10，默认 5） |
| `lockName` | String | 当前正在等待的锁对象名称/类名及 Hash（未等锁时为空字符串） |
| `lockOwner`| String | 当前持有该锁的线程名称（如果处于 `BLOCKED` 且锁被其他线程占用时非空） |

---

### 2.9 `history` (60秒滑动时序数组)

环形滑动窗口时序序列，固定保留最近 60 秒的秒级数据点（每秒 1 点）：

```json
{
  "t": 1790337296136,
  "inTps": 8250,
  "inMBps": 12.8,
  "outTps": 8250,
  "outMBps": 18.4,
  "maxQueue": 142,
  "paused": false
}
```

| 字段名 | 类型 | 说明 |
| :--- | :--- | :--- |
| `t` | Long | 该秒对应的时间戳（Unix 毫秒） |
| `inTps` | Long | 该秒内的平均拉取 TPS |
| `inMBps` | Double | 该秒内的拉取带宽（MB/s） |
| `outTps` | Long | 该秒内的平均落盘 TPS |
| `outMBps` | Double | 该秒内的落盘写入带宽（MB/s） |
| `maxQueue`| Integer | 该秒采样时所有 Worker 的最大队列积压 |
| `paused` | Boolean | 该秒是否处于背压暂停状态 |

---

## 3. 多语言对接代码示例

### 3.1 Python (拉取快照与死锁/背压报警)

```python
import requests
import sys

METRICS_URL = "http://localhost:8080/api/metrics"

def check_consumer_health():
    try:
        resp = requests.get(METRICS_URL, timeout=3)
        resp.raise_for_status()
        data = resp.json()

        # 1. 检查死锁
        deadlocks = data.get("threads", {}).get("deadlockedCount", 0)
        if deadlocks > 0:
            print(f"[CRITICAL] 发现 {deadlocks} 个死锁线程！", file=sys.stderr)

        # 2. 检查背压状态
        is_paused = data.get("backpressure", {}).get("isPaused", False)
        tps = data.get("rates", {}).get("ingressTps", 0)
        in_mb = data.get("rates", {}).get("ingressMBps", 0.0)

        print(f"TPS: {tps} msg/s | 流量: {in_mb} MB/s | 背压状态: {'PAUSED' if is_paused else 'OK'}")

        # 3. 检查 Worker 队列堆积
        max_q = data.get("maxQueueDepth", 0)
        q_cap = data.get("queueCapacity", 5000)
        if max_q > q_cap * 0.8:
            print(f"[WARN] 队列堆积接近高水位: {max_q}/{q_cap}")

    except Exception as e:
        print(f"[ERROR] 获取指标失败: {e}", file=sys.stderr)

if __name__ == "__main__":
    check_consumer_health()
```

---

### 3.2 Python (SSE 实时推流监听)

```python
import json
import urllib.request

SSE_URL = "http://localhost:8080/api/stream"

def stream_metrics():
    req = urllib.request.Request(SSE_URL, headers={"Accept": "text/event-stream"})
    with urllib.request.urlopen(req) as stream:
        for line in stream:
            decoded = line.decode('utf-8').strip()
            if decoded.startswith("data:"):
                payload_json = decoded[len("data:"):].strip()
                metrics = json.loads(payload_json)
                rates = metrics["rates"]
                threads = metrics["threads"]
                print(f"[{metrics['uptimeSeconds']}s] 拉取: {rates['ingressMBps']} MB/s ({rates['ingressTps']} TPS) | "
                      f"活跃线程: {threads['totalLive']} (RUNNING: {threads['states']['RUNNABLE']})")

if __name__ == "__main__":
    stream_metrics()
```

---

### 3.3 Go (REST 查询与 Struct 反序列化)

```go
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type ConsumerMetrics struct {
	UptimeSeconds int64 `json:"uptimeSeconds"`
	Rates         struct {
		IngressTps  int64   `json:"ingressTps"`
		IngressMBps float64 `json:"ingressMBps"`
		WriteTps    int64   `json:"writeTps"`
		WriteMBps   float64 `json:"writeMBps"`
	} `json:"rates"`
	Backpressure struct {
		IsPaused     bool  `json:"isPaused"`
		TriggerCount int64 `json:"triggerCount"`
	} `json:"backpressure"`
	Threads struct {
		TotalLive       int `json:"totalLive"`
		DeadlockedCount int `json:"deadlockedCount"`
		States          struct {
			Runnable     int `json:"RUNNABLE"`
			TimedWaiting int `json:"TIMED_WAITING"`
			Blocked      int `json:"BLOCKED"`
		} `json:"states"`
	} `json:"threads"`
}

func main() {
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get("http://localhost:8080/api/metrics")
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	var m ConsumerMetrics
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		panic(err)
	}

	fmt.Printf("消费 TPS: %d msg/s, 流量: %.2f MB/s\n", m.Rates.IngressTps, m.Rates.IngressMBps)
	fmt.Printf("线程总数: %d, 死锁: %d, 阻塞(BLOCKED): %d\n",
		m.Threads.TotalLive, m.Threads.DeadlockedCount, m.Threads.States.Blocked)
}
```

---

### 3.4 浏览器原生 JavaScript (SSE 对接)

```javascript
const eventSource = new EventSource('http://localhost:8080/api/stream');

eventSource.onmessage = function(event) {
    const data = JSON.parse(event.data);
    
    // 更新流量指标
    console.log(`拉取吞吐: ${data.rates.ingressMBps} MB/s, 写入吞吐: ${data.rates.writeMBps} MB/s`);
    
    // 检测死锁
    if (data.threads.deadlockedCount > 0) {
        alert(`警告：消费者系统出现 ${data.threads.deadlockedCount} 个死锁线程！`);
    }
};

eventSource.onerror = function(err) {
    console.warn("SSE 连接中断，准备重试或降级为轮询 /api/metrics");
};
```

---

## 4. 关键告警规则推荐 (Prometheus / 监控系统)

| 监控项 | 告警表达式 / 规则 | 告警级别 | 建议处理动作 |
| :--- | :--- | :--- | :--- |
| **死锁探测** | `threads.deadlockedCount > 0` | **P0 (灾难)** | 立即执行 jstack 导出线程堆栈，准备重启实例 |
| **锁竞争严重**| `threads.states.BLOCKED > 0` 且持续 > 5s | **P1 (严重)** | 排查 Worker 或写入器是否有锁未释放 |
| **持续背压** | `backpressure.isPaused == true` 持续 > 30s | **P1 (严重)** | 检查磁盘 I/O 速度（iostat）或调大 Worker 数 |
| **数据毒丸** | `totals.decodeErrors 增量 > 0` | **P2 (警告)** | 检查上游是否发送了格式损坏的 Protobuf 载荷 |
| **队列高水位**| `maxQueueDepth >= queueCapacity * 0.8` | **P2 (警告)** | 下游处理能力即将饱和，预警背压 |
| **吞吐跌零** | `rates.ingressTps == 0` 持续 > 60s (且上游有流)| **P2 (警告)** | 检查 Fetcher 线程状态是否异常或消费组断连 |
