package com.demo.kafka.consumer.metrics;

import java.util.concurrent.atomic.AtomicLongArray;

/**
 * 线程业务占空比（Duty Cycle）与细分耗时追踪器。
 * <p>
 * 设计目标与特性：
 * 1. Zero-GC：在业务循环中状态切换无任何堆对象分配，单次切换耗时 <= 20ns。
 * 2. Lock-Free：单写多读模型，业务线程纳秒级单向记录，无锁竞争。
 * 3. 增量采样：由采样线程周期性（如 1s）调用 sampleDelta() 计算秒级利用率画像与细分占比。
 */
public class ThreadDutyTracker {

    /**
     * 业务状态枚举定义。
     */
    public enum State {
        IDLE(false),
        WAIT_BATCH(false),
        BUSY_POLL(true),
        BUSY_DISPATCH(true),
        BUSY_DECODE(true),
        BUSY_SORT(true),
        BUSY_WRITE(true);

        private final boolean busy;

        State(boolean busy) {
            this.busy = busy;
        }

        public boolean isBusy() {
            return busy;
        }
    }

    // 兼容文档与扩展别名
    public static final State BUSY_FETCH_POLL = State.BUSY_POLL;
    public static final State BUSY_FETCH_DISPATCH = State.BUSY_DISPATCH;

    /**
     * 采样快照数据模型。
     */
    public record DutyCycleSnapshot(
            double busyPercent,
            double idlePercent,
            String status,
            double decodePercent,
            double sortPercent,
            double writePercent,
            double pollPercent,
            double dispatchPercent,
            double waitBatchPercent
    ) {
        public static final DutyCycleSnapshot EMPTY = new DutyCycleSnapshot(
                0.0, 100.0, "IDLE", 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
        );
    }

    private final String threadName;
    private final AtomicLongArray stateDurations;
    private final long[] lastSampledCumulative;
    private final long[] deltaBuffer;

    private volatile State currentState = State.IDLE;
    private volatile long lastTransitionTime = System.nanoTime();
    private volatile DutyCycleSnapshot lastSnapshot = DutyCycleSnapshot.EMPTY;

    public ThreadDutyTracker(String threadName) {
        this.threadName = threadName;
        int numStates = State.values().length;
        this.stateDurations = new AtomicLongArray(numStates);
        this.lastSampledCumulative = new long[numStates];
        this.deltaBuffer = new long[numStates];
    }

    /**
     * 业务线程状态转移（单写者极简无锁，Zero-GC）。
     *
     * @param newState 目标状态
     */
    public void transitionTo(State newState) {
        if (newState == null || newState == this.currentState) {
            return;
        }
        long now = System.nanoTime();
        long elapsed = now - this.lastTransitionTime;
        this.lastTransitionTime = now;
        if (elapsed > 0) {
            this.stateDurations.addAndGet(this.currentState.ordinal(), elapsed);
        }
        this.currentState = newState;
    }

    /**
     * 将当前处于进行中的状态直接更名为新状态。
     * 例如 Fetcher 在 poll 结束且无数据时，可将其归为 IDLE 状态。
     *
     * @param newState 纠偏后的目标状态
     */
    public void reclassifyActiveState(State newState) {
        if (newState != null) {
            this.currentState = newState;
        }
    }

    /**
     * 由定时采样线程（单读线程）调用，计算上个采样区间到当前的差值并更新快照。
     *
     * @return 最新的秒级利用率快照
     */
    public DutyCycleSnapshot sampleDelta() {
        long now = System.nanoTime();
        State activeState = this.currentState;
        long lastTrans = this.lastTransitionTime;
        long activeElapsed = Math.max(0, now - lastTrans);

        State[] states = State.values();
        long totalDeltaNanos = 0;
        long totalBusyNanos = 0;

        for (int i = 0; i < states.length; i++) {
            long currentCumulative = this.stateDurations.get(i);
            if (states[i] == activeState) {
                currentCumulative += activeElapsed;
            }
            long delta = Math.max(0, currentCumulative - this.lastSampledCumulative[i]);
            this.deltaBuffer[i] = delta;
            this.lastSampledCumulative[i] = currentCumulative;

            totalDeltaNanos += delta;
            if (states[i].isBusy()) {
                totalBusyNanos += delta;
            }
        }

        if (totalDeltaNanos <= 0) {
            DutyCycleSnapshot snap = DutyCycleSnapshot.EMPTY;
            this.lastSnapshot = snap;
            return snap;
        }

        double rawBusyPercent = (totalBusyNanos * 100.0) / totalDeltaNanos;
        double busyPercent = Math.min(100.0, Math.max(0.0, Math.round(rawBusyPercent * 10.0) / 10.0));
        double idlePercent = Math.round((100.0 - busyPercent) * 10.0) / 10.0;

        String status;
        if (busyPercent < 20.0) {
            status = "IDLE";
        } else if (busyPercent < 70.0) {
            status = "NORMAL";
        } else if (busyPercent < 90.0) {
            status = "BUSY";
        } else {
            status = "OVERLOAD";
        }

        double decodePercent = roundPercent(deltaBuffer[State.BUSY_DECODE.ordinal()], totalDeltaNanos);
        double sortPercent = roundPercent(deltaBuffer[State.BUSY_SORT.ordinal()], totalDeltaNanos);
        double writePercent = roundPercent(deltaBuffer[State.BUSY_WRITE.ordinal()], totalDeltaNanos);
        double pollPercent = roundPercent(deltaBuffer[State.BUSY_POLL.ordinal()], totalDeltaNanos);
        double dispatchPercent = roundPercent(deltaBuffer[State.BUSY_DISPATCH.ordinal()], totalDeltaNanos);
        double waitBatchPercent = roundPercent(deltaBuffer[State.WAIT_BATCH.ordinal()], totalDeltaNanos);

        DutyCycleSnapshot snapshot = new DutyCycleSnapshot(
                busyPercent,
                idlePercent,
                status,
                decodePercent,
                sortPercent,
                writePercent,
                pollPercent,
                dispatchPercent,
                waitBatchPercent
        );

        this.lastSnapshot = snapshot;
        return snapshot;
    }

    private double roundPercent(long delta, long total) {
        if (total <= 0 || delta <= 0) return 0.0;
        double pct = (delta * 100.0) / total;
        return Math.round(pct * 10.0) / 10.0;
    }

    public DutyCycleSnapshot getLastSnapshot() {
        return lastSnapshot;
    }

    public State getCurrentState() {
        return currentState;
    }

    public String getThreadName() {
        return threadName;
    }
}
