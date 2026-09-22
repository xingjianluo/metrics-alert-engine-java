package com.github.xingjianluo.metricsalertenginejava.model;

import java.util.EnumMap;
import java.util.Map;

/**
 * 容量保护丢弃计数（按原因区分）。线程安全：所有计数与最近一条样例在同一把锁下更新。
 */
public final class DropStats {

    private final Map<DropReason, Long> counters = new EnumMap<>(DropReason.class);
    private final Map<DropReason, String> lastSample = new EnumMap<>(DropReason.class);
    private final Object lock = new Object();

    public DropStats() {
        synchronized (lock) {
            for (DropReason reason : DropReason.values()) {
                counters.put(reason, 0L);
            }
        }
    }

    /** 记录一次丢弃，detail 保留最近一条可观测证据（如指标名、窗口起点）。 */
    public void record(DropReason reason, String detail) {
        synchronized (lock) {
            counters.merge(reason, 1L, Long::sum);
            lastSample.put(reason, detail);
        }
    }

    public long count(DropReason reason) {
        synchronized (lock) {
            return counters.getOrDefault(reason, 0L);
        }
    }

    /** 最近一条丢弃证据，便于日志与排障。 */
    public String lastSample(DropReason reason) {
        synchronized (lock) {
            return lastSample.get(reason);
        }
    }

    public Map<DropReason, Long> snapshot() {
        synchronized (lock) {
            return new EnumMap<>(counters);
        }
    }

    public long totalDropped() {
        synchronized (lock) {
            long total = 0L;
            for (long v : counters.values()) {
                total += v;
            }
            return total;
        }
    }
}
