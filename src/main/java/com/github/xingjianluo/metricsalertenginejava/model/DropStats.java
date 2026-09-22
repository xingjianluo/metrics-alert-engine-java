package com.github.xingjianluo.metricsalertenginejava.model;

import java.util.EnumMap;
import java.util.Map;

/**
 * 容量保护丢弃计数（按原因区分）。线程安全。
 */
public final class DropStats {

    private final Map<DropReason, Long> counters = new EnumMap<>(DropReason.class);
    private final Map<DropReason, String> lastSample = new EnumMap<>(DropReason.class);

    public DropStats() {
        for (DropReason reason : DropReason.values()) {
            counters.put(reason, 0L);
        }
    }

    public void record(DropReason reason, String detail) {
        // TODO 骨架
    }

    public long count(DropReason reason) {
        // TODO 骨架
        return 0L;
    }

    public Map<DropReason, Long> snapshot() {
        // TODO 骨架
        return Map.of();
    }

    public long totalDropped() {
        // TODO 骨架
        return 0L;
    }
}
