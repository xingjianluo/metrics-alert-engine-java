package com.github.xingjianluo.metricsalertenginejava.store;

import com.github.xingjianluo.metricsalertenginejava.model.RejectReason;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 可观测计数：接受量、各类拒绝 / 丢弃原因计数，全部原子累加。
 */
public class MetricsRegistry {

    private final AtomicLong accepted = new AtomicLong();
    private final EnumMap<RejectReason, AtomicLong> rejected = new EnumMap<>(RejectReason.class);

    public MetricsRegistry() {
        for (RejectReason reason : RejectReason.values()) {
            rejected.put(reason, new AtomicLong());
        }
    }

    public void recordAccepted() {
        accepted.incrementAndGet();
    }

    public void recordRejected(RejectReason reason) {
        rejected.get(reason).incrementAndGet();
    }

    public long acceptedCount() {
        return accepted.get();
    }

    public Map<RejectReason, Long> rejectedCounts() {
        EnumMap<RejectReason, Long> snapshot = new EnumMap<>(RejectReason.class);
        rejected.forEach((reason, counter) -> snapshot.put(reason, counter.get()));
        return snapshot;
    }

    public long rejectedTotal() {
        return rejected.values().stream().mapToLong(AtomicLong::get).sum();
    }
}
