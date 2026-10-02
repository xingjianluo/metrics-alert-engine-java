package com.github.xingjianluo.metricsalertenginejava.store;

import com.github.xingjianluo.metricsalertenginejava.model.RejectReason;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 可观测计数：接受量、各类拒绝 / 丢弃原因计数、指标定义操作计数，全部原子累加。
 * 定义容量超限不会被悄悄吞掉：除拒绝调用外还累加 {@code definitionRejected}。
 */
public class MetricsRegistry {

    private final AtomicLong accepted = new AtomicLong();
    private final AtomicLong definitionsCreated = new AtomicLong();
    private final AtomicLong definitionsUpdated = new AtomicLong();
    private final AtomicLong definitionsRejected = new AtomicLong();
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

    /** 指标定义首次登记成功。 */
    public void recordDefinitionCreated() {
        definitionsCreated.incrementAndGet();
    }

    /** 指标定义更新产生新版本成功。 */
    public void recordDefinitionUpdated() {
        definitionsUpdated.incrementAndGet();
    }

    /** 指标定义登记 / 更新被拒绝（容量超限等），保证异常路径可观测。 */
    public void recordDefinitionRejected() {
        definitionsRejected.incrementAndGet();
    }

    public long acceptedCount() {
        return accepted.get();
    }

    public long definitionsCreatedCount() {
        return definitionsCreated.get();
    }

    public long definitionsUpdatedCount() {
        return definitionsUpdated.get();
    }

    public long definitionsRejectedCount() {
        return definitionsRejected.get();
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
