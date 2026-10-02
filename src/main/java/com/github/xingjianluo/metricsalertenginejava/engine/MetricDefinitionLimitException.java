package com.github.xingjianluo.metricsalertenginejava.engine;

import com.github.xingjianluo.metricsalertenginejava.model.RejectReason;

/**
 * 指标定义 / 定义版本数量超过容量上限时的显式拒绝。
 * 该异常路径同时累加 {@code MetricsRegistry.definitionsRejected}，可观测、不静默。
 */
public class MetricDefinitionLimitException extends RuntimeException {

    private final RejectReason reason;

    public MetricDefinitionLimitException(RejectReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public RejectReason reason() {
        return reason;
    }
}
