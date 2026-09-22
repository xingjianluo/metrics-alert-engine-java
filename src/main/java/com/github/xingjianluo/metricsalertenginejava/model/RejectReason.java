package com.github.xingjianluo.metricsalertenginejava.model;

/** 写入被拒绝的可区分原因。 */
public enum RejectReason {
    METRIC_UNKNOWN,
    INVALID_LABEL,
    CARDINALITY_EXCEEDED,
    TIMESTAMP_OUT_OF_RANGE,
    INVALID_VALUE,
    INVALID_METRIC_NAME
}
