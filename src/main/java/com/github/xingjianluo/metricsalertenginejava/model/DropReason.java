package com.github.xingjianluo.metricsalertenginejava.model;

/**
 * 容量保护 / 消费过慢时的丢弃（降级）原因。
 */
public enum DropReason {
    SERIES_BUFFER_FULL,
    GLOBAL_BUFFER_FULL,
    EVAL_QUEUE_FULL,
    RETENTION_TRIM
}
