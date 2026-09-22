package com.github.xingjianluo.metricsalertenginejava.model;

/**
 * 容量保护 / 消费过慢时的丢弃（降级）原因。
 */
public enum DropReason {
    /** 单条序列点数超过上限，最旧点被淘汰。 */
    RETENTION_TRIM,
    /** 异步评估队列已满，评估任务被快速丢弃（数据本身仍已同步落库）。 */
    EVAL_QUEUE_FULL
}
