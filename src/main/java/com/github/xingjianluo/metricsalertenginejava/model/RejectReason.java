package com.github.xingjianluo.metricsalertenginejava.model;

/**
 * 数据被拒绝 / 丢弃的可区分原因。
 */
public enum RejectReason {
    /** 标签名或标签值不合法 */
    INVALID_LABEL,
    /** 指标名不合法 */
    INVALID_METRIC_NAME,
    /** 超出单指标或全局时间线基数上限 */
    CARDINALITY_LIMIT,
    /** 时间戳早于保留窗口或超出未来容差 */
    TIMESTAMP_OUT_OF_RANGE,
    /** 同一时间线同一时间戳重复写入 */
    DUPLICATE,
    /** 写入队列已满，按降级策略丢弃新数据（消费过慢） */
    QUEUE_FULL,
    /** 服务已关闭，拒绝新写入 */
    SHUTDOWN,
    /** 值超出已登记指标定义（当前最新版本）允许的取值范围 */
    VALUE_OUT_OF_RANGE,
    /** 使用了已登记指标定义（当前最新版本）不允许的标签键 */
    LABEL_KEY_NOT_ALLOWED
}
