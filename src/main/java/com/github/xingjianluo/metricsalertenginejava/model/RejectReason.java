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
    /** 指标定义声明的取值范围之外（定义口径拒绝，区别于数据格式问题） */
    DEFINITION_VALUE_OUT_OF_RANGE,
    /** 用到了指标定义白名单之外的标签键（定义口径拒绝，区别于标签格式非法） */
    DEFINITION_LABEL_KEY_NOT_ALLOWED,
    /** 指标定义数量或单指标定义版本数量超过容量上限 */
    DEFINITION_LIMIT_EXCEEDED
}
