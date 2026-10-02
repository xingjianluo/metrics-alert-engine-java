package com.github.xingjianluo.metricsalertenginejava.model;

/**
 * 单次写入的结论。accepted=false 时 reason 给出可区分的拒绝原因。
 *
 * @param accepted               是否成功落库
 * @param reason                 拒绝原因（接受时为 null）
 * @param detail                 判定明细
 * @param sample                 被写入 / 被拒绝的采样
 * @param definitionVersion      校验时依据的指标定义版本；该指标未登记定义时为 null（宽松模式）
 */
public record WriteResult(boolean accepted, RejectReason reason, String detail, Sample sample,
                          MetricVersion definitionVersion) {

    public static WriteResult accepted(Sample sample) {
        return accepted(sample, null);
    }

    public static WriteResult accepted(Sample sample, MetricVersion definitionVersion) {
        return new WriteResult(true, null, "accepted", sample, definitionVersion);
    }

    public static WriteResult rejected(RejectReason reason, String detail, Sample sample) {
        return rejected(reason, detail, sample, null);
    }

    public static WriteResult rejected(RejectReason reason, String detail, Sample sample,
                                       MetricVersion definitionVersion) {
        return new WriteResult(false, reason, detail, sample, definitionVersion);
    }
}
