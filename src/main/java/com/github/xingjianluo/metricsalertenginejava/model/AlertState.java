package com.github.xingjianluo.metricsalertenginejava.model;

/**
 * 告警状态：
 * PENDING   待定：连续命中次数尚未达到触发阈值；
 * FIRING    触发：已发出触发通知（静默期内只记录不通知）；
 * RESOLVED  恢复：连续不命中达到恢复次数后回到正常态。
 */
public enum AlertState {
    PENDING,
    FIRING,
    RESOLVED
}
