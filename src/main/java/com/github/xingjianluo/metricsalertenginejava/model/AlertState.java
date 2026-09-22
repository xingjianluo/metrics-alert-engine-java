package com.github.xingjianluo.metricsalertenginejava.model;

/**
 * 告警状态机：
 * <pre>
 *  NORMAL --连续满足 forThreshold 次--> PENDING --再连续满足一次--> FIRING
 *  FIRING/PENDING --连续不满足 recoveryThreshold 次--> RECOVERING
 *  RECOVERING --再连续不满足一次--> NORMAL；一旦中途满足则立即回到 FIRING
 * </pre>
 */
public enum AlertState {
    NORMAL, PENDING, FIRING, RECOVERING
}
