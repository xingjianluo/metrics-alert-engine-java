package com.github.xingjianluo.metricsalertenginejava.engine;

import com.github.xingjianluo.metricsalertenginejava.rule.AlertRule;
import com.github.xingjianluo.metricsalertenginejava.model.LabelSet;

/**
 * 异步评估任务：对某条规则的某个序列，评估到窗口 toWindowStart 为止。
 * 异步队列仅用于“消费过慢时降级”的演示与验证；窗口内状态推进仍由 AlertEvaluator 串行确定。
 */
public record EvalTask(AlertRule rule, LabelSet labels, long toWindowStart, long enqueueNanos) {
}
