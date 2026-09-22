package com.github.xingjianluo.metricsalertenginejava.engine;

import com.github.xingjianluo.metricsalertenginejava.model.AggregateFunction;
import com.github.xingjianluo.metricsalertenginejava.model.AlertEvaluation;
import com.github.xingjianluo.metricsalertenginejava.model.AlertRule;
import com.github.xingjianluo.metricsalertenginejava.model.AlertState;
import com.github.xingjianluo.metricsalertenginejava.model.Labels;
import com.github.xingjianluo.metricsalertenginejava.model.Notification;
import com.github.xingjianluo.metricsalertenginejava.model.WindowStats;
import com.github.xingjianluo.metricsalertenginejava.store.AlertStateStore;
import com.github.xingjianluo.metricsalertenginejava.store.NotificationLog;
import com.github.xingjianluo.metricsalertenginejava.store.RuleStore;
import com.github.xingjianluo.metricsalertenginejava.store.TimeSeriesStore;
import com.github.xingjianluo.metricsalertenginejava.store.TimeSeriesStore.SeriesSnapshot;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 告警求值器：按规则版本对窗口聚合结果做确定的状态机迁移。
 *
 * <pre>
 *  NORMAL      --hits==forThreshold-->     PENDING（首次满足阈值，尚无通知）
 *  PENDING     --再满足一次(hits==forThreshold+1)--> FIRING（发 FIRING 通知）
 *  FIRING/PENDING --misses 增长但未达 recoveryThreshold，且再次满足--> 回到 FIRING（不重复通知）
 *  FIRING/PENDING --misses==recoveryThreshold--> RECOVERING
 *  RECOVERING  --再不满足一次(misses==recoveryThreshold+1)--> NORMAL（发 RECOVERED 通知）
 *  RECOVERING  --中途满足--> FIRING（不发通知：抖动不重复触发）
 * </pre>
 * 每个 (规则版本, 标签) 组合在 per-key 锁内串行迁移，同一窗口重复求值被跳过，
 * 因此规则抖动不会造成重复触发或状态错乱。
 */
public class AlertEvaluator {

    private final TimeSeriesStore store;
    private final RuleStore ruleStore;
    private final AlertStateStore stateStore;
    private final NotificationService notificationService;
    private final NotificationLog notificationLog;
    private final EngineClock clock;
    private final ConcurrentLinkedQueue<Notification> emitted = new ConcurrentLinkedQueue<>();

    public AlertEvaluator(TimeSeriesStore store,
                          RuleStore ruleStore,
                          AlertStateStore stateStore,
                          NotificationService notificationService,
                          NotificationLog notificationLog,
                          EngineClock clock) {
        this.store = store;
        this.ruleStore = ruleStore;
        this.stateStore = stateStore;
        this.notificationService = notificationService;
        this.notificationLog = notificationLog;
        this.clock = clock;
    }

    /** 对单条规则版本、单个标签组合求值一个窗口；窗口重复求值时返回 null。 */
    public AlertEvaluation evaluateWindow(AlertRule rule, Labels labels, long windowStart) {
        long aligned = floorWindow(windowStart, rule.windowMillis());
        double value = aggregateWindow(rule, labels, aligned);
        boolean met = !Double.isNaN(value) && rule.operator().test(value, rule.threshold());

        AlertEvaluation evaluation = stateStore.transition(rule.version(), labels, state -> {
            // 幂等：同一窗口不允许第二次驱动状态。
            if (state.lastEvaluatedWindowStart == aligned) {
                return null;
            }
            // 乱序保护：只接受时间向前的窗口，保证迁移确定。
            if (aligned < state.lastEvaluatedWindowStart) {
                return null;
            }
            AlertState from = state.state;
            AlertState to;
            String reason;
            if (met) {
                state.consecutiveHits++;
                state.consecutiveMisses = 0;
                if (from == AlertState.RECOVERING) {
                    to = AlertState.FIRING;
                    reason = "condition met during recovery -> back to FIRING without renotification";
                } else if (from == AlertState.FIRING) {
                    to = AlertState.FIRING;
                    reason = "condition still met -> remain FIRING";
                } else if (state.consecutiveHits == rule.forThreshold()) {
                    to = AlertState.PENDING;
                    reason = "forThreshold reached -> PENDING";
                } else if (from == AlertState.PENDING) {
                    to = AlertState.FIRING;
                    reason = "one more satisfying window after PENDING -> FIRING";
                } else {
                    to = AlertState.NORMAL;
                    reason = "condition met but forThreshold not reached ("
                            + state.consecutiveHits + "/" + rule.forThreshold() + ")";
                }
            } else {
                state.consecutiveMisses++;
                state.consecutiveHits = 0;
                if (from == AlertState.NORMAL) {
                    to = AlertState.NORMAL;
                    reason = "condition not met -> remain NORMAL";
                } else if (from == AlertState.RECOVERING) {
                    to = AlertState.NORMAL;
                    reason = "one more missing/failing window after RECOVERING -> NORMAL";
                } else if (state.consecutiveMisses == rule.recoveryThreshold()) {
                    to = AlertState.RECOVERING;
                    reason = "recoveryThreshold reached -> RECOVERING";
                } else {
                    to = from;
                    reason = "not met but recoveryThreshold not reached ("
                            + state.consecutiveMisses + "/" + rule.recoveryThreshold() + ")";
                }
            }
            state.state = to;
            return new AlertEvaluation(rule.version(), rule.metric(), labels, aligned,
                    Double.isNaN(value) ? Double.NaN : value, met, from, to,
                    state.consecutiveHits, state.consecutiveMisses, reason);
        });

        if (evaluation != null) {
            Notification n = notificationService.onEvaluation(evaluation, rule.silenceMillis());
            if (n != null && !n.suppressed()) {
                emitted.add(n);
            }
        }
        return evaluation;
    }

    private double aggregateWindow(AlertRule rule, Labels labels, long windowStart) {
        List<SeriesSnapshot> snapshots =
                store.snapshotMatching(rule.metric(), labels.asMap());
        WindowStats stats = new WindowStats(windowStart, rule.windowMillis());
        long end = windowStart + rule.windowMillis();
        for (SeriesSnapshot snapshot : snapshots) {
            NavigableMap<Long, Double> in = snapshot.points().subMap(windowStart, true, end - 1, true);
            in.forEach((t, v) -> stats.add(v, t));
        }
        return stats.count() == 0 ? Double.NaN : rule.function().apply(stats);
    }

    /** 对单条规则版本，从 fromWindowStart 起逐窗口（升序）求值其全部匹配时间线。 */
    public List<AlertEvaluation> evaluateRule(AlertRule rule, long fromWindowStart) {
        long now = clock.nowMillis();
        long first = floorWindow(fromWindowStart, rule.windowMillis());
        List<AlertEvaluation> evaluations = new ArrayList<>();
        for (SeriesSnapshot snapshot : store.snapshotMatching(rule.metric(), rule.labelSelectors())) {
            for (long start = first; start + rule.windowMillis() <= now; start += rule.windowMillis()) {
                AlertEvaluation e = evaluateWindow(rule, snapshot.labels(), start);
                if (e != null) {
                    evaluations.add(e);
                }
            }
        }
        return evaluations;
    }

    /** 对所有规则的最新版本求值 [fromWindowStart, now) 的窗口。 */
    public List<AlertEvaluation> evaluateAll(long fromWindowStart) {
        List<AlertEvaluation> evaluations = new ArrayList<>();
        for (AlertRule rule : ruleStore.allLatest()) {
            evaluations.addAll(evaluateRule(rule, fromWindowStart));
        }
        return evaluations;
    }

    /** 取出（并清空）本次运行以来实际发出的通知，便于调用方观察。 */
    public List<Notification> drainEmittedNotifications() {
        List<Notification> out = new ArrayList<>();
        Notification n;
        while ((n = emitted.poll()) != null) {
            out.add(n);
        }
        return out;
    }

    private static long floorWindow(long ts, long window) {
        return Math.floorDiv(ts, window) * window;
    }
}
