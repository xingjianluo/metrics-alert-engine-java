package com.github.xingjianluo.metricsalertenginejava.store;

import com.github.xingjianluo.metricsalertenginejava.model.AlertEvaluation;
import com.github.xingjianluo.metricsalertenginejava.model.AlertState;
import com.github.xingjianluo.metricsalertenginejava.model.Labels;
import com.github.xingjianluo.metricsalertenginejava.model.RuleVersion;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 告警状态库：状态按 (规则版本, 标签组合) 隔离；求值结论按版本追加保存，永不改写，
 * 因而规则更新后历史结论可按版本逐条复现。
 * <p>每个组合有独立监视器（per-key lock）：状态读取—迁移—记录在锁内一次完成，
 * 保证并发求值下计数守恒、状态迁移确定。
 */
public class AlertStateStore {

    /** 单条状态机的可变内部状态。 */
    public static final class MutableState {
        public AlertState state = AlertState.NORMAL;
        public int consecutiveHits;
        public int consecutiveMisses;
        public long lastEvaluatedWindowStart = Long.MIN_VALUE;

        public StateRecord snapshot() {
            return new StateRecord(state, consecutiveHits, consecutiveMisses, lastEvaluatedWindowStart);
        }
    }

    /** 单条状态机记录的只读视图。 */
    public record StateRecord(AlertState state, int consecutiveHits, int consecutiveMisses,
                              long lastEvaluatedWindowStart) {
    }

    /** (规则版本, 标签规范串) -> 状态 */
    private final ConcurrentHashMap<StateKey, MutableState> states = new ConcurrentHashMap<>();
    /** 状态锁按 key 提供；ConcurrentHashMap.computeIfAbsent 保证唯一。 */
    private final ConcurrentHashMap<StateKey, Object> locks = new ConcurrentHashMap<>();
    /** (规则版本, 标签规范串) -> 追加式、只增不改的求值结论 */
    private final ConcurrentHashMap<StateKey, List<AlertEvaluation>> history = new ConcurrentHashMap<>();

    private record StateKey(String ruleId, int version, String labelsKey) {
        static StateKey of(RuleVersion version, Labels labels) {
            return new StateKey(version.ruleId(), version.version(), labels.canonicalString());
        }
    }

    public MutableState getOrCreate(RuleVersion version, Labels labels) {
        return states.computeIfAbsent(StateKey.of(version, labels), ignored -> new MutableState());
    }

    /** 在该组合的独占锁内执行一次状态迁移，迁移函数返回的结论同时追加进历史。 */
    public AlertEvaluation transition(RuleVersion version, Labels labels, TransitionFn fn) {
        StateKey key = StateKey.of(version, labels);
        Object lock = locks.computeIfAbsent(key, ignored -> new Object());
        synchronized (lock) {
            MutableState state = getOrCreate(version, labels);
            AlertEvaluation evaluation = fn.apply(state);
            if (evaluation != null) {
                state.lastEvaluatedWindowStart = evaluation.windowStart();
                history.computeIfAbsent(key, ignored -> new ArrayList<>()).add(evaluation);
            }
            return evaluation;
        }
    }

    /** 状态迁移函数：读取当前可变状态，返回本次求值结论（可为 null 表示窗口重复、跳过）。 */
    @FunctionalInterface
    public interface TransitionFn {
        AlertEvaluation apply(MutableState state);
    }

    public StateRecord get(RuleVersion version, Labels labels) {
        MutableState state = states.get(StateKey.of(version, labels));
        return state == null ? new StateRecord(AlertState.NORMAL, 0, 0, Long.MIN_VALUE) : state.snapshot();
    }

    public List<StateRecord> list(RuleVersion version) {
        List<StateRecord> out = new ArrayList<>();
        for (Map.Entry<StateKey, MutableState> entry : states.entrySet()) {
            StateKey key = entry.getKey();
            if (key.ruleId().equals(version.ruleId()) && key.version() == version.version()) {
                synchronized (locks.getOrDefault(key, this)) {
                    out.add(entry.getValue().snapshot());
                }
            }
        }
        return out;
    }

    public List<AlertEvaluation> history(RuleVersion version) {
        List<AlertEvaluation> out = new ArrayList<>();
        for (Map.Entry<StateKey, List<AlertEvaluation>> entry : history.entrySet()) {
            StateKey key = entry.getKey();
            if (key.ruleId().equals(version.ruleId()) && key.version() == version.version()) {
                synchronized (locks.getOrDefault(key, this)) {
                    out.addAll(entry.getValue());
                }
            }
        }
        return out;
    }

    public List<AlertEvaluation> history(RuleVersion version, Labels labels) {
        StateKey key = StateKey.of(version, labels);
        List<AlertEvaluation> list = history.get(key);
        if (list == null) {
            return List.of();
        }
        synchronized (locks.getOrDefault(key, this)) {
            return new ArrayList<>(list);
        }
    }
}
