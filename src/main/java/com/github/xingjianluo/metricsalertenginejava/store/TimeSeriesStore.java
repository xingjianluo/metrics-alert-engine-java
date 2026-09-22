package com.github.xingjianluo.metricsalertenginejava.store;

import com.github.xingjianluo.metricsalertenginejava.config.EngineProperties;
import com.github.xingjianluo.metricsalertenginejava.engine.EngineClock;
import com.github.xingjianluo.metricsalertenginejava.model.AggregateFunction;
import com.github.xingjianluo.metricsalertenginejava.model.Labels;
import com.github.xingjianluo.metricsalertenginejava.model.Sample;
import com.github.xingjianluo.metricsalertenginejava.model.SeriesAggregation;
import com.github.xingjianluo.metricsalertenginejava.model.WindowStats;
import com.github.xingjianluo.metricsalertenginejava.model.WriteResult;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * 内存时间线存储：metric + 标签组合 为一条时间线，线内按时间戳去重保存采样。
 * <p>所有写入由单 worker 线程串行进入；查询基于点集合的快照拷贝，读到的是某个时刻自洽的数据。
 * <p>拒绝原因可区分：{@code INVALID_METRIC_NAME}、{@code TIMESTAMP_OUT_OF_RANGE}、
 * {@code CARDINALITY_LIMIT}（单指标 / 全局时间线）、{@code DUPLICATE}。
 */
public class TimeSeriesStore {

    private static final Pattern METRIC_NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_.]*");

    private final EngineProperties properties;
    private final EngineClock clock;

    /** metric -> (seriesKey -> series) */
    private final ConcurrentMap<String, ConcurrentMap<String, Series>> metrics = new ConcurrentHashMap<>();

    public TimeSeriesStore(EngineProperties properties, EngineClock clock) {
        this.properties = properties;
        this.clock = clock;
    }

    /** 一条时间线：不可变标签 + 时间戳有序且去重的点表（写线程单一，查询线程快照读取）。 */
    public static final class Series {
        private final Labels labels;
        private final ConcurrentSkipListMap<Long, Double> points = new ConcurrentSkipListMap<>();

        Series(Labels labels) {
            this.labels = labels;
        }

        public Labels labels() {
            return labels;
        }

        public NavigableMap<Long, Double> pointsSnapshot() {
            return new TreeMap<>(points);
        }
    }

    /** 写入一条采样；基数、时间戳越界、重复在此给出明确拒绝原因。 */
    public WriteResult ingest(Sample sample) {
        String metric = sample.metric();
        if (metric == null || !METRIC_NAME.matcher(metric).matches()) {
            return WriteResult.rejected(com.github.xingjianluo.metricsalertenginejava.model.RejectReason.INVALID_METRIC_NAME,
                    "invalid metric name: " + metric, sample);
        }
        long now = clock.nowMillis();
        long ts = sample.timestampMillis();
        long earliest = now - properties.getRetentionMillis();
        long latest = now + properties.getFutureToleranceMillis();
        if (ts < earliest || ts > latest) {
            return WriteResult.rejected(com.github.xingjianluo.metricsalertenginejava.model.RejectReason.TIMESTAMP_OUT_OF_RANGE,
                    "ts=" + ts + " out of [" + earliest + "," + latest + "]", sample);
        }

        ConcurrentMap<String, Series> seriesByKey =
                metrics.computeIfAbsent(metric, ignored -> new ConcurrentHashMap<>());
        String seriesKey = sample.labels().canonicalString();
        Series series = seriesByKey.get(seriesKey);
        if (series == null) {
            // 基数保护：先统计后插入，避免超限时间线进入存储。
            if (seriesByKey.size() >= properties.getMaxSeriesPerMetric()) {
                return WriteResult.rejected(com.github.xingjianluo.metricsalertenginejava.model.RejectReason.CARDINALITY_LIMIT,
                        "per-metric series limit " + properties.getMaxSeriesPerMetric()
                                + " exceeded for metric " + metric, sample);
            }
            if (totalSeriesCount() >= properties.getMaxTotalSeries()) {
                return WriteResult.rejected(com.github.xingjianluo.metricsalertenginejava.model.RejectReason.CARDINALITY_LIMIT,
                        "global series limit " + properties.getMaxTotalSeries() + " exceeded", sample);
            }
            Series created = new Series(sample.labels());
            Series existing = seriesByKey.putIfAbsent(seriesKey, created);
            series = existing != null ? existing : created;
            if (existing == null && metrics.get(metric) != seriesByKey) {
                // 极端竞争兜底（正常单写线程不会发生）。
                seriesByKey.remove(seriesKey, created);
                return WriteResult.rejected(com.github.xingjianluo.metricsalertenginejava.model.RejectReason.CARDINALITY_LIMIT,
                        "race during series creation", sample);
            }
        }

        Double previous = series.points.putIfAbsent(ts, sample.value());
        if (previous != null) {
            return WriteResult.rejected(com.github.xingjianluo.metricsalertenginejava.model.RejectReason.DUPLICATE,
                    "duplicate timestamp " + ts + " for " + metric + " " + seriesKey, sample);
        }
        // 容量保护：超出单线点数上限时淘汰最旧点（FIFO），不拒绝新写入。
        while (series.points.size() > properties.getMaxPointsPerSeries()) {
            series.points.pollFirstEntry();
        }
        return WriteResult.accepted(sample);
    }

    /** 按 [from,to) 时间范围、滚动窗口聚合；窗口按 epoch 对齐，缺失窗口以 missing 标注。 */
    public List<SeriesAggregation> query(String metric,
                                         Map<String, String> selectors,
                                         long fromMillis,
                                         long toMillis,
                                         long windowMillis,
                                         AggregateFunction function) {
        if (windowMillis <= 0) {
            throw new IllegalArgumentException("windowMillis must be positive");
        }
        if (toMillis <= fromMillis) {
            throw new IllegalArgumentException("toMillis must be greater than fromMillis");
        }
        List<SeriesAggregation> results = new ArrayList<>();
        for (SeriesSnapshot snapshot : snapshotMatching(metric, selectors)) {
            List<SeriesAggregation.WindowValue> windows = aggregate(snapshot.points(),
                    fromMillis, toMillis, windowMillis, function);
            results.add(new SeriesAggregation(metric, snapshot.labels(), windows));
        }
        results.sort(Comparator.comparing(s -> s.labels().canonicalString()));
        return results;
    }

    private List<SeriesAggregation.WindowValue> aggregate(NavigableMap<Long, Double> points,
                                                          long from, long to, long window,
                                                          AggregateFunction function) {
        List<SeriesAggregation.WindowValue> out = new ArrayList<>();
        long firstStart = floorWindow(from, window);
        for (long start = firstStart; start < to; start += window) {
            long end = start + window;
            WindowStats stats = new WindowStats(start, window);
            // [start,end) 且与查询范围 [from,to) 相交的点
            Map<Long, Double> in = points.subMap(Math.max(start, from), true,
                    Math.min(end, to) - 1, true);
            in.forEach((t, v) -> stats.add(v, t));
            if (stats.count() == 0) {
                out.add(new SeriesAggregation.WindowValue(start, end, Double.NaN, true, 0));
            } else {
                out.add(new SeriesAggregation.WindowValue(start, end, function.apply(stats),
                        false, stats.count()));
            }
        }
        return out;
    }

    private static long floorWindow(long ts, long window) {
        return Math.floorDiv(ts, window) * window;
    }

    /** 删除早于 now-retention 的数据并清理空时间线；返回删除的采样数。 */
    public int evict(long nowMillis) {
        int removed = 0;
        long cutoff = nowMillis - properties.getRetentionMillis();
        for (Map.Entry<String, ConcurrentMap<String, Series>> metricEntry : metrics.entrySet()) {
            for (Map.Entry<String, Series> entry : metricEntry.getValue().entrySet()) {
                while (true) {
                    Map.Entry<Long, Double> oldest = entry.getValue().points.firstEntry();
                    if (oldest == null || oldest.getKey() >= cutoff) {
                        break;
                    }
                    if (entry.getValue().points.pollFirstEntry() != null) {
                        removed++;
                    }
                }
            }
            metricEntry.getValue().entrySet().removeIf(e -> e.getValue().points.isEmpty());
        }
        return removed;
    }

    public int totalSeriesCount() {
        int total = 0;
        for (ConcurrentMap<String, Series> map : metrics.values()) {
            total += map.size();
        }
        return total;
    }

    public int seriesCount(String metric) {
        ConcurrentMap<String, Series> map = metrics.get(metric);
        return map == null ? 0 : map.size();
    }

    public int totalSampleCount() {
        int total = 0;
        for (ConcurrentMap<String, Series> map : metrics.values()) {
            for (Series series : map.values()) {
                total += series.points.size();
            }
        }
        return total;
    }

    /** 选择某指标下匹配选择器的时间线快照（标签不可变，点集合为快照拷贝）。 */
    public List<SeriesSnapshot> snapshotMatching(String metric, Map<String, String> selectors) {
        List<SeriesSnapshot> out = new ArrayList<>();
        ConcurrentMap<String, Series> map = metrics.get(metric);
        if (map == null) {
            return out;
        }
        Map<String, String> sel = selectors == null ? Map.of() : selectors;
        for (Series series : map.values()) {
            if (matches(series.labels(), sel)) {
                out.add(new SeriesSnapshot(metric, series.labels(), new TreeMap<>(series.points)));
            }
        }
        return out;
    }

    private static boolean matches(Labels labels, Map<String, String> selectors) {
        for (Map.Entry<String, String> e : selectors.entrySet()) {
            if (!e.getValue().equals(labels.get(e.getKey()))) {
                return false;
            }
        }
        return true;
    }

    /** 一条时间线的不可变点快照。 */
    public record SeriesSnapshot(String metric, Labels labels, NavigableMap<Long, Double> points) {
    }
}
