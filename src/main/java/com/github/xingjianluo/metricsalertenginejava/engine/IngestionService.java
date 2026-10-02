package com.github.xingjianluo.metricsalertenginejava.engine;

import com.github.xingjianluo.metricsalertenginejava.config.EngineProperties;
import com.github.xingjianluo.metricsalertenginejava.model.Labels;
import com.github.xingjianluo.metricsalertenginejava.model.MetricDefinition;
import com.github.xingjianluo.metricsalertenginejava.model.MetricVersion;
import com.github.xingjianluo.metricsalertenginejava.model.RejectReason;
import com.github.xingjianluo.metricsalertenginejava.model.Sample;
import com.github.xingjianluo.metricsalertenginejava.model.WriteResult;
import com.github.xingjianluo.metricsalertenginejava.store.MetricsRegistry;
import com.github.xingjianluo.metricsalertenginejava.store.TimeSeriesStore;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * 写入入口：非法标签 / 时间戳越界 / 指标定义口径不符等在入队前同步拒绝；合法数据进入有界队列，
 * 由单 worker 串行落库（基数、重复检测因此天然无竞争）。
 * <p>容量保护与降级策略：队列满时立即以 {@code QUEUE_FULL} 拒绝新写入（调用方不阻塞），
 * 消费速度慢只会丢“新数据”并留下可观测计数，写入路径绝不被消费速度拖垮。
 * <p>指标定义校验（若指标已登记定义）同样在入队前完成，并把<b>当时取到的定义版本快照</b>
 * 随任务带入队列：一次写入从校验、落库到返回结论始终依据同一个版本，
 * 定义更新与写入并发交错时不会出现“按新版校验、按旧版记账”的口径漂移；
 * 未登记定义的指标完全走宽松路径，行为与接入定义能力前一致。
 */
public class IngestionService implements AutoCloseable {

    private static final Pattern METRIC_NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_.]*");

    private final TimeSeriesStore store;
    private final MetricsRegistry registry;
    private final EngineProperties properties;
    private final EngineClock clock;
    /** 指标定义来源；null 表示不启用定义校验（未登记指标一律宽松，兼容旧行为）。 */
    private final MetricDefinitionService definitionService;
    private final BlockingQueue<Task> queue;

    private volatile boolean started;
    private volatile boolean shutdown;
    private ExecutorService worker;

    private record Task(Sample sample, CompletableFuture<WriteResult> future,
                        MetricVersion definitionVersion) {
    }

    public IngestionService(TimeSeriesStore store,
                            MetricsRegistry registry,
                            EngineProperties properties,
                            EngineClock clock) {
        this(store, registry, properties, clock, null);
    }

    public IngestionService(TimeSeriesStore store,
                            MetricsRegistry registry,
                            EngineProperties properties,
                            EngineClock clock,
                            MetricDefinitionService definitionService) {
        this.store = store;
        this.registry = registry;
        this.properties = properties;
        this.clock = clock;
        this.definitionService = definitionService;
        this.queue = new ArrayBlockingQueue<>(Math.max(1, properties.getIngestQueueCapacity()));
    }

    public void start() {
        if (started) {
            return;
        }
        started = true;
        worker = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "metrics-ingest-worker");
            t.setDaemon(true);
            return t;
        });
        worker.submit(this::runLoop);
    }

    private void runLoop() {
        while (!shutdown || !queue.isEmpty()) {
            try {
                Task task = queue.poll(100, TimeUnit.MILLISECONDS);
                if (task == null) {
                    continue;
                }
                complete(task);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (RuntimeException e) {
                // 单条异常不能杀死 worker。
            }
        }
        queue.forEach(this::completeAsShutdown);
    }

    private void complete(Task task) {
        WriteResult result;
        try {
            WriteResult stored = store.ingest(task.sample());
            result = stored.accepted()
                    ? WriteResult.accepted(task.sample(), task.definitionVersion())
                    : WriteResult.rejected(stored.reason(), stored.detail(), task.sample(),
                            task.definitionVersion());
        } catch (RuntimeException e) {
            result = WriteResult.rejected(RejectReason.SHUTDOWN,
                    "unexpected store failure: " + e.getMessage(), task.sample(),
                    task.definitionVersion());
        }
        if (result.accepted()) {
            registry.recordAccepted();
        } else {
            registry.recordRejected(result.reason());
        }
        task.future().complete(result);
    }

    private void completeAsShutdown(Task task) {
        WriteResult result = WriteResult.rejected(RejectReason.SHUTDOWN,
                "service shut down before ingestion", task.sample(), task.definitionVersion());
        registry.recordRejected(RejectReason.SHUTDOWN);
        task.future().complete(result);
    }

    /** 异步入队，立即返回 Future；非法入参 / 定义口径不符 / 队列满 / 关闭都会得到明确的失败结论。 */
    public CompletableFuture<WriteResult> submit(String metric, Map<String, String> rawLabels,
                                                 double value, long timestampMillis) {
        Labels labels;
        try {
            labels = Labels.of(rawLabels);
        } catch (IllegalArgumentException e) {
            return rejectBeforeQueue(RejectReason.INVALID_LABEL, e.getMessage(),
                    new Sample(metric, Labels.empty(), value, timestampMillis), null);
        }
        return submit(metric, labels, value, timestampMillis);
    }

    public CompletableFuture<WriteResult> submit(String metric, Labels labels, double value,
                                                 long timestampMillis) {
        Sample sample = new Sample(metric, labels, value, timestampMillis);
        if (metric == null || !METRIC_NAME.matcher(metric).matches()) {
            return rejectBeforeQueue(RejectReason.INVALID_METRIC_NAME,
                    "invalid metric name: " + metric, sample, null);
        }
        // 取定义快照：本次写入的口径版本在此固定，后续定义更新不影响本任务。
        MetricDefinition definition = definitionService == null ? null : definitionService.latest(metric);
        MetricVersion definitionVersion = definition == null ? null : definition.version();

        // 定义口径的标签键白名单校验（与 INVALID_LABEL 的“标签格式非法”明确区分）。
        if (definition != null) {
            List<String> badKeys = definition.disallowedKeys(labels);
            if (!badKeys.isEmpty()) {
                return rejectBeforeQueue(RejectReason.DEFINITION_LABEL_KEY_NOT_ALLOWED,
                        "metric " + metric + " " + definitionVersion
                                + " does not allow label keys " + badKeys
                                + "; allowed=" + definition.allowedLabelKeys(),
                        sample, definitionVersion);
            }
            // 定义口径的值域校验（NaN/无穷大也算越界）。
            if (!definition.isValueInRange(value)) {
                return rejectBeforeQueue(RejectReason.DEFINITION_VALUE_OUT_OF_RANGE,
                        "value " + value + " out of declared range " + definition.rangeDescription()
                                + " for metric " + metric + " " + definitionVersion,
                        sample, definitionVersion);
            }
        }

        long now = clock.nowMillis();
        long earliest = now - properties.getRetentionMillis();
        long latest = now + properties.getFutureToleranceMillis();
        if (timestampMillis < earliest || timestampMillis > latest) {
            return rejectBeforeQueue(RejectReason.TIMESTAMP_OUT_OF_RANGE,
                    "ts=" + timestampMillis + " out of [" + earliest + "," + latest + "]", sample,
                    definitionVersion);
        }
        if (shutdown) {
            return rejectBeforeQueue(RejectReason.SHUTDOWN, "ingestion service is shut down", sample,
                    definitionVersion);
        }
        Task task = new Task(sample, new CompletableFuture<>(), definitionVersion);
        if (!queue.offer(task)) {
            // 降级策略：消费过慢时丢弃“新到”数据，立即失败，绝不反压调用方。
            return rejectBeforeQueue(RejectReason.QUEUE_FULL,
                    "ingest queue full (capacity=" + properties.getIngestQueueCapacity()
                            + ", depth=" + queue.size() + ")", sample, definitionVersion);
        }
        return task.future();
    }

    public CompletableFuture<WriteResult> submit(Sample sample) {
        return submit(sample.metric(), sample.labels(), sample.value(), sample.timestampMillis());
    }

    private CompletableFuture<WriteResult> rejectBeforeQueue(RejectReason reason, String detail,
                                                             Sample sample,
                                                             MetricVersion definitionVersion) {
        registry.recordRejected(reason);
        return CompletableFuture.completedFuture(
                WriteResult.rejected(reason, detail, sample, definitionVersion));
    }

    /** 供测试使用：同步等待落库完成。 */
    public WriteResult submitAndWait(String metric, Labels labels, double value, long timestampMillis) {
        return submit(metric, labels, value, timestampMillis).join();
    }

    public int queueDepth() {
        return queue.size();
    }

    @Override
    public void close() {
        shutdown = true;
        if (worker != null) {
            worker.shutdown();
            try {
                worker.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
