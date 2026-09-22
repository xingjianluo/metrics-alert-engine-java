package com.github.xingjianluo.metricsalertenginejava.engine;

import com.github.xingjianluo.metricsalertenginejava.config.EngineProperties;
import com.github.xingjianluo.metricsalertenginejava.model.Labels;
import com.github.xingjianluo.metricsalertenginejava.model.RejectReason;
import com.github.xingjianluo.metricsalertenginejava.model.Sample;
import com.github.xingjianluo.metricsalertenginejava.model.WriteResult;
import com.github.xingjianluo.metricsalertenginejava.store.MetricsRegistry;
import com.github.xingjianluo.metricsalertenginejava.store.TimeSeriesStore;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * 写入入口：非法标签 / 时间戳越界等在入队前同步拒绝；合法数据进入有界队列，
 * 由单 worker 串行落库（基数、重复检测因此天然无竞争）。
 * <p>容量保护与降级策略：队列满时立即以 {@code QUEUE_FULL} 拒绝新写入（调用方不阻塞），
 * 消费速度慢只会丢“新数据”并留下可观测计数，写入路径绝不被拖垮。
 */
public class IngestionService implements AutoCloseable {

    private static final Pattern METRIC_NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_.]*");

    private final TimeSeriesStore store;
    private final MetricsRegistry registry;
    private final EngineProperties properties;
    private final EngineClock clock;
    private final BlockingQueue<Task> queue;

    private volatile boolean started;
    private volatile boolean shutdown;
    private ExecutorService worker;

    private record Task(Sample sample, CompletableFuture<WriteResult> future) {
    }

    public IngestionService(TimeSeriesStore store,
                            MetricsRegistry registry,
                            EngineProperties properties,
                            EngineClock clock) {
        this.store = store;
        this.registry = registry;
        this.properties = properties;
        this.clock = clock;
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
            result = store.ingest(task.sample());
        } catch (RuntimeException e) {
            result = WriteResult.rejected(RejectReason.SHUTDOWN,
                    "unexpected store failure: " + e.getMessage(), task.sample());
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
                "service shut down before ingestion", task.sample());
        registry.recordRejected(RejectReason.SHUTDOWN);
        task.future().complete(result);
    }

    /** 异步入队，立即返回 Future；非法入参 / 队列满 / 关闭都会得到明确的失败结论。 */
    public CompletableFuture<WriteResult> submit(String metric, Map<String, String> rawLabels,
                                                 double value, long timestampMillis) {
        Labels labels;
        try {
            labels = Labels.of(rawLabels);
        } catch (IllegalArgumentException e) {
            return rejectBeforeQueue(RejectReason.INVALID_LABEL, e.getMessage(),
                    new Sample(metric, Labels.empty(), value, timestampMillis));
        }
        return submit(metric, labels, value, timestampMillis);
    }

    public CompletableFuture<WriteResult> submit(String metric, Labels labels, double value,
                                                 long timestampMillis) {
        Sample sample = new Sample(metric, labels, value, timestampMillis);
        if (metric == null || !METRIC_NAME.matcher(metric).matches()) {
            return rejectBeforeQueue(RejectReason.INVALID_METRIC_NAME,
                    "invalid metric name: " + metric, sample);
        }
        long now = clock.nowMillis();
        long earliest = now - properties.getRetentionMillis();
        long latest = now + properties.getFutureToleranceMillis();
        if (timestampMillis < earliest || timestampMillis > latest) {
            return rejectBeforeQueue(RejectReason.TIMESTAMP_OUT_OF_RANGE,
                    "ts=" + timestampMillis + " out of [" + earliest + "," + latest + "]", sample);
        }
        if (shutdown) {
            return rejectBeforeQueue(RejectReason.SHUTDOWN, "ingestion service is shut down", sample);
        }
        Task task = new Task(sample, new CompletableFuture<>());
        if (!queue.offer(task)) {
            // 降级策略：消费过慢时丢弃“新到”数据，立即失败，绝不反压调用方。
            return rejectBeforeQueue(RejectReason.QUEUE_FULL,
                    "ingest queue full (capacity=" + properties.getIngestQueueCapacity()
                            + ", depth=" + queue.size() + ")", sample);
        }
        return task.future();
    }

    public CompletableFuture<WriteResult> submit(Sample sample) {
        return submit(sample.metric(), sample.labels(), sample.value(), sample.timestampMillis());
    }

    private CompletableFuture<WriteResult> rejectBeforeQueue(RejectReason reason, String detail,
                                                             Sample sample) {
        registry.recordRejected(reason);
        return CompletableFuture.completedFuture(WriteResult.rejected(reason, detail, sample));
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
