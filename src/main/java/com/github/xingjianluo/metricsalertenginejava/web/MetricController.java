package com.github.xingjianluo.metricsalertenginejava.web;

import com.github.xingjianluo.metricsalertenginejava.engine.EngineClock;
import com.github.xingjianluo.metricsalertenginejava.engine.IngestionService;
import com.github.xingjianluo.metricsalertenginejava.engine.QueryService;
import com.github.xingjianluo.metricsalertenginejava.model.AggregateFunction;
import com.github.xingjianluo.metricsalertenginejava.model.MetricQueryResult;
import com.github.xingjianluo.metricsalertenginejava.model.RejectReason;
import com.github.xingjianluo.metricsalertenginejava.model.WriteResult;
import com.github.xingjianluo.metricsalertenginejava.store.MetricsRegistry;
import com.github.xingjianluo.metricsalertenginejava.store.TimeSeriesStore;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/** 指标写入 / 聚合查询 / 容量与丢弃原因可观测 HTTP 接口。 */
@RestController
@RequestMapping("/api/metrics")
public class MetricController {

    /** 写入请求；timestamp 省略时取服务端当前时间。 */
    public record WriteRequest(String metric, Map<String, String> labels, Double value, Long timestamp) {
    }

    private final IngestionService ingestion;
    private final QueryService queryService;
    private final TimeSeriesStore store;
    private final MetricsRegistry registry;
    private final EngineClock clock;

    public MetricController(IngestionService ingestion, QueryService queryService,
                            TimeSeriesStore store, MetricsRegistry registry, EngineClock clock) {
        this.ingestion = ingestion;
        this.queryService = queryService;
        this.store = store;
        this.registry = registry;
        this.clock = clock;
    }

    @PostMapping("/write")
    public ResponseEntity<WriteResult> write(@RequestBody WriteRequest request) {
        if (request.metric() == null || request.value() == null) {
            return ResponseEntity.badRequest().body(WriteResult.rejected(RejectReason.INVALID_METRIC_NAME,
                    "metric and value are required", null));
        }
        long ts = request.timestamp() == null ? clock.nowMillis() : request.timestamp();
        WriteResult result = ingestion.submit(request.metric(), request.labels(),
                request.value(), ts).join();
        return ResponseEntity.status(result.accepted() ? 202 : 422).body(result);
    }

    @GetMapping("/query")
    public MetricQueryResult query(@RequestParam String metric,
                                   @RequestParam(defaultValue = "0") long from,
                                   @RequestParam(defaultValue = "0") long to,
                                   @RequestParam long windowMillis,
                                   @RequestParam(required = false) AggregateFunction function,
                                   @RequestParam(required = false) Integer definitionVersion,
                                   @RequestParam(required = false) Map<String, String> labels) {
        long toTs = to == 0 ? clock.nowMillis() : to;
        return queryService.queryWithDefinition(metric, labels, from, toTs, windowMillis,
                function, definitionVersion);
    }

    @GetMapping("/stats")
    public Map<String, Object> stats() {
        return Map.of(
                "totalSeries", store.totalSeriesCount(),
                "totalSamples", store.totalSampleCount(),
                "queueDepth", ingestion.queueDepth(),
                "accepted", registry.acceptedCount(),
                "rejected", registry.rejectedCounts(),
                "definitionLimitRejected", registry.definitionLimitRejectedCount());
    }
}
