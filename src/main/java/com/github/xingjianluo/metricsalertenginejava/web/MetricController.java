package com.github.xingjianluo.metricsalertenginejava.web;

import com.github.xingjianluo.metricsalertenginejava.engine.EngineClock;
import com.github.xingjianluo.metricsalertenginejava.engine.IngestionService;
import com.github.xingjianluo.metricsalertenginejava.engine.QueryService;
import com.github.xingjianluo.metricsalertenginejava.model.AggregateFunction;
import com.github.xingjianluo.metricsalertenginejava.model.MetricQueryResult;
import com.github.xingjianluo.metricsalertenginejava.model.MetricVersion;
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

import java.util.LinkedHashMap;
import java.util.Map;

/** 指标写入 / 聚合查询（可按定义版本解读）/ 容量与丢弃原因可观测 HTTP 接口。 */
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

    /**
     * 窗口聚合查询：
     * <ul>
     *   <li>{@code defVersion} 省略：用最新指标定义版本的默认聚合口径（未登记指标必须显式给 function）；</li>
     *   <li>{@code defVersion=n}：用第 n 版历史定义解读同一批数据，结论可按版本复现；</li>
     *   <li>{@code function} 显式给出时覆盖定义默认口径，返回里仍标明解析到的定义版本。</li>
     * </ul>
     */
    @GetMapping("/query")
    public MetricQueryResult query(@RequestParam String metric,
                                   @RequestParam(defaultValue = "0") long from,
                                   @RequestParam(defaultValue = "0") long to,
                                   @RequestParam long windowMillis,
                                   @RequestParam(required = false) AggregateFunction function,
                                   @RequestParam(required = false) Integer defVersion,
                                   @RequestParam(required = false) Map<String, String> labels) {
        long toTs = to == 0 ? clock.nowMillis() : to;
        MetricVersion requested = defVersion == null ? null : new MetricVersion(metric, defVersion);
        return queryService.queryWithDefinition(metric, labels, from, toTs, windowMillis,
                function, requested);
    }

    @GetMapping("/stats")
    public Map<String, Object> stats() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("totalSeries", store.totalSeriesCount());
        out.put("totalSamples", store.totalSampleCount());
        out.put("queueDepth", ingestion.queueDepth());
        out.put("accepted", registry.acceptedCount());
        out.put("rejected", registry.rejectedCounts());
        out.put("definitionsCreated", registry.definitionsCreatedCount());
        out.put("definitionsUpdated", registry.definitionsUpdatedCount());
        out.put("definitionsRejected", registry.definitionsRejectedCount());
        return out;
    }
}
