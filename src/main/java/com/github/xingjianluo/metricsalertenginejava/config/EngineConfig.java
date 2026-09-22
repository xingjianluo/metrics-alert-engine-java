package com.github.xingjianluo.metricsalertenginejava.config;

import com.github.xingjianluo.metricsalertenginejava.engine.AlertEvaluator;
import com.github.xingjianluo.metricsalertenginejava.engine.EngineClock;
import com.github.xingjianluo.metricsalertenginejava.engine.IngestionService;
import com.github.xingjianluo.metricsalertenginejava.engine.NotificationService;
import com.github.xingjianluo.metricsalertenginejava.engine.QueryService;
import com.github.xingjianluo.metricsalertenginejava.engine.RetentionService;
import com.github.xingjianluo.metricsalertenginejava.engine.SystemEngineClock;
import com.github.xingjianluo.metricsalertenginejava.store.AlertStateStore;
import com.github.xingjianluo.metricsalertenginejava.store.MetricsRegistry;
import com.github.xingjianluo.metricsalertenginejava.store.NotificationLog;
import com.github.xingjianluo.metricsalertenginejava.store.RuleStore;
import com.github.xingjianluo.metricsalertenginejava.store.TimeSeriesStore;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** 引擎全部组件的 Spring 装配。 */
@Configuration
@EnableConfigurationProperties(EngineProperties.class)
public class EngineConfig {

    @Bean
    public EngineClock engineClock() {
        return new SystemEngineClock();
    }

    @Bean
    public MetricsRegistry metricsRegistry() {
        return new MetricsRegistry();
    }

    @Bean
    public TimeSeriesStore timeSeriesStore(EngineProperties properties, EngineClock clock) {
        return new TimeSeriesStore(properties, clock);
    }

    @Bean
    public RuleStore ruleStore() {
        return new RuleStore();
    }

    @Bean
    public AlertStateStore alertStateStore() {
        return new AlertStateStore();
    }

    @Bean
    public NotificationLog notificationLog() {
        return new NotificationLog();
    }

    @Bean
    public NotificationService notificationService(NotificationLog log, EngineClock clock) {
        return new NotificationService(log, clock);
    }

    @Bean
    public AlertEvaluator alertEvaluator(TimeSeriesStore store, RuleStore ruleStore,
                                         AlertStateStore stateStore,
                                         NotificationService notificationService,
                                         NotificationLog notificationLog, EngineClock clock) {
        return new AlertEvaluator(store, ruleStore, stateStore, notificationService,
                notificationLog, clock);
    }

    @Bean
    public IngestionService ingestionService(TimeSeriesStore store, MetricsRegistry registry,
                                             EngineProperties properties, EngineClock clock) {
        IngestionService service = new IngestionService(store, registry, properties, clock);
        service.start();
        return service;
    }

    @Bean
    public QueryService queryService(TimeSeriesStore store) {
        return new QueryService(store);
    }

    @Bean
    public RetentionService retentionService(TimeSeriesStore store, EngineClock clock) {
        return new RetentionService(store, clock);
    }
}
