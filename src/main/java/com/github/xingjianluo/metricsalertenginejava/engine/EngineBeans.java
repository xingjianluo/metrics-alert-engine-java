package com.github.xingjianluo.metricsalertenginejava.engine;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Spring 装配：默认单例引擎（系统时钟、默认容量配置）。 */
@Configuration
public class EngineBeans {

    @Bean
    public MetricsAlertEngine metricsAlertEngine() {
        return MetricsAlertEngine.create(EngineConfig.defaults());
    }
}
