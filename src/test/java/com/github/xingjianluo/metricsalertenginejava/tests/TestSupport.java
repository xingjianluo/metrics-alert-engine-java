package com.github.xingjianluo.metricsalertenginejava.tests;

import com.github.xingjianluo.metricsalertenginejava.config.EngineProperties;

/** 测试用配置工厂。 */
final class TestSupport {

    private TestSupport() {
    }

    static EngineProperties properties() {
        EngineProperties p = new EngineProperties();
        return p;
    }
}
