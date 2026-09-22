package com.github.xingjianluo.metricsalertenginejava.web;

import com.github.xingjianluo.metricsalertenginejava.engine.MetricsAlertEngine;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 演示用 HTTP 接口（写入/查询/规则/状态）。核心能力均在引擎层，控制器仅做薄封装。
 */
@RestController
@RequestMapping("/api")
public class MetricsController {

    private final MetricsAlertEngine engine;

    public MetricsController(MetricsAlertEngine engine) {
        this.engine = engine;
    }

    // TODO 骨架：write / query / rules / state 端点
}
