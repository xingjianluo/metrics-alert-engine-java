package com.github.xingjianluo.metricsalertenginejava.web;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

/**
 * 把查询 / 定义管理中的非法入参（不存在的定义版本、版本与指标对不上、
 * 未登记指标未显式给算子等）映射为明确的 400 响应，
 * 绝不静默返回可能误导的结论。
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> badRequest(IllegalArgumentException e) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of(
                "error", e.getMessage(),
                "reason", "INVALID_ARGUMENT"));
    }
}
