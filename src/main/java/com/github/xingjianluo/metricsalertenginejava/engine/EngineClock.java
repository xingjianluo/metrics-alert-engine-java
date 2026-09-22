package com.github.xingjianluo.metricsalertenginejava.engine;

/** 时间源抽象，便于测试窗口边界与静默期。 */
public interface EngineClock {
    long nowMillis();
}
