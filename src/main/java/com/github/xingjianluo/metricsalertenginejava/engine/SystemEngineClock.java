package com.github.xingjianluo.metricsalertenginejava.engine;

/** 系统墙钟。 */
public class SystemEngineClock implements EngineClock {
    @Override
    public long nowMillis() {
        return System.currentTimeMillis();
    }
}
