package com.github.xingjianluo.metricsalertenginejava.tests;

import com.github.xingjianluo.metricsalertenginejava.engine.EngineClock;

import java.util.concurrent.atomic.AtomicLong;

/** 可手动推进的确定性时钟。 */
public class ManualClock implements EngineClock {

    private final AtomicLong now;

    public ManualClock(long startMillis) {
        this.now = new AtomicLong(startMillis);
    }

    @Override
    public long nowMillis() {
        return now.get();
    }

    public void set(long millis) {
        now.set(millis);
    }

    public void advance(long deltaMillis) {
        now.addAndGet(deltaMillis);
    }
}
