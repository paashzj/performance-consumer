package com.github.shoothzj.pf.consumer.module;

import lombok.Data;

/**
 * @author hezhangjian
 */
@Data
public class ThreadMetricsAux {

    private long usedNanoTime;

    private long lastNanoTime;

    public ThreadMetricsAux() {
    }

    public ThreadMetricsAux(long usedNanoTime, long lastNanoTime) {
        this.usedNanoTime = usedNanoTime;
        this.lastNanoTime = lastNanoTime;
    }

}
