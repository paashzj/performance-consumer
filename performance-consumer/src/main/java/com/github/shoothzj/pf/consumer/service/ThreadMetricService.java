package com.github.shoothzj.pf.consumer.service;

import com.github.shoothzj.pf.consumer.module.ThreadMetricsAux;
import com.google.common.util.concurrent.AtomicDouble;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.HashMap;

/**
 * @author hezhangjian
 */
@Slf4j
@Service
public class ThreadMetricService {

    @Autowired
    private MeterRegistry meterRegistry;

    private final ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();

    private final HashMap<Long, ThreadMetricsAux> map = new HashMap<>();

    private final HashMap<Meter.Id, AtomicDouble> dynamicGauges = new HashMap<>();

    /**
     * one minutes
     */
    @Scheduled(cron = "0 * * * * ?")
    public void schedule() {
        final long[] allThreadIds = threadBean.getAllThreadIds();
        for (long threadId : allThreadIds) {
            final ThreadInfo threadInfo = threadBean.getThreadInfo(threadId);
            if (threadInfo == null) {
                continue;
            }
            final long threadNanoTime = getThreadCPUTime(threadId);
            if (threadNanoTime == 0) {
                // abnormal, clean map
                map.remove(threadId);
            }
            // check if map has data
            final long nanoTime = System.nanoTime();
            ThreadMetricsAux oldMetrics = map.get(threadId);
            if (oldMetrics != null) {
                double percent = (double) (threadNanoTime - oldMetrics.getUsedNanoTime()) / (double) (nanoTime - oldMetrics.getLastNanoTime());
                handleDynamicGauge("jvm.threads.cpu", "threadName", threadInfo.getThreadName(), percent);
            }
            map.put(threadId, new ThreadMetricsAux(threadNanoTime, nanoTime));
        }
    }

    private void handleDynamicGauge(String meterName, String labelKey, String labelValue, double snapshot) {
        Meter.Id id = new Meter.Id(meterName, Tags.of(labelKey, labelValue), null, null, Meter.Type.GAUGE);

        dynamicGauges.compute(id, (key, current) -> {
            if (current == null) {
                AtomicDouble initialValue = new AtomicDouble(snapshot);
                meterRegistry.gauge(key.getName(), key.getTags(), initialValue);
                return initialValue;
            } else {
                current.set(snapshot);
                return current;
            }
        });
    }

    long getThreadCPUTime(long threadId) {
        long time = threadBean.getThreadCpuTime(threadId);
        /* thread of the specified ID is not alive or does not exist */
        return time == -1 ? 0 : time;
    }

}
