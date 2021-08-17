package com.github.shoothzj.pf.consumer.pulsar;

import com.github.shoothzj.pf.consumer.common.AbstractPullThread;
import com.google.common.util.concurrent.RateLimiter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;

import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * @author hezhangjian
 */
@Slf4j
public class PulsarPullThread extends AbstractPullThread {

    private final List<Consumer<byte[]>> consumers;

    private final PulsarConfig pulsarConfig;

    private final RateLimiter rateLimiter;

    private final List<Semaphore> semaphores;

    public PulsarPullThread(int i, List<Semaphore> semaphores, List<Consumer<byte[]>> consumers, PulsarConfig pulsarConfig) {
        super(i);
        this.semaphores = semaphores;
        this.consumers = consumers;
        this.pulsarConfig = pulsarConfig;
        this.rateLimiter = pulsarConfig.rateLimiter == -1 ? null : RateLimiter.create(pulsarConfig.rateLimiter);
    }

    @Override
    protected void pull() throws Exception {
        if (pulsarConfig.rateLimiter != -1 && !rateLimiter.tryAcquire(5, TimeUnit.MILLISECONDS)) {
            return;
        }
        for (int i = 0; i < consumers.size(); i++) {
            Consumer<byte[]> consumer = consumers.get(i);
            Semaphore semaphore = semaphores.get(i);
            if (pulsarConfig.consumeBatch) {
                if (pulsarConfig.consumeAsync) {
                    if (semaphore.tryAcquire()  || pulsarConfig.receiveLimiter == -1) {
                        consumer.batchReceiveAsync().thenAccept(messages -> {
                                    consumer.acknowledgeAsync(messages);
                                    semaphore.release();
                                }
                        );
                    }
                } else {
                    final Messages<byte[]> messages = consumer.batchReceive();
                    consumer.acknowledge(messages);
                }
            } else {
                final Message<byte[]> message = consumer.receive();
                consumer.acknowledge(message);
            }
        }
    }

}
