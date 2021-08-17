package com.github.shoothzj.pf.consumer.pulsar;

import com.github.shoothzj.pf.consumer.common.AbstractPullThread;
import com.google.common.util.concurrent.RateLimiter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author hezhangjian
 */
@Slf4j
public class PulsarPullThread extends AbstractPullThread {

    private final List<Consumer<byte[]>> consumers;

    private final PulsarConfig pulsarConfig;

    private final RateLimiter rateLimiter;

    public PulsarPullThread(int i, List<Consumer<byte[]>> consumers, PulsarConfig pulsarConfig) {
        super(i);
        this.consumers = consumers;
        this.pulsarConfig = pulsarConfig;
        this.rateLimiter = pulsarConfig.rateLimiter == -1 ? null : RateLimiter.create(pulsarConfig.rateLimiter);
    }

    @Override
    protected void pull() throws Exception {
        if (pulsarConfig.rateLimiter != -1 && !rateLimiter.tryAcquire(5, TimeUnit.MILLISECONDS)) {
            return;
        }
        for (Consumer<byte[]> consumer : consumers) {
            if (pulsarConfig.consumeBatch) {
                if (pulsarConfig.consumeAsync) {
                    consumer.batchReceiveAsync().thenAccept(consumer::acknowledgeAsync);
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
