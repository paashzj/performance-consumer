package com.github.shoothzj.pf.consumer.pulsar;

import com.github.shoothzj.pf.consumer.common.AbstractPullThread;
import com.github.shoothzj.pf.consumer.action.module.ActionMsg;
import com.github.shoothzj.pf.consumer.common.service.ActionService;
import com.google.common.util.concurrent.RateLimiter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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

    public PulsarPullThread(int i, ActionService actionService, List<Semaphore> semaphores,
                            List<Consumer<byte[]>> consumers, PulsarConfig pulsarConfig) {
        super(i, actionService);
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
        if (pulsarConfig.consumeAsync) {
            asyncReceive();
        } else {
            syncReceive();
        }
    }

    private void asyncReceive() throws Exception {
        for (int i = 0; i < consumers.size(); i++) {
            Consumer<byte[]> consumer = consumers.get(i);
            Semaphore semaphore = semaphores.get(i);
            asyncReceiveConsumer(consumer, semaphore);
        }
    }

    private void asyncReceiveConsumer(Consumer<byte[]> consumer, Semaphore semaphore) {
        if (semaphore != null && !semaphore.tryAcquire()) {
            return;
        }
        if (pulsarConfig.consumeBatch) {
            consumer.batchReceiveAsync().thenAcceptAsync(messages -> {
                        handleBatch(messages);
                        consumer.acknowledgeAsync(messages);
                        if (semaphore != null) {
                            semaphore.release();
                        }
                    }
            ).exceptionally(ex -> {
                log.error("batch receive ", ex);
                return null;
            });
        } else {
            consumer.receiveAsync().thenAcceptAsync(message -> {
                handle(message);
                consumer.acknowledgeAsync(message);
                if (semaphore != null) {
                    semaphore.release();
                }
            }).exceptionally(ex -> {
                log.error("receive ex ", ex);
                return null;
            });
        }
    }

    private void syncReceive() throws Exception {
        for (Consumer<byte[]> consumer : consumers) {
            if (pulsarConfig.consumeBatch) {
                final Messages<byte[]> messages = consumer.batchReceive();
                handleBatch(messages);
                consumer.acknowledge(messages);
            } else {
                final Message<byte[]> message = consumer.receive();
                handle(message);
                consumer.acknowledge(message);
            }
        }
    }

    private void handleBatch(Messages<byte[]> messages) {
        final ArrayList<ActionMsg> list = new ArrayList<>();
        for (Message<byte[]> message : messages) {
            list.add(new ActionMsg(new String(message.getValue(), StandardCharsets.UTF_8)));
        }
        this.actionService.handleBatchMsg(list);
    }

    private void handle(Message<byte[]> message) {
        this.actionService.handleMsg(new ActionMsg(new String(message.getValue(), StandardCharsets.UTF_8)));
    }

}
