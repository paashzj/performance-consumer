/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.github.shoothzj.pf.consumer.pulsar;

import com.github.shoothzj.pf.consumer.common.AbstractPullThread;
import com.github.shoothzj.pf.consumer.action.module.ActionMsg;
import com.github.shoothzj.pf.consumer.common.service.ActionService;
import com.google.common.util.concurrent.RateLimiter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;
import org.jetbrains.annotations.NotNull;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

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
        if (rateLimiter != null && !rateLimiter.tryAcquire(5, TimeUnit.MILLISECONDS)) {
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
                if (semaphore != null) {
                    semaphore.release();
                }
                log.error("batch receive error ", ex);
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
                if (semaphore != null) {
                    semaphore.release();
                }
                log.error("receive error ", ex);
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
                final Message<byte[]> message =
                        consumer.receive(pulsarConfig.syncReceiveTimeoutMs, TimeUnit.MILLISECONDS);
                if (message == null) {
                    continue;
                }
                handle(message);
                consumer.acknowledge(message);
            }
        }
    }

    private void handleBatch(Messages<byte[]> messages) {
        final ArrayList<ActionMsg<String>> list = new ArrayList<>();
        for (Message<byte[]> message : messages) {
            list.add(new ActionMsg<>(message.getMessageId().toString(),
                    new String(message.getValue(), StandardCharsets.UTF_8)));
        }
        this.actionService.handleStrBatchMsg(list);
    }

    private void handle(@NotNull Message<byte[]> message) {
        this.actionService.handleStrMsg(new ActionMsg<>(message.getMessageId().toString(),
                new String(message.getValue(), StandardCharsets.UTF_8)));
        PushKafka.getInstance("test").handleStrMsg(new ActionMsg<>(new String(message.getValue(), StandardCharsets.UTF_8)));
    }
}
