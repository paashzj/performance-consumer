/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.github.shoothzj.pf.consumer.pulsar;

import com.github.shoothzj.pf.consumer.action.module.ActionMsg;
import com.github.shoothzj.pf.consumer.common.module.ExchangeType;
import com.github.shoothzj.pf.consumer.common.service.ActionService;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

public class PulsarPullByteBufferThread  extends AbstractPulsarPullThread<ByteBuffer> {

    public PulsarPullByteBufferThread(int i, ActionService actionService, List<Semaphore> semaphores,
                                 List<Consumer<ByteBuffer>> consumers, ExchangeType exchangeType, PulsarConfig pulsarConfig) {
        super(i, actionService, semaphores, consumers, exchangeType, pulsarConfig);
    }

    protected void handleBatch(Messages<ByteBuffer> messages) {
        final ArrayList<ActionMsg<ByteBuffer>> list = new ArrayList<>();
        for (Message<ByteBuffer> message : messages) {
            list.add(new ActionMsg<>(message.getMessageId().toString(), message.getValue()));
        }
        this.actionService.handleByteBufferBatchMsg(list);
    }

    protected void handle(@NotNull Message<ByteBuffer> message) {
        this.actionService.handleByteBufferMsg(new ActionMsg<>(message.getMessageId().toString(), message.getValue()));
    }

}