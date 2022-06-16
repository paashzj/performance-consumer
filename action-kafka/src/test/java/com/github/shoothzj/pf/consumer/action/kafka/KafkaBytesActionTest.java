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

package com.github.shoothzj.pf.consumer.action.kafka;

import com.github.shoothzj.pf.consumer.TestSbConfig;
import com.github.shoothzj.pf.consumer.action.MsgCallback;
import com.github.shoothzj.pf.consumer.action.module.ActionMsg;
import com.github.shoothzj.test.kafka.TestKfkServer;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = TestSbConfig.class)
class KafkaBytesActionTest {

    @Autowired
    private MeterRegistry meterRegistry;

    @Test
    public void testKfkBytesProduce() throws Exception {
        final TestKfkServer testKfkServer = new TestKfkServer();
        testKfkServer.start();
        ActionKafkaConfig kafkaConfig = new ActionKafkaConfig();
        kafkaConfig.addr = String.format("localhost:%d", testKfkServer.getKafkaPort());
        kafkaConfig.topic = UUID.randomUUID().toString();
        KafkaBytesAction bytesAction = new KafkaBytesAction(kafkaConfig, meterRegistry);
        bytesAction.init();
        ActionMsg<byte[]> msg = new ActionMsg<>("msgId", "content".getBytes(StandardCharsets.UTF_8));
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        bytesAction.handleMsg(msg, Optional.of(new MsgCallback() {
            @Override
            public void success(String msgId) {
                future.complete(true);
            }

            @Override
            public void fail(String msgId) {
                future.complete(false);
            }
        }));
        Assertions.assertTrue(future.get(5, TimeUnit.SECONDS));
    }

}
