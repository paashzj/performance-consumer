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

package com.github.shoothzj.pf.consumer.scene;

import com.github.shoothzj.javatool.util.CommonUtil;
import com.github.shoothzj.pf.consumer.TestSbConfig;
import com.github.shoothzj.pf.consumer.common.service.ActionService;
import com.github.shoothzj.test.kafka.TestKfkServer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Slf4j
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = TestSbConfig.class, properties = {"MIDDLEWARE=KAFKA"})
public class KafkaConsumeEarliestTest {

    final TestKfkServer testKfkServer = new TestKfkServer();

    final String topic = UUID.randomUUID().toString();

    final String group = UUID.randomUUID().toString();

    final String kafkaAddr;

    {
        try {
            testKfkServer.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        kafkaAddr = String.format("localhost:%d", testKfkServer.getKafkaPort());
        System.setProperty("KAFKA_ADDR", kafkaAddr);
        System.setProperty("KAFKA_TOPIC", topic);
        System.setProperty("KAFKA_GROUP_ID", group);
        System.setProperty("KAFKA_AUTO_OFFSET_RESET_CONFIG", "earliest");
    }

    @MockBean
    private ActionService actionService;

    @Test
    public void testKafkaConsume() throws Exception {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaAddr);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        RecordMetadata recordMetadata = kafkaProducer.send(new ProducerRecord<>(topic, "xxxx")).get();
        log.info("record metadata is {}", recordMetadata);
        CommonUtil.sleep(TimeUnit.SECONDS, 10);
        Mockito.verify(actionService, Mockito.times(1)).handleStrMsg(Mockito.any());
    }

    @AfterEach
    public void after() throws Exception {
        testKfkServer.close();
    }

}
