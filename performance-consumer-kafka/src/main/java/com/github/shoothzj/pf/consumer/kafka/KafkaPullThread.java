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

package com.github.shoothzj.pf.consumer.kafka;

import com.github.shoothzj.pf.consumer.action.module.ActionMsg;
import com.github.shoothzj.pf.consumer.common.AbstractPullThread;
import com.github.shoothzj.pf.consumer.common.module.ExchangeType;
import com.github.shoothzj.pf.consumer.common.service.ActionService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * @author hezhangjian
 */
@Slf4j
public class KafkaPullThread extends AbstractPullThread {

    private final ExchangeType exchangeType;

    private final KafkaConfig kafkaConfig;

    private KafkaConsumer<byte[], byte[]> bytesConsumer;

    private KafkaConsumer<String, String> stringConsumer;

    public KafkaPullThread(int i, ActionService actionService, List<String> topics, ExchangeType exchangeType,
                           KafkaConfig kafkaConfig) {
        super(i, actionService);
        this.exchangeType = exchangeType;
        this.kafkaConfig = kafkaConfig;
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.addr);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaConfig.groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaConfig.autoOffsetResetConfig);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, kafkaConfig.maxPollRecords);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        if (exchangeType.equals(ExchangeType.BYTES)) {
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
            bytesConsumer = new KafkaConsumer<>(props);
            bytesConsumer.subscribe(topics);
        } else if (exchangeType.equals(ExchangeType.STRING)) {
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            stringConsumer = new KafkaConsumer<>(props);
            stringConsumer.subscribe(topics);
        }
    }

    @Override
    protected void pull() throws Exception {
        if (exchangeType.equals(ExchangeType.BYTES)) {
            bytesPull();
        } else if (exchangeType.equals(ExchangeType.STRING)) {
            stringPull();
        }
    }

    protected void bytesPull() throws Exception {
        ConsumerRecords<byte[], byte[]> consumerRecords = bytesConsumer.poll(Duration.ofMillis(kafkaConfig.pollMs));
        for (ConsumerRecord<byte[], byte[]> consumerRecord : consumerRecords) {
            log.debug("receive a record, offset is [{}]", consumerRecord.offset());
            ActionMsg<byte[]> actionMsg = new ActionMsg<>();
            actionMsg.setMessageId(String.valueOf(consumerRecord.offset()));
            actionMsg.setContent(consumerRecord.value());
            actionService.handleBytesMsg(actionMsg);
        }
    }

    protected void stringPull() throws Exception {
        ConsumerRecords<String, String> consumerRecords = stringConsumer.poll(Duration.ofMillis(kafkaConfig.pollMs));
        for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
            log.debug("receive a record, offset is [{}]", consumerRecord.offset());
            ActionMsg<String> actionMsg = new ActionMsg<>();
            actionMsg.setMessageId(String.valueOf(consumerRecord.offset()));
            actionMsg.setContent(consumerRecord.value());
            actionService.handleStrMsg(actionMsg);
        }
    }

}
