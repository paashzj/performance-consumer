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

import com.github.shoothzj.pf.consumer.common.AbstractPullThread;
import com.github.shoothzj.pf.consumer.common.service.ActionService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

@Slf4j
public abstract class AbstractKafkaPullThread<T> extends AbstractPullThread {

    private final KafkaConfig kafkaConfig;

    private final KafkaConsumer<T, T> consumer;

    public AbstractKafkaPullThread(int i, ActionService actionService, List<String> topics, KafkaConfig kafkaConfig) {
        super(i, actionService);
        this.kafkaConfig = kafkaConfig;
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.addr);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaConfig.groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaConfig.autoOffsetResetConfig);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, kafkaConfig.maxPollRecords);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, getKeyDeserializerName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, getValueDeserializerName());
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(topics);
    }

    protected abstract String getKeyDeserializerName();

    protected abstract String getValueDeserializerName();

    @Override
    protected void pull() throws Exception {
        ConsumerRecords<T, T> consumerRecords = consumer.poll(Duration.ofMillis(kafkaConfig.pollMs));
        for (ConsumerRecord<T, T> record : consumerRecords) {
            log.debug("receive a record, offset is [{}]", record.offset());
            this.handle(record);
        }
    }

    protected abstract void handle(ConsumerRecord<T, T> record);

}
