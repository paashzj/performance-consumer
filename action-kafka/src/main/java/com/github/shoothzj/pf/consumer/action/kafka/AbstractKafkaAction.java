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

import com.github.shoothzj.pf.consumer.action.ActionMetricsBean;
import com.github.shoothzj.pf.consumer.action.IAction;
import com.github.shoothzj.pf.consumer.action.module.ActionMsg;
import com.github.shoothzj.pf.consumer.action.module.ActionType;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;

@Slf4j
public abstract class AbstractKafkaAction<T> implements IAction<T> {

    private final String kafkaAddr;

    private final String topic;

    private ActionMetricsBean metricsBean;

    private KafkaProducer<String, T> producer;

    public AbstractKafkaAction(ActionKafkaConfig kafkaConfig, MeterRegistry meterRegistry) {
        this.kafkaAddr = kafkaConfig.addr;
        this.topic = kafkaConfig.topic;
        this.metricsBean = new ActionMetricsBean(meterRegistry, ActionType.KAFKA);
    }

    @Override
    public void init() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaAddr);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, getValueSerializerName());
        this.producer = new KafkaProducer<>(properties);
    }

    protected abstract String getValueSerializerName();

    @Override
    public void handleBatchMsg(List<ActionMsg<T>> actionMsgs) {
        for (ActionMsg<T> actionMsg : actionMsgs) {
            this.handleMsg(actionMsg);
        }
    }

    @Override
    public void handleMsg(ActionMsg<T> msg) {
        long startTime = System.currentTimeMillis();
        producer.send(new ProducerRecord<>(topic, msg.getContent()), new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    metricsBean.success(System.currentTimeMillis() - startTime);
                    log.info("send kafka message id {} partition {} success {}",
                            msg.getMessageId(), metadata.partition(), metadata.offset());
                } else {
                    metricsBean.fail(System.currentTimeMillis() - startTime);
                    log.error("send kafka fail, message id {}", msg.getMessageId(), exception);
                }
            }
        });
    }

}
