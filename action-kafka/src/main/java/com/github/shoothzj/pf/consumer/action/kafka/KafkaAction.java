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

import com.github.shoothzj.pf.consumer.action.AbstractAction;
import com.github.shoothzj.pf.consumer.action.module.ActionMsg;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Properties;

public class KafkaAction extends AbstractAction {
    private String topic;
    private String kafkaAddress;
    private KafkaProducer<String, String> producer =  null;

    public static KafkaAction getInstance(String kafkaAddress) {
        return new KafkaAction(kafkaAddress);
    }

    public KafkaAction(@NotNull String topic, @NotNull String kafkaAddr) {
        this.topic = topic;
        this.kafkaAddress = kafkaAddr;
    }

    public KafkaAction(@NotNull String KafkaAddress) {
        this.kafkaAddress = KafkaAddress;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(@NotNull String topic) {
        this.topic = topic;
    }

    @Override
    public void init() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaAddress);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producer = new KafkaProducer<String, String>(properties);
    }

    @Override
    public void handleStrBatchMsg(List<ActionMsg<String>> msgList) {

    }

    @Override
    public void handleStrMsg(ActionMsg<String> msg) {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(this.topic, msg.toString());
        try {this.producer.send(record);} catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void handleBytesBatchMsg(List<ActionMsg<byte[]>> msgList) {

    }

    @Override
    public void handleBytesMsg(ActionMsg<byte[]> msg) {

    }

}
