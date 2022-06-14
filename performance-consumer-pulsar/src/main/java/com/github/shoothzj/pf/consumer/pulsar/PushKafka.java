package com.github.shoothzj.pf.consumer.pulsar;

import com.github.shoothzj.pf.consumer.action.AbstractAction;
import com.github.shoothzj.pf.consumer.action.module.ActionMsg;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;

public class PushKafka extends AbstractAction {
    private KafkaProducer<String, String> producer =  null;

    private final String topic;

    public PushKafka(String topic) {
        this.topic = topic;
    }

    public static PushKafka getInstance(String topic) {
        return new PushKafka(topic);
    }

    @Override
    public void init() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "");
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
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(this.topic, msg.toString());
        try {this.producer.send(record);} catch (Exception e) {
            e.printStackTrace();
        }
    }
}
