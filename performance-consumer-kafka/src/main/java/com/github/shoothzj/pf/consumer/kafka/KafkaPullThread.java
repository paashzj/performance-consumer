package com.github.shoothzj.pf.consumer.kafka;

import com.github.shoothzj.pf.consumer.common.AbstractPullThread;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/**
 * @author hezhangjian
 */
@Slf4j
public class KafkaPullThread extends AbstractPullThread {

    private final KafkaConfig kafkaConfig;

    private final KafkaConsumer<String, String> consumer;

    public KafkaPullThread(int i, List<String> topics, KafkaConfig kafkaConfig) {
        super(i);
        this.kafkaConfig = kafkaConfig;
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.addr);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, kafkaConfig.maxPollRecords);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(topics);
    }

    @Override
    protected void pull() throws Exception {
        ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(kafkaConfig.pollMs));
        for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
            log.debug("receive a record, offset is [{}]", consumerRecord.offset());
        }
    }

}
