package com.github.shoothzj.pf.consumer.kafka;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

/**
 * @author hezhangjian
 */
@Configuration
@Service
public class KafkaConfig {

    @Value("${KAFKA_ADDR:localhost:9092}")
    public String addr;

    @Value("${KAFKA_TOPIC:0}")
    public String topic;

    @Value("${KAFKA_TOPIC_SUFFIX_NUM:0}")
    public int topicSuffixNum;

    @Value("${KAFKA_MAX_POLL_RECORDS:500}")
    public int maxPollRecords;

    @Value("${KAFKA_POLL_MS:500}")
    public int pollMs;

}
