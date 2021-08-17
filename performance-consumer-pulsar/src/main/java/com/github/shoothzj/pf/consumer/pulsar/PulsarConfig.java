package com.github.shoothzj.pf.consumer.pulsar;

import org.apache.pulsar.client.api.SubscriptionType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

/**
 * @author hezhangjian
 */
@Configuration
@Service
public class PulsarConfig {

    @Value("${PULSAR_HOST:localhost}")
    public String host;

    @Value("${PULSAR_PORT:8080}")
    public int port;

    @Value("${PULSAR_IO_THREADS:4}")
    public int ioThreads;

    @Value("${PULSAR_TENANT:public}")
    public String tenant;

    @Value("${PULSAR_NAMESPACE:default}")
    public String namespace;

    @Value("${PULSAR_TOPIC:topic}")
    public String topic;

    @Value("${PULSAR_TOPIC_SUFFIX_NUM:0}")
    public int topicSuffixNum;

    @Value("${PULSAR_AUTO_UPDATE_PARTITION:false}")
    public boolean autoUpdatePartition;

    @Value("${PULSAR_OPERATION_TIMEOUT_SECONDS:15}")
    public int operationTimeoutSeconds;

    @Value("${PULSAR_SUBSCRIPTION_TYPE:Exclusive}")
    public SubscriptionType subscriptionType;

    @Value("${PULSAR_CONSUME_BATCH:false}")
    public boolean consumeBatch;

    @Value("${PULSAR_CONSUME_BATCH_TIMEOUT_MS:50}")
    public int consumeBatchTimeoutMs;

    @Value("${PULSAR_CONSUME_BATCH_MAX_MESSAGES:1}")
    public int consumeBatchMaxMessages;

    @Value("${PULSAR_CONSUME_ASYNC:false}")
    public boolean consumeAsync;

}
