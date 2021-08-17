package com.github.shoothzj.pf.consumer.pulsar;

import com.github.shoothzj.pf.consumer.common.CommonConfig;
import com.github.shoothzj.pf.consumer.common.ConsumeMode;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @author hezhangjian
 */
@Slf4j
@Service
public class PulsarBootService {

    private PulsarClient pulsarClient;

    private final PulsarConfig pulsarConfig;

    private final CommonConfig commonConfig;

    public PulsarBootService(@Autowired PulsarConfig pulsarConfig, @Autowired CommonConfig commonConfig) {
        this.pulsarConfig = pulsarConfig;
        this.commonConfig = commonConfig;
    }

    public void boot() throws Exception {
        try {
            pulsarClient = PulsarClient.builder()
                    .operationTimeout(pulsarConfig.operationTimeoutSeconds, TimeUnit.SECONDS)
                    .ioThreads(pulsarConfig.ioThreads)
                    .serviceUrl(String.format("http://%s:%s", pulsarConfig.host, pulsarConfig.port))
                    .build();
        } catch (Exception e) {
            log.error("create pulsar client exception ", e);
            throw new IllegalArgumentException("build pulsar client exception, exit");
        }
        // now we have pulsar client, we start pulsar consumer
        List<String> topics = new ArrayList<>();
        if (pulsarConfig.topicSuffixNum == 0) {
            topics.add(topicFn(pulsarConfig.tenant, pulsarConfig.namespace, pulsarConfig.topic));
        } else {
            for (int i = 0; i < pulsarConfig.topicSuffixNum; i++) {
                topics.add(topicFn(pulsarConfig.tenant, pulsarConfig.namespace, pulsarConfig.topic + i));
            }
        }
        createConsumers(topics);
    }

    public void createConsumers(List<String> topics) throws PulsarClientException {
        if (commonConfig.consumeMode.equals(ConsumeMode.LISTEN)) {
            for (String topic : topics) {
                createConsumerBuilder(topic).messageListener((MessageListener<byte[]>) (consumer, msg) -> log.debug("do nothing {}", msg.getMessageId())).subscriptionName(UUID.randomUUID().toString()).subscribe();
            }
            return;
        }
        List<List<Consumer<byte[]>>> consumerListList = new ArrayList<>();
        for (int i = 0; i < commonConfig.pullThreads; i++) {
            consumerListList.add(new ArrayList<>());
        }
        int aux = 0;
        for (String topic : topics) {
            final Consumer<byte[]> consumer = createConsumerBuilder(topic).subscriptionName(UUID.randomUUID().toString()).subscribe();
            int index = aux % commonConfig.pullThreads;
            consumerListList.get(index).add(consumer);
            aux++;
        }
        for (int i = 0; i < commonConfig.pullThreads; i++) {
            new PulsarPullThread(i, consumerListList.get(i), pulsarConfig).start();
        }
    }

    public ConsumerBuilder<byte[]> createConsumerBuilder(String topic) {
        ConsumerBuilder<byte[]> builder = pulsarClient.newConsumer().topic(topic)
                .subscriptionName(UUID.randomUUID().toString());
        builder = builder.subscriptionType(pulsarConfig.subscriptionType);
        if (!pulsarConfig.consumeBatch) {
            return builder;
        }
        final BatchReceivePolicy batchReceivePolicy = BatchReceivePolicy.builder()
                .timeout(pulsarConfig.consumeBatchTimeoutMs, TimeUnit.SECONDS)
                .maxNumMessages(pulsarConfig.consumeBatchMaxMessages).build();
        return builder.batchReceivePolicy(batchReceivePolicy);
    }

    private String topicFn(String tenant, String namespace, String topic) {
        return String.format("persistent://%s/%s/%s", tenant, namespace, topic);
    }

}
