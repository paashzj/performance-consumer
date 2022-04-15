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

package com.github.shoothzj.pf.consumer.pulsar;

import com.github.shoothzj.pf.consumer.common.service.ActionService;
import com.github.shoothzj.pf.consumer.common.config.CommonConfig;
import com.github.shoothzj.pf.consumer.common.module.ConsumeMode;
import com.github.shoothzj.pf.consumer.common.util.NameUtil;
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
import java.util.concurrent.Semaphore;
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

    private final ActionService actionService;

    public PulsarBootService(@Autowired PulsarConfig pulsarConfig, @Autowired CommonConfig commonConfig,
                             @Autowired ActionService actionService) {
        this.pulsarConfig = pulsarConfig;
        this.commonConfig = commonConfig;
        this.actionService = actionService;
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
        log.info("tenant prefix name [{}].", pulsarConfig.tenantPrefix);
        if (!pulsarConfig.tenantPrefix.isBlank()) {
            if (pulsarConfig.namespacePrefix.isBlank()) {
                log.info("namespace prefix name is blank.");
                return;
            }
            for (int i = 0; i <= pulsarConfig.tenantSuffixNum; i++) {
                String tenantName;
                if (pulsarConfig.tenantSuffixNum == 0) {
                    tenantName = pulsarConfig.tenantPrefix;
                } else {
                    tenantName = NameUtil.name(pulsarConfig.tenantPrefix, i, pulsarConfig.tenantSuffixNumOfDigits);
                }
                for (int j = 0; j <= pulsarConfig.namespaceSuffixNum; j++) {
                    String namespaceName;
                    if (pulsarConfig.namespaceSuffixNum == 0) {
                        namespaceName = pulsarConfig.namespacePrefix;
                    } else {
                        namespaceName = NameUtil.name(pulsarConfig.namespacePrefix,
                                j, pulsarConfig.namespaceSuffixNumOfDigits);
                    }
                    createTopic(topics, tenantName, namespaceName);
                }
            }
        } else {
            if (pulsarConfig.topicSuffixNum == 0) {
                topics.add(topicFn(pulsarConfig.tenant, pulsarConfig.namespace, pulsarConfig.topic));
            } else {
                for (int i = 0; i < pulsarConfig.topicSuffixNum; i++) {
                    topics.add(topicFn(pulsarConfig.tenant, pulsarConfig.namespace, pulsarConfig.topic + i));
                }
            }
        }
        createConsumers(topics);
    }

    public void createConsumers(List<String> topics) throws PulsarClientException {
        String subscriptionName = UUID.randomUUID().toString();
        if (commonConfig.consumeMode.equals(ConsumeMode.LISTEN)) {
            for (String topic : topics) {
                createConsumerBuilder(topic)
                        .messageListener((MessageListener<byte[]>) (consumer, msg)
                                -> log.debug("do nothing {}", msg.getMessageId())).subscriptionName(subscriptionName)
                        .receiverQueueSize(pulsarConfig.receiveQueueSize)
                        .subscribe();
            }
            return;
        }
        List<List<Consumer<byte[]>>> consumerListList = new ArrayList<>();
        List<Semaphore> semaphores = new ArrayList<>();
        for (int i = 0; i < commonConfig.pullThreads; i++) {
            consumerListList.add(new ArrayList<>());
        }
        int aux = 0;
        for (String topic : topics) {
            final Consumer<byte[]> consumer = createConsumerBuilder(topic)
                    .receiverQueueSize(pulsarConfig.receiveQueueSize)
                    .subscriptionName(subscriptionName).subscribe();
            int index = aux % commonConfig.pullThreads;
            consumerListList.get(index).add(consumer);
            if (pulsarConfig.receiveLimiter == -1) {
                semaphores.add(null);
            } else {
                semaphores.add(new Semaphore(pulsarConfig.receiveLimiter));
            }
            aux++;
        }
        for (int i = 0; i < commonConfig.pullThreads; i++) {
            new PulsarPullThread(i, actionService, semaphores, consumerListList.get(i), pulsarConfig).start();
        }
    }

    public ConsumerBuilder<byte[]> createConsumerBuilder(String topic) {
        ConsumerBuilder<byte[]> builder = pulsarClient.newConsumer().topic(topic)
                .subscriptionName(UUID.randomUUID().toString());
        builder = builder.subscriptionType(pulsarConfig.subscriptionType);
        if (pulsarConfig.autoUpdatePartition) {
            builder.autoUpdatePartitions(true);
            builder.autoUpdatePartitionsInterval(pulsarConfig.autoUpdatePartitionSeconds, TimeUnit.SECONDS);
        }
        if (pulsarConfig.enableAckTimeout) {
            builder.ackTimeout(pulsarConfig.ackTimeoutMilliseconds, TimeUnit.MILLISECONDS);
            builder.ackTimeoutTickTime(pulsarConfig.ackTimeoutTickTimeMilliseconds, TimeUnit.MILLISECONDS);
        }
        if (!pulsarConfig.consumeBatch) {
            return builder;
        }
        final BatchReceivePolicy batchReceivePolicy = BatchReceivePolicy.builder()
                .timeout(pulsarConfig.consumeBatchTimeoutMs, TimeUnit.MILLISECONDS)
                .maxNumMessages(pulsarConfig.consumeBatchMaxMessages).build();
        return builder.batchReceivePolicy(batchReceivePolicy);
    }

    private String topicFn(String tenant, String namespace, String topic) {
        return String.format("persistent://%s/%s/%s", tenant, namespace, topic);
    }

    private void createTopic(List<String> topics, String tenantName, String namespaceName) {
        for (int k = 0; k <= pulsarConfig.topicSuffixNum; k++) {
            String topicName = "";
            if (pulsarConfig.topicSuffixNum == 0) {
                tenantName = pulsarConfig.topic;
            } else {
                topicName = NameUtil.name(pulsarConfig.topic, k, pulsarConfig.topicSuffixNumOfDigits);
            }
            topics.add(topicFn(tenantName, namespaceName, topicName));
        }
    }

}
