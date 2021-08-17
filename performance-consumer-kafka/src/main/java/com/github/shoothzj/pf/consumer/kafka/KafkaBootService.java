package com.github.shoothzj.pf.consumer.kafka;

import com.github.shoothzj.pf.consumer.common.CommonConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * @author hezhangjian
 */
@Slf4j
@Service
public class KafkaBootService {

    private final KafkaConfig kafkaConfig;

    private final CommonConfig commonConfig;

    public KafkaBootService(@Autowired KafkaConfig kafkaConfig, @Autowired CommonConfig commonConfig) {
        this.kafkaConfig = kafkaConfig;
        this.commonConfig = commonConfig;
    }

    public void boot() {
        List<String> topics = new ArrayList<>();
        if (kafkaConfig.topicSuffixNum == 0) {
            topics.add(kafkaConfig.topic);
        } else {
            for (int i = 0; i < kafkaConfig.topicSuffixNum; i++) {
                topics.add(kafkaConfig.topic + i);
            }
        }
        createConsumers(topics);
    }

    public void createConsumers(List<String> topics) {
        List<List<String>> strListList = new ArrayList<>();
        for (int i = 0; i < commonConfig.pullThreads; i++) {
            strListList.add(new ArrayList<>());
        }
        int aux = 0;
        for (String topic : topics) {
            int index = aux % commonConfig.pullThreads;
            strListList.get(index).add(topic);
            aux++;
        }
        for (int i = 0; i < commonConfig.pullThreads; i++) {
            new KafkaPullThread(i, strListList.get(i), kafkaConfig).start();
        }
    }

}