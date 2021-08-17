package com.github.shoothzj.pf.consumer.service;

import com.github.shoothzj.pf.consumer.config.PfConfig;
import com.github.shoothzj.pf.consumer.kafka.KafkaBootService;
import com.github.shoothzj.pf.consumer.module.Middleware;
import com.github.shoothzj.pf.consumer.pulsar.PulsarBootService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

/**
 * @author hezhangjian
 */
@Slf4j
@Service
public class BootService {

    @Autowired
    private PfConfig pfConfig;

    @Autowired
    private PulsarBootService pulsarBootService;

    @Autowired
    private KafkaBootService kafkaBootService;

    @PostConstruct
    public void init() throws Exception {
        if (pfConfig.middleware.equals(Middleware.PULSAR)) {
            pulsarBootService.boot();
        } else if (pfConfig.middleware.equals(Middleware.KAFKA)) {
            kafkaBootService.boot();
        }
    }

}
