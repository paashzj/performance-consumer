package com.github.shoothzj.pf.consumer.service;

import com.github.shoothzj.pf.consumer.config.PfConfig;
import com.github.shoothzj.pf.consumer.kafka.KafkaBootService;
import com.github.shoothzj.pf.consumer.mqtt.MqttBootService;
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
    private KafkaBootService kafkaBootService;

    @Autowired
    private MqttBootService mqttBootService;

    @Autowired
    private PulsarBootService pulsarBootService;

    @PostConstruct
    public void init() throws Exception {
        switch (pfConfig.middleware) {
            case KAFKA:
                kafkaBootService.boot();
                break;
            case MQTT:
                mqttBootService.boot();
                break;
            case PULSAR:
                pulsarBootService.boot();
                break;
            default:
                break;
        }
    }

}
