package com.github.shoothzj.pf.consumer.mqtt;

import com.github.shoothzj.pf.consumer.action.module.ActionMsg;
import com.github.shoothzj.pf.consumer.common.config.CommonConfig;
import com.github.shoothzj.pf.consumer.common.service.ActionService;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;

@Slf4j
@Service
public class MqttBootService implements MqttCallback {

    private final MqttConfig mqttConfig;

    private final CommonConfig commonConfig;

    private final ActionService actionService;

    public MqttBootService(@Autowired MqttConfig mqttConfig, @Autowired CommonConfig commonConfig,
                           @Autowired ActionService actionService) {
        this.mqttConfig = mqttConfig;
        this.commonConfig = commonConfig;
        this.actionService = actionService;
    }

    public void boot() throws Exception {
        MqttClient mqttClient = new MqttClient(String.format("tcp://%s:%d", mqttConfig.host, mqttConfig.port),
                mqttConfig.clientId);
        mqttClient.setCallback(this);
        mqttClient.subscribe(mqttConfig.topic);
        mqttClient.setCallback(this);
    }

    @Override
    public void connectionLost(Throwable throwable) {

    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        ActionMsg actionMsg = new ActionMsg();
        actionMsg.setContent(new String(message.getPayload(), StandardCharsets.UTF_8));
        this.actionService.handleMsg(actionMsg);
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {

    }
}
