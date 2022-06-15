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

package com.github.shoothzj.pf.consumer.common.service;

import com.github.shoothzj.javatool.util.CommonUtil;
import com.github.shoothzj.pf.consumer.action.IAction;
import com.github.shoothzj.pf.consumer.action.influx.InfluxStrAction;
import com.github.shoothzj.pf.consumer.action.kafka.KafkaByteBufferAction;
import com.github.shoothzj.pf.consumer.action.kafka.KafkaBytesAction;
import com.github.shoothzj.pf.consumer.action.kafka.KafkaStrAction;
import com.github.shoothzj.pf.consumer.action.log.LogStrAction;
import com.github.shoothzj.pf.consumer.action.module.ActionMsg;
import com.github.shoothzj.pf.consumer.common.config.ActionConfig;
import com.github.shoothzj.pf.consumer.common.config.CommonConfig;
import com.github.shoothzj.pf.consumer.common.module.ActionType;
import com.github.shoothzj.pf.consumer.common.module.ExchangeType;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * @author hezhangjian
 */
@Slf4j
@Service
public class ActionService {

    @Autowired
    private ActionConfig actionConfig;

    @Autowired
    private CommonConfig commonConfig;

    private Optional<IAction<ByteBuffer>> byteBufferAction = Optional.empty();

    private Optional<IAction<byte[]>> bytesAction = Optional.empty();

    private Optional<IAction<String>> strAction = Optional.empty();

    @PostConstruct
    public void init() {
        if (commonConfig.exchangeType.equals(ExchangeType.BYTE_BUFFER)) {
            if (actionConfig.actionType.equals(ActionType.KAFKA)) {
                byteBufferAction = Optional.of(new KafkaByteBufferAction(actionConfig.kafkaAddr));
            }
        }
        if (commonConfig.exchangeType.equals(ExchangeType.BYTES)) {
            if (actionConfig.actionType.equals(ActionType.KAFKA)) {
                bytesAction = Optional.of(new KafkaBytesAction(actionConfig.kafkaAddr));
            }
        }
        if (commonConfig.exchangeType.equals(ExchangeType.STRING)) {
            if (actionConfig.actionType.equals(ActionType.INFLUX)) {
                strAction = Optional.of(new InfluxStrAction());
            } else if (actionConfig.actionType.equals(ActionType.KAFKA)) {
                strAction = Optional.of(new KafkaStrAction(actionConfig.kafkaAddr));
            } else if (actionConfig.actionType.equals(ActionType.LOG)) {
                strAction = Optional.of(new LogStrAction(actionConfig.logRegex));
            }
        }
        byteBufferAction.ifPresent(IAction::init);
        bytesAction.ifPresent(IAction::init);
        strAction.ifPresent(IAction::init);
    }

    public void handleStrBatchMsg(List<ActionMsg<String>> msgList) {
        blockIfNeeded();
        strAction.ifPresent(action -> action.handleBatchMsg(msgList));
    }

    public void handleStrMsg(@NotNull ActionMsg<String> msg) {
        blockIfNeeded();
        strAction.ifPresent(action -> action.handleMsg(msg));
    }

    public void handleBytesBatchMsg(List<ActionMsg<byte[]>> msgList) {
        blockIfNeeded();
        bytesAction.ifPresent(action -> action.handleBatchMsg(msgList));
    }

    public void handleBytesMsg(@NotNull ActionMsg<byte[]> msg) {
        blockIfNeeded();
        bytesAction.ifPresent(action -> action.handleMsg(msg));
    }

    public void handleByteBufferBatchMsg(List<ActionMsg<ByteBuffer>> msgList) {
        blockIfNeeded();
        byteBufferAction.ifPresent(action -> action.handleBatchMsg(msgList));
    }

    public void handleByteBufferMsg(@NotNull ActionMsg<ByteBuffer> msg) {
        blockIfNeeded();
        byteBufferAction.ifPresent(action -> action.handleMsg(msg));
    }

    private void blockIfNeeded() {
        if (actionConfig.actionBlockDelayMs != 0) {
            CommonUtil.sleep(TimeUnit.MILLISECONDS, actionConfig.actionBlockDelayMs);
        }
    }

}
