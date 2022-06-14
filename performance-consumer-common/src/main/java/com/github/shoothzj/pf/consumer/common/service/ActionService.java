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
import com.github.shoothzj.pf.consumer.action.AbstractAction;
import com.github.shoothzj.pf.consumer.action.influx.InfluxAction;
import com.github.shoothzj.pf.consumer.action.kafka.KafkaAction;
import com.github.shoothzj.pf.consumer.action.log.LogAction;
import com.github.shoothzj.pf.consumer.action.module.ActionMsg;
import com.github.shoothzj.pf.consumer.common.config.ActionConfig;
import com.github.shoothzj.pf.consumer.common.module.ActionType;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author hezhangjian
 */
@Slf4j
@Service
public class ActionService {

    @Autowired
    private ActionConfig actionConfig;

    private AbstractAction action;

    @PostConstruct
    public void init() {
        if (actionConfig.actionType.equals(ActionType.INFLUX)) {
            action = new InfluxAction();
            action.init();
        } else if (actionConfig.actionType.equals(ActionType.KAFKA)) {
            action = new KafkaAction();
            action.init();
        } else if (actionConfig.actionType.equals(ActionType.LOG)) {
            action = new LogAction();
            action.init();
        } else {
            action = null;
        }
    }

    public void handleStrBatchMsg(List<ActionMsg<String>> msgList) {
        blockIfNeeded();
        if (action != null) {
            action.handleStrBatchMsg(msgList);
        }
    }

    public void handleStrMsg(@NotNull ActionMsg<String> msg) {
        blockIfNeeded();
        if (action != null) {
            action.handleStrMsg(msg);
        }
    }

    private void blockIfNeeded() {
        if (actionConfig.actionBlockDelayMs != 0) {
            CommonUtil.sleep(TimeUnit.MILLISECONDS, actionConfig.actionBlockDelayMs);
        }
    }

}
