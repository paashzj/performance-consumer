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

package com.github.shoothzj.pf.consumer.common.config;

import com.github.shoothzj.pf.consumer.common.module.ActionType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

/**
 * @author hezhangjian
 */
@Configuration
@Service
public class ActionConfig {

    @Value("${ACTION_TYPE:LOG}")
    public ActionType actionType;

    @Value("${ACTION_BLOCK_DELAY_MS:0}")
    public int actionBlockDelayMs;

    @Value("${INFLUX_ADDR:null}")
    public String influxAddr;

    @Value("${INFLUX_TOKEN:null}")
    public String influxToken;

    @Value("${INFLUX_ORG:null}")
    public String influxOrg;

    @Value("${INFLUX_BUCKET:null}")
    public String influxBucket;

    @Value("${KAFKA_ADDR:null}")
    public String kafkaAddr;

    @Value("${LOG_REGEX:null}")
    public String logRegex;

}
