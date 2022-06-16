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

    @Value("${ACTION_INFLUX_ADDR:}")
    public String influxAddr;

    @Value("${ACTION_INFLUX_TOKEN:}")
    public String influxToken;

    @Value("${ACTION_INFLUX_ORG:}")
    public String influxOrg;

    @Value("${ACTION_INFLUX_BUCKET:}")
    public String influxBucket;

    @Value("${ACTION_KAFKA_ADDR:}")
    public String kafkaAddr;

    @Value("${ACTION_LOG_REGEX:}")
    public String logRegex;

}
