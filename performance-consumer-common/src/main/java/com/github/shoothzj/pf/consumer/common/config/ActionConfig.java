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

    @Value("${ACTION_TYPE}")
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

}
