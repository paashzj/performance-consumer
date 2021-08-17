package com.github.shoothzj.pf.consumer.common;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

/**
 * @author hezhangjian
 */
@Configuration
@Service
public class CommonConfig {

    @Value("${CONSUME_MODE:PULL}")
    public ConsumeMode consumeMode;

    @Value("${PULL_THREADS:1}")
    public int pullThreads;

}
