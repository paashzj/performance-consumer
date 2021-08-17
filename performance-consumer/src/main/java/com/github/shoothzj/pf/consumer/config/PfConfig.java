package com.github.shoothzj.pf.consumer.config;

import com.github.shoothzj.pf.consumer.module.Middleware;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

/**
 * @author hezhangjian
 */
@Configuration
@Service
public class PfConfig {

    @Value("${MIDDLEWARE}")
    public Middleware middleware;

}
