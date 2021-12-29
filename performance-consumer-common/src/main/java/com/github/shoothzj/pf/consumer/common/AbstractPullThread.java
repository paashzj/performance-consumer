package com.github.shoothzj.pf.consumer.common;

import com.github.shoothzj.pf.consumer.common.service.ActionService;
import lombok.extern.slf4j.Slf4j;

/**
 * @author hezhangjian
 */
@Slf4j
public abstract class AbstractPullThread extends Thread {

    protected final ActionService actionService;

    public AbstractPullThread(int i, ActionService actionService) {
        setName("pull- " + i);
        this.actionService = actionService;
    }

    @Override
    public void run() {
        while (true) {
            try {
                pull();
            } catch (Exception e) {
                log.error("ignore exception ", e);
            }
        }
    }

    protected abstract void pull() throws Exception;

}
