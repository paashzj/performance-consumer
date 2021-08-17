package com.github.shoothzj.pf.consumer.common;

import lombok.extern.slf4j.Slf4j;

/**
 * @author hezhangjian
 */
@Slf4j
public abstract class AbstractPullThread extends Thread {

    public AbstractPullThread(int i) {
        setName("pull- " + i);
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

    abstract protected void pull() throws Exception;

}
