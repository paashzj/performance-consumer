package com.github.shoothzj.pf.consumer.action.log;

import com.github.shoothzj.pf.consumer.action.AbstractAction;
import com.github.shoothzj.pf.consumer.action.module.ActionMsg;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class LogAction extends AbstractAction {

    @Override
    public void init() {

    }

    @Override
    public void handleBatchMsg(List<ActionMsg> msgList) {
        for (ActionMsg actionMsg : msgList) {
            this.handleMsg(actionMsg);
        }
    }

    @Override
    public void handleMsg(ActionMsg msg) {
        log.info("action msg is {}", msg);
    }
}
