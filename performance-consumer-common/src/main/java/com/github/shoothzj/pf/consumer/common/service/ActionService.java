package com.github.shoothzj.pf.consumer.common.service;

import com.github.shoothzj.javatool.util.CommonUtil;
import com.github.shoothzj.pf.consumer.action.AbstractAction;
import com.github.shoothzj.pf.consumer.action.influx.InfluxAction;
import com.github.shoothzj.pf.consumer.action.module.ActionMsg;
import com.github.shoothzj.pf.consumer.common.config.ActionConfig;
import com.github.shoothzj.pf.consumer.common.module.ActionType;
import lombok.extern.slf4j.Slf4j;
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
        } else {
            action = null;
        }
    }

    public void handleBatchMsg(List<ActionMsg> msgList) {
        blockIfNeeded();
        if (action != null) {
            action.handleBatchMsg(msgList);
        }
    }

    public void handleMsg(ActionMsg msg) {
        blockIfNeeded();
        if (action != null) {
            action.handleMsg(msg);
        }
    }

    private void blockIfNeeded() {
        if (actionConfig.actionBlockDelayMs != 0) {
            CommonUtil.sleep(TimeUnit.MILLISECONDS, actionConfig.actionBlockDelayMs);
        }
    }

}
