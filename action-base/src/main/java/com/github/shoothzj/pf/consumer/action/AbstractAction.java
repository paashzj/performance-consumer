package com.github.shoothzj.pf.consumer.action;

import com.github.shoothzj.pf.consumer.action.module.ActionMsg;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * @author hezhangjian
 */
@Slf4j
public abstract class AbstractAction {

    public abstract void init();

    public abstract void handleBatchMsg(List<ActionMsg> msgList);

    public abstract void handleMsg(ActionMsg msg);

}
