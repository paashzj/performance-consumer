package com.github.shoothzj.pf.consumer.action;

import com.github.shoothzj.pf.consumer.action.module.ActionMsg;

import java.util.List;

public interface IBytesAction extends IAction {

    void handleBytesBatchMsg(List<ActionMsg<byte[]>> msgList);

    void handleBytesMsg(ActionMsg<byte[]> msg);

}
