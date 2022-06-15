package com.github.shoothzj.pf.consumer.action;

import com.github.shoothzj.pf.consumer.action.module.ActionMsg;

import java.nio.ByteBuffer;
import java.util.List;

public interface IByteBufferAction extends IAction {

    void handleByteBufferBatchMsg(List<ActionMsg<ByteBuffer>> msgList);

    void handleByteBufferMsg(ActionMsg<ByteBuffer> msg);
    
}
