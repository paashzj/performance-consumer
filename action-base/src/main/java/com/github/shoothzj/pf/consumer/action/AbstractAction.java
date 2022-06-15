/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.github.shoothzj.pf.consumer.action;

import com.github.shoothzj.pf.consumer.action.module.ActionMsg;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author hezhangjian
 */
@Slf4j
public abstract class AbstractAction {

    public abstract void init();

    public abstract void handleStrBatchMsg(List<ActionMsg<String>> msgList);

    public abstract void handleStrMsg(ActionMsg<String> msg);

    public abstract void handleBytesBatchMsg(List<ActionMsg<byte[]>> msgList);

    public abstract void handleBytesMsg(ActionMsg<byte[]> msg);

    public abstract void handleByteBufferBatchMsg(List<ActionMsg<ByteBuffer>> msgList);

    public abstract void handleByteBufferMsg(ActionMsg<ByteBuffer> msg);

}
