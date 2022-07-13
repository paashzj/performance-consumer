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

package com.github.shoothzj.pf.consumer.action.http.okhttp;

import com.github.shoothzj.pf.consumer.action.IAction;
import com.github.shoothzj.pf.consumer.action.MsgCallback;
import com.github.shoothzj.pf.consumer.action.http.common.ActionHttpConfig;
import com.github.shoothzj.pf.consumer.action.module.ActionMsg;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.ConnectionPool;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Slf4j
public class OkhttpStrAction implements IAction<String> {

    private final ActionHttpConfig config;

    private final OkHttpClient client;

    private Request.Builder builder;

    public static final MediaType MEDIA_TYPE_JSON = MediaType.parse("application/json; charset=utf-8");

    public OkhttpStrAction(ActionHttpConfig config) {
        this.config = config;
        this.client = new OkHttpClient.Builder()
                .callTimeout(config.requestTimeoutSeconds, TimeUnit.SECONDS)
                .connectTimeout(config.connectionTimeoutSeconds, TimeUnit.SECONDS)
                .connectionPool(new ConnectionPool(config.maxIdleConnections,
                        config.connectionKeepaliveMinutes,
                        TimeUnit.MINUTES))
                .build();
    }

    @Override
    public void init() {
        String requestUrl = String.format("http://%s:%s/%s", config.host, config.port, config.uri);
        builder = new Request.Builder().url(requestUrl);
    }

    @Override
    public void handleBatchMsg(List<ActionMsg<String>> actionMsgs) {
        actionMsgs.forEach(msg -> {
            handleMsg(msg, Optional.empty());
        });
    }

    @Override
    public void handleMsg(ActionMsg<String> msg, Optional<MsgCallback> msgCallback) {
        Request request = builder.post(RequestBody.create(msg.getContent(), MEDIA_TYPE_JSON)).build();
        client.newCall(request).enqueue(new Callback() {
            @Override
            public void onFailure(@NotNull Call call, @NotNull IOException e) {
                log.error("send msg {} to http server {} failed", msg.getMessageId(), request.url(), e);
                msgCallback.ifPresent(msgCallback -> msgCallback.fail(msg.getMessageId()));
            }

            @Override
            public void onResponse(@NotNull Call call, @NotNull Response response) throws IOException {
                try (ResponseBody responseBody = response.body()) {
                    if (!response.isSuccessful()) {
                        if (responseBody != null) {
                            log.error("send msg {} to http server {} failed, response code: {}, reason: {}",
                                    msg.getMessageId(), request.url(), response.code(), responseBody);
                        } else {
                            log.error("send msg {} to http server {} failed, response code: {}",
                                    msg.getMessageId(), request.url(), response.code());
                        }
                        msgCallback.ifPresent(msgCallback -> msgCallback.fail(msg.getMessageId()));
                        return;
                    }
                    log.info("send msg {} succeed", msg.getMessageId());
                    msgCallback.ifPresent(msgCallback -> msgCallback.success(msg.getMessageId()));
                }
            }
        });
    }

}
