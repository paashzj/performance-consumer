package com.github.shoothzj.pf.consumer.action.kafka;

import java.net.ServerSocket;

public class TestUtil {
    public static int getFreePort() throws Exception {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        }
    }

}
