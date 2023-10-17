package org.emma.spark.streaming.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;

public class CommonUtils {
    private static final Logger LOG = LoggerFactory.getLogger(CommonUtils.class);

    public static int getRandomFreePort() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            int randomPort = serverSocket.getLocalPort();
            LOG.info("#getRandomFreePort random port {}", randomPort);
            return randomPort;
        }
    }
}
