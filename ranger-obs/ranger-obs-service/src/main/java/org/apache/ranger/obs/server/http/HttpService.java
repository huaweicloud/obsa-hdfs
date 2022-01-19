/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package org.apache.ranger.obs.server.http;

import com.sun.net.httpserver.HttpServer;

import org.apache.hadoop.conf.Configuration;
import org.apache.ranger.obs.server.ServerConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * The functions of the HttpService include:
 * 1.jmx service
 * 2.health examination service
 */
public class HttpService {
    private static final Logger LOG = LoggerFactory.getLogger(HttpService.class);

    private HttpServer httpServer;

    private Configuration conf;

    public HttpService(Configuration conf) {
        this.conf = conf;
    }

    public void start() throws IOException {
        int httpPort = this.conf.getInt(ServerConstants.RANGER_OBS_SERVICE_STATUS_PORT,
            ServerConstants.DEFAULT_RANGER_OBS_SERVICE_STATUS_PORT);
        httpServer = HttpServer.create(new InetSocketAddress(httpPort), 0);
        httpServer.createContext("/status", new StatusHttpHandler());
        httpServer.createContext("/jmx", new JMXHttpHandler());

        Thread httpServerThread = new Thread(new Runnable() {
            @Override
            public void run() {
                httpServer.start();
            }
        });
        httpServerThread.setDaemon(true);
        httpServerThread.start();
        LOG.info("HttpService listen on {}", httpPort);
    }

    public void stop() {
        httpServer.stop(2);
    }
}
