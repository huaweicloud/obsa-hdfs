/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package org.apache.ranger.obs.server.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class StatusHttpHandler implements HttpHandler {
    private static final Logger LOG = LoggerFactory.getLogger(StatusHttpHandler.class);

    private ExecutorService threadPool = Executors.newFixedThreadPool(4);

    @Override
    public void handle(final HttpExchange httpExchange) {
        this.threadPool.submit(new Runnable() {
            @Override
            public void run() {
                OutputStream out = null;
                try {
                    String statusStr = StatusExporter.INSTANCE.export();
                    httpExchange.sendResponseHeaders(200, (long) statusStr.length());
                    out = httpExchange.getResponseBody();
                    out.write(statusStr.getBytes("UTF-8"));
                } catch (IOException e) {
                    LOG.error("caught an exception while processing status request", e);
                    try {
                        httpExchange.sendResponseHeaders(500, 0);
                    } catch (IOException e1) {
                        LOG.error("caught an exception while processing status request", e1);
                    }
                } finally {
                    if (out != null) {
                        try {
                            out.close();
                        } catch (IOException e) {
                            LOG.error("caught an exception while processing status request", e);
                        }
                    }
                }
            }
        });
    }
}
