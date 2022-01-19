/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package org.apache.ranger.obs.server;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.ranger.obs.server.http.HttpService;
import org.apache.ranger.obs.server.http.StatusExporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * ranger obs service have scale out ability
 */
public class Server {
    private static final Logger LOG = LoggerFactory.getLogger(Server.class);

    private Configuration conf;

    private HttpService httpService;

    private RPCService rpcService;

    volatile private boolean running = false;

    public Server(Configuration conf) {
        this.conf = conf;
    }

    public static void main(String[] args) {
        Configuration conf = loadConfiguration();
        Server server = new Server(conf);
        try {
            server.start();
            server.join();
        } catch (InterruptedException | IOException e) {
            LOG.error("ranger obs service start failed", e);
            return;
        }

        ShutDownHookThread shutDownHookThread = new ShutDownHookThread();
        shutDownHookThread.setServer(server);
        Runtime.getRuntime().addShutdownHook(shutDownHookThread);
    }

    private static Configuration loadConfiguration() {
        Configuration conf = new Configuration();
        conf.addResource("ranger-obs.xml");
        conf.addResource("core-site.xml");
        conf.addResource("hdfs-site.xml");
        conf.addResource("hadoop-policy.xml");
        return conf;
    }

    public void join() throws InterruptedException {
        this.rpcService.join();
    }

    public void start() throws IOException {
        DefaultMetricsSystem.initialize("ranger-obs-service");

        this.rpcService = new RPCService(this.conf);
        try {
            LOG.info("starting RPCService");
            this.rpcService.init();
            this.rpcService.start();
        } catch (Exception e) {
            LOG.error("init/start RPCService failed", e);
            return;
        }

        this.httpService = new HttpService(this.conf);
        try {
            LOG.info("starting HttpService");
            this.httpService.start();
        } catch (IOException e) {
            LOG.error("start HttpService failed", e);
            return;
        }

        this.running = true;
        StatusExporter.INSTANCE.setInfo(rpcService);
    }

    public void stop() {
        if (rpcService != null) {
            rpcService.stop();
        }
        if (httpService != null) {
            httpService.stop();
        }
        DefaultMetricsSystem.shutdown();
        running = false;
    }

    public boolean isRunning() {
        return this.running;
    }

    private static class ShutDownHookThread extends Thread {
        Server server;

        public void setServer(Server server) {
            this.server = server;
        }

        @Override
        public void run() {
            if (this.server != null) {
                this.server.stop();
            }
        }
    }
}
