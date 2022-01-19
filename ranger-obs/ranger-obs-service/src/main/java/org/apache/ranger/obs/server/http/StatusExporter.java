/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package org.apache.ranger.obs.server.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.ranger.obs.server.RPCService;

import java.io.IOException;
import java.net.InetAddress;

/**
 * 功能描述
 *
 * @since 2021-09-29
 */
public class StatusExporter {
    public static final StatusExporter INSTANCE = new StatusExporter();

    private long serviceStartTimeMillSec = System.currentTimeMillis();

    private RPCService rpcService;

    private volatile boolean isSecurity = false;

    private volatile boolean isSTSenable = false;

    private volatile boolean isAuthorizeEnable = true;

    private volatile String address;

    private StatusExporter() {
    }

    public void setInfo(RPCService rpcService) throws IOException {
        this.rpcService = rpcService;
        int port = rpcService.getRpcServerAddress().getPort();
        this.address = InetAddress.getLocalHost().toString() + ":" + port;
        this.isSTSenable = rpcService.isSTSenable();
        this.isAuthorizeEnable = rpcService.isAuthorizeEnable();
        this.isSecurity = rpcService.isSecurity();
    }

    public String export() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode rootNode = mapper.createObjectNode();
        rootNode.put("serviceStartTime", this.serviceStartTimeMillSec);
        rootNode.put("status", rpcService.isRunning());
        rootNode.put("address", this.address);
        rootNode.put("isSecurity", this.isSecurity);
        rootNode.put("isSTSenable", this.isSTSenable);
        rootNode.put("isAuthorizeEnable", this.isAuthorizeEnable);
        return mapper.writeValueAsString(rootNode);
    }
}
