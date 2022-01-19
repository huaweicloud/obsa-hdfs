/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package org.apache.ranger.obs.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.ranger.obs.protocol.RangerObsServiceProtocol;
import org.apache.ranger.obs.protocolpb.RangerObsServiceProtocolPB;
import org.apache.ranger.obs.protocolpb.RangerObsServiceProtocolTranslatorPB;
import org.apache.ranger.obs.security.authorization.PermissionRequest;
import org.apache.ranger.obs.security.sts.GetSTSRequest;
import org.apache.ranger.obs.security.sts.GetSTSResponse;
import org.apache.ranger.obs.security.token.DelegationTokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.SocketFactory;

/**
 * because ranger obs service is scale out; this client impl must with the help of LoadBalance component
 */
public class RangerObsClientImpl implements RangerObsClient {

    private static final Logger LOG = LoggerFactory.getLogger(RangerObsClientImpl.class);

    private Configuration conf;

    private String serviceName;

    private volatile AtomicReference<RangerObsServiceProtocol> rangerClientProxy = new AtomicReference();

    @Override
    public Token<DelegationTokenIdentifier> getDelegationToken(String renewer) throws IOException {
        int execCnt = 1;

        while (true) {
            RangerObsServiceProtocol rangerClient = this.rangerClientProxy.get();
            if (rangerClient == null) {
                throw new IOException(
                    "ranger client is null, maybe ranger server for qcloud object storage is not deployed!");
            }

            try {
                return rangerClient.getDelegationToken(new Text(renewer));
            } catch (IOException var5) {
                if (!this.backOff(execCnt)) {
                    LOG.error("getDelegationToken failed", var5);
                    throw var5;
                }

                ++execCnt;
            }
        }
    }

    private boolean backOff(int execCnt) {
        int maxRetryCnt = this.conf.getInt("ranger.obs.client.retry.max", 3);
        if (execCnt > maxRetryCnt) {
            return false;
        } else {
            try {
                Thread.sleep((long) ThreadLocalRandom.current().nextInt(500, 2000));
            } catch (InterruptedException var4) {
            }

            return true;
        }
    }

    @Override
    public String getCanonicalServiceName() {
        return this.serviceName;
    }

    @Override
    public boolean checkPermission(PermissionRequest permissionRequest) throws IOException {
        int execCnt = 1;

        while (true) {
            RangerObsServiceProtocol rangerClient = this.rangerClientProxy.get();
            if (rangerClient == null) {
                throw new IOException(
                    "ranger client is null, maybe ranger server for qcloud object storage is not deployed!");
            }

            try {
                return rangerClient.checkPermission(permissionRequest);
            } catch (IOException var5) {
                if (!this.backOff(execCnt)) {
                    LOG.error("checkPermission failed", var5);
                    throw var5;
                }

                ++execCnt;
            }
        }
    }

    @Override
    public long renewDelegationToken(Token<?> token, Configuration configuration) throws IOException {
        if (this.rangerClientProxy.get() == null) {
            this.init(configuration);
        }

        int execCnt = 1;

        while (true) {
            RangerObsServiceProtocol rangerClient = this.rangerClientProxy.get();
            if (rangerClient == null) {
                throw new IOException(
                    "ranger client is null, maybe ranger server for qcloud object storage is not deployed!");
            }

            try {
                return rangerClient.renewDelegationToken((Token<DelegationTokenIdentifier>) token);
            } catch (IOException var6) {
                if (!this.backOff(execCnt)) {
                    LOG.error("renew token failed", var6);
                    throw var6;
                }

                ++execCnt;
            }
        }
    }

    @Override
    public synchronized void init(Configuration configuration) throws IOException {
        if (this.rangerClientProxy.get() != null) {
            return;
        }
        this.conf = configuration;
        RPC.setProtocolEngine(conf, RangerObsServiceProtocolPB.class, ProtobufRpcEngine.class);
        this.initServiceName();

        long version = RPC.getProtocolVersion(RangerObsServiceProtocolPB.class);
        String target = conf.get(ClientConstants.RANGER_OBS_SERVICE_RPC_ADDRESS);
        InetSocketAddress serverIpAddr = NetUtils.createSocketAddr(target, 26901);
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        SocketFactory socketFactory = NetUtils.getDefaultSocketFactory(this.conf);
        int timeout = Client.getRpcTimeout(this.conf);
        RetryPolicy retryPolicy = null;

        RangerObsServiceProtocolPB proxy = RPC.getProtocolProxy(
            RangerObsServiceProtocolPB.class, version, serverIpAddr, ugi, this.conf,
            socketFactory, timeout, retryPolicy, new AtomicBoolean(false)).getProxy();
        RangerObsServiceProtocol translatorProxy = new RangerObsServiceProtocolTranslatorPB(proxy);
        this.rangerClientProxy.set(translatorProxy);
    }

    private void initServiceName() {
        String address = this.conf.get(ClientConstants.RANGER_OBS_SERVICE_DT_SERVICE_NAME,
            ClientConstants.DEFAULT_RANGER_OBS_SERVICE_DT_SERVICE_NAME);
        InetSocketAddress addr = NetUtils.createSocketAddr(address, 26901);
        String host = addr.getAddress().getHostAddress();
        this.serviceName = host + ":" + addr.getPort();
    }

    @Override
    public Void cancelDelegationToken(Token<?> token, Configuration configuration)
        throws IOException {
        if (this.rangerClientProxy.get() == null) {
            this.init(configuration);
        }

        int execCnt = 1;

        while (true) {
            RangerObsServiceProtocol rangerClient
                = this.rangerClientProxy.get();
            if (rangerClient == null) {
                throw new IOException(
                    "ranger client is null, maybe ranger server for qcloud object storage is not deployed!");
            }

            try {
                rangerClient.cancelDelegationToken((Token<DelegationTokenIdentifier>) token);
                return null;
            } catch (IOException var6) {
                if (!this.backOff(execCnt)) {
                    LOG.error("cancel token failed", var6);
                    throw var6;
                }

                ++execCnt;
            }
        }
    }

    @Override
    public GetSTSResponse getSTS(String region, String bucketName, String allowPrefix) throws IOException {
        int execCnt = 1;
        GetSTSRequest getSTSRequest = new GetSTSRequest();
        getSTSRequest.setRegion(region);
        getSTSRequest.setBucketName(bucketName);
        getSTSRequest.setAllowPrefix(allowPrefix);

        while (true) {
            RangerObsServiceProtocol rangerClient = this.rangerClientProxy.get();

            try {
                return rangerClient.getSTS(getSTSRequest);
            } catch (IOException var7) {
                if (!this.backOff(execCnt)) {
                    LOG.error("get sts failed", var7);
                    throw var7;
                }

                ++execCnt;
            }
        }
    }

    @Override
    public void close() {
    }
}