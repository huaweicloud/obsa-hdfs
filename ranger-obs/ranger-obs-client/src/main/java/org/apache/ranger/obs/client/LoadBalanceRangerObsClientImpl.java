/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package org.apache.ranger.obs.client;

import com.google.common.base.Preconditions;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryPolicy.RetryAction;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Time;
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
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.SocketFactory;

/**
 * because ranger obs service is scale out; this client impl have LoadBalance ability
 */
public class LoadBalanceRangerObsClientImpl implements RangerObsClient {

    private static final Logger LOG = LoggerFactory.getLogger(LoadBalanceRangerObsClientImpl.class);

    private Configuration conf;

    private String serviceName;

    private Provider[] providers;

    private AtomicInteger currentIdx;

    private RetryPolicy retryPolicy = null;

    private static Provider[] shuffle(Provider[] providers) {
        List<Provider> list = Arrays.asList(providers);
        Collections.shuffle(list);
        return list.toArray(providers);
    }

    @Override
    public Token<DelegationTokenIdentifier> getDelegationToken(String renewer) throws IOException {
        return doOp(new ProviderCallable<Token<DelegationTokenIdentifier>>() {
            @Override
            public Token<DelegationTokenIdentifier> call(RangerObsServiceProtocol client) throws IOException {
                return client.getDelegationToken(new Text(renewer));
            }
        }, nextIdx());
    }

    @Override
    public String getCanonicalServiceName() {
        return this.serviceName;
    }

    @Override
    public boolean checkPermission(PermissionRequest permissionRequest) throws IOException {
        return doOp(new ProviderCallable<Boolean>() {
            @Override
            public Boolean call(RangerObsServiceProtocol client) throws IOException {
                return client.checkPermission(permissionRequest);
            }
        }, nextIdx());
    }

    @Override
    public long renewDelegationToken(Token<?> token, Configuration configuration) throws IOException {
        if (this.providers == null) {
            this.init(configuration);
        }
        return doOp(new ProviderCallable<Long>() {
            @Override
            public Long call(RangerObsServiceProtocol client) throws IOException {
                return client.renewDelegationToken((Token<DelegationTokenIdentifier>) token);
            }
        }, nextIdx());
    }

    @Override
    public synchronized void init(Configuration configuration) throws IOException {
        if (this.providers != null) {
            return;
        }
        this.conf = configuration;
        this.initServiceName();

        RPC.setProtocolEngine(conf, RangerObsServiceProtocolPB.class, ProtobufRpcEngine.class);
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        SocketFactory socketFactory = NetUtils.getDefaultSocketFactory(this.conf);
        int timeout = Client.getRpcTimeout(this.conf);
        RetryPolicy retryPolicy = RetryPolicies.TRY_ONCE_THEN_FAIL;
        long version = RPC.getProtocolVersion(RangerObsServiceProtocolPB.class);

        String target = conf.get(ClientConstants.RANGER_OBS_SERVICE_RPC_ADDRESS,"").trim();
        if (target.length() == 0){
            throw new IOException(ClientConstants.RANGER_OBS_SERVICE_RPC_ADDRESS + "is not set");
        }
        String[] split = target.split(";");
        providers = new Provider[split.length];
        for (int i = 0; i < split.length; i++) {
            InetSocketAddress serverIpAddr = NetUtils.createSocketAddr(split[i], 26901);
            RangerObsServiceProtocolPB proxy = RPC.getProtocolProxy(
                RangerObsServiceProtocolPB.class, version, serverIpAddr, ugi, this.conf,
                socketFactory, timeout, retryPolicy, new AtomicBoolean(false)).getProxy();
            RangerObsServiceProtocol translatorProxy = new RangerObsServiceProtocolTranslatorPB(proxy);
            providers[i] = new Provider(translatorProxy, split[i]);
        }
        providers = shuffle(providers);

        int maxNumRetries = conf.getInt(ClientConstants.RANGER_OBS_CLIENT_FAILOVER_MAX_RETRIES, providers.length);
        int sleepBaseMillis = conf.getInt(ClientConstants.RANGER_OBS_CLIENT_FAILOVER_SLEEP_BASE_MILLIS,
            ClientConstants.DEFAULT_RANGER_OBS_CLIENT_FAILOVER_SLEEP_BASE_MILLIS);
        int sleepMaxMillis = conf.getInt(ClientConstants.RANGER_OBS_CLIENT_FAILOVER_SLEEP_MAX_MILLIS,
            ClientConstants.DEFAULT_RANGER_OBS_CLIENT_FAILOVER_SLEEP_MAX_MILLIS);
        Preconditions.checkState(maxNumRetries >= 0);
        Preconditions.checkState(sleepBaseMillis >= 0);
        Preconditions.checkState(sleepMaxMillis >= 0);
        this.retryPolicy = RetryPolicies.failoverOnNetworkException(
            RetryPolicies.TRY_ONCE_THEN_FAIL, maxNumRetries, 0, sleepBaseMillis, sleepMaxMillis);

        long seed = Time.monotonicNow();
        this.currentIdx = new AtomicInteger((int) (seed % providers.length));

    }

    private void initServiceName() throws IOException {
        String address = this.conf.get(ClientConstants.RANGER_OBS_SERVICE_DT_SERVICE_NAME,
            ClientConstants.DEFAULT_RANGER_OBS_SERVICE_DT_SERVICE_NAME);
        InetSocketAddress addr = NetUtils.createSocketAddr(address, 26901);
        String host = addr.getAddress().getHostAddress();
        this.serviceName = host + ":" + addr.getPort();
    }

    @Override
    public Void cancelDelegationToken(Token<?> token, Configuration configuration)
        throws IOException {
        if (this.providers == null) {
            this.init(configuration);
        }
        return doOp(new ProviderCallable<Void>() {
            @Override
            public Void call(RangerObsServiceProtocol client) throws IOException {
                client.cancelDelegationToken((Token<DelegationTokenIdentifier>) token);
                return null;
            }
        }, nextIdx());
    }

    @Override
    public GetSTSResponse getSTS(String region, String bucketName, String allowPrefix) throws IOException {
        return doOp(new ProviderCallable<GetSTSResponse>() {
            @Override
            public GetSTSResponse call(RangerObsServiceProtocol client) throws IOException {
                GetSTSRequest getSTSRequest = new GetSTSRequest();
                getSTSRequest.setRegion(region);
                getSTSRequest.setBucketName(bucketName);
                getSTSRequest.setAllowPrefix(allowPrefix);
                return client.getSTS(getSTSRequest);
            }
        }, nextIdx());
    }

    private int nextIdx() {
        while (true) {
            int current = currentIdx.get();
            int next = (current + 1) % providers.length;
            if (currentIdx.compareAndSet(current, next)) {
                return current;
            }
        }
    }

    private <T> T doOp(ProviderCallable<T> op, int currPos)
        throws IOException {
        if (providers.length == 0) {
            throw new IOException("No providers configured !");
        }
        IOException ex = null;
        int numFailovers = 0;
        for (int i = 0; ; i++, numFailovers++) {
            Provider provider = providers[(currPos + i) % providers.length];
            try {
                return op.call(provider.getClient());
            } catch (IOException ioe) {
                LOG.warn("client at [{}] threw an IOException: ", provider.getTarget(), ioe);
                ex = ioe;

                RetryAction action = null;
                try {
                    action = retryPolicy.shouldRetry(ioe, 0, numFailovers, false);
                } catch (Exception e) {
                    throw new IOException(e);
                }
                // make sure each provider is tried at least once
                if (action.action == RetryAction.RetryDecision.FAIL
                    && numFailovers >= providers.length - 1) {
                    LOG.error("Aborting since the Request has failed with all ranger-obs-service"
                            + " providers(depending on {}={} setting and numProviders={})"
                            + " in the group OR the exception is not recoverable",
                        ClientConstants.RANGER_OBS_CLIENT_FAILOVER_MAX_RETRIES,
                        conf.getInt(ClientConstants.RANGER_OBS_CLIENT_FAILOVER_MAX_RETRIES, providers.length),
                        providers.length);
                    throw ex;
                }
                if ((numFailovers + 1) % providers.length == 0) {
                    // Sleep only after we try all the providers for every cycle.
                    try {
                        Thread.sleep(action.delayMillis);
                    } catch (InterruptedException e) {
                        throw new InterruptedIOException("Thread Interrupted");
                    }
                }
                LOG.warn("client retring [{} times]", numFailovers + 1);
            }
        }
    }

    @Override
    public void close() {
    }

    interface ProviderCallable<T> {
        T call(RangerObsServiceProtocol client) throws IOException;
    }

    static class Provider {
        private RangerObsServiceProtocol client;

        private String target;

        public Provider(RangerObsServiceProtocol client, String target) {
            this.client = client;
            this.target = target;
        }

        public RangerObsServiceProtocol getClient() {
            return client;
        }

        public String getTarget() {
            return target;
        }
    }
}