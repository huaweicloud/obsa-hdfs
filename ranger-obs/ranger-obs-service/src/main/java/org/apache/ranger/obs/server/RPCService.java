/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package org.apache.ranger.obs.server;

import com.google.protobuf.BlockingService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.InvalidRequestException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Builder;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.apache.ranger.obs.authorize.Authorizer;
import org.apache.ranger.obs.protocol.RangerObsServiceProtocol;
import org.apache.ranger.obs.protocol.proto.RangerObsServiceProtocolProtos;
import org.apache.ranger.obs.protocolpb.RangerObsServiceProtocolPB;
import org.apache.ranger.obs.protocolpb.RangerObsServiceProtocolServerSideTranslatorPB;
import org.apache.ranger.obs.security.authorization.PermissionRequest;
import org.apache.ranger.obs.security.policy.RangerObsServicePolicyProvider;
import org.apache.ranger.obs.security.sts.ECSSTSProvider;
import org.apache.ranger.obs.security.sts.GetSTSRequest;
import org.apache.ranger.obs.security.sts.GetSTSResponse;
import org.apache.ranger.obs.security.sts.IAMSTSProvider;
import org.apache.ranger.obs.security.sts.STSProvider;
import org.apache.ranger.obs.security.token.DelegationTokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * The functions of the RPCService include:
 * 1.check obs request permission
 * 2.supply access obs temporary aksk
 * 3.supply delegation token in security cluster
 */
public class RPCService implements RangerObsServiceProtocol {
    private static final Logger LOG = LoggerFactory.getLogger(RPCService.class);

    private Configuration conf;

    private AbstractDelegationTokenSecretManager dtSecretManager;

    private Authorizer authorizer;

    private Server serviceRpcServer;

    private String serviceName;

    private STSProvider stsProvider;

    private boolean isSecurity;

    private boolean isSTSEnable;

    private boolean isAuthorizeEnable;

    volatile private boolean running = false;

    public RPCService(Configuration conf) {
        this.conf = conf;
    }

    private static UserGroupInformation getRemoteUser() throws IOException {
        UserGroupInformation ugi = org.apache.hadoop.ipc.Server.getRemoteUser();
        return ugi != null ? ugi : UserGroupInformation.getCurrentUser();
    }

    public void init() throws Exception {
        this.isSecurity = SecurityUtil.getAuthenticationMethod(conf)
            == UserGroupInformation.AuthenticationMethod.KERBEROS;
        this.isSTSEnable = this.conf.getBoolean(ServerConstants.RANGER_OBS_SERVICE_STS_ENABLE,
            ServerConstants.DEFAULT_RANGER_OBS_SERVICE_STS_ENABLE);
        this.isAuthorizeEnable = this.conf.getBoolean(ServerConstants.RANGER_OBS_SERVICE_AUTHORIZE_ENABLE,
            ServerConstants.DEFAULT_RANGER_OBS_SERVICE_AUTHORIZE_ENABLE);

        if (this.isSecurity) {
            this.login();
            this.initSecretManager();
        }

        if (this.isSTSEnable) {
            this.initSTSProvider();
        }
        if (this.isAuthorizeEnable) {
            this.initAuthorizer();
        }
    }

    private void startSTSProvider() throws IOException {
        if (this.stsProvider != null) {
            synchronized (this.stsProvider) {
                this.stsProvider.start();
            }
        }
    }

    private void initSTSProvider() throws Exception {
        try {
            Class<?> stsProviderClassName = this.conf.getClass(ServerConstants.RANGER_OBS_SERVICE_STS_PROVIDER,
                null);
            if(stsProviderClassName!=null){
                this.stsProvider = (STSProvider) stsProviderClassName.newInstance();
                stsProvider.init(this.conf);
            } else{
                throw new Exception("stsProvider not set");
            }
        } catch (InstantiationException | IllegalAccessException e) {
            throw new IOException("get stsProvider error", e);
        }
    }

    private void initSecretManager() throws IOException {
        String address = this.conf.get(ServerConstants.RANGER_OBS_SERVICE_DT_SERVICE_NAME,
            ServerConstants.DEFAULT_RANGER_OBS_SERVICE_DT_SERVICE_NAME);
        InetSocketAddress addr = NetUtils.createSocketAddr(address, 26901);
        String host = addr.getAddress().getHostAddress();
        this.serviceName = host + ":" + addr.getPort();

        Class<?> className = conf.getClass(ServerConstants.RANGER_OBS_SERVICE_DT_MANAGER,
            ServerConstants.DEFAULT_RANGER_OBS_SERVICE_DT_MANAGER);
        if (className != null) {
            try {
                Constructor<?> declaredConstructor = className.getDeclaredConstructor(Configuration.class);
                dtSecretManager = (AbstractDelegationTokenSecretManager) declaredConstructor.newInstance(conf);
            } catch (NoSuchMethodException | InstantiationException
                | IllegalAccessException | InvocationTargetException e) {
                throw new IOException("SimpleSecretManager initSecret failed", e);
            }
        }
    }

    private void initAuthorizer() throws IOException {
        try {
            Class<?> authorizerClassName = this.conf.getClass(ServerConstants.RANGER_OBS_SERVICE_AUTHORIZER,
                ServerConstants.DEFAULT_RANGER_OBS_SERVICE_AUTHORIZER);
            this.authorizer = (Authorizer) authorizerClassName.newInstance();
            authorizer.init(this.conf);
        } catch (InstantiationException | IllegalAccessException e) {
            throw new IOException("get Authorizer error", e);
        }
    }

    private void startSecretManager() {
        if (this.dtSecretManager != null) {
            synchronized (this.dtSecretManager) {
                if (!this.dtSecretManager.isRunning()) {
                    try {
                        this.dtSecretManager.startThreads();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    private void startAuthorizer() {
        if (this.authorizer != null) {
            synchronized (this.authorizer) {
                this.authorizer.start();
            }
        }
    }

    private void login() throws IOException {
        UserGroupInformation.setConfiguration(this.conf);
        InetSocketAddress socAddr = this.getRpcServerAddress();
        SecurityUtil.login(this.conf, ServerConstants.RANGER_OBS_SERVICE_KERBEROS_KEYTAB,
            ServerConstants.RANGER_OBS_SERVICE_KERBEROS_PRINCIPAL,
            socAddr.getHostName());
    }

    public InetSocketAddress getRpcServerAddress() throws IOException {
        String address = this.conf.get(ServerConstants.RANGER_OBS_SERVICE_RPC_ADDRESS,
            ServerConstants.DEFAULT_RANGER_OBS_SERVICE_RPC_ADDRESS);
        if (address != null && !address.isEmpty()) {
            return NetUtils.createSocketAddr(address, 26901);
        } else {
            throw new IOException(
                String.format("invalid address for config %s", ServerConstants.RANGER_OBS_SERVICE_RPC_ADDRESS));
        }
    }

    public void start() throws IOException {
        this.startSecretManager();
        this.startRPCServer();
        this.startSTSProvider();
        this.startAuthorizer();
        running = true;
    }

    private void startRPCServer() throws IOException {
        RPC.setProtocolEngine(this.conf, RangerObsServiceProtocolPB.class, ProtobufRpcEngine.class);
        RangerObsServiceProtocolServerSideTranslatorPB serverSideTranslatorPB
            = new RangerObsServiceProtocolServerSideTranslatorPB(this);
        BlockingService rangerObsService
            = RangerObsServiceProtocolProtos.RangerObsServiceProtocol.newReflectiveBlockingService(
            serverSideTranslatorPB);
        InetSocketAddress socAddr = this.getRpcServerAddress();
        int handlerCount = this.conf.getInt(ServerConstants.RANGER_OBS_SERVICE_RPC_HANDLER_COUNT,
            ServerConstants.DEFAULT_RANGER_OBS_SERVICE_RPC_HANDLER_COUNT);

        this.serviceRpcServer = new Builder(this.conf).setProtocol(RangerObsServiceProtocolPB.class)
            .setInstance(rangerObsService)
            .setBindAddress(socAddr.getHostName())
            .setPort(socAddr.getPort())
            .setNumHandlers(handlerCount)
            .setVerbose(false)
            .setSecretManager(dtSecretManager)
            .build();

        if (this.conf.getBoolean("hadoop.security.authorization", false)) {
            this.serviceRpcServer.refreshServiceAcl(this.conf, new RangerObsServicePolicyProvider());
        }

        this.serviceRpcServer.start();
        LOG.info("RPCService listen on {}", socAddr);
    }

    public void join() throws InterruptedException {
        this.serviceRpcServer.join();
    }

    public void stop() {
        this.stopSTSProvider();
        this.stopAuthorizer();
        this.stopSecretManager();
        this.stopRPCServer();
        running = false;
    }

    public boolean isRunning() {
        return running;
    }

    public boolean isSecurity() {
        return isSecurity;
    }

    public boolean isSTSenable() {
        return isSTSEnable;
    }

    public boolean isAuthorizeEnable() {
        return isAuthorizeEnable;
    }

    private void stopRPCServer() {
        if (this.serviceRpcServer != null) {
            synchronized (this.serviceRpcServer) {
                this.serviceRpcServer.stop();
                this.serviceRpcServer = null;
            }
        }
    }

    private void stopSTSProvider() {
        if (this.stsProvider != null) {
            synchronized (this.stsProvider) {
                this.stsProvider.stop();
            }
        }
    }

    private void stopSecretManager() {
        if (this.dtSecretManager != null) {
            synchronized (this.dtSecretManager) {
                if (this.dtSecretManager.isRunning()) {
                    this.dtSecretManager.stopThreads();
                }
            }
        }
    }

    private void stopAuthorizer() {
        if (this.authorizer != null) {
            synchronized (this.authorizer) {
                this.authorizer.stop();
            }
        }
    }

    /**
     * supply delegation token in security cluster
     *
     * @param renewer
     * @return delegation token
     * @throws IOException
     */
    @Override
    public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer) throws IOException {
        if (this.dtSecretManager != null && this.dtSecretManager.isRunning()) {

            UserGroupInformation ugi = getRemoteUser();
            String user = ugi.getUserName();
            Text owner = new Text(user);
            Text realUser = null;
            if (ugi.getRealUser() != null) {
                realUser = new Text(ugi.getRealUser().getUserName());
            }

            DelegationTokenIdentifier dtId = new DelegationTokenIdentifier(owner, renewer, realUser);
            Token<DelegationTokenIdentifier> token = new Token(dtId, this.dtSecretManager);
            token.setService(new Text(this.serviceName));
            LOG.info("getDelegationToken: [ugi: {}], [renewer: {}]", ugi, renewer);
            return token;
        } else {
            LOG.warn("trying to get DT with no secret manager running");
            throw new IOException("secret manager is not running");
        }
    }

    private String formatDate(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdf.format(date);
    }

    /**
     * renewDelegationToken
     *
     * @param token DelegationToken
     * @return renew time,exceed this time must renew again
     * @throws IOException
     */
    @Override
    public long renewDelegationToken(Token<DelegationTokenIdentifier> token) throws IOException {
        if (this.dtSecretManager != null && this.dtSecretManager.isRunning()) {
            String renewer = getRemoteUser().getShortUserName();
            long expiryTime = this.dtSecretManager.renewToken(token, renewer);
            DelegationTokenIdentifier dtId = DelegationTokenIdentifier.decodeDelegationToken(token);
            LOG.info("renewDelegationToken: [renewer: {}], [DelegationTokenIdentifier: {}], [expiryTime: {}]",
                renewer, dtId.toString(), this.formatDate(new Date(expiryTime)));
            return expiryTime;
        } else {
            LOG.warn("trying to renew DT with no secret manager running");
            throw new IOException("secret manager is not running");
        }
    }

    /**
     * cancel DelegationToken
     *
     * @param token DelegationToken
     * @throws IOException
     */
    @Override
    public void cancelDelegationToken(Token<DelegationTokenIdentifier> token) throws IOException {
        if (this.dtSecretManager != null && this.dtSecretManager.isRunning()) {
            String canceller = getRemoteUser().getUserName();
            DelegationTokenIdentifier dtId = (DelegationTokenIdentifier) this.dtSecretManager.cancelToken(token,
                canceller);
            LOG.info("cancelDelegationToken, [canceller: {}], [DelegationTokenIdentifier: {}]", canceller,
                dtId.toString());
        } else {
            LOG.warn("trying to cancel DT with no secret manager running");
            throw new IOException("secret manager is not running");
        }
    }

    /**
     * check obs request permission
     *
     * @param permissionReq include bucket,key,action
     * @return true can going obs request
     * @throws IOException
     */
    @Override
    public boolean checkPermission(PermissionRequest permissionReq) throws IOException {
        if (!this.isAuthorizeEnable) {
            return true;
        }
        long st = System.currentTimeMillis();
        UserGroupInformation ugi = getRemoteUser();
        if (permissionReq.getBucketName() == null || permissionReq.getBucketName().isEmpty()) {
            throw new InvalidRequestException("PermissionRequest bucket param invalid");
        }
        if (permissionReq.getObjectKey() == null) {
            throw new InvalidRequestException("PermissionRequest object param invalid");
        }

        boolean allow = this.authorizer.checkPermission(permissionReq, ugi);
        long et = System.currentTimeMillis();
        LOG.debug("checkPermission: [ugi: {}], [bucket: {}], [object: {}], [action: {}], [costTime: {}], [result: {}]",
            ugi, permissionReq.getBucketName(), permissionReq.getObjectKey(), permissionReq.getAccessType(), et - st,
            allow);
        return allow;
    }

    /**
     * supply access obs temporary aksk
     *
     * @param request
     * @return temporary aksk
     * @throws IOException
     */
    @Override
    public GetSTSResponse getSTS(GetSTSRequest request) throws IOException {
        if (!this.isSTSEnable) {
            throw new UnsupportedOperationException("not support sts service");
        }
        UserGroupInformation ugi = getRemoteUser();
        return this.stsProvider.getSTS(request,ugi);
    }

}