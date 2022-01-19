/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package org.apache.ranger.obs.security.token;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.HadoopKerberosName;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.apache.hadoop.util.Time;
import org.apache.ranger.obs.server.ServerConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.MessageDigest;

/**
 * implement hadoop DelegationToken Manager,don't store DelegationToken
 */
public class SimpleSecretManager extends AbstractDelegationTokenSecretManager<DelegationTokenIdentifier> {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleSecretManager.class);

    private long tokenMaxLifetime;

    private long tokenRenewInterval;

    private byte[] secret;

    public SimpleSecretManager(Configuration conf) throws IOException {
        super(conf.getLong("ranger.obs.service.dt.update-interval", 86400000L),
            conf.getLong(ServerConstants.RANGER_OBS_SERVICE_DT_MAX_LIFETIME,
                ServerConstants.DEFAULT_RANGER_OBS_SERVICE_DT_MAX_LIFETIME),
            conf.getLong(ServerConstants.RANGER_OBS_SERVICE_DT_RENEW_INTERVAL,
                ServerConstants.DEFAULT_RANGER_OBS_SERVICE_DT_RENEW_INTERVAL),
            conf.getLong("ranger.obs.service.dt.remove-scan-interval", 3600000L));

        this.tokenMaxLifetime = conf.getLong(ServerConstants.RANGER_OBS_SERVICE_DT_MAX_LIFETIME,
            ServerConstants.DEFAULT_RANGER_OBS_SERVICE_DT_MAX_LIFETIME);
        this.tokenRenewInterval = conf.getLong(ServerConstants.RANGER_OBS_SERVICE_DT_RENEW_INTERVAL,
            ServerConstants.DEFAULT_RANGER_OBS_SERVICE_DT_RENEW_INTERVAL);
        initSecret(conf);
    }

    private void initSecret(Configuration conf) throws IOException {
        Class<?> className = conf.getClass(ServerConstants.RANGER_OBS_SERVICE_DT_SECRET_PROVIDER,
            ServerConstants.DEFAULT_RANGER_OBS_SERVICE_DT_SECRET_PROVIDER);
        if (className != null) {
            try {
                SecretProvider secretProvider = (SecretProvider) className.newInstance();
                secret = secretProvider.getSecret(conf);
            } catch (InstantiationException | IllegalAccessException | IOException e) {
                throw new IOException("SimpleSecretManager initSecret failed", e);
            }
        }
    }

    @Override
    protected byte[] createPassword(DelegationTokenIdentifier tokenIdentifier) {
        long now = Time.now();
        tokenIdentifier.setIssueDate(now);
        tokenIdentifier.setMaxDate(now + this.tokenMaxLifetime);
        tokenIdentifier.setMasterKeyId(10);
        tokenIdentifier.setSequenceNumber(10);

        return getPassword(tokenIdentifier);
    }

    private byte[] getPassword(DelegationTokenIdentifier tokenIdentifier) {

        byte[] password = createPassword(tokenIdentifier.getBytes(),
            createSecretKey(secret));
        return password;
    }

    @Override
    public synchronized DelegationTokenIdentifier cancelToken(Token<DelegationTokenIdentifier> token, String canceller)
        throws IOException {
        DelegationTokenIdentifier delegationTokenIdentifier = DelegationTokenIdentifier.decodeDelegationToken(token);
        if (delegationTokenIdentifier.getUser() == null) {
            throw new InvalidToken("Token with no owner " + delegationTokenIdentifier);
        } else {
            String owner = delegationTokenIdentifier.getUser().getUserName();
            Text renewer = delegationTokenIdentifier.getRenewer();
            HadoopKerberosName cancelerKrbName = new HadoopKerberosName(canceller);
            String cancelerShortName = cancelerKrbName.getShortName();
            if (canceller.equals(owner) || renewer != null && !renewer.toString().isEmpty() && cancelerShortName.equals(
                renewer.toString())) {
                return delegationTokenIdentifier;
            } else {
                throw new AccessControlException(
                    canceller + " is not authorized to cancel the token " + delegationTokenIdentifier);
            }
        }
    }

    @Override
    public synchronized long renewToken(Token<DelegationTokenIdentifier> token, String renewer)
        throws IOException {

        DelegationTokenIdentifier id = DelegationTokenIdentifier.decodeDelegationToken(token);

        long now = Time.now();
        if (id.getMaxDate() < now) {
            throw new InvalidToken(
                renewer + " tried to renew an expired token (" + id + ") max expiration date: " + Time.formatTime(
                    id.getMaxDate()) + " currentTime: " + Time.formatTime(now));
        } else if (id.getRenewer() != null && !id.getRenewer().toString().isEmpty()) {
            if (!id.getRenewer().toString().equals(renewer)) {
                throw new AccessControlException(
                    renewer + " tries to renew a token (" + id + ") with non-matching renewer " + id.getRenewer());
            } else {
                byte[] password = getPassword(id);
                if (!MessageDigest.isEqual(password, token.getPassword())) {
                    throw new AccessControlException(
                        renewer + " is trying to renew a token (" + id + ") with wrong password");
                } else {
                    long renewTime = Math.min(id.getMaxDate(), now + this.tokenRenewInterval);
                    return renewTime;
                }
            }
        } else {
            throw new AccessControlException(renewer + " tried to renew a token (" + id + ") without a renewer");
        }
    }

    @Override
    public byte[] retrievePassword(DelegationTokenIdentifier tokenIdentifier) {
        long st = System.currentTimeMillis();
        byte[] password = getPassword(tokenIdentifier);
        long et = System.currentTimeMillis();
        LOG.debug("retrievePassword: costTime:" + (et - st));
        return password;
    }

    @Override
    public DelegationTokenIdentifier createIdentifier() {
        return new DelegationTokenIdentifier();
    }

    @Override
    public void startThreads() {
        synchronized (this) {
            this.running = true;
        }
    }

    @Override
    public void stopThreads() {
        this.running = false;
    }
}
