/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package org.apache.hadoop.fs.obs.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.Token;
import org.apache.ranger.obs.client.ClientConstants;
import org.apache.ranger.obs.client.RangerObsClient;
import org.apache.ranger.obs.security.authorization.PermissionRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * implement AuthorizeProvider
 *
 * @since 2021-08-16
 */
public class RangerAuthorizeProvider implements AuthorizeProvider, DelegationTokenCapability {

    public static final Logger LOG = LoggerFactory.getLogger(RangerAuthorizeProvider.class);

    private RangerObsClient rangerObsClient;

    @Override
    public synchronized void init(Configuration conf) throws IOException {
        if (rangerObsClient == null) {
            try {
                Class<?> clientClassName = conf.getClass(ClientConstants.RANGER_OBS_CLIENT_IMPL,
                    ClientConstants.DEFAULT_RANGER_OBS_CLIENT_IMPL);
                rangerObsClient = (RangerObsClient) clientClassName.newInstance();
                rangerObsClient.init(conf);
            } catch (IOException | InstantiationException | IllegalAccessException e) {
                throw new IOException("getRangerClient error", e);
            }
        }
    }

    @Override
    public boolean isAuthorized(String bucket, String key, AccessType action) throws IOException {
        org.apache.ranger.obs.security.authorization.AccessType accessType
            = org.apache.ranger.obs.security.authorization.AccessType.valueOf(action.name());
        PermissionRequest permissionReq = new PermissionRequest(accessType, bucket, key);
        return rangerObsClient.checkPermission(permissionReq);
    }

    @Override
    public Token<?> getDelegationToken(String renewer) throws IOException {
        return rangerObsClient.getDelegationToken(renewer);
    }

    @Override
    public String getCanonicalServiceName() {
        return rangerObsClient.getCanonicalServiceName();
    }
}
