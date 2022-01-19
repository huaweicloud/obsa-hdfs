/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package org.apache.ranger.obs.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.Token;
import org.apache.ranger.obs.security.authorization.PermissionRequest;
import org.apache.ranger.obs.security.sts.GetSTSResponse;

import java.io.IOException;

/**
 * client of ranger obs service interface
 */
public interface RangerObsClient {
    void init(Configuration configuration) throws IOException;

    String getCanonicalServiceName();

    boolean checkPermission(PermissionRequest permissionRequest) throws IOException;

    Token<?> getDelegationToken(String renewer) throws IOException;

    long renewDelegationToken(Token<?> token, Configuration configuration) throws IOException;

    Void cancelDelegationToken(Token<?> token, Configuration configuration) throws IOException;

    GetSTSResponse getSTS(String region, String bucketName, String allowPrefix) throws IOException;

    void close();

}