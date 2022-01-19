/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package org.apache.ranger.obs.authorize;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.obs.security.authorization.PermissionRequest;

/**
 * description
 *
 * @since 2021-10-09
 */
public interface Authorizer {
    void init(Configuration conf);

    void start();

    void stop();

    boolean checkPermission(PermissionRequest permissionReq, UserGroupInformation ugi);
}
