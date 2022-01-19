/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package org.apache.ranger.obs.security.token;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.delegation.ZKDelegationTokenSecretManager;

/**
 * Through zookeeper implement hadoop DelegationToken Manager,zookeeper store DelegationToken
 */
@Deprecated
public class ZKSecretManager extends ZKDelegationTokenSecretManager<DelegationTokenIdentifier> {
    public ZKSecretManager(Configuration conf) {
        super(conf);
    }

    @Override
    public DelegationTokenIdentifier createIdentifier() {
        return new DelegationTokenIdentifier();
    }
}
