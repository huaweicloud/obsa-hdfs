/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package org.apache.ranger.obs.security.policy;

import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.Service;
import org.apache.ranger.obs.protocol.RangerObsServiceProtocol;

public class RangerObsServicePolicyProvider extends PolicyProvider {
    private final Service[] rangerObsServicePolicyProvider = new Service[] {
        new Service("security.ranger.obs.service.acl", RangerObsServiceProtocol.class)
    };

    @Override
    public Service[] getServices() {
        return rangerObsServicePolicyProvider.clone();
    }
}
