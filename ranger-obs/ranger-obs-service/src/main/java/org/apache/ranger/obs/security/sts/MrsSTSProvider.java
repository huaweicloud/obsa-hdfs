/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package org.apache.ranger.obs.security.sts;

import com.obs.services.internal.security.LimitedTimeSecurityKey;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * get tmp ak/sk from ecs meta
 */
public class MrsSTSProvider implements STSProvider {

    MrsCredentialsProvider credentialsProvider;

    @Override
    public void init(Configuration configuration) throws Exception {
        credentialsProvider = new MrsCredentialsProvider(null,configuration);
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public GetSTSResponse getSTS(GetSTSRequest request, UserGroupInformation ugi) {
        LimitedTimeSecurityKey sts = (LimitedTimeSecurityKey) credentialsProvider.getSecurityKey(ugi);
        GetSTSResponse getSTSResponse = new GetSTSResponse();
        getSTSResponse.setAk(sts.getAccessKey());
        getSTSResponse.setSk(sts.getSecretKey());
        getSTSResponse.setToken(sts.getSecurityToken());
        getSTSResponse.setExpiryTime(sts.getExpiryDate().getTime());

        return getSTSResponse;
    }
}
