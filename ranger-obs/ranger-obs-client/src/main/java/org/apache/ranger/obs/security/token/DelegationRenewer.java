/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package org.apache.ranger.obs.security.token;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenRenewer;
import org.apache.ranger.obs.client.ClientConstants;
import org.apache.ranger.obs.client.RangerObsClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DelegationRenewer extends TokenRenewer {
    private static final Logger LOG = LoggerFactory.getLogger(DelegationRenewer.class);

    RangerObsClient rangerObsClient;

    @Override
    public boolean handleKind(Text text) {
        return DelegationTokenIdentifier.RANGER_OBS_SERVICE_DELEGATION_TOKEN.equals(text);
    }

    @Override
    public boolean isManaged(Token<?> token) {
        return true;
    }

    @Override
    public long renew(Token<?> token, Configuration configuration) throws IOException {
        return getRangerClient(configuration).renewDelegationToken(token, configuration);
    }

    @Override
    public void cancel(Token<?> token, Configuration configuration) throws IOException {
        getRangerClient(configuration).cancelDelegationToken(token, configuration);
    }

    private synchronized RangerObsClient getRangerClient(Configuration conf) throws IOException {
        if (rangerObsClient != null) {
            return rangerObsClient;
        }
        try {
            Class<?> clientClassName = conf.getClass(ClientConstants.RANGER_OBS_CLIENT_IMPL,
                ClientConstants.DEFAULT_RANGER_OBS_CLIENT_IMPL);
            LOG.debug("using {} to renew/cancel DelegationToken", clientClassName.getName());
            rangerObsClient = (RangerObsClient) clientClassName.newInstance();
            rangerObsClient.init(conf);
            return rangerObsClient;
        } catch (IOException | InstantiationException | IllegalAccessException e) {
            throw new IOException("getRangerClient error", e);
        }
    }
}