/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package org.apache.ranger.obs.security.sts;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

/**
 * sts Provider interface
 *
 * @since 2021-08-24
 */
public interface STSProvider {
    void init(Configuration configuration) throws Exception;

    void start() throws IOException;

    void stop();

    GetSTSResponse getSTS(GetSTSRequest request, UserGroupInformation ugi) throws IOException;
}
