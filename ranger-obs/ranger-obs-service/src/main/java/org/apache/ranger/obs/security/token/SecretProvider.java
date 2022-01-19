/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package org.apache.ranger.obs.security.token;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * SimpleSecretManager use SecretProvider get DelegationToken secret key
 */
public interface SecretProvider {
    byte[] getSecret(Configuration conf) throws IOException;
}
