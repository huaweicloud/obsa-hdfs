package org.apache.hadoop.fs.obs.security;

import org.apache.hadoop.security.token.Token;

import java.io.IOException;

/**
 * 功能描述
 *
 * @since 2021-09-15
 */
public interface DelegationTokenCapability {
    String getCanonicalServiceName();

    Token<?> getDelegationToken(String renewer) throws IOException;
}
