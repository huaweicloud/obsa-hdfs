package org.apache.hadoop.fs.obs.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.token.Token;

import java.net.URI;

/**
 * description
 *
 * @since 2022-05-24
 */
public interface DelegationTokenProvider {

    void initialize(FileSystem fs, URI uri, Configuration conf);

    Token<?> getDelegationToken(String renewer);

    String getCanonicalServiceName();
}
