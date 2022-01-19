package org.apache.hadoop.fs.obs.security;

import org.apache.hadoop.conf.Configuration;
import java.io.IOException;

/**
 * 功能描述
 *
 * @since 2021-06-21
 */
public interface AuthorizeProvider {

    void init(Configuration conf) throws IOException;

    boolean isAuthorized(String bucket, String key, AccessType action) throws IOException;

}
