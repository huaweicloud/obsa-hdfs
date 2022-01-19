package org.apache.hadoop.fs.obs.security;

import java.io.IOException;

/**
 * 功能描述
 *
 * @since 2021-09-15
 */
public class OBSAuthorizationException extends IOException {
    private static final long serialVersionUID = 1L;

    public OBSAuthorizationException(String message, Exception e) {
        super(message, e);
    }

    public OBSAuthorizationException(String message) {
        super(message);
    }

    public OBSAuthorizationException(Throwable e) {
        super(e);
    }
}
