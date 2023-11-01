package org.apache.hadoop.fs.obs;

import java.io.IOException;

/**
 * description
 *
 * @since 2022-02-07
 */
public class OBSIllegalArgumentException extends IOException implements WithErrCode {

    private static final long serialVersionUID = 2188013092663783231L;
    private String errCode;

    OBSIllegalArgumentException(final String message) {
        super(message);
    }

    public void setErrCode(String errCode) {
        this.errCode = errCode;
    }

    @Override
    public String getErrCode() {
        return this.errCode;
    }
}
