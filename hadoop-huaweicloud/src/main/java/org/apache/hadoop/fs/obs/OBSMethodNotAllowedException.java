package org.apache.hadoop.fs.obs;

import java.io.IOException;

public class OBSMethodNotAllowedException extends IOException implements WithErrCode {

    private static final long serialVersionUID = 2461327923217975442L;
    private String errCode;

    OBSMethodNotAllowedException(final String message) {
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
