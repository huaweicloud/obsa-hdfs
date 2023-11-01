package org.apache.hadoop.fs.obs;

import java.io.IOException;

/**
 * OBS file conflict exception.
 */
class OBSFileConflictException extends IOException implements WithErrCode {
    private static final long serialVersionUID = -897856973823710492L;
    private String errCode;

    /**
     * Constructs a <code>FileConflictException</code> with the specified detail
     * message. The string <code>s</code> can be retrieved later by the
     * <code>{@link java.lang.Throwable#getMessage}</code>
     * method of class <code>java.lang.Throwable</code>.
     *
     * @param message the detail message.
     */
    OBSFileConflictException(final String message) {
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
