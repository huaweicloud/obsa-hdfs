package org.apache.hadoop.fs.obs;

import java.io.IOException;

/**
 * OBS file conflict exception.
 */
class FileConflictException extends IOException {
    private static final long serialVersionUID = -897856973823710492L;

    /**
     * Constructs a <code>FileConflictException</code> with the specified detail
     * message. The string <code>s</code> can be retrieved later by the
     * <code>{@link java.lang.Throwable#getMessage}</code>
     * method of class <code>java.lang.Throwable</code>.
     *
     * @param s the detail message.
     */
    FileConflictException(final String s) {
        super(s);
    }
}
