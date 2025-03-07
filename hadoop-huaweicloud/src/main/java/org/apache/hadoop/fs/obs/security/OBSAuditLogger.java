package org.apache.hadoop.fs.obs.security;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Locale;

public class OBSAuditLogger {
    private static final Logger LOG = LoggerFactory.getLogger(OBSAuditLogger.class);

    public void logAuditEvent(boolean succeeded, long timeSpan,
                              String ugi, String addr, String cmd, String src,
                              String dst, String owner, String group, FsPermission permission, String protocol, String context) {
        LOG.debug(String.format(Locale.ROOT,"succeeded is [%s];timeSpan is [%d];ugi is [%s];addr is [%s];cmd is [%s];src is [%s];" +
                "dst is [%s];owner is [%s];group is [%s];permission is [%s];protocol is [%s];context is [%s];",
                succeeded, timeSpan, ugi, addr, cmd, src, dst, owner, group, permission, protocol, context));
    }

    public void logAuditEvent(boolean succeeded, long timeSpan, String cmd, String src,
                              String dst, FileStatus stat, String context, String ugi, String addr) throws IOException {
        if (stat == null) {
            logAuditEvent(succeeded, timeSpan, ugi, addr,
                    cmd, src, dst, null, null, null, "http", context);
        } else {
            logAuditEvent(succeeded, timeSpan, ugi, addr,
                    cmd, src, dst, stat.getOwner(), stat.getGroup(), stat.getPermission(), "http", context);
        }
    }
}
