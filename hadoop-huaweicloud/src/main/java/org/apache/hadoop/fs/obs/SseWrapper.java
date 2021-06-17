package org.apache.hadoop.fs.obs;

import com.obs.services.model.SseCHeader;
import com.obs.services.model.SseKmsHeader;

import org.apache.hadoop.conf.Configuration;

import static org.apache.hadoop.fs.obs.OBSConstants.SSE_KEY;
import static org.apache.hadoop.fs.obs.OBSConstants.SSE_TYPE;

/**
 * Wrapper for Server-Side Encryption (SSE).
 */
class SseWrapper {
    /**
     * SSE-KMS: Server-Side Encryption with Key Management Service.
     */
    private static final String SSE_KMS = "sse-kms";

    /**
     * SSE-C: Server-Side Encryption with Customer-Provided Encryption Keys.
     */
    private static final String SSE_C = "sse-c";

    /**
     * SSE-C header.
     */
    private SseCHeader sseCHeader;

    /**
     * SSE-KMS header.
     */
    private SseKmsHeader sseKmsHeader;

    SseWrapper(final Configuration conf) {
        String sseType = conf.getTrimmed(SSE_TYPE);
        if (null != sseType) {
            String sseKey = conf.getTrimmed(SSE_KEY);
            if (sseType.equalsIgnoreCase(SSE_C) && null != sseKey) {
                sseCHeader = new SseCHeader();
                sseCHeader.setSseCKeyBase64(sseKey);
            } else if (sseType.equalsIgnoreCase(SSE_KMS)) {
                sseKmsHeader = new SseKmsHeader();
                sseKmsHeader.setKmsKeyId(sseKey);
            }
        }
    }

    boolean isSseCEnable() {
        return sseCHeader != null;
    }

    boolean isSseKmsEnable() {
        return sseKmsHeader != null;
    }

    SseCHeader getSseCHeader() {
        return sseCHeader;
    }

    SseKmsHeader getSseKmsHeader() {
        return sseKmsHeader;
    }
}
