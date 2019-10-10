package org.apache.hadoop.fs.obs;

import com.obs.services.model.ServerAlgorithm;
import com.obs.services.model.ServerEncryption;
import com.obs.services.model.SseCHeader;
import com.obs.services.model.SseKmsHeader;
import org.apache.hadoop.conf.Configuration;

import static org.apache.hadoop.fs.obs.Constants.SSE_KEY;
import static org.apache.hadoop.fs.obs.Constants.SSE_TYPE;

class SseWrapper {

  private static final String SSE_KMS = "sse-kms";
  private static final String SSE_C = "sse-c";

  private SseCHeader sseCHeader;
  private SseKmsHeader sseKmsHeader;

  SseWrapper(Configuration conf) {
    String sseType = conf.getTrimmed(SSE_TYPE);
    if (null != sseType) {
      String sseKey = conf.getTrimmed(SSE_KEY);
      if (sseType.equalsIgnoreCase(SSE_C) && null != sseKey) {
        sseCHeader = new SseCHeader();
        sseCHeader.setSseCKeyBase64(sseKey);
        sseCHeader.setAlgorithm(ServerAlgorithm.AES256);
      } else if (sseType.equalsIgnoreCase(SSE_KMS)) {
        sseKmsHeader = new SseKmsHeader();
        sseKmsHeader.setEncryption(ServerEncryption.OBS_KMS);
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
