/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package org.apache.ranger.obs.protocol;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.Idempotent;
import org.apache.hadoop.security.token.Token;
import org.apache.ranger.obs.security.authorization.PermissionRequest;
import org.apache.ranger.obs.security.sts.GetSTSRequest;
import org.apache.ranger.obs.security.sts.GetSTSResponse;
import org.apache.ranger.obs.security.token.DelegationTokenIdentifier;

import java.io.IOException;

/**
 * RangerObsServiceProtocol is used by user code via the RangerObsClient class to
 * communicate with the ranger obs service.
 */
public interface RangerObsServiceProtocol {
    @Idempotent
    Token<DelegationTokenIdentifier> getDelegationToken(Text var1) throws IOException;

    @Idempotent
    long renewDelegationToken(Token<DelegationTokenIdentifier> var1) throws IOException;

    @Idempotent
    void cancelDelegationToken(Token<DelegationTokenIdentifier> var1) throws IOException;

    @Idempotent
    boolean checkPermission(PermissionRequest var1) throws IOException;

    @Deprecated
    @Idempotent
    GetSTSResponse getSTS(GetSTSRequest var1) throws IOException;
}
