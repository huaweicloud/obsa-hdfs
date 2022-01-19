/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package org.apache.ranger.obs.protocolpb;

import org.apache.hadoop.ipc.ProtocolInfo;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.token.TokenInfo;
import org.apache.ranger.obs.protocol.proto.RangerObsServiceProtocolProtos;
import org.apache.ranger.obs.security.token.DelegationTokenSelector;

@KerberosInfo(
    serverPrincipal = "ranger.obs.service.kerberos.principal"
)
@TokenInfo(DelegationTokenSelector.class)
@ProtocolInfo(
    protocolName = "org.apache.ranger.obs.protocol.RangerObsServiceProtocol",
    protocolVersion = 1L
)
public interface RangerObsServiceProtocolPB extends
    RangerObsServiceProtocolProtos.RangerObsServiceProtocol.BlockingInterface, VersionedProtocol {
}
