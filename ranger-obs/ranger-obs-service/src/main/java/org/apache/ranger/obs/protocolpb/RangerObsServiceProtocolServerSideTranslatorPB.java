/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package org.apache.ranger.obs.protocolpb;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.proto.SecurityProtos.CancelDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.CancelDelegationTokenResponseProto;
import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenResponseProto;
import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenResponseProto.Builder;
import org.apache.hadoop.security.proto.SecurityProtos.RenewDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.RenewDelegationTokenResponseProto;
import org.apache.hadoop.security.token.Token;
import org.apache.ranger.obs.protocol.RangerObsServiceProtocol;
import org.apache.ranger.obs.protocol.proto.RangerObsServiceProtocolProtos.GetSTSRequestProto;
import org.apache.ranger.obs.protocol.proto.RangerObsServiceProtocolProtos.GetSTSResponseProto;
import org.apache.ranger.obs.protocol.proto.RangerObsServiceProtocolProtos.PermissionRequestProto;
import org.apache.ranger.obs.protocol.proto.RangerObsServiceProtocolProtos.PermissionResponseProto;
import org.apache.ranger.obs.security.sts.GetSTSResponse;
import org.apache.ranger.obs.security.token.DelegationTokenIdentifier;

import java.io.IOException;

public class RangerObsServiceProtocolServerSideTranslatorPB implements RangerObsServiceProtocolPB {
    private static final CancelDelegationTokenResponseProto VOID_CANCELDELEGATIONTOKEN_RESPONSE
        = CancelDelegationTokenResponseProto.newBuilder().build();

    private final RangerObsServiceProtocol server;

    public RangerObsServiceProtocolServerSideTranslatorPB(RangerObsServiceProtocol server) {
        this.server = server;
    }

    @Override
    public GetDelegationTokenResponseProto getDelegationToken(RpcController controller,
        GetDelegationTokenRequestProto request) throws ServiceException {
        try {
            Token<DelegationTokenIdentifier> token = this.server.getDelegationToken(new Text(request.getRenewer()));
            Builder rspBuilder = GetDelegationTokenResponseProto.newBuilder();
            if (token != null) {
                rspBuilder.setToken(PBHelperClient.convert(token));
            }

            return rspBuilder.build();
        } catch (IOException var5) {
            throw new ServiceException(var5);
        }
    }

    @Override
    public RenewDelegationTokenResponseProto renewDelegationToken(RpcController controller,
        RenewDelegationTokenRequestProto request) throws ServiceException {
        try {
            long result = this.server.renewDelegationToken(PBHelperClient.convertDelegationToken(request.getToken()));
            return RenewDelegationTokenResponseProto.newBuilder().setNewExpiryTime(result).build();
        } catch (IOException var5) {
            throw new ServiceException(var5);
        }
    }

    @Override
    public CancelDelegationTokenResponseProto cancelDelegationToken(RpcController controller,
        CancelDelegationTokenRequestProto request) throws ServiceException {
        try {
            this.server.cancelDelegationToken(PBHelperClient.convertDelegationToken(request.getToken()));
            return VOID_CANCELDELEGATIONTOKEN_RESPONSE;
        } catch (IOException var4) {
            throw new ServiceException(var4);
        }
    }

    @Override
    public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
        return RPC.getProtocolVersion(RangerObsServiceProtocolPB.class);
    }

    @Override
    public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash)
        throws IOException {
        if (!protocol.equals(RPC.getProtocolName(RangerObsServiceProtocolPB.class))) {
            throw new IOException("ServerSide implements " + RPC.getProtocolName(RangerObsServiceProtocolPB.class)
                + ". The following requested protocol is unknown: " + protocol);
        } else {
            return ProtocolSignature.getProtocolSignature(clientMethodsHash,
                RPC.getProtocolVersion(RangerObsServiceProtocolPB.class), RangerObsServiceProtocolPB.class);
        }
    }

    @Override
    public PermissionResponseProto checkPermission(RpcController controller, PermissionRequestProto request)
        throws ServiceException {
        try {
            boolean isAllowed = this.server.checkPermission(PBHelperClient.convertPermissionRequest(request));
            return PermissionResponseProto.newBuilder().setAllowed(isAllowed).build();
        } catch (IOException var4) {
            throw new ServiceException(var4);
        }
    }

    @Override
    public GetSTSResponseProto getSTS(RpcController controller, GetSTSRequestProto request) throws ServiceException {
        try {
            GetSTSResponse stsResponse = this.server.getSTS(PBHelperClient.convertGetSTSRequest(request));
            return GetSTSResponseProto.newBuilder()
                .setAk(stsResponse.getAk())
                .setSk(stsResponse.getSk())
                .setToken(stsResponse.getToken())
                .setExpiryTime(stsResponse.getExpiryTime())
                .build();
        } catch (IOException var4) {
            throw new ServiceException(var4);
        }
    }
}
