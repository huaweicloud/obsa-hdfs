/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package org.apache.ranger.obs.protocolpb;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtocolMetaInterface;
import org.apache.hadoop.ipc.ProtocolTranslator;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.RpcKind;
import org.apache.hadoop.ipc.RpcClientUtil;
import org.apache.hadoop.security.proto.SecurityProtos.CancelDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenResponseProto;
import org.apache.hadoop.security.proto.SecurityProtos.RenewDelegationTokenRequestProto;
import org.apache.hadoop.security.token.Token;
import org.apache.ranger.obs.protocol.RangerObsServiceProtocol;
import org.apache.ranger.obs.protocol.proto.RangerObsServiceProtocolProtos;
import org.apache.ranger.obs.security.authorization.PermissionRequest;
import org.apache.ranger.obs.security.sts.GetSTSRequest;
import org.apache.ranger.obs.security.sts.GetSTSResponse;
import org.apache.ranger.obs.security.token.DelegationTokenIdentifier;

import java.io.Closeable;
import java.io.IOException;

public class RangerObsServiceProtocolTranslatorPB
    implements RangerObsServiceProtocol, ProtocolMetaInterface, Closeable, ProtocolTranslator {
    private final RangerObsServiceProtocolPB rpcProxy;

    public RangerObsServiceProtocolTranslatorPB(RangerObsServiceProtocolPB rpcProxy) {
        this.rpcProxy = rpcProxy;
    }

    @Override
    public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer) throws IOException {
        GetDelegationTokenRequestProto req = GetDelegationTokenRequestProto.newBuilder()
            .setRenewer(renewer == null ? "" : renewer.toString())
            .build();

        try {
            GetDelegationTokenResponseProto resp = this.rpcProxy.getDelegationToken((RpcController) null, req);
            return resp.hasToken() ? PBHelperClient.convertDelegationToken(resp.getToken()) : null;
        } catch (ServiceException var4) {
            throw ProtobufHelper.getRemoteException(var4);
        }
    }

    @Override
    public long renewDelegationToken(Token<DelegationTokenIdentifier> token) throws IOException {
        RenewDelegationTokenRequestProto req = RenewDelegationTokenRequestProto.newBuilder()
            .setToken(PBHelperClient.convert(token))
            .build();

        try {
            return this.rpcProxy.renewDelegationToken((RpcController) null, req).getNewExpiryTime();
        } catch (ServiceException var4) {
            throw ProtobufHelper.getRemoteException(var4);
        }
    }

    @Override
    public void cancelDelegationToken(Token<DelegationTokenIdentifier> token) throws IOException {
        CancelDelegationTokenRequestProto req = CancelDelegationTokenRequestProto.newBuilder()
            .setToken(PBHelperClient.convert(token))
            .build();

        try {
            this.rpcProxy.cancelDelegationToken((RpcController) null, req);
        } catch (ServiceException var4) {
            throw ProtobufHelper.getRemoteException(var4);
        }
    }

    @Override
    public boolean checkPermission(PermissionRequest request) throws IOException {
        RangerObsServiceProtocolProtos.PermissionRequestProto
            req = RangerObsServiceProtocolProtos.PermissionRequestProto.newBuilder()
            .setAccessType(
                RangerObsServiceProtocolProtos.AccessTypeProto.valueOf(request.getAccessType().name()))
            .setBucketName(request.getBucketName())
            .setObjectKey(request.getObjectKey())
            .build();

        try {
            RangerObsServiceProtocolProtos.PermissionResponseProto resp = this.rpcProxy.checkPermission(
                (RpcController) null, req);
            return resp.getAllowed();
        } catch (ServiceException var4) {
            throw ProtobufHelper.getRemoteException(var4);
        }
    }

    @Override
    public GetSTSResponse getSTS(GetSTSRequest request) throws IOException {
        RangerObsServiceProtocolProtos.GetSTSRequestProto protoReq
            = RangerObsServiceProtocolProtos.GetSTSRequestProto.newBuilder()
            .setBucketName(request.getBucketName())
            .setRegion(request.getRegion())
            .setAllowPrefix(request.getAllowPrefix())
            .build();

        try {
            RangerObsServiceProtocolProtos.GetSTSResponseProto protoResp = this.rpcProxy.getSTS((RpcController) null,
                protoReq);
            if (protoResp == null) {
                return null;
            } else {
                GetSTSResponse stsResponse = new GetSTSResponse();
                stsResponse.setAk(protoResp.getAk());
                stsResponse.setSk(protoResp.getSk());
                stsResponse.setToken(protoResp.getToken());
                return stsResponse;
            }
        } catch (ServiceException var5) {
            throw ProtobufHelper.getRemoteException(var5);
        }
    }

    @Override
    public void close() throws IOException {
        RPC.stopProxy(this.rpcProxy);
    }

    @Override
    public boolean isMethodSupported(String methodName) throws IOException {
        return RpcClientUtil.isMethodSupported(this.rpcProxy, RangerObsServiceProtocolPB.class,
            RpcKind.RPC_PROTOCOL_BUFFER, RPC.getProtocolVersion(
                RangerObsServiceProtocolPB.class), methodName);
    }

    @Override
    public Object getUnderlyingProxyObject() {
        return this.rpcProxy;
    }
}
