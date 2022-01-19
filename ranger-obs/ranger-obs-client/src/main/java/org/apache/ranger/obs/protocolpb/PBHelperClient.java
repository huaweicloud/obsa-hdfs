/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package org.apache.ranger.obs.protocolpb;

import com.google.protobuf.ByteString;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.security.proto.SecurityProtos.TokenProto.Builder;
import org.apache.hadoop.security.token.Token;
import org.apache.ranger.obs.protocol.proto.RangerObsServiceProtocolProtos.GetSTSRequestProto;
import org.apache.ranger.obs.protocol.proto.RangerObsServiceProtocolProtos.PermissionRequestProto;
import org.apache.ranger.obs.security.authorization.AccessType;
import org.apache.ranger.obs.security.authorization.PermissionRequest;
import org.apache.ranger.obs.security.sts.GetSTSRequest;
import org.apache.ranger.obs.security.token.DelegationTokenIdentifier;

import java.util.concurrent.ConcurrentHashMap;

public class PBHelperClient {
    private static final ConcurrentHashMap<Object, ByteString> FIXED_BYTESTRING_CACHE = new ConcurrentHashMap();

    public static Token<DelegationTokenIdentifier> convertDelegationToken(TokenProto blockToken) {
        return new Token(blockToken.getIdentifier().toByteArray(), blockToken.getPassword().toByteArray(),
            new Text(blockToken.getKind()), new Text(blockToken.getService()));
    }

    public static TokenProto convert(Token<?> tok) {
        Builder builder = TokenProto.newBuilder()
            .setIdentifier(getByteString(tok.getIdentifier()))
            .setPassword(getByteString(tok.getPassword()))
            .setKindBytes(getFixedByteString(tok.getKind()))
            .setServiceBytes(getFixedByteString(tok.getService()));
        return builder.build();
    }

    public static ByteString getByteString(byte[] bytes) {
        return bytes.length == 0 ? ByteString.EMPTY : ByteString.copyFrom(bytes);
    }

    public static ByteString getFixedByteString(Text key) {
        ByteString value = (ByteString) FIXED_BYTESTRING_CACHE.get(key);
        if (value == null) {
            value = ByteString.copyFromUtf8(key.toString());
            FIXED_BYTESTRING_CACHE.putIfAbsent(new Text(key.copyBytes()), value);
        }

        return value;
    }

    public static PermissionRequest convertPermissionRequest(
        PermissionRequestProto permissionReqProto) {
        return new PermissionRequest(AccessType.valueOf(permissionReqProto.getAccessType().name()),
            permissionReqProto.getBucketName(), permissionReqProto.getObjectKey());
    }

    public static GetSTSRequest convertGetSTSRequest(
        GetSTSRequestProto getSTSRequestProto) {
        GetSTSRequest req = new GetSTSRequest();
        req.setBucketName(getSTSRequestProto.getBucketName());
        req.setAllowPrefix(getSTSRequestProto.getAllowPrefix());
        req.setRegion(getSTSRequestProto.getRegion());
        return req;
    }
}

