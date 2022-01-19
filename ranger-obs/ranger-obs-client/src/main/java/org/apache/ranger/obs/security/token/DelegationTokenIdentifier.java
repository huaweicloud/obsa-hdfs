/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package org.apache.ranger.obs.security.token;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

public class DelegationTokenIdentifier extends AbstractDelegationTokenIdentifier {
    public static final Text RANGER_OBS_SERVICE_DELEGATION_TOKEN = new Text("RANGER_OBS_SERVICE_DELEGATION_TOKEN");

    public DelegationTokenIdentifier() {
    }

    public DelegationTokenIdentifier(Text owner, Text renewer, Text realUser) {
        super(owner, renewer, realUser);
    }

    public static DelegationTokenIdentifier decodeDelegationToken(Token<DelegationTokenIdentifier> token)
        throws IOException {
        DelegationTokenIdentifier id = new DelegationTokenIdentifier();
        ByteArrayInputStream buf = new ByteArrayInputStream(token.getIdentifier());
        DataInputStream in = new DataInputStream(buf);
        Throwable var4 = null;

        try {
            id.readFields(in);
        } catch (Throwable var13) {
            var4 = var13;
            throw var13;
        } finally {
            if (in != null) {
                if (var4 != null) {
                    try {
                        in.close();
                    } catch (Throwable var12) {
                        var4.addSuppressed(var12);
                    }
                } else {
                    in.close();
                }
            }

        }

        return id;
    }

    @Override
    public Text getKind() {
        return RANGER_OBS_SERVICE_DELEGATION_TOKEN;
    }
}