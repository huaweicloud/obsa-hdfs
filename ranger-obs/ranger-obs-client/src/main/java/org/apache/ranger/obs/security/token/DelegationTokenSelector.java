/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package org.apache.ranger.obs.security.token;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSelector;

import java.util.Collection;
import java.util.Iterator;

public class DelegationTokenSelector extends AbstractDelegationTokenSelector<DelegationTokenIdentifier> {
    public DelegationTokenSelector() {
        super(DelegationTokenIdentifier.RANGER_OBS_SERVICE_DELEGATION_TOKEN);
    }

    @Override
    public Token<DelegationTokenIdentifier> selectToken(
        Text service, Collection<Token<? extends TokenIdentifier>> tokens) {
        if (service == null) {
            return null;
        } else {
            Iterator tokenIter = tokens.iterator();

            while (tokenIter.hasNext()) {
                Token token = (Token) tokenIter.next();
                if (DelegationTokenIdentifier.RANGER_OBS_SERVICE_DELEGATION_TOKEN.equals(token.getKind())) {
                    return token;
                }
            }
            return null;
        }
    }
}
