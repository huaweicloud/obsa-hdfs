/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package org.apache.ranger.obs.server;

import org.apache.ranger.obs.authorize.RangerAuthorizer;
import org.apache.ranger.obs.security.sts.MrsSTSProvider;
import org.apache.ranger.obs.security.token.ShareFileSecretProvider;
import org.apache.ranger.obs.security.token.SimpleSecretManager;

/**
 * server configuration
 *
 * @since 2021-09-22
 */
public class ServerConstants {

    public static final String RANGER_OBS_SERVICE_RPC_ADDRESS = "ranger.obs.service.rpc.address";

    public static final String DEFAULT_RANGER_OBS_SERVICE_RPC_ADDRESS = "0.0.0.0:26901";

    public static final String RANGER_OBS_SERVICE_RPC_HANDLER_COUNT = "ranger.obs.service.rpc.handler.count";

    public static final int DEFAULT_RANGER_OBS_SERVICE_RPC_HANDLER_COUNT = 10;

    public static final String RANGER_OBS_SERVICE_STATUS_PORT = "ranger.obs.service.status.port";

    public static final int DEFAULT_RANGER_OBS_SERVICE_STATUS_PORT = 26900;

    public static final String RANGER_OBS_SERVICE_AUTHORIZE_ENABLE = "ranger.obs.service.authorize.enable";

    public static final Boolean DEFAULT_RANGER_OBS_SERVICE_AUTHORIZE_ENABLE = true;

    public static final String RANGER_OBS_SERVICE_STS_ENABLE = "ranger.obs.service.sts.enable";

    public static final Boolean DEFAULT_RANGER_OBS_SERVICE_STS_ENABLE = false;

    public static final String RANGER_OBS_SERVICE_STS_PROVIDER = "ranger.obs.service.sts.provider";

    public static final Class<MrsSTSProvider>  DEFAULT_RANGER_OBS_SERVICE_STS_PROVIDER = MrsSTSProvider.class;

    public static final String RANGER_OBS_SERVICE_STS_SOURCE = "ranger.obs.service.sts.source";

    public static final String DEFAULT_RANGER_OBS_SERVICE_STS_SOURCE = "ecs";

    public static final String RANGER_OBS_SERVICE_STS_TOKEN_URL = "ranger.obs.service.sts.token.url";

    public static final String DEFAULT_RANGER_OBS_SERVICE_STS_TOKEN_URL
        = "https://iam.myhuaweicloud.com/v3/auth/tokens?nocatalog=true";

    public static final String RANGER_OBS_SERVICE_STS_DOMAIN_NAME = "ranger.obs.service.sts.domain.name";

    public static final String RANGER_OBS_SERVICE_STS_USER_NAME = "ranger.obs.service.sts.user.name";

    public static final String RANGER_OBS_SERVICE_STS_USER_PASSWORD = "ranger.obs.service.sts.user.password";

    public static final String RANGER_OBS_SERVICE_STS_SECURITYTOKEN_URL = "ranger.obs.service.sts.securitytoken.url";

    public static final String DEFAULT_RANGER_OBS_SERVICE_STS_SECURITYTOKEN_URL
        = "https://iam.myhuaweicloud.com/v3.0/OS-CREDENTIAL/securitytokens";

    public static final String RANGER_OBS_SERVICE_STS_SECURITYTOKEN_DURATION
        = "ranger.obs.service.sts.securitytoken.duration";

    public static final int DEFAULT_RANGER_OBS_SERVICE_STS_SECURITYTOKEN_DURATION = 86400;

    public static final String RANGER_OBS_SERVICE_KERBEROS_PRINCIPAL = "ranger.obs.service.kerberos.principal";

    public static final String RANGER_OBS_SERVICE_KERBEROS_KEYTAB = "ranger.obs.service.kerberos.keytab";

    public static final String RANGER_OBS_SERVICE_DT_SERVICE_NAME = "ranger.obs.service.dt.service.name";

    public static final String DEFAULT_RANGER_OBS_SERVICE_DT_SERVICE_NAME = "0.0.0.0:26901";

    public static final String RANGER_OBS_SERVICE_AUTHORIZER = "ranger.obs.service.authorizer";

    public static final Class<RangerAuthorizer> DEFAULT_RANGER_OBS_SERVICE_AUTHORIZER =
        RangerAuthorizer.class;

    public static final String RANGER_OBS_SERVICE_DT_MANAGER = "ranger.obs.service.dt.manager";

    public static final Class<SimpleSecretManager> DEFAULT_RANGER_OBS_SERVICE_DT_MANAGER = SimpleSecretManager.class;

    public static final String RANGER_OBS_SERVICE_DT_SECRET_PROVIDER = "ranger.obs.service.dt.secret.provider";

    public static final Class<ShareFileSecretProvider> DEFAULT_RANGER_OBS_SERVICE_DT_SECRET_PROVIDER
        = ShareFileSecretProvider.class;

    public static final String RANGER_OBS_SERVICE_DT_SECRET_FILE = "ranger.obs.service.dt.secret.file";

    public static final String RANGER_OBS_SERVICE_DT_RENEW_INTERVAL = "ranger.obs.service.dt.renew-interval";

    public static final int DEFAULT_RANGER_OBS_SERVICE_DT_RENEW_INTERVAL = 86400000;

    public static final String RANGER_OBS_SERVICE_DT_MAX_LIFETIME = "ranger.obs.service.dt.max-lifetime";

    public static final int DEFAULT_RANGER_OBS_SERVICE_DT_MAX_LIFETIME = 604800000;
}
