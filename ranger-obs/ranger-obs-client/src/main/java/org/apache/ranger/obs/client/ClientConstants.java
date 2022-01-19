/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package org.apache.ranger.obs.client;

/**
 * client configuration
 *
 * @since 2021-09-22
 */
public class ClientConstants {

    public static final String RANGER_OBS_SERVICE_RPC_ADDRESS = "ranger.obs.service.rpc.address";

    public static final String RANGER_OBS_CLIENT_IMPL = "ranger.obs.client.impl";

    public static final Class<LoadBalanceRangerObsClientImpl> DEFAULT_RANGER_OBS_CLIENT_IMPL
        = LoadBalanceRangerObsClientImpl.class;

    public static final String RANGER_OBS_CLIENT_FAILOVER_MAX_RETRIES = "ranger.obs.client.failover.max.retries";

    public static final int DEFAULT_RANGER_OBS_CLIENT_FAILOVER_MAX_RETRIES = 3;

    public static final String RANGER_OBS_CLIENT_FAILOVER_SLEEP_BASE_MILLIS
        = "ranger.obs.client.failover.sleep.base.millis";

    public static final int DEFAULT_RANGER_OBS_CLIENT_FAILOVER_SLEEP_BASE_MILLIS = 100;

    public static final String RANGER_OBS_CLIENT_FAILOVER_SLEEP_MAX_MILLIS
        = "ranger.obs.client.failover.sleep.max.millis";

    public static final int DEFAULT_RANGER_OBS_CLIENT_FAILOVER_SLEEP_MAX_MILLIS = 2000;

    public static final String RANGER_OBS_SERVICE_KERBEROS_PRINCIPAL = "ranger.obs.service.kerberos.principal";

    public static final String RANGER_OBS_SERVICE_DT_SERVICE_NAME = "ranger.obs.service.dt.service.name";

    public static final String DEFAULT_RANGER_OBS_SERVICE_DT_SERVICE_NAME = "0.0.0.0:26901";
}
