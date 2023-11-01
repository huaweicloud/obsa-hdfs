/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.obs;

import com.obs.services.IObsCredentialsProvider;
import com.obs.services.ObsClient;
import com.obs.services.internal.ext.ExtObsConfiguration;
import com.obs.services.model.AuthTypeEnum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

/**
 * The default factory implementation, which calls the OBS SDK to configure and
 * create an {@link ObsClient} that communicates with the OBS service.
 */
class DefaultOBSClientFactory extends Configured implements OBSClientFactory {

    /**
     * Class logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(DefaultOBSClientFactory.class);

    /**
     * Initializes all OBS SDK settings related to connection management.
     *
     * @param conf    Hadoop configuration
     * @param obsConf OBS SDK configuration
     */
    private static void initConnectionSettings(final Configuration conf, final ExtObsConfiguration obsConf) {

        obsConf.setMaxConnections(
            OBSCommonUtils.intOption(conf, OBSConstants.MAXIMUM_CONNECTIONS, OBSConstants.DEFAULT_MAXIMUM_CONNECTIONS,
                1));

        boolean secureConnections = conf.getBoolean(OBSConstants.SECURE_CONNECTIONS,
            OBSConstants.DEFAULT_SECURE_CONNECTIONS);
        String originEndPoint = conf.getTrimmed(OBSConstants.ENDPOINT, "");

        if (!originEndPoint.isEmpty() && !originEndPoint.startsWith(OBSConstants.HTTP_PREFIX)
            && !originEndPoint.startsWith(OBSConstants.HTTPS_PREFIX)) {
            String newEndPointWithSchema;
            if (secureConnections) {
                newEndPointWithSchema = OBSConstants.HTTPS_PREFIX + originEndPoint;
            } else {
                newEndPointWithSchema = OBSConstants.HTTP_PREFIX + originEndPoint;
            }
            conf.set(OBSConstants.ENDPOINT, newEndPointWithSchema);
        }

        String endPoint = conf.getTrimmed(OBSConstants.ENDPOINT, "");
        obsConf.setEndPoint(endPoint);

        obsConf.setMaxErrorRetry(
            OBSCommonUtils.intOption(conf, OBSConstants.MAX_ERROR_RETRIES, OBSConstants.DEFAULT_MAX_ERROR_RETRIES, 0));

        obsConf.setConnectionTimeout(
            OBSCommonUtils.intOption(conf, OBSConstants.ESTABLISH_TIMEOUT, OBSConstants.DEFAULT_ESTABLISH_TIMEOUT, 0));

        obsConf.setSocketTimeout(
            OBSCommonUtils.intOption(conf, OBSConstants.SOCKET_TIMEOUT, OBSConstants.DEFAULT_SOCKET_TIMEOUT, 0));

        obsConf.setIdleConnectionTime(
            OBSCommonUtils.intOption(conf, OBSConstants.IDLE_CONNECTION_TIME, OBSConstants.DEFAULT_IDLE_CONNECTION_TIME,
                1));

        obsConf.setMaxIdleConnections(
            OBSCommonUtils.intOption(conf, OBSConstants.MAX_IDLE_CONNECTIONS, OBSConstants.DEFAULT_MAX_IDLE_CONNECTIONS,
                1));

        obsConf.setReadBufferSize(
            OBSCommonUtils.intOption(conf, OBSConstants.READ_BUFFER_SIZE, OBSConstants.DEFAULT_READ_BUFFER_SIZE,
                -1)); // to be
        // modified
        obsConf.setWriteBufferSize(
            OBSCommonUtils.intOption(conf, OBSConstants.WRITE_BUFFER_SIZE, OBSConstants.DEFAULT_WRITE_BUFFER_SIZE,
                -1)); // to be

        obsConf.setSocketReadBufferSize(
            OBSCommonUtils.intOption(conf, OBSConstants.SOCKET_RECV_BUFFER, OBSConstants.DEFAULT_SOCKET_RECV_BUFFER,
                -1));
        obsConf.setSocketWriteBufferSize(
            OBSCommonUtils.intOption(conf, OBSConstants.SOCKET_SEND_BUFFER, OBSConstants.DEFAULT_SOCKET_SEND_BUFFER,
                -1));

        obsConf.setKeepAlive(conf.getBoolean(OBSConstants.KEEP_ALIVE, OBSConstants.DEFAULT_KEEP_ALIVE));
        obsConf.setValidateCertificate(
            conf.getBoolean(OBSConstants.VALIDATE_CERTIFICATE, OBSConstants.DEFAULT_VALIDATE_CERTIFICATE));
        obsConf.setVerifyResponseContentType(conf.getBoolean(OBSConstants.VERIFY_RESPONSE_CONTENT_TYPE,
            OBSConstants.DEFAULT_VERIFY_RESPONSE_CONTENT_TYPE));
        obsConf.setCname(conf.getBoolean(OBSConstants.CNAME, OBSConstants.DEFAULT_CNAME));
        obsConf.setIsStrictHostnameVerification(conf.getBoolean(OBSConstants.STRICT_HOSTNAME_VERIFICATION,
            OBSConstants.DEFAULT_STRICT_HOSTNAME_VERIFICATION));

        // sdk auth type negotiation enable
        obsConf.setAuthTypeNegotiation(conf.getBoolean(OBSConstants.SDK_AUTH_TYPE_NEGOTIATION_ENABLE,
            OBSConstants.DEFAULT_SDK_AUTH_TYPE_NEGOTIATION_ENABLE));
        // set SDK AUTH TYPE to OBS when auth type negotiation unenabled
        if (!obsConf.isAuthTypeNegotiation()) {
            obsConf.setAuthType(AuthTypeEnum.OBS);
        }

        // okhttp retryOnConnectionFailure switch, default set to true
        obsConf.retryOnConnectionFailureInOkhttp(conf.getBoolean(OBSConstants.SDK_RETRY_ON_CONNECTION_FAILURE_ENABLE,
            OBSConstants.DEFAULT_SDK_RETRY_ON_CONNECTION_FAILURE_ENABLE));

        // sdk max retry times on unexpected end of stream exception,
        // default: -1，don't retry
        int retryTime = conf.getInt(OBSConstants.SDK_RETRY_TIMES_ON_UNEXPECTED_END_EXCEPTION,
            OBSConstants.DEFAULT_SDK_RETRY_TIMES_ON_UNEXPECTED_END_EXCEPTION);
        if (retryTime > 0 && retryTime < OBSConstants.DEFAULT_MAX_SDK_CONNECTION_RETRY_TIMES
            || !obsConf.isRetryOnConnectionFailureInOkhttp() && retryTime < 0) {
            retryTime = OBSConstants.DEFAULT_MAX_SDK_CONNECTION_RETRY_TIMES;
        }
        obsConf.setMaxRetryOnUnexpectedEndException(retryTime);
    }

    /**
     * Initializes OBS SDK proxy support if configured.
     *
     * @param conf    Hadoop configuration
     * @param obsConf OBS SDK configuration
     * @throws IllegalArgumentException if misconfigured
     * @throws IOException              on any failure to initialize proxy
     */
    private static void initProxySupport(final Configuration conf, final ExtObsConfiguration obsConf)
        throws IllegalArgumentException, IOException {
        String proxyHost = conf.getTrimmed(OBSConstants.PROXY_HOST, "");
        int proxyPort = conf.getInt(OBSConstants.PROXY_PORT, -1);

        if (!proxyHost.isEmpty() && proxyPort < 0) {
            if (conf.getBoolean(OBSConstants.SECURE_CONNECTIONS, OBSConstants.DEFAULT_SECURE_CONNECTIONS)) {
                LOG.warn("Proxy host set without port. Using HTTPS default " + OBSConstants.DEFAULT_HTTPS_PORT);
                obsConf.getHttpProxy().setProxyPort(OBSConstants.DEFAULT_HTTPS_PORT);
            } else {
                LOG.warn("Proxy host set without port. Using HTTP default " + OBSConstants.DEFAULT_HTTP_PORT);
                obsConf.getHttpProxy().setProxyPort(OBSConstants.DEFAULT_HTTP_PORT);
            }
        }
        String proxyUsername = conf.getTrimmed(OBSConstants.PROXY_USERNAME);
        String proxyPassword = null;
        char[] proxyPass = conf.getPassword(OBSConstants.PROXY_PASSWORD);
        if (proxyPass != null) {
            proxyPassword = new String(proxyPass).trim();
        }
        if ((proxyUsername == null) != (proxyPassword == null)) {
            String msg = "Proxy error: " + OBSConstants.PROXY_USERNAME + " or " + OBSConstants.PROXY_PASSWORD
                + " set without the other.";
            LOG.error(msg);
            throw new IllegalArgumentException(msg);
        }
        obsConf.setHttpProxy(proxyHost, proxyPort, proxyUsername, proxyPassword);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Using proxy server {}:{} as user {} on " + "domain {} as workstation {}",
                obsConf.getHttpProxy().getProxyAddr(), obsConf.getHttpProxy().getProxyPort(),
                obsConf.getHttpProxy().getProxyUName(), obsConf.getHttpProxy().getDomain(),
                obsConf.getHttpProxy().getWorkstation());
        }
    }

    @Override
    public ObsClient createObsClient(final URI name) throws IOException {
        Configuration conf = getConf();
        ExtObsConfiguration obsConf = new ExtObsConfiguration();
        initConnectionSettings(conf, obsConf);
        initProxySupport(conf, obsConf);

        IObsCredentialsProvider securityProvider = OBSSecurityProviderUtil.createObsSecurityProvider(conf, name);
        return new ObsClient(securityProvider, obsConf);
    }
}
