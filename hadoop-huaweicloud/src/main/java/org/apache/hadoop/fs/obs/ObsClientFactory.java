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
import com.obs.services.ObsConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.slf4j.Logger;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.Optional;

import static org.apache.hadoop.fs.obs.Constants.*;
import static org.apache.hadoop.fs.obs.OBSUtils.getOBSAccessKeys;
import static org.apache.hadoop.fs.obs.OBSUtils.intOption;

/** Factory for creation of OBS client instances to be used by {@link }. */
@InterfaceAudience.Private
@InterfaceStability.Unstable
interface ObsClientFactory {
  /**
   * Creates a new {@link ObsClient} client. This method accepts the OBS file system URI both in raw
   * input form and validated form as separate arguments, because both values may be useful in
   * logging.
   *
   * @param name raw input OBS file system URI
   * @return OBS client
   * @throws IOException IO problem
   */
  ObsClient createObsClient(URI name) throws IOException;

  /**
   * The default factory implementation, which calls the OBS SDK to configure and create an {@link
   * ObsClient} that communicates with the OBS service.
   */
  static class DefaultObsClientFactory extends Configured implements ObsClientFactory {

    private static final Logger LOG = OBSFileSystem.LOG;

    /**
     * Initializes all OBS SDK settings related to connection management.
     *
     * @param conf Hadoop configuration
     * @param obsConf OBS SDK configuration
     */
    private static void initConnectionSettings(Configuration conf, ObsConfiguration obsConf) {

      obsConf.setMaxConnections(
          intOption(conf, MAXIMUM_CONNECTIONS, DEFAULT_MAXIMUM_CONNECTIONS, 1));

      boolean secureConnections = conf.getBoolean(SECURE_CONNECTIONS, DEFAULT_SECURE_CONNECTIONS);

      obsConf.setHttpsOnly(secureConnections);

      obsConf.setMaxErrorRetry(intOption(conf, MAX_ERROR_RETRIES, DEFAULT_MAX_ERROR_RETRIES, 0));

      obsConf.setConnectionTimeout(
          intOption(conf, ESTABLISH_TIMEOUT, DEFAULT_ESTABLISH_TIMEOUT, 0));

      obsConf.setSocketTimeout(intOption(conf, SOCKET_TIMEOUT, DEFAULT_SOCKET_TIMEOUT, 0));

      obsConf.setIdleConnectionTime(
          intOption(conf, IDLE_CONNECTION_TIME, DEFAULT_IDLE_CONNECTION_TIME, 1));

      obsConf.setMaxIdleConnections(
          intOption(conf, MAX_IDLE_CONNECTIONS, DEFAULT_MAX_IDLE_CONNECTIONS, 1));

      obsConf.setReadBufferSize(
          intOption(conf, READ_BUFFER_SIZE, DEFAULT_READ_BUFFER_SIZE, -1)); // to be
      // modified
      obsConf.setWriteBufferSize(
          intOption(conf, WRITE_BUFFER_SIZE, DEFAULT_WRITE_BUFFER_SIZE, -1)); // to be
      // modified
      obsConf.setUploadStreamRetryBufferSize(
          intOption(conf, UPLOAD_STREAM_RETRY_SIZE, DEFAULT_UPLOAD_STREAM_RETRY_SIZE, 1));

      obsConf.setSocketReadBufferSize(
          intOption(conf, SOCKET_RECV_BUFFER, DEFAULT_SOCKET_RECV_BUFFER, -1));
      obsConf.setSocketWriteBufferSize(
          intOption(conf, SOCKET_SEND_BUFFER, DEFAULT_SOCKET_SEND_BUFFER, -1));

      obsConf.setKeepAlive(conf.getBoolean(KEEP_ALIVE, DEFAULT_KEEP_ALIVE));
      obsConf.setValidateCertificate(
          conf.getBoolean(VALIDATE_CERTIFICATE, DEFAULT_VALIDATE_CERTIFICATE));
      obsConf.setVerifyResponseContentType(
          conf.getBoolean(VERIFY_RESPONSE_CONTENT_TYPE, DEFAULT_VERIFY_RESPONSE_CONTENT_TYPE));
      obsConf.setCname(conf.getBoolean(CNAME, DEFAULT_CNAME));
      obsConf.setIsStrictHostnameVerification(
          conf.getBoolean(STRICT_HOSTNAME_VERIFICATION, DEFAULT_STRICT_HOSTNAME_VERIFICATION));
    }

    /**
     * Initializes OBS SDK proxy support if configured.
     *
     * @param conf Hadoop configuration
     * @param obsConf OBS SDK configuration
     * @throws IllegalArgumentException if misconfigured
     */
    private static void initProxySupport(Configuration conf, ObsConfiguration obsConf)
        throws IllegalArgumentException, IOException {
      String proxyHost = conf.getTrimmed(PROXY_HOST, "");
      int proxyPort = conf.getInt(PROXY_PORT, -1);

      if (!proxyHost.isEmpty() && proxyPort < 0) {
        if (conf.getBoolean(SECURE_CONNECTIONS, DEFAULT_SECURE_CONNECTIONS)) {
          LOG.warn("Proxy host set without port. Using HTTPS default 443");
          obsConf.getHttpProxy().setProxyPort(443);
        } else {
          LOG.warn("Proxy host set without port. Using HTTP default 80");
          obsConf.getHttpProxy().setProxyPort(80);
        }
      }
      String proxyUsername = conf.getTrimmed(PROXY_USERNAME);
      String proxyPassword = null;
      char[] proxyPass = conf.getPassword(PROXY_PASSWORD);
      if (proxyPass != null) {
        proxyPassword = new String(proxyPass).trim();
      }
      if ((proxyUsername == null) != (proxyPassword == null)) {
        String msg =
            "Proxy error: " + PROXY_USERNAME + " or " + PROXY_PASSWORD + " set without the other.";
        LOG.error(msg);
        throw new IllegalArgumentException(msg);
      }
      obsConf.setHttpProxy(proxyHost, proxyPort, proxyUsername, proxyPassword);
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Using proxy server {}:{} as user {} on " + "domain {} as workstation {}",
            obsConf.getHttpProxy().getProxyAddr(),
            obsConf.getHttpProxy().getProxyPort(),
            String.valueOf(obsConf.getHttpProxy().getProxyUName()),
            obsConf.getHttpProxy().getDomain(),
            obsConf.getHttpProxy().getWorkstation());
      }
    }

    /**
     * Creates an {@link ObsClient} from the established configuration.
     *
     * @param conf Hadoop configuration
     * @param obsConf ObsConfiguration
     * @param name URL
     * @return ObsClient client
     */
    private static ObsClient createHuaweiObsClient(
        Configuration conf, ObsConfiguration obsConf, URI name) throws IOException {
      Class<?> credentialsProviderClass;
      BasicSessionCredential credentialsProvider;
      ObsClient obsClient = null;

      try {
        credentialsProviderClass = conf.getClass(OBS_CREDENTIALS_PROVIDER, null);
      } catch (RuntimeException e) {
        Throwable c = e.getCause() != null ? e.getCause() : e;
        throw new IOException("From option " + OBS_CREDENTIALS_PROVIDER + ' ' + c, c);
      }

      if (credentialsProviderClass == null) {
        OBSLoginHelper.Login creds = getOBSAccessKeys(name, conf);

        String Ak = creds.getUser();
        String Sk = creds.getPassword();
        String token = creds.getToken();

        String endPoint = conf.getTrimmed(ENDPOINT, "");
        obsConf.setEndPoint(endPoint);
        if (StringUtils.isEmpty(Ak) && StringUtils.isEmpty(Sk)) {
          Class<?> securityProviderClass;
          IObsCredentialsProvider securityProvider;
          try {
            securityProviderClass = conf.getClass(OBS_SECURITY_PROVIDER, null);
            LOG.info("From option {} get {}", OBS_SECURITY_PROVIDER, securityProviderClass);
          } catch (RuntimeException e) {
            Throwable c = e.getCause() != null ? e.getCause() : e;
            throw new IOException("From option " + OBS_SECURITY_PROVIDER + ' ' + c, c);
          }
          if (securityProviderClass != null) {
            try {
              Optional<Constructor> cons = tryGetConstructor(securityProviderClass,
                      new Class[]{URI.class, Configuration.class});

              if (cons.isPresent()) {
                securityProvider = (IObsCredentialsProvider) cons.get().newInstance(name, conf);
              } else {
                securityProvider = (IObsCredentialsProvider) securityProviderClass.getDeclaredConstructor().newInstance();
              }

            } catch (NoSuchMethodException | SecurityException e) {
              Throwable c = e.getCause() != null ? e.getCause() : e;
              throw new IOException("From option " + OBS_SECURITY_PROVIDER + ' ' + c, c);
            } catch (IllegalAccessException e) {
              Throwable c = e.getCause() != null ? e.getCause() : e;
              throw new IOException("From option " + OBS_SECURITY_PROVIDER + ' ' + c, c);
            } catch (InstantiationException e) {
              Throwable c = e.getCause() != null ? e.getCause() : e;
              throw new IOException("From option " + OBS_SECURITY_PROVIDER + ' ' + c, c);
            } catch (InvocationTargetException e) {
              Throwable c = e.getCause() != null ? e.getCause() : e;
              throw new IOException("From option " + OBS_SECURITY_PROVIDER + ' ' + c, c);
            } catch (RuntimeException e) {
              Throwable c = e.getCause() != null ? e.getCause() : e;
              throw new IOException("From option " + OBS_SECURITY_PROVIDER + ' ' + c, c);
            }
            obsClient = new ObsClient(securityProvider, obsConf);
          } else {
            obsClient = new ObsClient(Ak, Sk, token, obsConf);
          }
        } else {
          obsClient = new ObsClient(Ak, Sk, token, obsConf);
        }
      } else {
        try {
          Constructor cons =
                  credentialsProviderClass.getDeclaredConstructor(URI.class, Configuration.class);
          credentialsProvider = (BasicSessionCredential) cons.newInstance(name, conf);
        } catch (NoSuchMethodException | SecurityException e) {
          Throwable c = e.getCause() != null ? e.getCause() : e;
          throw new IOException("From option " + OBS_CREDENTIALS_PROVIDER + ' ' + c, c);
        } catch (IllegalAccessException e) {
          Throwable c = e.getCause() != null ? e.getCause() : e;
          throw new IOException("From option " + OBS_CREDENTIALS_PROVIDER + ' ' + c, c);
        } catch (InstantiationException e) {
          Throwable c = e.getCause() != null ? e.getCause() : e;
          throw new IOException("From option " + OBS_CREDENTIALS_PROVIDER + ' ' + c, c);
        } catch (InvocationTargetException e) {
          Throwable c = e.getCause() != null ? e.getCause() : e;
          throw new IOException("From option " + OBS_CREDENTIALS_PROVIDER + ' ' + c, c);
        }

        String sessionToken = credentialsProvider.getSessionToken();
        String ak = credentialsProvider.getOBSAccessKeyId();
        String sk = credentialsProvider.getOBSSecretKey();
        String endPoint = conf.getTrimmed(ENDPOINT, "");
        obsConf.setEndPoint(endPoint);
        if (sessionToken != null && sessionToken.length() != 0) {
          obsClient = new ObsClient(ak, sk, sessionToken, obsConf);
        } else {
          obsClient = new ObsClient(ak, sk, obsConf);
        }
      }
      return obsClient;
    }

    public static Optional<Constructor> tryGetConstructor(Class mainClss, Class[] args) {
      try {
        Constructor constructor = mainClss.getDeclaredConstructor(args);
        return Optional.ofNullable(constructor);
      } catch (NoSuchMethodException e) {
        // ignore
        return Optional.empty();
      }
    }

    @Override
    public ObsClient createObsClient(URI name) throws IOException {
      Configuration conf = getConf();
      ObsConfiguration obsConf = new ObsConfiguration();
      initConnectionSettings(conf, obsConf);
      initProxySupport(conf, obsConf);

      return createHuaweiObsClient(conf, obsConf, name);
    }
  }
}
