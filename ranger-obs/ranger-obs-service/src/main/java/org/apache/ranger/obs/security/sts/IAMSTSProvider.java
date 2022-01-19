/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package org.apache.ranger.obs.security.sts;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.obs.server.ServerConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.X509TrustManager;

import okhttp3.Authenticator;
import okhttp3.Credentials;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.Route;

/**
 * 功能描述
 *
 * @since 2021-08-24
 */
@Deprecated
public class IAMSTSProvider implements STSProvider {

    private static final Logger LOG = LoggerFactory.getLogger(IAMSTSProvider.class);

    int iamSecurityTokenDuration;

    long tokenExpire;

    RetryPolicy retryPolicy;

    private OkHttpClient client;

    private String iamTokenUrl;

    private String iamDomainName;

    private String iamUserName;

    private String iamUserPassword;

    private String iamSecurityTokenUrl;

    private String token;

    private String ak;

    private String sk;

    private String securityToken;

    private Long securityTokenExpire;

    private Timer securityTokenRenewalTimer;

    private int maxRetryTimes = 5;

    @Override
    public void init(Configuration configuration) throws Exception {
        this.iamTokenUrl = configuration.get(ServerConstants.RANGER_OBS_SERVICE_STS_TOKEN_URL,
            ServerConstants.DEFAULT_RANGER_OBS_SERVICE_STS_TOKEN_URL);
        this.iamDomainName = configuration.get(ServerConstants.RANGER_OBS_SERVICE_STS_DOMAIN_NAME);
        this.iamUserName = configuration.get(ServerConstants.RANGER_OBS_SERVICE_STS_USER_NAME);
        this.iamUserPassword = configuration.get(ServerConstants.RANGER_OBS_SERVICE_STS_USER_PASSWORD);
        Preconditions.checkNotNull(iamDomainName);
        Preconditions.checkNotNull(iamUserName);
        Preconditions.checkNotNull(iamUserPassword);

        this.iamSecurityTokenUrl = configuration.get(ServerConstants.RANGER_OBS_SERVICE_STS_SECURITYTOKEN_URL,
            ServerConstants.DEFAULT_RANGER_OBS_SERVICE_STS_SECURITYTOKEN_URL);
        this.iamSecurityTokenDuration = configuration.getInt(
            ServerConstants.RANGER_OBS_SERVICE_STS_SECURITYTOKEN_DURATION,
            ServerConstants.DEFAULT_RANGER_OBS_SERVICE_STS_SECURITYTOKEN_DURATION);

        createHTTPClient();

        RetryPolicy defaultPolicy =
            RetryPolicies.retryUpToMaximumCountWithFixedSleep(maxRetryTimes, 1000, TimeUnit.MILLISECONDS);

        Map<Class<? extends Exception>, RetryPolicy> policies = new HashMap<>();
        policies.put(IOException.class, defaultPolicy);
        this.retryPolicy = RetryPolicies.retryByException(defaultPolicy, policies);
    }

    private void createHTTPClient() throws Exception {
        OkHttpClient.Builder builder = new OkHttpClient.Builder();

        X509TrustManager manager = SSLSocketClientUtil.getX509TrustManager();
        builder
            .sslSocketFactory(SSLSocketClientUtil.getSocketFactory(manager), manager)
            .hostnameVerifier(SSLSocketClientUtil.getHostnameVerifier());
        client = builder.build();
    }

    @Override
    public GetSTSResponse getSTS(GetSTSRequest stsrequest, UserGroupInformation ugi) throws IOException {
        if (ak == null || sk == null || securityToken == null || securityTokenExpire == null) {
            throw new IOException("sts interface error");
        }
        GetSTSResponse getSTSResponse = new GetSTSResponse();
        getSTSResponse.setAk(this.ak);
        getSTSResponse.setSk(this.sk);
        getSTSResponse.setToken(this.securityToken);
        getSTSResponse.setExpiryTime(this.securityTokenExpire);
        return getSTSResponse;
    }

    @Override
    public void start() throws IOException {
        getSTSFromIam();
        securityTokenRenewalTimer = new Timer(true);
        setTimerForSecurityTokenRenewal();
    }

    @Override
    public void stop() {
        if (securityTokenRenewalTimer != null) {
            securityTokenRenewalTimer.cancel();
        }
    }

    private void setTimerForSecurityTokenRenewal() {
        long expiresIn = securityTokenExpire - System.currentTimeMillis();
        long renewIn = securityTokenExpire - expiresIn / 10;
        SecurityTokenRenewalTimerTask tTask = new SecurityTokenRenewalTimerTask();
        securityTokenRenewalTimer.schedule(tTask, new Date(renewIn));
    }

    private void getSTSFromIam() throws IOException {
        long now = System.currentTimeMillis();
        if (this.token == null || this.tokenExpire < now) {
            getTokenFromIam();
        }

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode rootNode = mapper.createObjectNode();
        ObjectNode auth = mapper.createObjectNode();
        ObjectNode identity = mapper.createObjectNode();
        ArrayNode methods = mapper.createArrayNode();
        methods.add("token");
        identity.set("methods", methods);
        ObjectNode token = mapper.createObjectNode();
        token.put("id", this.token);
        token.put("duration_seconds", this.iamSecurityTokenDuration);
        identity.set("token", token);
        auth.set("identity", identity);
        rootNode.set("auth", auth);

        int retries = 0;
        while (true) {
            Response response = null;
            try {
                String jsonString = mapper.writeValueAsString(rootNode);
                MediaType mediaType = MediaType.parse("application/json;charset=utf8");

                Request request = new Request.Builder()
                    .url(this.iamSecurityTokenUrl)
                    .addHeader("Content-Type", "application/json;charset=utf8")
                    .post(RequestBody.create(mediaType, jsonString))
                    .build();
                response = client.newCall(request).execute();
                if (response.isSuccessful()) {
                    String body = response.body().string();
                    JsonNode jsonNode = mapper.readTree(body);
                    JsonNode credential = jsonNode.get("credential");
                    this.ak = credential.get("access").asText();
                    this.sk = credential.get("secret").asText();
                    this.securityToken = credential.get("securitytoken").asText();

                    String expiresAt = credential.get("expires_at").asText();
                    expiresAt = expiresAt.replace("Z", " UTC");
                    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS Z", Locale.getDefault());
                    try {
                        this.securityTokenExpire = format.parse(expiresAt).getTime();
                    } catch (ParseException e) {
                        throw new IOException("get sts from IAM failed ", e);
                    }
                    break;
                } else {
                    throw new IOException("get sts from IAM failed " + response);
                }
            } catch (IOException e) {
                handleConnectionFailure(retries++, e, "get sts from IAM");
            } finally {
                if (response != null) {
                    response.close();
                }
            }
        }
    }

    private void getTokenFromIam() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode rootNode = mapper.createObjectNode();
        ObjectNode auth = mapper.createObjectNode();
        ObjectNode identity = mapper.createObjectNode();
        ArrayNode methods = mapper.createArrayNode();
        methods.add("password");
        identity.set("methods", methods);
        ObjectNode domain = mapper.createObjectNode();
        domain.put("name", this.iamDomainName);
        ObjectNode password = mapper.createObjectNode();
        ObjectNode user = mapper.createObjectNode();
        user.put("name", this.iamUserName);
        user.put("password", this.iamUserPassword);
        user.set("domain", domain);
        password.set("user", user);
        identity.set("password", password);
        auth.set("identity", identity);
        ObjectNode scope = mapper.createObjectNode();
        scope.set("domain", domain);
        auth.set("scope", identity);
        rootNode.set("auth", auth);

        int retries = 0;
        while (true) {
            Response response = null;
            try {
                String jsonString = mapper.writeValueAsString(rootNode);
                MediaType mediaType = MediaType.parse("application/json;charset=utf8");
                Request request = new Request.Builder()
                    .url(this.iamTokenUrl)
                    .addHeader("Content-Type", "application/json;charset=utf8")
                    .post(RequestBody.create(mediaType, jsonString))
                    .build();
                response = client.newCall(request).execute();
                if (response.isSuccessful()) {
                    this.token = response.header("X-Subject-Token");
                    String body = response.body().string();

                    JsonNode jsonNode = mapper.readTree(body);
                    String expiresAt = jsonNode.get("token").get("expires_at").asText();
                    expiresAt = expiresAt.replace("Z", " UTC");

                    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS Z", Locale.getDefault());

                    try {
                        this.tokenExpire = format.parse(expiresAt).getTime();
                    } catch (ParseException e) {
                        throw new IOException(e);
                    }
                    break;

                } else {
                    throw new IOException("get token from IAM failed " + response);
                }
            } catch (IOException e) {
                handleConnectionFailure(retries++, e, "get token from IAM");
            } finally {
                if (response != null) {
                    response.close();
                }
            }
        }
    }

    private void handleConnectionFailure(int curRetries, IOException ioe, String message) throws IOException {
        final RetryPolicy.RetryAction action;
        try {
            action = retryPolicy.shouldRetry(ioe, curRetries, 0, true);
        } catch (Exception e) {
            throw new IOException(e);
        }
        if (action.action == RetryPolicy.RetryAction.RetryDecision.FAIL) {
            if (action.reason != null) {
                LOG.error(message + ": " + action.reason, ioe);
            }
            throw ioe;
        }

        // Throw the exception if the thread is interrupted
        if (Thread.currentThread().isInterrupted()) {
            LOG.warn("Interrupted while " + message);
            throw ioe;
        }

        try {
            Thread.sleep(action.delayMillis);
        } catch (InterruptedException e) {
            throw (IOException) new InterruptedIOException("Interrupted: action="
                + action + ", retry policy=" + retryPolicy).initCause(e);
        }
        LOG.warn(message + ". Already tried " + curRetries + " time(s); retry policy is " + retryPolicy);

    }

    private class SecurityTokenRenewalTimerTask extends TimerTask {
        @Override
        public void run() {
            try {
                getSTSFromIam();
                setTimerForSecurityTokenRenewal();
            } catch (IOException e) {
                LOG.error("get sts from IAM failed", e);
            }
        }
    }
}
