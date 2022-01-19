package org.apache.ranger.obs.security.sts;

import com.huawei.mrs.AgencyMappingLoader;
import com.huawei.mrs.ECSMetaHolder;
import com.huawei.mrs.EcsMeta;
import com.huawei.mrs.IassHttpClient;
import com.huawei.mrs.JavaSdkClient;
import com.huawei.mrs.exception.TooManyRequestsException;

import com.obs.services.EcsObsCredentialsProvider;
import com.obs.services.exception.ObsException;
import com.obs.services.internal.security.LimitedTimeSecurityKey;
import com.obs.services.internal.security.SecurityKey;
import com.obs.services.internal.security.SecurityKeyBean;
import com.obs.services.model.ISecurityKey;
import okhttp3.MediaType;
import okhttp3.RequestBody;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.URI;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;


public class MrsCredentialsProvider{
    private final String MAPPING_KEY_NAME = "fs.obs.auth.agency-mapping.localpath";
    private final String NODE_CACHE_ENABLE = "fs.obs.auth.node-cache.enable";
    private final String NODE_CACHE_SHORT_CIRCUIT = "fs.obs.auth.node-cache-short-circuit.enable";
    private final String MRS_META_URL = "fs.obs.mrs.meta.url";
    private final String ECS_MEAT_URL = "fs.obs.ecs.meta.url";
    private final String OBTAIN_KEY_MAX_RETRY = "mrs.provider.key.max.retry";
    private final int DEFAULT_OBTAIN_KEY_MAX_RETRY = 3;
    private String agencyMappingLocalPath;
    private String iamDomainUrl;
    private String userDomainName;
    private String userDomainId;
    private String clusterAgencyName;
    private boolean nodeCacheEnable;
    private boolean shortCircuit;
    private String  metaUrl;
    private Configuration conf;
    // private UserGroupInformation userInfo;
    private HashMap<UserGroupInformation, ISecurityKey> securityKeyCacheMap = new HashMap<>();
    private IassHttpClient httpclient = new IassHttpClient();
    private int securityKeyMaxRetry;

    private static EcsObsCredentialsProvider ecsObsCredentialsProvider = new EcsObsCredentialsProvider();

    public static final MediaType JSON = MediaType.get("application/json; charset=utf-8");

    private static final Logger LOG = LoggerFactory.getLogger(MrsCredentialsProvider.class);

    public MrsCredentialsProvider() throws Exception {
        this(null, new Configuration());
    }

    public MrsCredentialsProvider(URI uri, Configuration conf) throws Exception {
        this.conf = conf;
        agencyMappingLocalPath = conf.get(MAPPING_KEY_NAME, "");
        nodeCacheEnable = conf.getBoolean(NODE_CACHE_ENABLE, true);
        shortCircuit = conf.getBoolean(NODE_CACHE_SHORT_CIRCUIT, false);
        metaUrl = conf.get(MRS_META_URL, "http://127.0.0.1:23443/rest/meta/security_key");
        // try {
        //     userInfo = UserGroupInformation.getCurrentUser();
        // } catch (IOException e) {
        //     LOG.warn("Get user group information failed" + e);
        //     userInfo = null;
        // }
        IassHttpClient.init(true);

        EcsMeta metadata = ECSMetaHolder.getInstance().getMetadata();
        if (metadata != null) {
            iamDomainUrl = metadata.getIamUrl();
            userDomainName = metadata.getUserDomainName();
            userDomainId = metadata.getUserDomainId();
            clusterAgencyName = metadata.getAgencyName();
        } else {
            LOG.warn("Get ecs meta is null, will disable assume role.");
        }
        securityKeyMaxRetry = conf.getInt(OBTAIN_KEY_MAX_RETRY, DEFAULT_OBTAIN_KEY_MAX_RETRY);
    }

    // public void setSecurityKey(ISecurityKey iSecurityKey) {
    //     throw new UnsupportedOperationException("EcsObsCredentialsProvider class does not support this method");
    // }

    public synchronized ISecurityKey getSecurityKey(UserGroupInformation userInfo) {
        ISecurityKey securityKeyCache = securityKeyCacheMap.get(userInfo);
        if (securityKeyCache instanceof LimitedTimeSecurityKey) {
            if (!((LimitedTimeSecurityKey) securityKeyCache).aboutToExpire()
                && !((LimitedTimeSecurityKey) securityKeyCache).willSoonExpire()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("SecurityKey cache is not expire, return securityKey from cache, expire date" +
                        ((LimitedTimeSecurityKey) securityKeyCache).getExpiryDate());
                }
                return securityKeyCache;
            }
        }
        LimitedTimeSecurityKey securityKey = null;
        if (nodeCacheEnable) {
            String agencyName = null;
            if (!agencyMappingLocalPath.isEmpty()) {
                agencyName = AgencyMappingLoader.matchMappingAgent(userInfo, agencyMappingLocalPath);
            }
            try {
                securityKey = httpclient.getKeyFromNodeCache(iamDomainUrl, userDomainName,
                    userDomainId, agencyName, metaUrl);
            } catch (TooManyRequestsException tme) {
                LOG.error("Too many request when get temporary security key, never try to request ecs directly");
                throw new ObsException("Too many request when get temporary security key. " + tme.getMessage());
            } catch (Exception e) {
                LOG.error("Failed to get temporary security key from mrs meta server", e);
            }
            // if get security key from mrs meta failed , do failover
            if (securityKey == null) {
                if (shortCircuit) {
                    LOG.warn("Failed to get security key from mrs meta, getting security key form ECS directly");
                    securityKey = getNewSecurityKey(userInfo);
                } else {
                    LOG.error("Failed to get security from mrs meta, will not get security key form ECS directly");
                }
            }
        } else {
            securityKey = getNewSecurityKey(userInfo);
        }
        if (securityKey != null) {
            LOG.info("Get security key expired at: " + securityKey.getExpiryDate());
            securityKeyCacheMap.put(userInfo, securityKey);
        } else {
            throw new ObsException("Exception when get temporary security key");
        }
        return securityKey;
    }

    private LimitedTimeSecurityKey getNewSecurityKey(UserGroupInformation userInfo) {
        LimitedTimeSecurityKey defaultAgencySecurityKey = null;
        int tries = 1;
        boolean isSuccess = false;
        do {
            try {
                defaultAgencySecurityKey = (LimitedTimeSecurityKey) ecsObsCredentialsProvider.getSecurityKey();
                isSuccess = true;
            } catch (Exception e) {
                if (tries >= securityKeyMaxRetry) {
                    LOG.error("Failed to get security key, exceed max retry time " + securityKeyMaxRetry, e);
                    throw e;
                }
                LOG.warn("Failed to get security key with exception, tries = " + (tries++) + "", e);
            }
        } while (!isSuccess);

        if (agencyMappingLocalPath.isEmpty()) {
            LOG.warn("Return default agency as agency mapping local is empty. User info:" + userInfo);
            return defaultAgencySecurityKey;
        }

        String agencyName;
        agencyName = AgencyMappingLoader.matchMappingAgent(userInfo, agencyMappingLocalPath);

        if (null == agencyName) {
            LOG.info("Assume agent name is null, return cluster agency security key. Agency name:" + clusterAgencyName);
            return defaultAgencySecurityKey;
        }
        else{
            LOG.info("Get new security key of User: " + userInfo + " Assume agency Name is: " + agencyName);
        }

        if (isAnyParameterNull()) {
            LOG.error("Iam domain url is empty or user domain name is empty: " + iamDomainUrl + userDomainName);
            return defaultAgencySecurityKey;
        }

        String requestBody = bowlingJson(userDomainId, agencyName);
        RequestBody body = RequestBody.create(JSON, requestBody);
        if (LOG.isDebugEnabled()) {
            LOG.debug("request body string format: " + requestBody + " request body json format: " + body);
            LOG.debug("request param user domain id: " + userDomainId);
        }
        SecurityKey assumeToken = new JavaSdkClient().getIamAssumeRoleToken(iamDomainUrl, defaultAgencySecurityKey.getSecurityToken(), defaultAgencySecurityKey.getAccessKey(),
            defaultAgencySecurityKey.getSecretKey(), requestBody, userDomainId);
        if (assumeToken == null) {
            LOG.warn("Invalid securityKey");
            return defaultAgencySecurityKey;
        } else {
            new Date();
            SecurityKeyBean bean = assumeToken.getBean();

            Date expiryDate;
            try {
                DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
                String strDate = bean.getExpiresDate();
                expiryDate = df.parse(strDate.substring(0, strDate.length() - 4));
            } catch (ParseException var9) {
                throw new IllegalArgumentException("Date parse failed :" + var9.getMessage());
            }

            return new LimitedTimeSecurityKey(bean.getAccessKey(), bean.getSecretKey(), bean.getSecurityToken(), expiryDate);
        }
    }

    private boolean isAnyParameterNull() {
        return null == iamDomainUrl || null == userDomainId || null == userDomainName ||
            iamDomainUrl.isEmpty() || userDomainId.isEmpty() || userDomainName.isEmpty();
    }

    String bowlingJson(String domainId, String agencyName) {
        return "{"
            + "\"auth\": {"
            + "\"identity\": {"
            + " \"methods\": ["
            + "\"assume_role\""
            + "],"
            + "\"assume_role\": {"
            + "\"domain_id\": \"" + domainId + "\","
            + "\"agency_name\": \"" + agencyName + "\","
            + "\"duration-seconds\": \"21600\""
            + "}"
            + "}"
            + "}"
            + "}";
    }

    public void setEcsObsCredentialsProvider(EcsObsCredentialsProvider ecsObsCredentialsProvider) {
        this.ecsObsCredentialsProvider = ecsObsCredentialsProvider;
    }

    public void setHttpclient(IassHttpClient httpclient) {
        this.httpclient = httpclient;
    }
}
