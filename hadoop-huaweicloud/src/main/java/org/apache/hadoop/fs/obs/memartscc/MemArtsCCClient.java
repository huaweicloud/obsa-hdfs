package org.apache.hadoop.fs.obs.memartscc;

import com.obs.services.IObsCredentialsProvider;
import com.obs.services.internal.security.LimitedTimeSecurityKey;
import com.obs.services.model.ISecurityKey;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.obs.OBSConstants;
import org.apache.hadoop.fs.obs.OBSSecurityProviderUtil;
import org.apache.hadoop.fs.obs.TrafficStatistics;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import sun.nio.ch.DirectBuffer;

/**
 * 功能描述
 *
 * @since 2021-05-18
 */
public class MemArtsCCClient {

    private static final String MRS_AZ_ENV_VARNAME = "AZ";

    private static final String SECURITY_ENABLE = "security_enable";

    private static final Logger LOG = LoggerFactory.getLogger(MemArtsCCClient.class);

    private static final boolean IS_FILE_LAYOUT = false;

    private IObsCredentialsProvider securityProvider;

    private String endpoint;

    private final String bucket;

    private final boolean enablePosix;

    private volatile boolean closed;

    private boolean initialized;

    private int akskRefreshInterval;

    AtomicReference<ISecurityKey> iSecurityKey = new AtomicReference<>();

    private ICache cache;

    private Configuration conf;

    private final AtomicReference<byte[]> password;

    public MemArtsCCClient(String bucket, boolean enablePosix) {
        this.bucket = bucket;
        this.enablePosix = enablePosix;
        this.password = new AtomicReference<>();
    }

    public boolean initialize(URI name, Configuration conf) {
        if (initialized) {
            LOG.warn("MemArtsCCClient have been initialized more than once");
            return false;
        }

        if (needEscapePyspark(conf)) {
            return false;
        }

        this.conf = conf;

        if (!createCacheClass(conf)) {
            return false;
        }

        setEndpoint(conf);

        akskRefreshInterval = conf.getInt(
                OBSConstants.MEMARTSCC_AKSK_REFRESH_INTERVAL,
                OBSConstants.DEFAULT_MEMARTSCC_AKSK_REFRESH_INTERVAL);
        try {
            securityProvider = OBSSecurityProviderUtil.createObsSecurityProvider(conf, name);
        } catch (IOException e) {
            LOG.warn("create security provider failed, {}", e.getMessage());
            return false;
        }
        setSecurityKey(securityProvider.getSecurityKey());

        if (!initCacheClass(conf)) {
            return false;
        }

        startRefreshJob();
        initBufferPool(conf);
        initialized = true;
        return true;
    }

    /**
     * Initialize MemArtsCCClient in the delegation token only scenario,
     * i.e. where DELEGATION_TOKEN_ONLY is set to true
     */
    public boolean initializeDtOnly(final Configuration conf) {
        if (initialized) {
            LOG.warn("MemArtsCCClient hav e been initialized more than once");
            return false;
        }

        this.conf = conf;

        if (needEscapePyspark(conf)) {
            return false;
        }

        if (!createCacheClass(conf)) {
            return false;
        }

        if (!initCacheClass(conf)) {
            return false;
        }

        initialized = true;
        return true;
    }

    private void initBufferPool(Configuration conf) {
        int maxNum = conf.getInt(OBSConstants.MEMARTSCC_INPUTSTREAM_BUFFER_POOL_MAX_SIZE,
            OBSConstants.MEMARTSCC_INPUTSTREAM_BUFFER_POOL_DEFAULT_MAX_SIZE);
        int bufferSize = conf.getInt(OBSConstants.MEMARTSCC_DIRECTBUFFER_SIZE,
            OBSConstants.DEFAULT_MEMARTSCC_DIRECTBUFFER_SIZE);
        bufferPool.initialize(maxNum, bufferSize);
    }

    private void setEndpoint(Configuration conf) {
        String ep = conf.getTrimmed(OBSConstants.ENDPOINT);
        if (ep.startsWith("http://")) {
            ep = ep.substring("http://".length());
        }
        if (ep.startsWith("https://")) {
            ep = ep.substring("https://".length());
        }
        this.endpoint = ep;
    }

    private boolean initCacheClass(Configuration conf) {
        try {
            String config = filterCCConfig(conf);
            String otherInfo = collectOtherInfo(conf);
            int result = init(config, otherInfo);
            if (result == 0) {
                LOG.debug("memArtsCCClient.ccInit OK!");
            } else {
                LOG.warn("memArtsCC init failed, ccInit ret code = {}, will trying to fallback", result);
                return false;
            }
        } catch (Throwable e) {
            LOG.warn("memArtsCC init exception, will trying to fallback, caused by {}", e.getMessage());
            return false;
        }
        return true;
    }

    private boolean createCacheClass(Configuration conf) {
        try {
            Class<? extends ICache> cacheClass = conf.getClass(OBSConstants.MEMARTSCC_CACHE_IMPL,
                null, ICache.class);
            if (cacheClass == null) {
                LOG.warn("get null ICache instance");
                return false;
            }
            this.cache = cacheClass.newInstance();
        } catch (InstantiationException | IllegalAccessException | RuntimeException e) {
            LOG.warn("get instance of ICache failed", e);
            return false;
        }
        return true;
    }

    private boolean needEscapePyspark(Configuration conf) {
        boolean isPySpark = conf.getBoolean("spark.yarn.isPython", false);
        if (isPySpark) {
            boolean pySparkOptimizedConf = conf.getBoolean(OBSConstants.MEMARTSCC_PYSPARK_OPTIMIZED,
                    OBSConstants.DEFAULT_MEMARTSCC_PYSPARK_OPTIMIZED);
            if (!pySparkOptimizedConf) {
                LOG.error("disable pyspark optimize from config");
            }
            boolean pySparkOptimizedProp = Boolean.parseBoolean(System.getProperty(OBSConstants.MEMARTSCC_PYSPARK_OPTIMIZED,
                    String.valueOf(OBSConstants.DEFAULT_MEMARTSCC_PYSPARK_OPTIMIZED)));
            if (!pySparkOptimizedProp) {
                LOG.error("disable pyspark optimize from properties");
            }
            boolean pySparkOptimized = pySparkOptimizedConf && pySparkOptimizedProp;
            if (!pySparkOptimized) {
                LOG.error("escape in pyspark");
                return true;
            }
        }
        return false;
    }

    private String filterCCConfig(Configuration conf) {
        String prefix = conf.get(OBSConstants.CACHE_CONFIG_PREFIX, OBSConstants.DEFAULT_CACHE_CONFIG_PREFIX);
        if (!prefix.endsWith(".")) {
            prefix = prefix + ".";
        }
        Map<String, String> confMap = conf.getPropsWithPrefix(prefix);
        String az = System.getenv(MRS_AZ_ENV_VARNAME);
        if (az != null && !az.equals("")) {
            confMap.put("zk_root_node", "/memartscc/" + az);
        }
        confMap.put(SECURITY_ENABLE, Boolean.toString(UserGroupInformation.isSecurityEnabled()));
        JSONObject jsonObject = new JSONObject(confMap);
        String jsonStr = jsonObject.toString();
        LOG.info("memArtsCC config json: {}", jsonStr);
        return jsonStr;
    }

    private byte[] getDtFromUgi() throws IOException {
        byte[] dt = this.password.get();
        if (dt != null) {
            return dt;
        }
        Credentials credentials = UserGroupInformation.getLoginUser().getCredentials();
        Text serviceName = new Text(MemArtsCCDelegationTokenProvider.getCanonicalName(conf));
        Token<? extends TokenIdentifier> token = credentials.getToken(serviceName);
        if (token == null) {
            return null;
        }
        dt = token.getPassword();
        this.password.set(dt);
        return dt;
    }

    private String collectOtherInfo(Configuration conf) {
        Map<String, Object> otherInfo = new HashMap<>();
        boolean locality = conf.getBoolean(OBSConstants.MEMARTSCC_LOCALITY_ENABLE, OBSConstants.DEFAULT_MEMARTSCC_LOCALITY_ENABLE);
        otherInfo.put("locality_switch", locality);
        otherInfo.put("client_type", "obsa");
        JSONObject jsonObject = new JSONObject(otherInfo);
        String jsonStr = jsonObject.toString();
        LOG.info("memArtsCC other info json: {}", jsonStr);
        return jsonStr;
    }

    private void startRefreshJob() {
        Thread refreshThread = new Thread(() -> {
            while(true) {
                try {
                    while (true) {
                        if (closed) {
                            this.cache.close();
                            return;
                        }
                        long sleepInSec = akskRefreshInterval;
                        ISecurityKey securityKey = securityProvider.getSecurityKey();
                        if (securityKey instanceof LimitedTimeSecurityKey) {
                            LimitedTimeSecurityKey lsk = (LimitedTimeSecurityKey) securityKey;
                            long expireAt = lsk.getExpiryDate().getTime();
                            long now = LimitedTimeSecurityKey.getUtcTime().getTime();
                            long keyAgeMill = expireAt - now;
                            if (keyAgeMill < akskRefreshInterval * 1000L) {
                                sleepInSec = keyAgeMill / 10000;
                                if (sleepInSec < 1) {
                                    sleepInSec = 1;
                                }
                                LOG.warn("Refresh MemArtsCC AK/SK interval reset to {} sec, "
                                    + "please check fs.obs.memartscc.aksk.refresh.interval", sleepInSec);
                            }
                        }
                        setSecurityKey(securityKey);
                        try {
                            Thread.sleep(sleepInSec * 1000L);
                        } catch (InterruptedException e) {
                            LOG.warn("Refresh ak sk interrupted", e);
                        }
                    }
                } catch (Exception e) {
                    LOG.warn("Refresh ak sk job failed, will trying to restart.", e);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException interruptedException) {
                        LOG.warn("Refresh ak sk interrupted", e);
                    }
                }
            }
        });
        refreshThread.setDaemon(true);
        refreshThread.start();
    }

    private void setSecurityKey(ISecurityKey key) {
        if (key == null) {
            LOG.warn("SecurityKey cannot be set, securityKey is null");
            return;
        }
        this.iSecurityKey.set(key);
    }

    public void close() {
        this.closed = true;
    }

    public int init(String config, String otherInfo) throws IOException {
        return this.cache.init(config, otherInfo);
    }

    public int read(boolean isPrefetch, long prefetchStart, long prefetchEnd, ByteBuffer buf, long offset,
        long len, String objectKey, long modifyTime, String etag, boolean isConsistencyCheck) throws IOException {
        if(!initialized) {
            throw new IOException("MemArtsCCClient read before initializing.");
        }

        ICache.BucketContext bucketContext = getBucketContext();

        ICache.ObjectAttr objectAttr = new ICache.ObjectAttr(objectKey, etag, modifyTime);

        ICache.ReadParam readParam = new ICache.ReadParam(offset, len, prefetchStart, prefetchEnd,
                isPrefetch, isConsistencyCheck, IS_FILE_LAYOUT, bucketContext, objectAttr);

        byte[] dt = getDtFromUgi();

        return this.cache.read(readParam, buf, dt);
    }

    private ICache.BucketContext getBucketContext() {
        String ak = iSecurityKey.get() == null ? "" : iSecurityKey.get().getAccessKey();
        String sk = iSecurityKey.get() == null ? "" : iSecurityKey.get().getSecretKey();
        String securityToken = iSecurityKey.get() == null ? "" : iSecurityKey.get().getSecurityToken();

        return new ICache.BucketContext(ak, sk, securityToken, endpoint, bucket, enablePosix);
    }

    public int getObjectShardInfo(CcGetShardParam ccGetShardParam) {
        return this.cache.getObjectShardInfo(ccGetShardParam);
    }

    public void reportReadStatistics(TrafficStatistics trafficStatistics) {
        if (!initialized || this.cache == null) {
            LOG.debug("MemArtsCCClient is not initialized, statistics cannot be reported.");
            return;
        }

        this.cache.reportReadStatistics(trafficStatistics.getStatistics(TrafficStatistics.TrafficType.Q),
                trafficStatistics.getStatistics(TrafficStatistics.TrafficType.QDot),
                trafficStatistics.getStatistics(TrafficStatistics.TrafficType.Q2),
                trafficStatistics.getStatistics(TrafficStatistics.TrafficType.Q1));
    }

    public byte[] getDT() {
        return this.cache.getDT();
    }

    public static final int CCREAD_RETCODE_CACHEMISS = -100;

    public static class ByteBufferPool {
        private final AtomicBoolean initialized = new AtomicBoolean(false);

        private final LinkedBlockingQueue<ByteBuffer> pool = new LinkedBlockingQueue<>();

        private int maxNum;

        private final AtomicInteger createdNum = new AtomicInteger(0);

        private int bufferSize;

        public void initialize(int maxNum, int bufferSize) {
            if (!initialized.compareAndSet(false, true)) {
                return;
            }
            this.maxNum = maxNum;
            this.bufferSize = bufferSize;
        }

        public ByteBuffer borrowBuffer(int timeout) throws InterruptedException, IOException {
            // 1.从队列中非阻塞获取
            ByteBuffer buffer = pool.poll();
            // 2. 若队列为空，尝试创建
            if (buffer == null) {
                buffer = creatBuffer();
            }
            // 3. 创建失败则等待
            if (buffer == null) {
                if (timeout < 0) {
                    buffer = pool.take();
                } else if (timeout > 0) {
                    buffer = pool.poll(timeout, TimeUnit.MILLISECONDS);
                }
                // 等于0不等待，直接报错
            }
            // 4. 等待不到则报错
            if (buffer == null) {
                throw new IOException("ByteBuffer pool exhausted");
            }
            return buffer;
        }

        public void returnBuffer(ByteBuffer buffer) {
            if (buffer == null) {
                return;
            }
            if (!pool.offer(buffer)) {
                destroyBuffer(buffer);
            }
        }

        private ByteBuffer creatBuffer() {
            final long newCreateCount = createdNum.incrementAndGet();
            if (newCreateCount > maxNum) {
                createdNum.decrementAndGet();
                return null;
            }
            return ByteBuffer.allocateDirect(bufferSize);
        }

        private void destroyBuffer(ByteBuffer buffer) {
            if (buffer instanceof DirectBuffer) {
                ((DirectBuffer)buffer).cleaner().clean();
            }
        }
    }

    public static final ByteBufferPool bufferPool = new ByteBufferPool();
}
