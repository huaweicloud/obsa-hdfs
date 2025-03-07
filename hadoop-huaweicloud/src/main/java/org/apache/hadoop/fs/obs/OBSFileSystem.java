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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;
import com.obs.services.model.AccessControlList;
import com.obs.services.model.fs.accesslabel.GetAccessLabelRequest;
import com.obs.services.model.fs.accesslabel.GetAccessLabelResult;
import com.obs.services.model.fs.accesslabel.SetAccessLabelRequest;
import com.obs.services.model.select.CsvInput;
import com.obs.services.model.select.CsvOutput;
import com.obs.services.model.select.ExpressionType;
import com.obs.services.model.select.FileHeaderInfo;
import com.obs.services.model.select.InputSerialization;
import com.obs.services.model.select.JsonInput;
import com.obs.services.model.select.JsonOutput;
import com.obs.services.model.select.JsonType;
import com.obs.services.model.select.OrcInput;
import com.obs.services.model.select.OutputSerialization;
import com.obs.services.model.select.ScanRange;
import com.obs.services.model.select.SelectObjectRequest;
import com.obs.services.model.select.SelectObjectResult;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.obs.input.OBSInputStream;
import org.apache.hadoop.fs.obs.input.InputPolicyFactory;
import org.apache.hadoop.fs.obs.input.InputPolicys;
import org.apache.hadoop.fs.obs.input.ObsSelectInputStream;
import org.apache.hadoop.fs.obs.memartscc.CcGetShardParam;
import org.apache.hadoop.fs.obs.memartscc.MemArtsCCClient;
import org.apache.hadoop.fs.obs.memartscc.ObjectShard;
import org.apache.hadoop.fs.obs.security.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.obs.services.internal.Constants.CommonHeaders.HASH_CRC32C;

/**
 * The core OBS Filesystem implementation.
 *
 * <p>This subclass is marked as private as code should not be creating it
 * directly; use {@link FileSystem#get(Configuration)} and variants to create
 * one.
 *
 * <p>If cast to {@code OBSFileSystem}, extra methods and features may be
 * accessed. Consider those private and unstable.
 *
 * <p>Because it prints some of the state of the instrumentation, the output of
 * {@link #toString()} must also be considered unstable.
 */
//CHECKSTYLE:OFF
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class OBSFileSystem extends FileSystem implements OpenFileWithJobConf {
    //CHECKSTYLE:ON

    /**
     * Class logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(OBSFileSystem.class);

    /**
     * Flag indicating if the filesystem instance is closed.
     */
    private volatile boolean closed = false;

    /**
     * URI of the filesystem.
     */
    private URI uri;

    /**
     * Current working directory of the filesystem.
     */
    private Path workingDir;

    /**
     * Short name of the user who instantiated the filesystem.
     */
    private String shortUserName;

    private boolean metricSwitch;

    private int invokeCountThreshold = 0;

    private ObsDelegationTokenManger obsDelegationTokenManger;
    /**
     * OBS client instance.
     */
    private ObsClient obs;

    /**
     * memartscc client instance.
     */
    private MemArtsCCClient memArtsCCClient;

    /**
     * Metrics consumer.
     */
    private BasicMetricsConsumer metricsConsumer;

    /**
     * Flag indicating if posix bucket is used.
     */
    private boolean enablePosix = false;

    /**
     * Flag indicating if OBS specific content summary is enabled.
     */
    private boolean obsContentSummaryEnable = true;

    /**
     * Flag indicating if OBS client specific depth first search (DFS) list is
     * enabled.
     */
    private boolean obsClientDFSListEnable = true;

    /**
     * Bucket name.
     */
    private String bucket;

    /**
     * Max number of keys to get while paging through a directory listing.
     */
    private int maxKeys;

    /**
     * OBSListing instance.
     */
    private OBSListing obsListing;

    /**
     * Helper for an ongoing write operation.
     */
    private OBSWriteOperationHelper writeHelper;

    /**
     * Part size for multipart upload.
     */
    private long partSize;

    /**
     * Flag indicating if multi-object delete is enabled.
     */
    private boolean enableMultiObjectDelete;

    /**
     * Minimum number of objects in one multi-object delete call.
     */
    private int multiDeleteThreshold;

    /**
     * Maximum number of entries in one multi-object delete call.
     */
    private int maxEntriesToDelete;

    /**
     * Bounded thread pool for multipart upload.
     */
    private ListeningExecutorService boundedMultipartUploadThreadPool;

    /**
     * Bounded thread pool for copy.
     */
    private ThreadPoolExecutor boundedCopyThreadPool;

    /**
     * Bounded thread pool for delete.
     */
    private ThreadPoolExecutor boundedDeleteThreadPool;

    /**
     * Bounded thread pool for copy part.
     */
    private ThreadPoolExecutor boundedCopyPartThreadPool;

    /**
     * Bounded thread pool for list.
     */
    private ThreadPoolExecutor boundedListThreadPool;

    /**
     * List parallel factor.
     */
    private int listParallelFactor;

    /**
     * Read ahead range.
     */
    private long readAheadRange;

    /**
     * Flag indicating if {@link OBSInputStream#read(long, byte[], int, int)}
     * will be transformed into {@link org.apache.hadoop.fs.FSInputStream#read(
     *long, byte[], int, int)}.
     */
    private boolean readTransformEnable = true;

    /**
     * Factory for creating blocks.
     */
    private OBSDataBlocks.BlockFactory blockFactory;

    private InputPolicyFactory inputPolicyFactory;

    /**
     * Maximum Number of active blocks a single output stream can submit to
     * {@link #boundedMultipartUploadThreadPool}.
     */
    private int blockOutputActiveBlocks;

    /**
     * Copy part size.
     */
    private long copyPartSize;

    /**
     * Flag indicating if fast delete is enabled.
     */
    private boolean enableFastDelete = false;

    private String fastDeleteVersion;

    /**
     * Trash directory for fast delete.
     */
    private String fastDeleteDir;

    private String hdfsTrashVersion;

    private String hdfsTrashPrefix;

    /**
     * OBS redefined access control list.
     */
    private AccessControlList cannedACL;

    /**
     * Server-side encryption wrapper.
     */
    private SseWrapper sse;

    /**
     * Block size for {@link FileSystem#getDefaultBlockSize()}.
     */
    private long blockSize;

    /**
     * Whether to implement  {@link FileSystem#getCanonicalServiceName()} switch.
     */
    private boolean enableCanonicalServiceName = false;

    AuthorizeProvider authorizer;

    /**
     * A map from file names to {@link FSDataOutputStream} objects that are
     * currently being written by this client. Note that a file can only be
     * written by a single filesystem.
     */
    private final Map<String, FSDataOutputStream> filesBeingWritten = new HashMap<>();

    private boolean enableFileVisibilityAfterCreate = false;

    private String readPolicy = OBSConstants.READAHEAD_POLICY_PRIMARY;

    private boolean localityEnabled;

    private TrafficStatisticsReporter statsReporter;

    private TrafficStatistics trafficStatistics;

    private OBSAuditLogger obsAuditLogger;

    private String endpoint;

    private String userName;

    /**
     * obs file system permissions mode.
     */
    private String permissionsMode;

    /**
     * Close all {@link FSDataOutputStream} opened by the owner {@link
     * OBSFileSystem}.
     */
    void closeAllFilesBeingWritten() {
        while (true) {
            String objectKey;
            FSDataOutputStream outputStream;
            synchronized (filesBeingWritten) {
                if (filesBeingWritten.isEmpty()) {
                    break;
                }
                objectKey = filesBeingWritten.keySet().iterator().next();

                outputStream = filesBeingWritten.remove(objectKey);
            }
            if (outputStream != null) {
                try {
                    outputStream.close();
                } catch (IOException ioe) {
                    LOG.warn("Failed to close file: " + objectKey, ioe);
                }
            }
        }
    }

    /**
     * Remove a file from the map of files being written.
     *
     * @param file the file being written
     */
    void removeFileBeingWritten(String file) {
        synchronized (filesBeingWritten) {
            filesBeingWritten.remove(file);
        }
    }

    /**
     * Check if a file is being written.
     *
     * @param file the file to be checked
     */
    boolean isFileBeingWritten(String file) {
        synchronized (filesBeingWritten) {
            return filesBeingWritten.containsKey(file);
        }
    }

    /**
     * Initialize a FileSystem. Called after a new FileSystem instance is
     * constructed.
     *
     * @param name         a URI whose authority section names the host, port,
     *                     etc. for this FileSystem
     * @param originalConf the configuration to use for the FS. The
     *                     bucket-specific options are patched over the base
     *                     ones before any use is made of the config.
     */
    @Override
    public void initialize(final URI name, final Configuration originalConf) throws IOException {
        uri = URI.create(name.getScheme() + "://" + name.getAuthority());
        bucket = name.getAuthority();

        // Delegation token only mode.
        if (originalConf.getBoolean(OBSConstants.DELEGATION_TOKEN_ONLY, OBSConstants.DEFAULT_DELEGATION_TOKEN_ONLY)
            && OBSCommonUtils.isStringEmpty(bucket)) {
            LOG.debug("Delegation-token-only mode");
            obsDelegationTokenManger = OBSCommonUtils.initDelegationTokenManger(this, uri, originalConf);
            if (originalConf.getTrimmed(OBSConstants.READAHEAD_POLICY,
                    OBSConstants.READAHEAD_POLICY_PRIMARY).equals(OBSConstants.READAHEAD_POLICY_MEMARTSCC)) {
                memArtsCCClient = new MemArtsCCClient(bucket, enablePosix);
                boolean initRes = memArtsCCClient.initializeDtOnly(originalConf);
                if (!initRes) {
                    LOG.warn("MemArtsCCClient dt-only initialize failed!");
                }
            }
            return;
        }

        // clone the configuration into one with propagated bucket options
        Configuration conf = OBSCommonUtils.propagateBucketOptions(originalConf, bucket);
        OBSCommonUtils.patchSecurityCredentialProviders(conf);
        super.initialize(name, conf);
        setConf(conf);
        try {
            obsAuditLogger = new OBSAuditLogger();
            userName = getUsernameOrigin();
            endpoint = conf.get("fs.obs.endpoint");
            if (conf.getBoolean(OBSConstants.VERIFY_BUFFER_DIR_ACCESSIBLE_ENABLE, false)) {
                OBSCommonUtils.verifyBufferDirAccessible(conf);
            }
            metricSwitch = conf.getBoolean(OBSConstants.METRICS_SWITCH, OBSConstants.DEFAULT_METRICS_SWITCH);
            invokeCountThreshold = conf.getInt(OBSConstants.METRICS_COUNT, OBSConstants.DEFAULT_METRICS_COUNT);

            // Username is the current user at the time the FS was instantiated.
            shortUserName = UserGroupInformation.getCurrentUser().getShortUserName();
            workingDir = new Path("/user", shortUserName).makeQualified(this.uri, this.getWorkingDirectory());

            Class<? extends OBSClientFactory> obsClientFactoryClass = conf.getClass(
                OBSConstants.OBS_CLIENT_FACTORY_IMPL, OBSConstants.DEFAULT_OBS_CLIENT_FACTORY_IMPL,
                OBSClientFactory.class);
            obs = ReflectionUtils.newInstance(obsClientFactoryClass, conf).createObsClient(name);
            OBSCommonUtils.init(this, conf);

            sse = new SseWrapper(conf);

            Class<? extends BasicMetricsConsumer> metricsConsumerClass = conf.getClass(
                OBSConstants.OBS_METRICS_CONSUMER, OBSConstants.DEFAULT_OBS_METRICS_CONSUMER,
                BasicMetricsConsumer.class);
            if (!metricsConsumerClass.equals(DefaultMetricsConsumer.class) || metricSwitch) {
                try {
                    Constructor<?> cons = metricsConsumerClass.getDeclaredConstructor(URI.class, Configuration.class);
                    metricsConsumer = (BasicMetricsConsumer) cons.newInstance(name, conf);
                } catch (NoSuchMethodException | SecurityException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
                    Throwable c = e.getCause() != null ? e.getCause() : e;
                    throw new IOException("From option " + OBSConstants.OBS_METRICS_CONSUMER, c);
                }
            }

            try {
                enablePosix = OBSCommonUtils.getBucketFsStatus(obs, bucket);
            } catch (IOException e) {
                OBSCommonUtils.setMetricsAbnormalInfo(this, OBSOperateAction.initialize, e);
                throw e;
            }

            maxKeys = OBSCommonUtils.intOption(conf, OBSConstants.MAX_PAGING_KEYS, OBSConstants.DEFAULT_MAX_PAGING_KEYS,
                1);
            obsListing = new OBSListing(this);
            partSize = OBSCommonUtils.getMultipartSizeProperty(conf, OBSConstants.MULTIPART_SIZE,
                OBSConstants.DEFAULT_MULTIPART_SIZE);

            // check but do not store the block size
            blockSize = OBSCommonUtils.longBytesOption(conf, OBSConstants.FS_OBS_BLOCK_SIZE,
                OBSConstants.DEFAULT_FS_OBS_BLOCK_SIZE, 1);
            enableMultiObjectDelete = conf.getBoolean(OBSConstants.ENABLE_MULTI_DELETE, true);
            maxEntriesToDelete = conf.getInt(OBSConstants.MULTI_DELETE_MAX_NUMBER,
                OBSConstants.DEFAULT_MULTI_DELETE_MAX_NUMBER);
            obsContentSummaryEnable = conf.getBoolean(OBSConstants.OBS_CONTENT_SUMMARY_ENABLE, true);
            readAheadRange = OBSCommonUtils.longBytesOption(conf, OBSConstants.READAHEAD_RANGE,
                OBSConstants.DEFAULT_READAHEAD_RANGE, 0);
            readTransformEnable = conf.getBoolean(OBSConstants.READAHEAD_TRANSFORM_ENABLE, true);
            multiDeleteThreshold = conf.getInt(OBSConstants.MULTI_DELETE_THRESHOLD,
                OBSConstants.MULTI_DELETE_DEFAULT_THRESHOLD);

            initThreadPools(conf);

            writeHelper = new OBSWriteOperationHelper(this);

            initCannedAcls(conf);

            OBSCommonUtils.initMultipartUploads(this, conf);

            String blockOutputBuffer = conf.getTrimmed(OBSConstants.FAST_UPLOAD_BUFFER,
                OBSConstants.FAST_UPLOAD_BUFFER_DISK);
            partSize = OBSCommonUtils.ensureOutputParameterInRange(OBSConstants.MULTIPART_SIZE, partSize);
            blockFactory = OBSDataBlocks.createFactory(this, blockOutputBuffer);
            blockOutputActiveBlocks = OBSCommonUtils.intOption(conf, OBSConstants.FAST_UPLOAD_ACTIVE_BLOCKS,
                OBSConstants.DEFAULT_FAST_UPLOAD_ACTIVE_BLOCKS, 1);
            LOG.debug("Using OBSBlockOutputStream with buffer = {}; block={};" + " queue limit={}", blockOutputBuffer,
                partSize, blockOutputActiveBlocks);

            readPolicy = conf.getTrimmed(OBSConstants.READAHEAD_POLICY, OBSConstants.READAHEAD_POLICY_PRIMARY);
            inputPolicyFactory = InputPolicys.createFactory(readPolicy);

            // initialize MemArtsCC
            if (readPolicy.equals(OBSConstants.READAHEAD_POLICY_MEMARTSCC)) {
                if (initMemArtsCC(conf, name)) {
                    // Traffic Report
                    initTrafficReport(conf);
                }
            }

            obsDelegationTokenManger = OBSCommonUtils.initDelegationTokenManger(this, uri, conf);
            localityEnabled = conf.getBoolean(OBSConstants.MEMARTSCC_LOCALITY_ENABLE, OBSConstants.DEFAULT_MEMARTSCC_LOCALITY_ENABLE);

            hdfsTrashVersion = conf.get(OBSConstants.HDFS_TRASH_VERSION, OBSConstants.HDFS_TRASH_VERSION_V1);
            hdfsTrashPrefix = conf.get(OBSConstants.HDFS_TRASH_PREFIX, OBSConstants.DEFAULT_HDFS_TRASH_PREFIX);
            enableFastDelete = conf.getBoolean(OBSConstants.FAST_DELETE_ENABLE, OBSConstants.DEFAULT_FAST_DELETE_ENABLE);
            if (enableFastDelete) {
                if (!isFsBucket()) {
                    String errorMsg = String.format("The bucket [%s] is not posix. not supported for " + "trash.",
                        bucket);
                    LOG.warn(errorMsg);
                    enableFastDelete = false;
                    fastDeleteDir = null;
                } else {
                    String fastDelete = "FastDelete";
                    String defaultTrashDir = OBSCommonUtils.maybeAddTrailingSlash(
                        this.getTrashRoot(new Path(fastDelete)).toUri().getPath()) + fastDelete;
                    fastDeleteDir = conf.get(OBSConstants.FAST_DELETE_DIR, defaultTrashDir);
                    if (OBSCommonUtils.isStringEmpty(fastDeleteDir)) {
                        String errorMsg = String.format(
                            "The fast delete feature(fs.obs.trash.enable) is " + "enabled, but the "
                                + "configuration(fs.obs.trash.dir [%s]) " + "is empty.", fastDeleteDir);
                        LOG.error(errorMsg);
                        throw new ObsException(errorMsg);
                    }
                    fastDeleteDir = OBSCommonUtils.maybeAddBeginningSlash(fastDeleteDir);
                    fastDeleteDir = OBSCommonUtils.maybeAddTrailingSlash(fastDeleteDir);
                    fastDeleteVersion = conf.get(OBSConstants.FAST_DELETE_VERSION, OBSConstants.FAST_DELETE_VERSION_V1);
                }
            }

            OBSCommonUtils.setRetryTime(conf.getLong(OBSConstants.RETRY_MAXTIME, OBSConstants.DEFAULT_RETRY_MAXTIME),
                conf.getLong(OBSConstants.RETRY_SLEEP_BASETIME, OBSConstants.DEFAULT_RETRY_SLEEP_BASETIME),
                conf.getLong(OBSConstants.RETRY_SLEEP_MAXTIME, OBSConstants.DEFAULT_RETRY_SLEEP_MAXTIME));
            enableFileVisibilityAfterCreate =
                conf.getBoolean(OBSConstants.FILE_VISIBILITY_AFTER_CREATE_ENABLE,
                    OBSConstants.DEFAULT_FILE_VISIBILITY_AFTER_CREATE_ENABLE);
            enableCanonicalServiceName = conf.getBoolean(OBSConstants.GET_CANONICAL_SERVICE_NAME_ENABLE,
                OBSConstants.DEFAULT_GET_CANONICAL_SERVICE_NAME_ENABLE);
            permissionsMode = conf.get(OBSConstants.PERMISSIONS_MODE, OBSConstants.DEFAULT_PERMISSIONS_MODE);
            this.authorizer = initAuthorizeProvider(conf);
        } catch (ObsException e) {
            LOG.error("initializing OBSFilesystem fail", e);
            throw OBSCommonUtils.translateException("initializing ", new Path(name), e);
        }

        LOG.info("Finish initializing filesystem instance for uri: {}", uri);
    }

    private void initTrafficReport(final Configuration conf) {
        boolean reportEnable = conf.getBoolean(OBSConstants.MEMARTSCC_TRAFFIC_REPORT_ENABLE, OBSConstants.DEFAULT_MEMARTSCC_TRAFFIC_REPORT_ENABLE);
        LOG.debug("get report enable from config:{}", reportEnable);
        if (!reportEnable) {
            return;
         }

        trafficStatistics = new TrafficStatistics();
        long interval = getConf().
                getLong(OBSConstants.MEMARTSCC_TRAFFIC_REPORT_INTERVAL, OBSConstants.MEMARTSCC_TRAFFIC_REPORT_DEFAULT_INTERVAL);
        LOG.debug("get report traffic interval from config: interval:{}", interval);
        statsReporter = new TrafficStatisticsReporter(trafficStatistics, memArtsCCClient, interval);
        statsReporter.startReport();
    }

    private AuthorizeProvider initAuthorizeProvider(Configuration conf) throws IOException {
        AuthorizeProvider authorizeProvider = null;
        Class<?> authClassName = conf.getClass(OBSConstants.AUTHORIZER_PROVIDER, null);
        if (authClassName != null) {
            try {
                LOG.info("authorize provider is " + authClassName.getName());
                authorizeProvider = (AuthorizeProvider)authClassName.newInstance();
                authorizeProvider.init(conf);
            } catch (Exception e) {
                LOG.error(String.format("init %s failed", OBSConstants.AUTHORIZER_PROVIDER), e);
                throw new IOException(String.format("init %s failed", OBSConstants.AUTHORIZER_PROVIDER), e);
            }
        }

        return authorizeProvider;
    }

    private void checkPermission(Path path, AccessType accessType) throws IOException {
        if (authorizer == null) {
            LOG.debug("authorize provider is not initialized. No authorization will be performed.");
        } else {
            boolean failFallback = this.getConf().getBoolean(OBSConstants.AUTHORIZE_FAIL_FALLBACK,
                OBSConstants.DEFAULT_AUTHORIZE_FAIL_FALLBACK);
            boolean exceptionFallback = this.getConf().getBoolean(OBSConstants.AUTHORIZE_EXCEPTION_FALLBACK,
                OBSConstants.DEFAULT_AUTHORIZE_EXCEPTION_FALLBACK);

            String key = OBSCommonUtils.pathToKey(this, path);
            Boolean result = true;
            UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
            try {
                long st = System.currentTimeMillis();
                result = authorizer.isAuthorized(this.bucket, key, accessType);
                long et = System.currentTimeMillis();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("authorizing:[user: {}], [action: {}], "
                            + "[bucket: {}], [path: {}], [result: {}], [cost: {}]",
                        currentUser,
                        accessType.toString(),
                        this.bucket,
                        path.toString(),
                        result, et - st);
                }
            } catch (Exception e) {
                if (exceptionFallback) {
                    LOG.warn("authorize exception fallback:[user: {}], [action: {}], [bucket: {}], [key: {}]",
                        currentUser, accessType.toString(),this.bucket,key);
                } else {
                    throw e;
                }
            }
            if (!result) {
                if (failFallback) {
                    LOG.warn("authorize fail fallback:[user: {}], [action: {}], [bucket: {}], [key: {}]",
                        currentUser, accessType.toString(),this.bucket,key);
                } else {
                    throw new OBSAuthorizationException(String.format("permission denied:[user: %s], [action: %s], "
                            + "[bucket: %s], [key: %s]",
                        currentUser,
                        accessType.toString(),
                        this.bucket,
                        key));
                }
            }
        }
    }

    private boolean initMemArtsCC(final Configuration conf, final URI name) {
        String obsBucketName = getBucket();
        memArtsCCClient = new MemArtsCCClient(obsBucketName, enablePosix);
        if (!memArtsCCClient.initialize(name, conf)) {
            LOG.warn("fallback to 'primary' read policy");
            // fall back to 'primary' read policy
            inputPolicyFactory = InputPolicys.createFactory(OBSConstants.READAHEAD_POLICY_PRIMARY);
            // do not set memArtsCCClient to null
            return false;
        }
        return true;
    }

    private void initThreadPools(final Configuration conf) {
        long keepAliveTime = OBSCommonUtils.longOption(conf, OBSConstants.KEEPALIVE_TIME,
            OBSConstants.DEFAULT_KEEPALIVE_TIME, 0);

        int maxThreads = conf.getInt(OBSConstants.MAX_THREADS, OBSConstants.DEFAULT_MAX_THREADS);
        if (maxThreads < 2) {
            LOG.warn(OBSConstants.MAX_THREADS + " must be at least 2: forcing to 2.");
            maxThreads = 2;
        }
        int totalTasks = OBSCommonUtils.intOption(conf, OBSConstants.MAX_TOTAL_TASKS,
            OBSConstants.DEFAULT_MAX_TOTAL_TASKS, 1);
        boundedMultipartUploadThreadPool = BlockingThreadPoolExecutorService.newInstance(maxThreads,
            maxThreads + totalTasks, keepAliveTime, "obs-transfer-shared");

        int maxDeleteThreads = conf.getInt(OBSConstants.MAX_DELETE_THREADS, OBSConstants.DEFAULT_MAX_DELETE_THREADS);
        if (maxDeleteThreads < 2) {
            LOG.warn(OBSConstants.MAX_DELETE_THREADS + " must be at least 2: forcing to 2.");
            maxDeleteThreads = 2;
        }
        int coreDeleteThreads = (int) Math.ceil(maxDeleteThreads / 2.0);
        boundedDeleteThreadPool = new ThreadPoolExecutor(coreDeleteThreads, maxDeleteThreads, keepAliveTime,
            TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
            BlockingThreadPoolExecutorService.newDaemonThreadFactory("obs-delete-transfer-shared"));
        boundedDeleteThreadPool.allowCoreThreadTimeOut(true);

        if (enablePosix) {
            obsClientDFSListEnable = conf.getBoolean(OBSConstants.OBS_CLIENT_DFS_LIST_ENABLE, true);
            if (obsClientDFSListEnable) {
                int coreListThreads = conf.getInt(OBSConstants.CORE_LIST_THREADS,
                    OBSConstants.DEFAULT_CORE_LIST_THREADS);
                int maxListThreads = conf.getInt(OBSConstants.MAX_LIST_THREADS, OBSConstants.DEFAULT_MAX_LIST_THREADS);
                int listWorkQueueCapacity = conf.getInt(OBSConstants.LIST_WORK_QUEUE_CAPACITY,
                    OBSConstants.DEFAULT_LIST_WORK_QUEUE_CAPACITY);
                listParallelFactor = conf.getInt(OBSConstants.LIST_PARALLEL_FACTOR,
                    OBSConstants.DEFAULT_LIST_PARALLEL_FACTOR);
                if (listParallelFactor < 1) {
                    LOG.warn(OBSConstants.LIST_PARALLEL_FACTOR + " must be at least 1: forcing to 1.");
                    listParallelFactor = 1;
                }
                boundedListThreadPool = new ThreadPoolExecutor(coreListThreads, maxListThreads, keepAliveTime,
                    TimeUnit.SECONDS, new LinkedBlockingQueue<>(listWorkQueueCapacity),
                    BlockingThreadPoolExecutorService.newDaemonThreadFactory("obs-list-transfer-shared"));
                boundedListThreadPool.allowCoreThreadTimeOut(true);
            }
        } else {
            int maxCopyThreads = conf.getInt(OBSConstants.MAX_COPY_THREADS, OBSConstants.DEFAULT_MAX_COPY_THREADS);
            if (maxCopyThreads < 2) {
                LOG.warn(OBSConstants.MAX_COPY_THREADS + " must be at least 2: forcing to 2.");
                maxCopyThreads = 2;
            }
            int coreCopyThreads = (int) Math.ceil(maxCopyThreads / 2.0);
            boundedCopyThreadPool = new ThreadPoolExecutor(coreCopyThreads, maxCopyThreads, keepAliveTime,
                TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
                BlockingThreadPoolExecutorService.newDaemonThreadFactory("obs-copy-transfer-shared"));
            boundedCopyThreadPool.allowCoreThreadTimeOut(true);

            copyPartSize = OBSCommonUtils.longOption(conf, OBSConstants.COPY_PART_SIZE,
                OBSConstants.DEFAULT_COPY_PART_SIZE, 0);
            if (copyPartSize > OBSConstants.MAX_COPY_PART_SIZE) {
                LOG.warn("obs: {} capped to ~5GB (maximum allowed part size with " + "current output mechanism)",
                    OBSConstants.COPY_PART_SIZE);
                copyPartSize = OBSConstants.MAX_COPY_PART_SIZE;
            }

            int maxCopyPartThreads = conf.getInt(OBSConstants.MAX_COPY_PART_THREADS,
                OBSConstants.DEFAULT_MAX_COPY_PART_THREADS);
            if (maxCopyPartThreads < 2) {
                LOG.warn(OBSConstants.MAX_COPY_PART_THREADS + " must be at least 2: forcing to 2.");
                maxCopyPartThreads = 2;
            }
            int coreCopyPartThreads = (int) Math.ceil(maxCopyPartThreads / 2.0);
            boundedCopyPartThreadPool = new ThreadPoolExecutor(coreCopyPartThreads, maxCopyPartThreads, keepAliveTime,
                TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
                BlockingThreadPoolExecutorService.newDaemonThreadFactory("obs-copy-part-transfer-shared"));
            boundedCopyPartThreadPool.allowCoreThreadTimeOut(true);
        }
    }

    /**
     * Is posix bucket or not.
     *
     * @return is it posix bucket
     */
    public boolean isFsBucket() {
        return enablePosix;
    }

    /**
     * Get read transform switch stat.
     *
     * @return is read transform enabled
     */
    public boolean isReadTransformEnabled() {
        return readTransformEnable;
    }

    /**
     * Initialize bucket acl for upload, write operation.
     *
     * @param conf the configuration to use for the FS.
     */
    private void initCannedAcls(final Configuration conf) {
        // No canned acl in obs
        String cannedACLName = conf.get(OBSConstants.CANNED_ACL, OBSConstants.DEFAULT_CANNED_ACL);
        if (!cannedACLName.isEmpty()) {
            switch (cannedACLName) {
                case "Private":
                    cannedACL = AccessControlList.REST_CANNED_PRIVATE;
                    break;
                case "PublicRead":
                    cannedACL = AccessControlList.REST_CANNED_PUBLIC_READ;
                    break;
                case "PublicReadWrite":
                    cannedACL = AccessControlList.REST_CANNED_PUBLIC_READ_WRITE;
                    break;
                case "AuthenticatedRead":
                    cannedACL = AccessControlList.REST_CANNED_AUTHENTICATED_READ;
                    break;
                case "LogDeliveryWrite":
                    cannedACL = AccessControlList.REST_CANNED_LOG_DELIVERY_WRITE;
                    break;
                case "BucketOwnerRead":
                    cannedACL = AccessControlList.REST_CANNED_BUCKET_OWNER_READ;
                    break;
                case "BucketOwnerFullControl":
                    cannedACL = AccessControlList.REST_CANNED_BUCKET_OWNER_FULL_CONTROL;
                    break;
                default:
                    cannedACL = null;
            }
        } else {
            cannedACL = null;
        }
    }

    /**
     * Get the bucket acl of user setting.
     *
     * @return bucket acl {@link AccessControlList}
     */
    AccessControlList getCannedACL() {
        return cannedACL;
    }

    /**
     * Return the protocol scheme for the FileSystem.
     *
     * @return "obs"
     */
    @Override
    public String getScheme() {
        return "obs";
    }

    /**
     * Return a URI whose scheme and authority identify this FileSystem.
     *
     * @return the URI of this filesystem.
     */
    @Override
    public URI getUri() {
        return uri;
    }

    /**
     * Return the default port for this FileSystem.
     *
     * @return -1 to indicate the port is undefined, which agrees with the
     * contract of {@link URI#getPort()}
     */
    @Override
    public int getDefaultPort() {
        return OBSConstants.OBS_DEFAULT_PORT;
    }

    /**
     * Return the OBS client used by this filesystem.
     *
     * @return OBS client
     */
    @VisibleForTesting
    public ObsClient getObsClient() {
        return obs;
    }

    /**
     * Return the MemArtsCC Client used by this filesystem.
     *
     * @return MemArtsCCClient
     */
    @VisibleForTesting
    public MemArtsCCClient getMemArtsCCClient() {
        return memArtsCCClient;
    }

    /**
     * Return the read ahead range used by this filesystem.
     *
     * @return read ahead range
     */
    @VisibleForTesting
    long getReadAheadRange() {
        return readAheadRange;
    }

    /**
     * Return the bucket of this filesystem.
     *
     * @return the bucket
     */
    String getBucket() {
        return bucket;
    }

    /**
     * Check that a Path belongs to this FileSystem. Unlike the superclass, this
     * version does not look at authority, but only hostname.
     *
     * @param path the path to check
     * @throws IllegalArgumentException if there is an FS mismatch
     */
    @Override
    public void checkPath(final Path path) {
        OBSLoginHelper.checkPath(getConf(), getUri(), path, getDefaultPort());
    }

    /**
     * Canonicalize the given URI.
     *
     * @param rawUri the URI to be canonicalized
     * @return the canonicalized URI
     */
    @Override
    protected URI canonicalizeUri(final URI rawUri) {
        return OBSLoginHelper.canonicalizeUri(rawUri, getDefaultPort());
    }

    /**
     * Open an FSDataInputStream at the indicated Path with the given configuration.
     *
     * @param f
     * @param jobConf
     * @return
     * @throws IOException
     */
    @Override
    public FSDataInputStream open(Path f, Configuration jobConf) throws IOException {
        String sqlExpr = jobConf.get("mapreduce.job.input.file.option.fs.obs.select.sql");
        if (sqlExpr != null) {
            String key = OBSCommonUtils.pathToKey(this, f);
            String scanStart = jobConf.get("mapreduce.job.input.file.option.fs.obs.select.scan.start");
            String scanEnd = jobConf.get("mapreduce.job.input.file.option.fs.obs.select.scan.end");
            LOG.info("OBSFileSystem.open(): bucket: {}; key: {}; sql: {}; range: [{}, {}]%n",
                bucket, key,
                sqlExpr,
                scanStart != null ? scanStart : "n/a",
                scanEnd != null ? scanEnd : "n/a");

            return select(f, sqlExpr, jobConf);
        }
        return open(f);
    }

    /**
     * Open an FSDataInputStream at the indicated Path.
     *
     * @param f          the file path to open
     * @param bufferSize the size of the buffer to be used
     * @return the FSDataInputStream for the file
     * @throws IOException on any failure to open the file
     */
    @Override
    public FSDataInputStream open(final Path f, final int bufferSize) throws IOException {
        checkOpen();
        long timeBeginStamp =System.currentTimeMillis();

        LOG.debug("Opening '{}' for reading.", f);
        final OBSFileStatus fileStatus;
        try {
            fileStatus = OBSCommonUtils.getFileStatusWithRetry(this, f);
        } catch (OBSFileConflictException e) {
            throw new AccessControlException(e);
        }

        if (fileStatus.isDirectory()) {
            throw new FileNotFoundException("Can't open " + f + " because it is a directory");
        }
        checkPermission(f, AccessType.READ);
        FSInputStream fsInputStream = inputPolicyFactory.create(this, bucket, OBSCommonUtils.pathToKey(this, f),
            fileStatus.getLen(), statistics, boundedMultipartUploadThreadPool, fileStatus);

        CallerContext ctx = CallerContext.getCurrent();
        long timespan = System.currentTimeMillis() - timeBeginStamp;
        obsAuditLogger.logAuditEvent(true, timespan, "open", OBSCommonUtils.pathToKey(this, f), null, null, (ctx==null?null:ctx.getContext()), userName, endpoint);

        return new FSDataInputStream(fsInputStream);
    }

    private FSDataInputStream select(Path f, String sqlExpr, Configuration jobConf) throws IOException {
        final FileStatus fileStatus = getFileStatus(f);
        if (fileStatus.isDirectory()) {
            throw new FileNotFoundException("Can't open " + f + " because it is a directory");
        }

        String key = OBSCommonUtils.pathToKey(this, f);

        final String obsSelectJobPath = "mapreduce.job.input.file.option.fs.obs.select.";
        String fileFormat = jobConf.get(obsSelectJobPath + "format");
        if (fileFormat == null) {
            throw new IllegalArgumentException("file format is missing");
        }

        if (!fileFormat.equals("orc") &&
            !fileFormat.equals("csv") &&
            !fileFormat.equals("json")) {
            throw new IllegalArgumentException("invalid file format '" + fileFormat + "', it must be one of { 'csv', 'json', 'orc' }");
        }

        SelectObjectRequest selectRequest = new SelectObjectRequest()
                                                    .withExpression(sqlExpr)
                                                    .withBucketName(bucket)
                                                    .withKey(key)
                                                    .withExpressionType(ExpressionType.SQL);

        if (!fileFormat.equals("json")) {
            // prepare input & output for CSV & ORC
            CsvOutput output = new CsvOutput();
            String delimiter = jobConf.get(obsSelectJobPath + "output.csv.delimiter");
            if (delimiter != null) {
                if (delimiter.length() != 1) {
                    throw new IllegalArgumentException("Invalid output delimiter " + delimiter);
                }

                output.withFieldDelimiter(delimiter.charAt(0));
            }

            // prepare the request
            if (fileFormat.equals("csv")) {
                CsvInput input = new CsvInput();

                delimiter = jobConf.get(obsSelectJobPath + "input.csv.delimiter");
                if (delimiter != null) {
                    if (delimiter.length() != 1)
                        throw new IllegalArgumentException("Invalid input delimiter " + delimiter);

                    input.withFieldDelimiter(delimiter.charAt(0));
                }

                String headerInfo = jobConf.get(obsSelectJobPath + "input.csv.header");
                if (headerInfo != null) {
                    boolean found = false;
                    for (FileHeaderInfo enumEntry : FileHeaderInfo.values()) {
                        found = (enumEntry.toString().equals(headerInfo));
                        if (found) {
                            input.withFileHeaderInfo(enumEntry);
                            break;
                        }
                    }

                    if (!found) {
                        throw new IllegalArgumentException("Invalid header " + headerInfo);
                    }
                }

                selectRequest.withInputSerialization(
                                new InputSerialization()
                                    .withCsv(input)
                            ).withOutputSerialization(
                                new OutputSerialization()
                                    .withCsv(output)
                            );

            } else {
                // it is ORC
                selectRequest.withInputSerialization(
                                new InputSerialization()
                                    .withOrc(
                                        new OrcInput()
                                    )
                            ).withOutputSerialization(
                                new OutputSerialization()
                                    .withCsv(output)
                            );
            }
        } else {
            // it is JSON
            JsonType type = jobConf.get(obsSelectJobPath + "input.json.type").equals("lines") ?
                                        JsonType.LINES : JsonType.DOCUMENT;

            selectRequest.withInputSerialization(
                            new InputSerialization()
                                .withJson(
                                    new JsonInput()
                                        .withType(type)
                                )
                        ).withOutputSerialization(
                            new OutputSerialization()
                                .withJson(
                                    new JsonOutput()
                                )
                        );
        }

        // Set Scan Range properties.
        String scanStart = jobConf.get(obsSelectJobPath + "scan.start");
        String scanEnd = jobConf.get(obsSelectJobPath + "scan.end");
        if (scanStart != null && scanEnd != null) {
            selectRequest.withScanRange(
                            new ScanRange()
                                .withStart(Long.parseLong(scanStart))
                                .withEnd(Long.parseLong(scanEnd)));
        }

        LOG.info("OBSFileSystem.select(): bucket: {}; key: {}; sql: {}; range: [{}, {}]\n",
            bucket, key, sqlExpr,
            scanStart != null ? scanStart : "n/a",
            scanEnd != null ? scanEnd : "n/a");

        SelectObjectResult selectResult = obs.selectObjectContent(selectRequest);
        return new FSDataInputStream(
                    new ObsSelectInputStream(bucket, key, selectResult));
    }

    /**
     * Create an FSDataOutputStream at the indicated Path with write-progress
     * reporting.
     *
     * @param f           the file path to create
     * @param permission  the permission to set
     * @param overwrite   if a file with this name already exists, then if true,
     *                    the file will be overwritten, and if false an error
     *                    will be thrown
     * @param bufferSize  the size of the buffer to be used
     * @param replication required block replication for the file
     * @param blkSize     the requested block size
     * @param progress    the progress reporter
     * @throws IOException on any failure to create the file
     * @see #setPermission(Path, FsPermission)
     */
    @Override
    public FSDataOutputStream create(final Path f, final FsPermission permission, final boolean overwrite,
        final int bufferSize, final short replication, final long blkSize, final Progressable progress)
        throws IOException {
        checkOpen();
        long timeBeginStamp =System.currentTimeMillis();

        String key = OBSCommonUtils.pathToKey(this, f);
        final FileStatus status;
        boolean exist = true;
        try {
            // get the status or throw an exception
            try {
                status = OBSCommonUtils.getFileStatusWithRetry(this, f);
            } catch (OBSFileConflictException e) {
                throw new ParentNotDirectoryException(e.getMessage());
            }

            // if the thread reaches here, there is something at the path
            if (status.isDirectory()) {
                // path references a directory: automatic error
                throw new FileAlreadyExistsException(f + " is a directory");
            }
            if (!overwrite) {
                // path references a file and overwrite is disabled
                throw new FileAlreadyExistsException(f + " already exists");
            }
            LOG.debug("create: Overwriting file {}", f);
        } catch (FileNotFoundException e) {
            // this means the file is not found
            LOG.debug("create: Creating new file {}", f);
            exist = false;
        }

        checkPermission(f,AccessType.WRITE);
        FSDataOutputStream outputStream = new FSDataOutputStream(new OBSBlockOutputStream(this, key, 0,
            new SemaphoredDelegatingExecutor(boundedMultipartUploadThreadPool, blockOutputActiveBlocks, true), false),
            null);

        if (enableFileVisibilityAfterCreate && !exist) {
            outputStream.close();
            outputStream = new FSDataOutputStream(new OBSBlockOutputStream(this, key, 0,
                new SemaphoredDelegatingExecutor(boundedMultipartUploadThreadPool, blockOutputActiveBlocks, true),
                false), null);
        }

        synchronized (filesBeingWritten) {
            filesBeingWritten.put(key, outputStream);
        }

        CallerContext ctx = CallerContext.getCurrent();
        long timespan = System.currentTimeMillis() - timeBeginStamp;
        obsAuditLogger.logAuditEvent(true, timespan, "create", key, null, null, (ctx==null?null:ctx.getContext()), userName, endpoint);

        return outputStream;
    }

    /**
     * Return the part size for multipart upload used by {@link
     * OBSBlockOutputStream}.
     *
     * @return the part size
     */
    long getPartSize() {
        return partSize;
    }

    /**
     * Return the block factory used by {@link OBSBlockOutputStream}.
     *
     * @return the block factory
     */
    OBSDataBlocks.BlockFactory getBlockFactory() {
        return blockFactory;
    }

    /**
     * Return the write helper used by {@link OBSBlockOutputStream}.
     *
     * @return the write helper
     */
    OBSWriteOperationHelper getWriteHelper() {
        return writeHelper;
    }

    /**
     * Create an FSDataOutputStream at the indicated Path with write-progress
     * reporting.
     *
     * @param f           the file name to create
     * @param permission  permission of
     * @param flags       {@link CreateFlag}s to use for this stream
     * @param bufferSize  the size of the buffer to be used
     * @param replication required block replication for the file
     * @param blkSize     block size
     * @param progress    progress
     * @param checksumOpt check sum option
     * @throws IOException io exception
     */
    @Override
    public FSDataOutputStream create(final Path f, final FsPermission permission, final EnumSet<CreateFlag> flags,
        final int bufferSize, final short replication, final long blkSize, final Progressable progress,
        final ChecksumOpt checksumOpt) throws IOException {
        checkOpen();
        long timeBeginStamp =System.currentTimeMillis();

        LOG.debug("create: Creating new file {}, flags:{}, isFsBucket:{}", f, flags, isFsBucket());
        OBSCommonUtils.checkCreateFlag(flags);
        FSDataOutputStream outputStream;
        if (null != flags && flags.contains(CreateFlag.APPEND)) {
            if (!isFsBucket()) {
                throw new UnsupportedOperationException(
                    "non-posix bucket. Append is not supported by " + "OBSFileSystem");
            }
            String key = OBSCommonUtils.pathToKey(this, f);
            FileStatus status;
            boolean exist = true;
            try {
                // get the status or throw an FNFE
                try {
                    status = OBSCommonUtils.getFileStatusWithRetry(this, f);
                } catch (OBSFileConflictException e) {
                    throw new ParentNotDirectoryException(e.getMessage());
                }

                // if the thread reaches here, there is something at the path
                if (status.isDirectory()) {
                    // path references a directory: automatic error
                    throw new FileAlreadyExistsException(f + " is a directory");
                }
            } catch (FileNotFoundException e) {
                LOG.debug("FileNotFoundException, create: Creating new file {}", f);
                exist = false;
            }

            checkPermission(f,AccessType.WRITE);
            outputStream = new FSDataOutputStream(new OBSBlockOutputStream(this, key, 0,
                new SemaphoredDelegatingExecutor(boundedMultipartUploadThreadPool, blockOutputActiveBlocks, true),
                true), null);
            if (enableFileVisibilityAfterCreate && !exist) {
                outputStream.close();
                outputStream = new FSDataOutputStream(new OBSBlockOutputStream(this, key, 0,
                    new SemaphoredDelegatingExecutor(boundedMultipartUploadThreadPool, blockOutputActiveBlocks, true),
                    true), null);
            }
            synchronized (filesBeingWritten) {
                filesBeingWritten.put(key, outputStream);
            }
        } else {
            outputStream = create(f, permission, flags == null || flags.contains(CreateFlag.OVERWRITE), bufferSize,
                replication, blkSize, progress);
        }

        CallerContext ctx = CallerContext.getCurrent();
        long timespan = System.currentTimeMillis() - timeBeginStamp;
        obsAuditLogger.logAuditEvent(true, timespan, "create", OBSCommonUtils.pathToKey(this, f), null, null, (ctx==null?null:ctx.getContext()), userName, endpoint);

        return outputStream;
    }

    /**
     * Open an FSDataOutputStream at the indicated Path with write-progress
     * reporting. Same as create(), except fails if parent directory doesn't
     * already exist.
     *
     * @param path        the file path to create
     * @param permission  file permission
     * @param flags       {@link CreateFlag}s to use for this stream
     * @param bufferSize  the size of the buffer to be used
     * @param replication required block replication for the file
     * @param blkSize     block size
     * @param progress    the progress reporter
     * @throws IOException IO failure
     */
    @Override
    public FSDataOutputStream createNonRecursive(final Path path, final FsPermission permission,
        final EnumSet<CreateFlag> flags, final int bufferSize, final short replication, final long blkSize,
        final Progressable progress) throws IOException {
        checkOpen();
        long timeBeginStamp =System.currentTimeMillis();

        OBSCommonUtils.checkCreateFlag(flags);
        if (path.getParent() != null && !this.exists(path.getParent())) {
            throw new FileNotFoundException(path.toString() + " parent directory not exist.");
        }

        CallerContext ctx = CallerContext.getCurrent();
        long timespan = System.currentTimeMillis() - timeBeginStamp;
        obsAuditLogger.logAuditEvent(true, timespan, "createNonRecursive", OBSCommonUtils.pathToKey(this, path), null, null, (ctx==null?null:ctx.getContext()), userName, endpoint);

        return create(path, permission, flags.contains(CreateFlag.OVERWRITE),
            bufferSize, replication, blkSize, progress);
    }

    /**
     * Append to an existing file (optional operation).
     *
     * @param f          the existing file to be appended
     * @param bufferSize the size of the buffer to be used
     * @param progress   for reporting progress if it is not null
     * @throws IOException indicating that append is not supported
     */
    @Override
    public FSDataOutputStream append(final Path f, final int bufferSize, final Progressable progress)
        throws IOException {
        checkOpen();
        long timeBeginStamp =System.currentTimeMillis();

        if (!isFsBucket()) {
            throw new UnsupportedOperationException("non-posix bucket. Append is not supported " + "by OBSFileSystem");
        }
        LOG.debug("append: Append file {}.", f);
        String key = OBSCommonUtils.pathToKey(this, f);

        // get the status or throw an FNFE
        FileStatus status;
        try {
            status = OBSCommonUtils.getFileStatusWithRetry(this, f);
        } catch (OBSFileConflictException e) {
            throw new AccessControlException(e);
        }

        long objectLen = status.getLen();
        // if the thread reaches here, there is something at the path
        if (status.isDirectory()) {
            // path references a directory: automatic error
            throw new FileAlreadyExistsException(f + " is a directory");
        }

        if (isFileBeingWritten(key)) {
            // AlreadyBeingCreatedException (on HDFS NameNode) is transformed
            // into IOException (on HDFS Client)
            throw new IOException("Cannot append " + f + " that is being written.");
        }

        checkPermission(f,AccessType.WRITE);
        FSDataOutputStream outputStream = new FSDataOutputStream(new OBSBlockOutputStream(this, key, objectLen,
            new SemaphoredDelegatingExecutor(boundedMultipartUploadThreadPool, blockOutputActiveBlocks, true), true),
            null,objectLen);
        synchronized (filesBeingWritten) {
            filesBeingWritten.put(key, outputStream);
        }

        CallerContext ctx = CallerContext.getCurrent();
        long timespan = System.currentTimeMillis() - timeBeginStamp;
        obsAuditLogger.logAuditEvent(true, timespan, "append", key, null, null, (ctx==null?null:ctx.getContext()), userName, endpoint);

        return outputStream;
    }

    @Override
    public boolean truncate(Path f, long newLength) throws IOException {
        checkOpen();
        long timeBeginStamp =System.currentTimeMillis();

        if (!enablePosix) {
            super.truncate(f, newLength);
        }
        if (newLength < 0) {
            throw new IOException(new HadoopIllegalArgumentException(
                "Cannot truncate " + f + " to a negative file size: " + newLength + "."));
        }

        FileStatus status;
        try {
            status = OBSCommonUtils.getFileStatusWithRetry(this, f);
        } catch (OBSFileConflictException e) {
            throw new AccessControlException(e);
        }

        if (!status.isFile()) {
            throw new FileNotFoundException("Path is not a file: " + f);
        }

        String key = OBSCommonUtils.pathToKey(this, f);
        if (isFileBeingWritten(key)) {
            // AlreadyBeingCreatedException (on HDFS NameNode) is transformed
            // into IOException (on HDFS Client)
            throw new OBSAlreadyBeingCreatedException("Cannot truncate " + f + " that is being written.");
        }

        // Truncate length check.
        long oldLength = status.getLen();
        if (oldLength == newLength) {
            return true;
        }
        if (oldLength < newLength) {
            throw new IOException(new HadoopIllegalArgumentException(
                "Cannot truncate " + f + " to a larger file size. Current size: " + oldLength + ", truncate size: "
                    + newLength + "."));
        }

        checkPermission(f, AccessType.WRITE);
        OBSPosixBucketUtils.innerFsTruncateWithRetry(this, f, newLength);

        CallerContext ctx = CallerContext.getCurrent();
        long timespan = System.currentTimeMillis() - timeBeginStamp;
        obsAuditLogger.logAuditEvent(true, timespan, "truncate", key, null, null, (ctx==null?null:ctx.getContext()), userName, endpoint);

        return true;
    }

    /**
     * Check if a path exists.
     *
     * @param f source path
     * @return true if the path exists
     * @throws IOException IO failure
     */
    @Override
    public boolean exists(final Path f) throws IOException {
        checkOpen();
        long timeBeginStamp =System.currentTimeMillis();

        try {
            return getFileStatus(f) != null;
        } catch (FileNotFoundException e) {
            return false;
        }
    }

    /**
     * Rename Path src to Path dst.
     *
     * @param src path to be renamed
     * @param dst new path after rename
     * @return true if rename is successful
     * @throws IOException on IO failure
     */
    @Override
    public boolean rename(final Path src, final Path dst) throws IOException {
        checkOpen();
        long timeBeginStamp =System.currentTimeMillis();

        long startTime = System.currentTimeMillis();
        long threadId = Thread.currentThread().getId();
        long endTime;
        LOG.debug("Rename path {} to {} start", src, dst);
        try {
            boolean success;
            if (enablePosix) {
                checkPermission(src, AccessType.WRITE);
                checkPermission(dst,AccessType.WRITE);
                success = OBSPosixBucketUtils.renameBasedOnPosix(this, src, dst);
                OBSCommonUtils.setMetricsNormalInfo(this, OBSOperateAction.rename, startTime);
            } else {
                checkPermission(src, AccessType.WRITE);
                checkPermission(dst, AccessType.WRITE);
                success = OBSObjectBucketUtils.renameBasedOnObject(this, src, dst);
                OBSCommonUtils.setMetricsNormalInfo(this, OBSOperateAction.rename, startTime);
            }

            CallerContext ctx = CallerContext.getCurrent();
        long timespan = System.currentTimeMillis() - timeBeginStamp;
            obsAuditLogger.logAuditEvent(true, timespan, "truncate", OBSCommonUtils.pathToKey(this, src), OBSCommonUtils.pathToKey(this, dst), null, (ctx==null?null:ctx.getContext()), userName, endpoint);

            return success;

        } catch (ObsException e) {
            OBSCommonUtils.setMetricsAbnormalInfo(this, OBSOperateAction.rename, e);
            throw OBSCommonUtils.translateException("rename(" + src + ", " + dst + ")", src, e);
        } catch (OBSRenameFailedException e) {
            OBSCommonUtils.setMetricsAbnormalInfo(this, OBSOperateAction.rename, e);
            LOG.error(e.getMessage());
            return e.getExitCode();
        } catch (FileNotFoundException e) {
            OBSCommonUtils.setMetricsAbnormalInfo(this, OBSOperateAction.rename, e);
            LOG.error("file not found when rename(" + src + ", " + dst + ")");
            return false;
        } catch (IOException e) {
            OBSCommonUtils.setMetricsAbnormalInfo(this, OBSOperateAction.rename, e);
            throw e;
        } finally {
            endTime = System.currentTimeMillis();
            LOG.debug("Rename path {} to {} finished, thread:{}, " + "timeUsedInMilliSec:{}.", src, dst, threadId,
                endTime - startTime);
        }
    }

    @Override
    public Path getTrashRoot(Path path) {
        if (OBSConstants.HDFS_TRASH_VERSION_V2.equals(hdfsTrashVersion)) {
            return this.makeQualified(new Path(hdfsTrashPrefix, getUsername()));
        }

        return super.getTrashRoot(path);
    }

    @Override
    public Collection<FileStatus> getTrashRoots(boolean allUsers) {
        if (OBSConstants.HDFS_TRASH_VERSION_V2.equals(hdfsTrashVersion)) {
            Path trashPrefix = new Path(hdfsTrashPrefix);
            List<FileStatus> ret = new ArrayList<>();
            try {
                if (!exists(trashPrefix)) {
                    return ret;
                }

                if (allUsers) {
                    FileStatus[] candidates = listStatus(trashPrefix);
                    for (FileStatus fs : candidates) {
                        ret.add(fs);
                    }
                    return ret;
                }

                Path userTrash = new Path(trashPrefix, getUsername());
                if (exists(userTrash)) {
                    ret.add(getFileStatus(userTrash));
                }
            } catch (IOException e) {
                LOG.warn("Cannot get all trash roots", e);
            }

            return ret;
        }

        return super.getTrashRoots(allUsers);
    }

    /**
     * Return maximum number of entries in one multi-object delete call.
     *
     * @return the maximum number of entries in one multi-object delete call
     */
    int getMaxEntriesToDelete() {
        return maxEntriesToDelete;
    }

    /**
     * Return list parallel factor.
     *
     * @return the list parallel factor
     */
    int getListParallelFactor() {
        return listParallelFactor;
    }

    /**
     * Return bounded thread pool for list.
     *
     * @return bounded thread pool for list
     */
    ThreadPoolExecutor getBoundedListThreadPool() {
        return boundedListThreadPool;
    }

    /**
     * Return a flag that indicates if OBS client specific depth first search
     * (DFS) list is enabled.
     *
     * @return the flag
     */
    boolean isObsClientDFSListEnable() {
        return obsClientDFSListEnable;
    }

    /**
     * Return the {@link FileSystem.Statistics} instance used by this
     * filesystem.
     *
     * @return the used {@link FileSystem.Statistics} instance
     */
    Statistics getSchemeStatistics() {
        return statistics;
    }

    /**
     * Return the minimum number of objects in one multi-object delete call.
     *
     * @return the minimum number of objects in one multi-object delete call
     */
    int getMultiDeleteThreshold() {
        return multiDeleteThreshold;
    }

    /**
     * Return a flag that indicates if multi-object delete is enabled.
     *
     * @return the flag
     */
    boolean isEnableMultiObjectDelete() {
        return enableMultiObjectDelete;
    }

    /**
     * Delete a Path. This operation is at least {@code O(files)}, with added
     * overheads to enumerate the path. It is also not atomic.
     *
     * @param f         the path to delete
     * @param recursive if path is a directory and set to true, the directory is
     *                  deleted else throws an exception. In case of a file the
     *                  recursive can be set to either true or false
     * @return true if delete is successful else false
     * @throws IOException due to inability to delete a directory or file
     */
    @Override
    public boolean delete(final Path f, final boolean recursive) throws IOException {
        checkOpen();
        long timeBeginStamp =System.currentTimeMillis();

        long startTime = System.currentTimeMillis();
        try {
            FileStatus status = OBSCommonUtils.getFileStatusWithRetry(this, f);
            LOG.debug("delete: path {} - recursive {}", status.getPath(), recursive);

            if (enablePosix) {
                checkPermission(f,AccessType.WRITE);
                boolean success = OBSPosixBucketUtils.fsDelete(this, status, recursive);
                OBSCommonUtils.setMetricsNormalInfo(this, OBSOperateAction.delete, startTime);
                return success;
            }
            checkPermission(f,AccessType.WRITE);

            CallerContext ctx = CallerContext.getCurrent();
        long timespan = System.currentTimeMillis() - timeBeginStamp;
            obsAuditLogger.logAuditEvent(true, timespan, "delete", OBSCommonUtils.pathToKey(this, f), null, null, (ctx==null?null:ctx.getContext()), userName, endpoint);

            return OBSObjectBucketUtils.objectDelete(this, status, recursive);
        } catch (FileNotFoundException e) {
            LOG.warn("Couldn't delete {} - does not exist", f);
            return false;
        } catch (OBSFileConflictException e) {
            OBSCommonUtils.setMetricsAbnormalInfo(this, OBSOperateAction.delete, e);
            throw new AccessControlException(e);
        } catch (ObsException e) {
            OBSCommonUtils.setMetricsAbnormalInfo(this, OBSOperateAction.delete, e);
            throw OBSCommonUtils.translateException("delete", f, e);
        } catch (IOException e) {
            OBSCommonUtils.setMetricsAbnormalInfo(this, OBSOperateAction.delete, e);
            throw e;
        }
    }

    /**
     * Return a flag that indicates if fast delete is enabled.
     *
     * @return the flag
     */
    boolean isEnableFastDelete() {
        return enableFastDelete;
    }

    /**
     * Return trash directory for fast delete.
     *
     * @return the trash directory
     */
    String getFastDeleteDir() {
        if (fastDeleteVersion.equals(OBSConstants.FAST_DELETE_VERSION_V2)) {
            SimpleDateFormat dateFmt = new SimpleDateFormat(OBSConstants.FAST_DELETE_VERSION_V2_CHECKPOINT_FORMAT);
            String checkpointStr = dateFmt.format(new Date());
            String checkpointDir = String.format(Locale.ROOT, "%s%s/",
                OBSCommonUtils.maybeAddTrailingSlash(fastDeleteDir), checkpointStr);
            return checkpointDir;
        }
        return fastDeleteDir;
    }

    /**
     * List the statuses of the files/directories in the given path if the path
     * is a directory.
     *
     * @param f given path
     * @return the statuses of the files/directories in the given patch
     * @throws FileNotFoundException when the path does not exist
     * @throws IOException           see specific implementation
     */
    @Override
    public FileStatus[] listStatus(final Path f) throws FileNotFoundException, IOException {
        checkOpen();
        long timeBeginStamp =System.currentTimeMillis();

        checkPermission(f,AccessType.READ);
        long startTime = System.currentTimeMillis();
        long threadId = Thread.currentThread().getId();
        long endTime;
        try {
            FileStatus[] statuses = OBSCommonUtils.listStatus(this, f, false);
            endTime = System.currentTimeMillis();
            LOG.debug("List status for path:{}, thread:{}, timeUsedInMilliSec:{}", f, threadId, endTime - startTime);

            CallerContext ctx = CallerContext.getCurrent();
        long timespan = System.currentTimeMillis() - timeBeginStamp;
            obsAuditLogger.logAuditEvent(true, timespan, "listStatus", OBSCommonUtils.pathToKey(this, f), null, null, (ctx==null?null:ctx.getContext()), userName, endpoint);

            return statuses;
        } catch (ObsException e) {
            throw OBSCommonUtils.translateException("listStatus", f, e);
        }
    }

    @Override
    public Path getHomeDirectory() {
        return this.makeQualified(new Path("/user/" + getUsername()));
    }

    static String getUsername() {
        String user;
        try {
            user = UserGroupInformation.getCurrentUser().getShortUserName();
        } catch(IOException ex) {
            LOG.error("get user fail,fallback to system property user.name", ex);
            user = System.getProperty("user.name");
        }
        return user;
    }

    static String getUsernameOrigin() {
        String user;
        try {
            user = UserGroupInformation.getCurrentUser().toString();
        } catch(IOException ex) {
            LOG.error("get user fail,fallback to system property user.name", ex);
            user = System.getProperty("user.name");
        }
        return user;
    }

    /**
     * This public interface is provided specially for Huawei MRS. List the
     * statuses of the files/directories in the given path if the path is a
     * directory. When recursive is true, iterator all objects in the given path
     * and its sub directories.
     *
     * @param f         given path
     * @param recursive whether to iterator objects in sub direcotries
     * @return the statuses of the files/directories in the given patch
     * @throws FileNotFoundException when the path does not exist
     * @throws IOException           see specific implementation
     */
    public FileStatus[] listStatus(final Path f, final boolean recursive) throws FileNotFoundException, IOException {
        checkOpen();
        long timeBeginStamp =System.currentTimeMillis();

        checkPermission(f,AccessType.READ);
        long startTime = System.currentTimeMillis();
        long threadId = Thread.currentThread().getId();
        long endTime;
        try {
            FileStatus[] statuses = OBSCommonUtils.listStatus(this, f, recursive);
            endTime = System.currentTimeMillis();
            LOG.debug("List status for path:{}, thread:{}, timeUsedInMilliSec:{}", f, threadId, endTime - startTime);

            CallerContext ctx = CallerContext.getCurrent();
        long timespan = System.currentTimeMillis() - timeBeginStamp;
            obsAuditLogger.logAuditEvent(true, timespan, "listStatus", OBSCommonUtils.pathToKey(this, f), null, null, (ctx==null?null:ctx.getContext()), userName, endpoint);

            return statuses;
        } catch (ObsException e) {

            CallerContext ctx = CallerContext.getCurrent();
        long timespan = System.currentTimeMillis() - timeBeginStamp;
            obsAuditLogger.logAuditEvent(false, timespan, "listStatus", OBSCommonUtils.pathToKey(this, f), null, null, (ctx==null?null:ctx.getContext()), userName, endpoint);

            throw OBSCommonUtils.translateException(
                "listStatus with recursive flag[" + (recursive ? "true] " : "false] "), f, e);
        }
    }

    /**
     * Return the OBSListing instance used by this filesystem.
     *
     * @return the OBSListing instance
     */
    OBSListing getObsListing() {
        return obsListing;
    }

    /**
     * Return the current working directory for the given file system.
     *
     * @return the directory pathname
     */
    @Override
    public Path getWorkingDirectory() {
        return workingDir;
    }

    /**
     * Set the current working directory for the file system. All relative paths
     * will be resolved relative to it.
     *
     * @param newDir the new working directory
     */
    @Override
    public void setWorkingDirectory(final Path newDir) {
        String result = fixRelativePart(newDir).toUri().getPath();
        if (!OBSCommonUtils.isValidName(result)) {
            throw new IllegalArgumentException("Invalid directory name " + result);
        }
        workingDir = fixRelativePart(newDir);
    }

    /**
     * Return the shortUserName of the filesystem.
     *
     * @return the short name of the user who instantiated the filesystem
     */
    String getShortUserName() {
        return shortUserName;
    }

    /**
     * Make the given path and all non-existent parents into directories. Has
     * the semantics of Unix {@code 'mkdir -p'}. Existence of the directory
     * hierarchy is not an error.
     *
     * @param path       path to create
     * @param permission to apply to f
     * @return true if a directory was created
     * @throws FileAlreadyExistsException there is a file at the path specified
     * @throws IOException                other IO problems
     */
    @Override
    public boolean mkdirs(final Path path, final FsPermission permission)
        throws IOException, FileAlreadyExistsException {
        checkOpen();
        long timeBeginStamp =System.currentTimeMillis();

        checkPermission(path,AccessType.WRITE);
        try {
            boolean success = OBSCommonUtils.mkdirs(this, path);

            CallerContext ctx = CallerContext.getCurrent();
        long timespan = System.currentTimeMillis() - timeBeginStamp;
            obsAuditLogger.logAuditEvent(success, timespan, "mkdirs", OBSCommonUtils.pathToKey(this, path), null, null, (ctx==null?null:ctx.getContext()), userName, endpoint);

            return success;
        } catch (ObsException e) {
            throw OBSCommonUtils.translateException("mkdirs", path, e);
        }
    }

    @Override
    public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException {
        if (!readPolicy.equals(OBSConstants.READAHEAD_POLICY_MEMARTSCC)
            || this.getMemArtsCCClient() == null || !localityEnabled) {
            // only support for MemArtsCC
            return super.getFileBlockLocations(file, start, len);
        }
        if (file == null || start < 0L || len < 0L) {
            return super.getFileBlockLocations(file, start, len);
        } else if (file.isDirectory()) {
            return super.getFileBlockLocations(file, start, len);
        } else if (file.getLen() <= start) {
            return new BlockLocation[0];
        } else {
            MemArtsCCClient ccClient = this.getMemArtsCCClient();
            CcGetShardParam shardParam = buildCcShardParam(file, start, len);
            int result = ccClient.getObjectShardInfo(shardParam);

            if (result != OBSConstants.GET_SHARD_INFO_SUCCESS) {
                LOG.error("Get memartscc shard info failed! ret code = {}", result);
                return new BlockLocation[0];
            }

            ObjectShard[] objectShards = shardParam.getObjectShard();
            int numOfBlocks = shardParam.getValidShardNum();
            BlockLocation[] locations = new BlockLocation[numOfBlocks];
            for(int i = 0; i < numOfBlocks; ++i) {
                long offset = objectShards[i].getStart();
                long length = objectShards[i].getEnd() - offset;
                String[] hosts = objectShards[i].getHosts();
                for (int j = 0; j < hosts.length; j++) {
                    InetAddress addr = InetAddress.getByName(hosts[j]);
                    String hostName = addr.getHostName();
                    hosts[j] = hostName;
                }
                locations[i] = new BlockLocation(hosts, hosts, offset, length);
            }
            return locations;
        }
    }

    private CcGetShardParam buildCcShardParam(FileStatus file, long start, long len) {
        String obsBucketName = getBucket();
        long end = start + len;
        String objKey = OBSCommonUtils.pathToKey(this, file.getPath());

        int shardNum = (int) (len / this.blockSize + 1);
        ObjectShard[] objectShards = new ObjectShard[shardNum];
        for (int i = 0; i < shardNum; i++) {
            String[] hosts = new String[OBSConstants.MAX_DUPLICATION_NUM];
            objectShards[i] = new ObjectShard(hosts);
        }

        return new CcGetShardParam(start, end, obsBucketName, enablePosix, objKey,
                objectShards, shardNum, 0);
    }

    /**
     * Return a file status object that represents the path.
     *
     * @param f the path we want information from
     * @return a FileStatus object
     * @throws FileNotFoundException when the path does not exist
     * @throws IOException           on other problems
     */
    @Override
    public FileStatus getFileStatus(final Path f) throws FileNotFoundException, IOException {
        checkOpen();
        long timeBeginStamp =System.currentTimeMillis();

        long startTime = System.currentTimeMillis();
        try {
            FileStatus fileStatus = OBSCommonUtils.getFileStatusWithRetry(this, f);
            OBSCommonUtils.setMetricsNormalInfo(this, OBSOperateAction.getFileStatus, startTime);

            CallerContext ctx = CallerContext.getCurrent();
        long timespan = System.currentTimeMillis() - timeBeginStamp;
            obsAuditLogger.logAuditEvent(true, timespan, "getFileStatus", OBSCommonUtils.pathToKey(this, f), null, fileStatus, (ctx==null?null:ctx.getContext()), userName, endpoint);

            return fileStatus;
        } catch (OBSFileConflictException e) {
            FileNotFoundException fileNotFoundException = new FileNotFoundException(e.getMessage());
            OBSCommonUtils.setMetricsAbnormalInfo(this, OBSOperateAction.getFileStatus, fileNotFoundException);
            // For super user, convert AccessControlException
            // to null on NameNode then to FileNotFoundException on Client
            throw fileNotFoundException;
        }
    }

    /**
     * Inner implementation without retry for {@link #getFileStatus(Path)}.
     *
     * @param f the path we want information from
     * @return a FileStatus object
     * @throws IOException on IO failure
     */
    @VisibleForTesting
    OBSFileStatus innerGetFileStatus(final Path f) throws IOException {
        if (enablePosix) {
            return OBSPosixBucketUtils.innerFsGetObjectStatus(this, f);
        }
        return OBSObjectBucketUtils.innerGetObjectStatus(this, f);
    }

    /**
     * Return the {@link ContentSummary} of a given {@link Path}.
     *
     * @param f path to use
     * @return the {@link ContentSummary}
     * @throws FileNotFoundException if the path does not resolve
     * @throws IOException           IO failure
     */
    @Override
    public ContentSummary getContentSummary(final Path f) throws FileNotFoundException, IOException {
        checkOpen();
        long timeBeginStamp =System.currentTimeMillis();

        ContentSummary contentSummary;
        if (!obsContentSummaryEnable) {
            ContentSummary resultSummary = super.getContentSummary(f);

            CallerContext ctx = CallerContext.getCurrent();
            long timespan = System.currentTimeMillis() - timeBeginStamp;
            obsAuditLogger.logAuditEvent(true, timespan, "getContentSummary", OBSCommonUtils.pathToKey(this, f), null, null, (ctx==null?null:ctx.getContext()), userName, endpoint);

            return resultSummary;
        }

        FileStatus status;
        try {
            status = OBSCommonUtils.getFileStatusWithRetry(this, f);
        } catch (OBSFileConflictException e) {
            throw new AccessControlException(e);
        }

        if (status.isFile()) {
            // f is a file
            long length = status.getLen();
            contentSummary = new ContentSummary.Builder().length(length)
                .fileCount(1)
                .directoryCount(0)
                .spaceConsumed(length)
                .build();

            CallerContext ctx = CallerContext.getCurrent();
            long timespan = System.currentTimeMillis() - timeBeginStamp;
            obsAuditLogger.logAuditEvent(true, timespan, "getContentSummary", OBSCommonUtils.pathToKey(this, f), null, status, (ctx==null?null:ctx.getContext()), userName, endpoint);

            return contentSummary;
        }

        // f is a directory
        if (enablePosix) {
            contentSummary = null;
            if (this.getConf()
                .get(OBSConstants.OBS_CONTENT_SUMMARY_VERSION, OBSConstants.OBS_CONTENT_SUMMARY_VERSION_V2)
                .equals(OBSConstants.OBS_CONTENT_SUMMARY_VERSION_V2)) {
                boolean fallback = false;
                try {
                    contentSummary = OBSPosixBucketUtils.fsGetDirectoryContentSummaryV2(this, status);
                    if (contentSummary.getFileCount() + contentSummary.getDirectoryCount()
                        < OBSConstants.OBS_CONTENT_SUMMARY_FALLBACK_THRESHOLD) {
                        fallback = true;
                    }
                } catch (OBSMethodNotAllowedException e) {
                    LOG.debug("bucket[{}] not support fsGetDirectoryContentSummaryV2, fallback to V1, path={}, cause {}"
                        , bucket, f.toString(), e.getMessage());
                    fallback = true;
                } catch (Exception e) {
                    LOG.warn("fsGetDirectoryContentSummaryV2 failed with exception, fallback to V1, path={}, cause {}"
                        , f.toString(), e.getMessage());
                    fallback = true;
                }

                if (!fallback) {

                    CallerContext ctx = CallerContext.getCurrent();
                    long timespan = System.currentTimeMillis() - timeBeginStamp;
                    obsAuditLogger.logAuditEvent(true, timespan, "getContentSummary", OBSCommonUtils.pathToKey(this, f), null, status, (ctx==null?null:ctx.getContext()), userName, endpoint);

                    return contentSummary;
                }
                LOG.debug("fallback to getContentSummaryV1, path={}", f.toString());
            }
            contentSummary = OBSPosixBucketUtils.fsGetDirectoryContentSummary(this, OBSCommonUtils.pathToKey(this, f));
        } else {
            contentSummary = OBSObjectBucketUtils.getDirectoryContentSummary(this, OBSCommonUtils.pathToKey(this, f));
        }

        CallerContext ctx = CallerContext.getCurrent();
        long timespan = System.currentTimeMillis() - timeBeginStamp;
        obsAuditLogger.logAuditEvent(true, timespan, "getContentSummary", OBSCommonUtils.pathToKey(this, f), null, status, (ctx==null?null:ctx.getContext()), userName, endpoint);
        
        return contentSummary;
    }

    /**
     * Copy the {@code src} file on the local disk to the filesystem at the
     * given {@code dst} name.
     *
     * @param delSrc    whether to delete the src
     * @param overwrite whether to overwrite an existing file
     * @param src       path
     * @param dst       path
     * @throws FileAlreadyExistsException if the destination file exists and
     *                                    overwrite == false
     * @throws IOException                IO problem
     */
    @Override
    public void copyFromLocalFile(final boolean delSrc, final boolean overwrite, final Path src, final Path dst)
        throws FileAlreadyExistsException, IOException {
        checkOpen();
        long timeBeginStamp =System.currentTimeMillis();

        try {
            super.copyFromLocalFile(delSrc, overwrite, src, dst);

            CallerContext ctx = CallerContext.getCurrent();
        long timespan = System.currentTimeMillis() - timeBeginStamp;
            obsAuditLogger.logAuditEvent(true, timespan, "getFileStatus", OBSCommonUtils.pathToKey(this, src), OBSCommonUtils.pathToKey(this, dst), null, (ctx==null?null:ctx.getContext()), userName, endpoint);

        } catch (ObsException e) {
            throw OBSCommonUtils.translateException("copyFromLocalFile(" + src + ", " + dst + ")", src, e);
        }
    }

    /**
     * Close the filesystem. This shuts down all transfers.
     */
    @Override
    public void close() throws IOException {
        LOG.debug("This Filesystem closed by user, clear resource.");
        if (closed) {
            // already closed
            return;
        }
        closeAllFilesBeingWritten();
        closed = true;

        if (statsReporter != null) {
            statsReporter.shutdownReport();
        }

        try {
            super.close();
            if (metricsConsumer != null) {
                metricsConsumer.close();
            }
            if (obs != null) {
                obs.close();
            }
        } finally {
            OBSCommonUtils.shutdownAll(boundedMultipartUploadThreadPool, boundedCopyThreadPool, boundedDeleteThreadPool,
                boundedCopyPartThreadPool, boundedListThreadPool);
        }

        if (memArtsCCClient != null) {
            memArtsCCClient.close();
        }

        LOG.info("Finish closing filesystem instance for uri: {}", uri);
    }

    @Override
    public String getCanonicalServiceName() {
        return obsDelegationTokenManger != null
            ? obsDelegationTokenManger.getCanonicalServiceName()
            : getScheme() + "://" + bucket;
    }

    @Override
    public Token<?>[] addDelegationTokens(String renewer, Credentials credentials) throws IOException {
        LOG.info("add delegation tokens for renewer {}", renewer);
        return obsDelegationTokenManger != null
                ? obsDelegationTokenManger.addDelegationTokens(renewer, credentials)
                : super.addDelegationTokens(renewer, credentials);
    }

    @Override
    public Token<?> getDelegationToken(String renewer) throws IOException {
        LOG.info("get delegation tokens for renewer {}", renewer);
        return obsDelegationTokenManger != null
            ? obsDelegationTokenManger.getDelegationToken(renewer)
            : super.getDelegationToken(renewer);
    }

    /**
     * Return copy part size.
     *
     * @return copy part size
     */
    long getCopyPartSize() {
        return copyPartSize;
    }

    /**
     * Return bounded thread pool for copy part.
     *
     * @return the bounded thread pool for copy part
     */
    ThreadPoolExecutor getBoundedCopyPartThreadPool() {
        return boundedCopyPartThreadPool;
    }

    /**
     * Return bounded thread pool for copy.
     *
     * @return the bounded thread pool for copy
     */
    ThreadPoolExecutor getBoundedCopyThreadPool() {
        return boundedCopyThreadPool;
    }

    /**
     * Imitate HDFS to return the number of bytes that large input files should
     * be optimally split into to minimize I/O time for compatibility.
     *
     * @deprecated use {@link #getDefaultBlockSize(Path)} instead
     */
    @Override
    public long getDefaultBlockSize() {
        return blockSize;
    }

    /**
     * Imitate HDFS to return the number of bytes that large input files should
     * be optimally split into to minimize I/O time.  The given path will be
     * used to locate the actual filesystem. The full path does not have to
     * exist.
     *
     * @param f path of file
     * @return the default block size for the path's filesystem
     */
    @Override
    public long getDefaultBlockSize(Path f) {
        return blockSize;
    }

    /**
     * Return a string that describes this filesystem instance.
     *
     * @return the string
     */
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("OBSFileSystem{");
        sb.append("uri=").append(uri);
        sb.append(", workingDir=").append(workingDir);
        sb.append(", partSize=").append(partSize);
        sb.append(", enableMultiObjectsDelete=").append(enableMultiObjectDelete);
        sb.append(", maxKeys=").append(maxKeys);
        if (cannedACL != null) {
            sb.append(", cannedACL=").append(cannedACL.toString());
        }
        sb.append(", readAheadRange=").append(readAheadRange);
        sb.append(", blockSize=").append(getDefaultBlockSize());
        if (blockFactory != null) {
            sb.append(", blockFactory=").append(blockFactory);
        }
        sb.append(", boundedMultipartUploadThreadPool=").append(boundedMultipartUploadThreadPool);
        sb.append(", statistics {").append(statistics).append("}");
        sb.append(", metrics {").append("}");
        sb.append('}');
        return sb.toString();
    }

    /**
     * Return the maximum number of keys to get while paging through a directory
     * listing.
     *
     * @return the maximum number of keys
     */
    int getMaxKeys() {
        return maxKeys;
    }

    /**
     * List the statuses and block locations of the files in the given path.
     * Does not guarantee to return the iterator that traverses statuses of the
     * files in a sorted order.
     *
     * <pre>
     * If the path is a directory,
     *   if recursive is false, returns files in the directory;
     *   if recursive is true, return files in the subtree rooted at the path.
     * If the path is a file, return the file's status and block locations.
     * </pre>
     *
     * @param f         a path
     * @param recursive if the subdirectories need to be traversed recursively
     * @return an iterator that traverses statuses of the files/directories in
     * the given path
     * @throws FileNotFoundException if {@code path} does not exist
     * @throws IOException           if any I/O error occurred
     */
    @Override
    public RemoteIterator<LocatedFileStatus> listFiles(final Path f, final boolean recursive)
        throws FileNotFoundException, IOException {
        checkOpen();
        long timeBeginStamp =System.currentTimeMillis();

        checkPermission(f,AccessType.READ);
        RemoteIterator<LocatedFileStatus> locatedFileStatus;
        Path path = OBSCommonUtils.qualify(this, f);

        LOG.debug("listFiles({}, {})", path, recursive);
        try {
            // lookup dir triggers existence check
            final FileStatus fileStatus;
            try {
                fileStatus = OBSCommonUtils.getFileStatusWithRetry(this, path);
            } catch (OBSFileConflictException e) {
                throw new AccessControlException(e);
            }
            if (fileStatus.isFile()) {
                locatedFileStatus = new OBSListing.SingleStatusRemoteIterator(
                    OBSCommonUtils.toLocatedFileStatus(this, fileStatus));
                // simple case: File
                LOG.debug("Path is a file");
            } else {
                LOG.debug("listFiles: doing listFiles of directory {} - recursive {}", path, recursive);
                // directory: do a bulk operation
                String key = OBSCommonUtils.maybeAddTrailingSlash(OBSCommonUtils.pathToKey(this, path));
                String delimiter = recursive ? null : "/";
                LOG.debug("Requesting all entries under {} with delimiter '{}'", key, delimiter);
                locatedFileStatus = obsListing.createLocatedFileStatusIterator(
                    obsListing.createFileStatusListingIterator(path,
                        OBSCommonUtils.createListObjectsRequest(this, key, delimiter),
                        org.apache.hadoop.fs.obs.OBSListing.ACCEPT_ALL, new OBSListing.AcceptFilesOnly(path)));
            }

            CallerContext ctx = CallerContext.getCurrent();
        long timespan = System.currentTimeMillis() - timeBeginStamp;
            obsAuditLogger.logAuditEvent(true, timespan, "listFiles", OBSCommonUtils.pathToKey(this, f), null, null, (ctx==null?null:ctx.getContext()), userName, endpoint);

            return locatedFileStatus;
        } catch (ObsException e) {
            throw OBSCommonUtils.translateException("listFiles", path, e);
        }
    }

    /**
     * List the statuses of the files/directories in the given path if the path
     * is a directory. Return the file's status and block locations If the path
     * is a file.
     * <p>
     * If a returned status is a file, it contains the file's block locations.
     *
     * @param f is the path
     * @return an iterator that traverses statuses of the files/directories in
     * the given path
     * @throws FileNotFoundException If <code>f</code> does not exist
     * @throws IOException           If an I/O error occurred
     */
    @Override
    public RemoteIterator<LocatedFileStatus> listLocatedStatus(final Path f) throws FileNotFoundException, IOException {
        checkOpen();
        return listLocatedStatus(f, org.apache.hadoop.fs.obs.OBSListing.ACCEPT_ALL);
    }

    /**
     * List a directory. The returned results include its block location if it
     * is a file The results are filtered by the given path filter
     *
     * @param f      a path
     * @param filter a path filter
     * @return an iterator that traverses statuses of the files/directories in
     * the given path
     * @throws FileNotFoundException if <code>f</code> does not exist
     * @throws IOException           if any I/O error occurred
     */
    @Override
    public RemoteIterator<LocatedFileStatus> listLocatedStatus(final Path f, final PathFilter filter)
        throws FileNotFoundException, IOException {
        checkOpen();
        long timeBeginStamp =System.currentTimeMillis();

        checkPermission(f,AccessType.READ);
        Path path = OBSCommonUtils.qualify(this, f);
        LOG.debug("listLocatedStatus({}, {}", path, filter);
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteList;
        try {
            // lookup dir triggers existence check
            final FileStatus fileStatus;
            try {
                fileStatus = OBSCommonUtils.getFileStatusWithRetry(this, path);
            } catch (OBSFileConflictException e) {
                throw new AccessControlException(e);
            }

            if (fileStatus.isFile()) {
                // simple case: File
                LOG.debug("Path is a file");
                locatedFileStatusRemoteList = new OBSListing.SingleStatusRemoteIterator(
                    filter.accept(path) ? OBSCommonUtils.toLocatedFileStatus(this, fileStatus) : null);
            } else {
                // directory: trigger a lookup
                String key = OBSCommonUtils.maybeAddTrailingSlash(OBSCommonUtils.pathToKey(this, path));
                locatedFileStatusRemoteList = obsListing.createLocatedFileStatusIterator(
                    obsListing.createFileStatusListingIterator(path,
                        OBSCommonUtils.createListObjectsRequest(this, key, "/"), filter,
                        new OBSListing.AcceptAllButSelfAndOBSDirs(path)));
            }

            CallerContext ctx = CallerContext.getCurrent();
        long timespan = System.currentTimeMillis() - timeBeginStamp;
            obsAuditLogger.logAuditEvent(true, timespan, "listLocatedStatus", OBSCommonUtils.pathToKey(this, f), null, fileStatus, (ctx==null?null:ctx.getContext()), userName, endpoint);

            return locatedFileStatusRemoteList;
        } catch (ObsException e) {
            throw OBSCommonUtils.translateException("listLocatedStatus", path, e);
        }
    }

    /**
     * Return server-side encryption wrapper used by this filesystem instance.
     *
     * @return the server-side encryption wrapper
     */
    public SseWrapper getSse() {
        return sse;
    }

    @VisibleForTesting
    void setBucketPolicy(String policy) {
        obs.setBucketPolicy(bucket, policy);
    }

    public void checkOpen() throws IOException {
        if (closed) {
            throw new IOException("OBSFilesystem closed");
        }
    }

    BasicMetricsConsumer getMetricsConsumer() {
        return metricsConsumer;
    }

    public boolean getMetricSwitch() {
        return metricSwitch;
    }

    int getInvokeCountThreshold() {
        return invokeCountThreshold;
    }

    public TrafficStatistics getTrafficStatistics() {
        return trafficStatistics;
    }

    /**
     * Get disguise permission mode support stat.
     *
     * @return is disguise permission mode supported
     */
    boolean supportDisguisePermissionsMode() {
        return OBSConstants.PERMISSIONS_MODE_DISGUISE.equals(permissionsMode) && enablePosix;
    }

    /**
     * Set permission for given file.
     *
     * @param f the file to set permission info
     * @param permission file's permission
     * @throws IOException If an I/O error occurred
     */
    @Override
    public void setPermission(final Path f, final FsPermission permission) throws IOException {
        checkOpen();
        checkPermission(f, AccessType.WRITE);
        if (supportDisguisePermissionsMode()) {
            LOG.debug("Set file {} permission to {}", f, permission);
            OBSPosixBucketUtils.fsSetPermission(this, f, permission);
        }
    }

    /**
     * Set owner and group for given file.
     *
     * @param f the file to set owner and group info
     * @param username file's owner
     * @param groupname file's group
     * @throws IOException If an I/O error occurred
     */
    @Override
    public void setOwner(final Path f, final String username, final String groupname) throws IOException {
        checkOpen();
        checkPermission(f, AccessType.WRITE);
        if (supportDisguisePermissionsMode()) {
            LOG.debug("Set file {} owner to {} and group to {}", f , username, groupname);
            OBSPosixBucketUtils.fsSetOwner(this, f, username, groupname);
        }
    }

    /**
     * Set owner and group for given file.
     * @param f the file to get FileChecksum
     * @return an FileChecksum that bytesPercrc set -1, type set DataChecksum.Type.CRC32C
     * @throws IOException If an I/O error occurred
     */
    @Override
    public FileChecksum getFileChecksum(Path f) throws IOException {
        checkOpen();
        long timeBeginStamp =System.currentTimeMillis();

        String key = OBSCommonUtils.pathToKey(this, f);
        Object getCrcContext = OBSCommonUtils.getObjectMetadataheader(this, key).get(HASH_CRC32C);
        if(getCrcContext == null){
            LOG.error("Get file crc32 failed, getCrcContext is null");
            return new CompositeCrcFileChecksum(-1, DataChecksum.Type.CRC32C, -1);
        }
        long unsignedInt = Long.parseLong(getCrcContext.toString()); // 将字符串转换为long类型
        int signedInt = (int) (unsignedInt & 0xFFFFFFFFL); // 使用位运算将其截断为int类型

        CallerContext ctx = CallerContext.getCurrent();
        long timespan = System.currentTimeMillis() - timeBeginStamp;
        obsAuditLogger.logAuditEvent(true, timespan, "getFileChecksum", OBSCommonUtils.pathToKey(this, f), null, null, (ctx==null?null:ctx.getContext()), userName, endpoint);

        return new CompositeCrcFileChecksum(signedInt, DataChecksum.Type.CRC32C, -1);
    }

    @Override
    public void setXAttr(Path path, String name, byte[] value) throws IOException {
        this.setXAttr(path, name, value, EnumSet.of(XAttrSetFlag.CREATE, XAttrSetFlag.REPLACE));
    }

    /**
     * XXX
     * @param path the file to setXAttr
     * @param name the attr should equal AccessLabel
     * @param value the value of AccessLabel
     * @param flag the value of XAttrSetFlag
     * @throws IOException If an I/O error occurred
     */
    @Override
    public void setXAttr(Path path, String name, byte[] value, EnumSet<XAttrSetFlag> flag) throws IOException {
        //设置目录accesslabel
        if (name.equals("AccessLabel")) {
            try{
                OBSFileStatus fileStatus = OBSCommonUtils.getFileStatusWithRetry(this, path);
                SetAccessLabelRequest setAccessLabelRequest = new SetAccessLabelRequest();
                setAccessLabelRequest.setBucketName(getBucket());
                if (!fileStatus.isDirectory()) {
                    throw new IOException(this.getClass().getSimpleName() + String.format("path is %s, which is not directory", path));
                }
                try{
                    String[] accessLabels = new String(value, StandardCharsets.UTF_8).split(";");
                    ArrayList<String> roleLabels = new ArrayList<>(Arrays.asList(accessLabels));
                    setAccessLabelRequest.setRoleLabel(roleLabels);
                } catch (Exception ex) {
                    throw new IOException(this.getClass().getSimpleName() + "value is %s, which is not wrong format");
                }

                setAccessLabelRequest.setDir(OBSCommonUtils.pathToKey(this, path));
                getObsClient().setAccessLabelFs(setAccessLabelRequest);
                return;
            }catch (ObsException e) {
                throw OBSCommonUtils.translateException(
                        String.format("setXAttr failed, value is %s", new String(value, StandardCharsets.UTF_8)), path, e);
            }
        }

        throw new IOException(this.getClass().getSimpleName() + String.format("name is %s, which is not equal to AccessLabel", name));
    }

    @Override
    public byte[] getXAttr(Path path, String name) throws IOException {
        if (name.equals("AccessLabel")) {
            try {
                return getXAttrs(path).get("AccessLabel");
            }catch (IOException e){
                throw new IOException(this.getClass().getSimpleName() + String.format("path is %s, which is getXAttrs failed", path));
            }
        }
        throw new UnsupportedOperationException(this.getClass().getSimpleName() + " doesn't support getXAttr");
    }

    @Override
    public Map<String, byte[]> getXAttrs(Path path) throws IOException {
        Map<String, byte[]> trgXAttrs = Maps.newHashMap();
        OBSFileStatus fileStatus = OBSCommonUtils.getFileStatusWithRetry(this, path);
        if (fileStatus.isFile()) {
            throw new IOException(this.getClass().getSimpleName() + String.format("path is %s, which is not directory", path));
        }

        GetAccessLabelRequest getAccessLabelRequest = new GetAccessLabelRequest();
        getAccessLabelRequest.setBucketName(this.getBucket());
        String key = OBSCommonUtils.pathToKey(this, path);
        if (key.isEmpty()) {
            return trgXAttrs;
        }

        getAccessLabelRequest.setDir(key);
        try{
            GetAccessLabelResult accessLabelFs = getObsClient().getAccessLabelFs(getAccessLabelRequest);
            List<String> roleLabels = accessLabelFs.getRoleLabel();
            StringBuilder xattr= new StringBuilder();
            for (String label:roleLabels) {
                xattr.append(label).append(";");
            }
            trgXAttrs.put("AccessLabel", xattr.toString().getBytes(StandardCharsets.UTF_8));
        } catch (ObsException e) {
            throw OBSCommonUtils.translateException("getAccessLabel failed", path, e);
        }
        return trgXAttrs;
    }

    @Override
    public Map<String, byte[]> getXAttrs(Path path, List<String> names) throws IOException {
        throw new UnsupportedOperationException(this.getClass().getSimpleName() + " doesn't support getXAttrs");
    }

    @Override
    public List<String> listXAttrs(Path path) throws IOException {
        throw new UnsupportedOperationException(this.getClass().getSimpleName() + " doesn't support listXAttrs");
    }

    @Override
    public void removeXAttr(Path path, String name) throws IOException {
        throw new UnsupportedOperationException(this.getClass().getSimpleName() + " doesn't support removeXAttr");
    }


}
