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
import com.google.common.util.concurrent.ListeningExecutorService;
import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;
import com.obs.services.model.AccessControlList;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.obs.input.InputPolicyFactory;
import org.apache.hadoop.fs.obs.input.InputPolicys;
import org.apache.hadoop.fs.obs.input.OBSInputStream;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
public class OBSFileSystem extends FileSystem {
    //CHECKSTYLE:ON

    /**
     * Class logger.
     */
    public static final Logger LOG = LoggerFactory.getLogger(OBSFileSystem.class);

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

    /**
     * OBS client instance.
     */
    private ObsClient obs;

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
    private boolean enableTrash = false;

    /**
     * Trash directory for fast delete.
     */
    private String trashDir;

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

    /**
     * A map from file names to {@link FSDataOutputStream} objects that are
     * currently being written by this client. Note that a file can only be
     * written by a single filesystem.
     */
    private Map<String, FSDataOutputStream> filesBeingWritten = new HashMap<>();

    private boolean enableFileVisibilityAfterCreate = false;

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
        // clone the configuration into one with propagated bucket options
        Configuration conf = OBSCommonUtils.propagateBucketOptions(originalConf, bucket);
        OBSCommonUtils.patchSecurityCredentialProviders(conf);
        super.initialize(name, conf);
        setConf(conf);
        try {
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

            OBSCommonUtils.verifyBucketExists(this);
            enablePosix = OBSCommonUtils.getBucketFsStatus(obs, bucket);

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

            String readPolicy = conf.getTrimmed(OBSConstants.READAHEAD_POLICY, OBSConstants.READAHEAD_POLICY_PRIMARY);
            inputPolicyFactory = InputPolicys.createFactory(readPolicy);

            enableTrash = conf.getBoolean(OBSConstants.TRASH_ENABLE, OBSConstants.DEFAULT_TRASH);
            if (enableTrash) {
                if (!isFsBucket()) {
                    String errorMsg = String.format("The bucket [%s] is not posix. not supported for " + "trash.",
                        bucket);
                    LOG.warn(errorMsg);
                    enableTrash = false;
                    trashDir = null;
                } else {
                    trashDir = conf.get(OBSConstants.TRASH_DIR);
                    if (StringUtils.isEmpty(trashDir)) {
                        String errorMsg = String.format(
                            "The trash feature(fs.obs.trash.enable) is " + "enabled, but the "
                                + "configuration(fs.obs.trash.dir [%s]) " + "is empty.", trashDir);
                        LOG.error(errorMsg);
                        throw new ObsException(errorMsg);
                    }
                    trashDir = OBSCommonUtils.maybeAddBeginningSlash(trashDir);
                    trashDir = OBSCommonUtils.maybeAddTrailingSlash(trashDir);
                }
            }
            OBSCommonUtils.setMaxTimeInMillisecondsToRetry(conf.getLong(OBSConstants.MAX_TIME_IN_MILLISECOND_TO_RETRY,
                OBSConstants.DEFAULT_TIME_IN_MILLISECOND_TO_RETRY));
            enableCanonicalServiceName =
                conf.getBoolean(OBSConstants.GET_CANONICAL_SERVICE_NAME_ENABLE,
                    OBSConstants.DEFAULT_GET_CANONICAL_SERVICE_NAME_ENABLE);
            enableFileVisibilityAfterCreate =
                conf.getBoolean(OBSConstants.FILE_VISIBILITY_AFTER_CREATE_ENABLE,
                    OBSConstants.DEFAULT_FILE_VISIBILITY_AFTER_CREATE_ENABLE);
        } catch (ObsException e) {
            throw OBSCommonUtils.translateException("initializing ", new Path(name), e);
        }

        LOG.info("Finish initializing filesystem instance for uri: {}", uri);
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
        long startTime = System.currentTimeMillis();
        long endTime;
        LOG.debug("Opening '{}' for reading.", f);
        final FileStatus fileStatus;
        try {
            fileStatus = OBSCommonUtils.innerGetFileStatusWithRetry(this, f);
        } catch (FileConflictException e) {
            endTime = System.currentTimeMillis();
            if (getMetricSwitch()) {
                BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(null,
                    BasicMetricsConsumer.MetricRecord.OPEN, false, endTime - startTime);
                OBSCommonUtils.setMetricsInfo(this, record);
            }
            throw new AccessControlException(e);
        }

        if (fileStatus.isDirectory()) {
            endTime = System.currentTimeMillis();
            if (getMetricSwitch()) {
                BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(null,
                    BasicMetricsConsumer.MetricRecord.OPEN, false, endTime - startTime);
                OBSCommonUtils.setMetricsInfo(this, record);
            }
            throw new FileNotFoundException("Can't open " + f + " because it is a directory");
        }
        // FSDataInputStream fsDataInputStream = new FSDataInputStream(
        //     new OBSInputStream(bucket, OBSCommonUtils.pathToKey(this, f),
        //         fileStatus.getLen(),
        //         obs, statistics, readAheadRange, this));

        FSInputStream fsInputStream = inputPolicyFactory.create(this, bucket, OBSCommonUtils.pathToKey(this, f),
            fileStatus.getLen(), statistics, boundedMultipartUploadThreadPool);
        FSDataInputStream fsDataInputStream = new FSDataInputStream(fsInputStream);

        endTime = System.currentTimeMillis();
        if (getMetricSwitch()) {
            BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(null,
                BasicMetricsConsumer.MetricRecord.OPEN, true, endTime - startTime);
            OBSCommonUtils.setMetricsInfo(this, record);
        }
        return fsDataInputStream;
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
        String key = OBSCommonUtils.pathToKey(this, f);
        final FileStatus status;
        long startTime = System.currentTimeMillis();
        long endTime;
        boolean exist = true;
        try {
            // get the status or throw an exception
            try {
                status = OBSCommonUtils.innerGetFileStatusWithRetry(this, f);
            } catch (FileConflictException e) {
                endTime = System.currentTimeMillis();
                if (getMetricSwitch()) {
                    BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(
                        BasicMetricsConsumer.MetricRecord.OVERWRITE, BasicMetricsConsumer.MetricRecord.CREATE, false,
                        endTime - startTime);
                    OBSCommonUtils.setMetricsInfo(this, record);
                }

                throw new ParentNotDirectoryException(e.getMessage());
            }

            // if the thread reaches here, there is something at the path
            if (status.isDirectory()) {
                endTime = System.currentTimeMillis();
                if (getMetricSwitch()) {
                    BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(
                        BasicMetricsConsumer.MetricRecord.OVERWRITE, BasicMetricsConsumer.MetricRecord.CREATE, false,
                        endTime - startTime);
                    OBSCommonUtils.setMetricsInfo(this, record);
                }
                // path references a directory: automatic error
                throw new FileAlreadyExistsException(f + " is a directory");
            }
            if (!overwrite) {
                endTime = System.currentTimeMillis();
                if (getMetricSwitch()) {
                    BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(
                        BasicMetricsConsumer.MetricRecord.OVERWRITE, BasicMetricsConsumer.MetricRecord.CREATE, false,
                        endTime - startTime);
                    OBSCommonUtils.setMetricsInfo(this, record);
                }
                // path references a file and overwrite is disabled
                throw new FileAlreadyExistsException(f + " already exists");
            }
            LOG.debug("create: Overwriting file {}", f);
        } catch (FileNotFoundException e) {
            // this means the file is not found
            LOG.debug("create: Creating new file {}", f);
            exist = false;
        }

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

        endTime = System.currentTimeMillis();
        if (getMetricSwitch()) {
            BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(
                BasicMetricsConsumer.MetricRecord.OVERWRITE, BasicMetricsConsumer.MetricRecord.CREATE, true,
                endTime - startTime);
            OBSCommonUtils.setMetricsInfo(this, record);
        }
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
        long startTime = System.currentTimeMillis();
        long endTime;
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
                    status = OBSCommonUtils.innerGetFileStatusWithRetry(this, f);
                } catch (FileConflictException e) {
                    endTime = System.currentTimeMillis();
                    if (getMetricSwitch()) {
                        BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(
                            BasicMetricsConsumer.MetricRecord.FLAGS, BasicMetricsConsumer.MetricRecord.CREATE, false,
                            endTime - startTime);
                        OBSCommonUtils.setMetricsInfo(this, record);
                    }
                    throw new ParentNotDirectoryException(e.getMessage());
                }

                // if the thread reaches here, there is something at the path
                if (status.isDirectory()) {
                    endTime = System.currentTimeMillis();
                    if (getMetricSwitch()) {
                        BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(
                            BasicMetricsConsumer.MetricRecord.FLAGS, BasicMetricsConsumer.MetricRecord.CREATE, false,
                            endTime - startTime);
                        OBSCommonUtils.setMetricsInfo(this, record);
                    }
                    // path references a directory: automatic error
                    throw new FileAlreadyExistsException(f + " is a directory");
                }
            } catch (FileNotFoundException e) {
                LOG.debug("FileNotFoundException, create: Creating new file {}", f);
                exist = false;
            }

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

            endTime = System.currentTimeMillis();
            if (getMetricSwitch()) {
                BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(
                    BasicMetricsConsumer.MetricRecord.FLAGS, BasicMetricsConsumer.MetricRecord.CREATE, true,
                    endTime - startTime);
                OBSCommonUtils.setMetricsInfo(this, record);
            }
            return outputStream;
        } else {
            outputStream = create(f, permission, flags == null || flags.contains(CreateFlag.OVERWRITE), bufferSize,
                replication, blkSize, progress);

            endTime = System.currentTimeMillis();
            if (getMetricSwitch()) {
                BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(
                    BasicMetricsConsumer.MetricRecord.FLAGS, BasicMetricsConsumer.MetricRecord.CREATE, true,
                    endTime - startTime);
                OBSCommonUtils.setMetricsInfo(this, record);
            }
            return outputStream;
        }
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
        long startTime = System.currentTimeMillis();
        long endTime;
        OBSCommonUtils.checkCreateFlag(flags);
        if (path.getParent() != null && !this.exists(path.getParent())) {
            endTime = System.currentTimeMillis();
            if (getMetricSwitch()) {
                BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(null,
                    BasicMetricsConsumer.MetricRecord.CREATE_NR, false, endTime - startTime);
                OBSCommonUtils.setMetricsInfo(this, record);
            }
            throw new FileNotFoundException(path.toString() + " parent directory not exist.");
        }

        FSDataOutputStream fsDataOutputStream = create(path, permission, flags.contains(CreateFlag.OVERWRITE),
            bufferSize, replication, blkSize, progress);
        endTime = System.currentTimeMillis();
        if (getMetricSwitch()) {
            BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(null,
                BasicMetricsConsumer.MetricRecord.CREATE_NR, true, endTime - startTime);
            OBSCommonUtils.setMetricsInfo(this, record);
        }
        return fsDataOutputStream;
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
        long startTime = System.currentTimeMillis();
        long endTime;
        if (!isFsBucket()) {
            throw new UnsupportedOperationException("non-posix bucket. Append is not supported " + "by OBSFileSystem");
        }
        LOG.debug("append: Append file {}.", f);
        String key = OBSCommonUtils.pathToKey(this, f);

        // get the status or throw an FNFE
        FileStatus status;
        try {
            status = OBSCommonUtils.innerGetFileStatusWithRetry(this, f);
        } catch (FileConflictException e) {
            endTime = System.currentTimeMillis();
            if (getMetricSwitch()) {
                BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(null,
                    BasicMetricsConsumer.MetricRecord.APPEND, false, endTime - startTime);
                OBSCommonUtils.setMetricsInfo(this, record);
            }
            throw new AccessControlException(e);
        }

        long objectLen = status.getLen();
        // if the thread reaches here, there is something at the path
        if (status.isDirectory()) {
            endTime = System.currentTimeMillis();
            if (getMetricSwitch()) {
                BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(null,
                    BasicMetricsConsumer.MetricRecord.APPEND, false, endTime - startTime);
                OBSCommonUtils.setMetricsInfo(this, record);
            }
            // path references a directory: automatic error
            throw new FileAlreadyExistsException(f + " is a directory");
        }

        if (isFileBeingWritten(key)) {
            // AlreadyBeingCreatedException (on HDFS NameNode) is transformed
            // into IOException (on HDFS Client)
            throw new IOException("Cannot append " + f + " that is being written.");
        }

        FSDataOutputStream outputStream = new FSDataOutputStream(new OBSBlockOutputStream(this, key, objectLen,
            new SemaphoredDelegatingExecutor(boundedMultipartUploadThreadPool, blockOutputActiveBlocks, true), true),
            null,objectLen);
        synchronized (filesBeingWritten) {
            filesBeingWritten.put(key, outputStream);
        }

        endTime = System.currentTimeMillis();
        if (getMetricSwitch()) {
            BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(null,
                BasicMetricsConsumer.MetricRecord.APPEND, true, endTime - startTime);
            OBSCommonUtils.setMetricsInfo(this, record);
        }
        return outputStream;
    }

    /**
     * Truncate the file in the indicated path to the indicated size.
     * <ul>
     * <li>Fails if path is a directory.</li>
     * <li>Fails if path does not exist.</li>
     * <li>Fails if path is not closed.</li>
     * <li>Fails if new size is greater than current size.</li>
     * </ul>
     *
     * @param f         The path to the file to be truncated
     * @param newLength The size the file is to be truncated to
     * @return <code>true</code> if the file has been truncated to the desired
     * <code>newLength</code> and is immediately available to be reused for
     * write operations such as <code>append</code>, or
     * <code>false</code> if a background process of adjusting the length of
     * the last block has been started, and clients should wait for it to
     * complete before proceeding with further file updates.
     * @throws IOException IO failure
     */
    @Override
    public boolean truncate(Path f, long newLength) throws IOException {
        checkOpen();
        if (!enablePosix) {
            super.truncate(f, newLength);
        }

        if (newLength < 0) {
            throw new IOException(new HadoopIllegalArgumentException(
                "Cannot truncate " + f + " to a negative file size: " + newLength + "."));
        }

        FileStatus status;
        try {
            status = OBSCommonUtils.innerGetFileStatusWithRetry(this, f);
        } catch (FileConflictException e) {
            throw new AccessControlException(e);
        }

        if (!status.isFile()) {
            throw new FileNotFoundException("Path is not a file: " + f);
        }

        String key = OBSCommonUtils.pathToKey(this, f);
        if (isFileBeingWritten(key)) {
            // AlreadyBeingCreatedException (on HDFS NameNode) is transformed
            // into IOException (on HDFS Client)
            throw new AlreadyBeingCreatedException("Cannot truncate " + f + " that is being written.");
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

        OBSPosixBucketUtils.innerFsTruncateWithRetry(this, f, newLength);

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
        long startTime = System.currentTimeMillis();
        long threadId = Thread.currentThread().getId();
        long endTime;
        LOG.debug("Rename path {} to {} start", src, dst);
        try {
            if (enablePosix) {
                boolean success = OBSPosixBucketUtils.renameBasedOnPosix(this, src, dst);
                endTime = System.currentTimeMillis();
                if (getMetricSwitch()) {
                    BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(null,
                        BasicMetricsConsumer.MetricRecord.RENAME, true, endTime - startTime);
                    OBSCommonUtils.setMetricsInfo(this, record);
                }
                return success;
            } else {
                boolean success = OBSObjectBucketUtils.renameBasedOnObject(this, src, dst);
                endTime = System.currentTimeMillis();
                if (getMetricSwitch()) {
                    BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(null,
                        BasicMetricsConsumer.MetricRecord.RENAME, true, endTime - startTime);
                    OBSCommonUtils.setMetricsInfo(this, record);
                }
                return success;
            }
        } catch (ObsException e) {
            endTime = System.currentTimeMillis();
            if (getMetricSwitch()) {
                BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(null,
                    BasicMetricsConsumer.MetricRecord.RENAME, false, endTime - startTime);
                OBSCommonUtils.setMetricsInfo(this, record);
            }
            throw OBSCommonUtils.translateException("rename(" + src + ", " + dst + ")", src, e);
        } catch (RenameFailedException e) {
            endTime = System.currentTimeMillis();
            if (getMetricSwitch()) {
                BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(null,
                    BasicMetricsConsumer.MetricRecord.RENAME, false, endTime - startTime);
                OBSCommonUtils.setMetricsInfo(this, record);
            }
            LOG.error(e.getMessage());
            return e.getExitCode();
        } catch (FileNotFoundException e) {
            endTime = System.currentTimeMillis();
            if (getMetricSwitch()) {
                BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(null,
                    BasicMetricsConsumer.MetricRecord.RENAME, false, endTime - startTime);
                OBSCommonUtils.setMetricsInfo(this, record);
            }
            LOG.error(e.toString());
            return false;
        } finally {
            endTime = System.currentTimeMillis();
            LOG.debug("Rename path {} to {} finished, thread:{}, " + "timeUsedInMilliSec:{}.", src, dst, threadId,
                endTime - startTime);
        }
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
        long startTime = System.currentTimeMillis();
        long endTime;
        try {
            FileStatus status = OBSCommonUtils.innerGetFileStatusWithRetry(this, f);
            LOG.debug("delete: path {} - recursive {}", status.getPath(), recursive);

            if (enablePosix) {
                boolean success = OBSPosixBucketUtils.fsDelete(this, status, recursive);
                endTime = System.currentTimeMillis();
                if (getMetricSwitch()) {
                    BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(null,
                        BasicMetricsConsumer.MetricRecord.DELETE, true, endTime - startTime);
                    OBSCommonUtils.setMetricsInfo(this, record);
                }
                return success;
            }

            boolean success = OBSObjectBucketUtils.objectDelete(this, status, recursive);
            endTime = System.currentTimeMillis();
            if (getMetricSwitch()) {
                BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(null,
                    BasicMetricsConsumer.MetricRecord.DELETE, true, endTime - startTime);
                OBSCommonUtils.setMetricsInfo(this, record);
            }
            return success;
        } catch (FileNotFoundException e) {
            LOG.warn("Couldn't delete {} - does not exist", f);
            endTime = System.currentTimeMillis();
            if (getMetricSwitch()) {
                BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(null,
                    BasicMetricsConsumer.MetricRecord.DELETE, false, endTime - startTime);
                OBSCommonUtils.setMetricsInfo(this, record);
            }
            return false;
        } catch (FileConflictException e) {
            endTime = System.currentTimeMillis();
            if (getMetricSwitch()) {
                BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(null,
                    BasicMetricsConsumer.MetricRecord.DELETE, false, endTime - startTime);
                OBSCommonUtils.setMetricsInfo(this, record);
            }
            throw new AccessControlException(e);
        } catch (ObsException e) {
            endTime = System.currentTimeMillis();
            if (getMetricSwitch()) {
                BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(null,
                    BasicMetricsConsumer.MetricRecord.DELETE, false, endTime - startTime);
                OBSCommonUtils.setMetricsInfo(this, record);
            }
            throw OBSCommonUtils.translateException("delete", f, e);
        }
    }

    /**
     * Return a flag that indicates if fast delete is enabled.
     *
     * @return the flag
     */
    boolean isEnableTrash() {
        return enableTrash;
    }

    /**
     * Return trash directory for fast delete.
     *
     * @return the trash directory
     */
    String getTrashDir() {
        return trashDir;
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
        long startTime = System.currentTimeMillis();
        long threadId = Thread.currentThread().getId();
        long endTime;
        try {
            FileStatus[] statuses = OBSCommonUtils.innerListStatus(this, f, false);
            endTime = System.currentTimeMillis();
            LOG.debug("List status for path:{}, thread:{}, timeUsedInMilliSec:{}", f, threadId, endTime - startTime);
            if (getMetricSwitch()) {
                BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(
                    BasicMetricsConsumer.MetricRecord.NONRECURSIVE, BasicMetricsConsumer.MetricRecord.LIST_STATUS, true,
                    endTime - startTime);
                OBSCommonUtils.setMetricsInfo(this, record);
            }
            return statuses;
        } catch (ObsException e) {
            endTime = System.currentTimeMillis();
            if (getMetricSwitch()) {
                BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(
                    BasicMetricsConsumer.MetricRecord.NONRECURSIVE, BasicMetricsConsumer.MetricRecord.LIST_STATUS,
                    false, endTime - startTime);
                OBSCommonUtils.setMetricsInfo(this, record);
            }
            throw OBSCommonUtils.translateException("listStatus", f, e);
        }
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
        long startTime = System.currentTimeMillis();
        long threadId = Thread.currentThread().getId();
        long endTime;
        try {
            FileStatus[] statuses = OBSCommonUtils.innerListStatus(this, f, recursive);
            endTime = System.currentTimeMillis();
            LOG.debug("List status for path:{}, thread:{}, timeUsedInMilliSec:{}", f, threadId, endTime - startTime);
            if (getMetricSwitch()) {
                BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(
                    BasicMetricsConsumer.MetricRecord.RECURSIVE, BasicMetricsConsumer.MetricRecord.LIST_STATUS, true,
                    endTime - startTime);
                OBSCommonUtils.setMetricsInfo(this, record);
            }
            return statuses;
        } catch (ObsException e) {
            endTime = System.currentTimeMillis();
            if (getMetricSwitch()) {
                BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(
                    BasicMetricsConsumer.MetricRecord.RECURSIVE, BasicMetricsConsumer.MetricRecord.LIST_STATUS, false,
                    endTime - startTime);
                OBSCommonUtils.setMetricsInfo(this, record);
            }
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
        long startTime = System.currentTimeMillis();
        long endTime;
        try {
            boolean success = OBSCommonUtils.innerMkdirs(this, path);
            endTime = System.currentTimeMillis();
            if (getMetricSwitch()) {
                BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(null,
                    BasicMetricsConsumer.MetricRecord.MKDIRS, true, endTime - startTime);
                OBSCommonUtils.setMetricsInfo(this, record);
            }
            return success;
        } catch (ObsException e) {
            endTime = System.currentTimeMillis();
            if (getMetricSwitch()) {
                BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(null,
                    BasicMetricsConsumer.MetricRecord.MKDIRS, false, endTime - startTime);
                OBSCommonUtils.setMetricsInfo(this, record);
            }
            throw OBSCommonUtils.translateException("mkdirs", path, e);
        }
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
        long startTime = System.currentTimeMillis();
        long endTime;
        try {
            FileStatus fileStatus = OBSCommonUtils.innerGetFileStatusWithRetry(this, f);
            endTime = System.currentTimeMillis();
            if (getMetricSwitch()) {
                BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(null,
                    BasicMetricsConsumer.MetricRecord.GET_FILE_STATUS, true, endTime - startTime);
                OBSCommonUtils.setMetricsInfo(this, record);
            }
            return fileStatus;
        } catch (FileConflictException e) {
            endTime = System.currentTimeMillis();
            if (getMetricSwitch()) {
                BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(null,
                    BasicMetricsConsumer.MetricRecord.GET_FILE_STATUS, false, endTime - startTime);
                OBSCommonUtils.setMetricsInfo(this, record);
            }
            // For super user, convert AccessControlException
            // to null on NameNode then to FileNotFoundException on Client
            throw new FileNotFoundException(e.getMessage());
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
        OBSFileStatus fileStatus;
        if (enablePosix) {
            fileStatus = OBSPosixBucketUtils.innerFsGetObjectStatus(this, f);
            return fileStatus;
        }
        fileStatus = OBSObjectBucketUtils.innerGetObjectStatus(this, f);
        return fileStatus;
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
        long startTime = System.currentTimeMillis();
        long endTime;
        ContentSummary contentSummary;
        if (!obsContentSummaryEnable) {
            return super.getContentSummary(f);
        }

        FileStatus status;
        try {
            status = OBSCommonUtils.innerGetFileStatusWithRetry(this, f);
        } catch (FileConflictException e) {
            endTime = System.currentTimeMillis();
            if (getMetricSwitch()) {
                BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(null,
                    BasicMetricsConsumer.MetricRecord.GET_CONTENT_SUMMARY, false, endTime - startTime);
                OBSCommonUtils.setMetricsInfo(this, record);
            }
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
            endTime = System.currentTimeMillis();
            if (getMetricSwitch()) {
                BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(null,
                    BasicMetricsConsumer.MetricRecord.GET_CONTENT_SUMMARY, true, endTime - startTime);

                OBSCommonUtils.setMetricsInfo(this, record);
            }
            return contentSummary;
        }

        // f is a directory
        if (enablePosix) {
            contentSummary = OBSPosixBucketUtils.fsGetDirectoryContentSummary(this, OBSCommonUtils.pathToKey(this, f));
            endTime = System.currentTimeMillis();
            if (getMetricSwitch()) {
                BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(null,
                    BasicMetricsConsumer.MetricRecord.GET_CONTENT_SUMMARY, true, endTime - startTime);
                OBSCommonUtils.setMetricsInfo(this, record);
            }
            return contentSummary;
        } else {
            contentSummary = OBSObjectBucketUtils.getDirectoryContentSummary(this, OBSCommonUtils.pathToKey(this, f));
            endTime = System.currentTimeMillis();
            if (getMetricSwitch()) {
                BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(null,
                    BasicMetricsConsumer.MetricRecord.GET_CONTENT_SUMMARY, true, endTime - startTime);
                OBSCommonUtils.setMetricsInfo(this, record);
            }
            return contentSummary;
        }
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
        long startTime = System.currentTimeMillis();
        long endTime;
        try {
            super.copyFromLocalFile(delSrc, overwrite, src, dst);
            endTime = System.currentTimeMillis();
            if (getMetricSwitch()) {
                BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(null,
                    BasicMetricsConsumer.MetricRecord.COPYFROMLOCAL, true, endTime - startTime);
                OBSCommonUtils.setMetricsInfo(this, record);
            }
        } catch (ObsException e) {
            endTime = System.currentTimeMillis();
            if (getMetricSwitch()) {
                BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(null,
                    BasicMetricsConsumer.MetricRecord.COPYFROMLOCAL, false, endTime - startTime);
                OBSCommonUtils.setMetricsInfo(this, record);
            }
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
        long startTime = System.currentTimeMillis();
        closeAllFilesBeingWritten();

        closed = true;
        long endTime = System.currentTimeMillis();
        if (getMetricSwitch()) {
            BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(
                BasicMetricsConsumer.MetricRecord.FS, BasicMetricsConsumer.MetricRecord.CLOSE, true,
                endTime - startTime);
            OBSCommonUtils.setMetricsInfo(this, record);
        }

        try {
            super.close();
            if (metricsConsumer != null) {
                metricsConsumer.close();
            }
            obs.close();
        } finally {
            OBSCommonUtils.shutdownAll(boundedMultipartUploadThreadPool, boundedCopyThreadPool, boundedDeleteThreadPool,
                boundedCopyPartThreadPool, boundedListThreadPool);
        }

        LOG.info("Finish closing filesystem instance for uri: {}", uri);
    }

    /**
     * Override {@code getCanonicalServiceName} and return {@code null} since
     * delegation token is not supported.
     */
    @Override
    public String getCanonicalServiceName() {
        // Does not support Token, only enable for HBase BulkLoad
        if (enableCanonicalServiceName) {
            return getScheme() + "://" + bucket;
        }
        return null;
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
        long startTime = System.currentTimeMillis();
        long endTime;
        RemoteIterator<LocatedFileStatus> locatedFileStatus;
        Path path = OBSCommonUtils.qualify(this, f);

        LOG.debug("listFiles({}, {})", path, recursive);
        try {
            // lookup dir triggers existence check
            final FileStatus fileStatus;
            try {
                fileStatus = OBSCommonUtils.innerGetFileStatusWithRetry(this, path);
            } catch (FileConflictException e) {
                endTime = System.currentTimeMillis();
                if (getMetricSwitch()) {
                    BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(null,
                        BasicMetricsConsumer.MetricRecord.LIST_FILES, false, endTime - startTime);
                    OBSCommonUtils.setMetricsInfo(this, record);
                }
                throw new AccessControlException(e);
            }

            if (fileStatus.isFile()) {
                locatedFileStatus = new OBSListing.SingleStatusRemoteIterator(
                    OBSCommonUtils.toLocatedFileStatus(this, fileStatus));

                endTime = System.currentTimeMillis();
                if (getMetricSwitch()) {
                    BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(null,
                        BasicMetricsConsumer.MetricRecord.LIST_FILES, true, endTime - startTime);
                    OBSCommonUtils.setMetricsInfo(this, record);
                }
                // simple case: File
                LOG.debug("Path is a file");
                return locatedFileStatus;
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

                endTime = System.currentTimeMillis();
                if (getMetricSwitch()) {
                    BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(null,
                        BasicMetricsConsumer.MetricRecord.LIST_FILES, true, endTime - startTime);
                    OBSCommonUtils.setMetricsInfo(this, record);
                }
                return locatedFileStatus;
            }
        } catch (ObsException e) {
            endTime = System.currentTimeMillis();
            if (getMetricSwitch()) {
                BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(null,
                    BasicMetricsConsumer.MetricRecord.LIST_FILES, false, endTime - startTime);
                OBSCommonUtils.setMetricsInfo(this, record);
            }
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
        Path path = OBSCommonUtils.qualify(this, f);
        LOG.debug("listLocatedStatus({}, {}", path, filter);
        long startTime = System.currentTimeMillis();
        long endTime;
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteList;
        try {
            // lookup dir triggers existence check
            final FileStatus fileStatus;
            try {
                fileStatus = OBSCommonUtils.innerGetFileStatusWithRetry(this, path);
            } catch (FileConflictException e) {
                endTime = System.currentTimeMillis();
                if (getMetricSwitch()) {
                    BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(null,
                        BasicMetricsConsumer.MetricRecord.LIST_LOCATED_STS, false, endTime - startTime);
                    OBSCommonUtils.setMetricsInfo(this, record);
                }
                throw new AccessControlException(e);
            }

            if (fileStatus.isFile()) {
                // simple case: File
                LOG.debug("Path is a file");
                locatedFileStatusRemoteList = new OBSListing.SingleStatusRemoteIterator(
                    filter.accept(path) ? OBSCommonUtils.toLocatedFileStatus(this, fileStatus) : null);
                endTime = System.currentTimeMillis();
                if (getMetricSwitch()) {
                    BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(null,
                        BasicMetricsConsumer.MetricRecord.LIST_LOCATED_STS, true, endTime - startTime);
                    OBSCommonUtils.setMetricsInfo(this, record);
                }
                return locatedFileStatusRemoteList;
            } else {
                // directory: trigger a lookup
                String key = OBSCommonUtils.maybeAddTrailingSlash(OBSCommonUtils.pathToKey(this, path));
                locatedFileStatusRemoteList = obsListing.createLocatedFileStatusIterator(
                    obsListing.createFileStatusListingIterator(path,
                        OBSCommonUtils.createListObjectsRequest(this, key, "/"), filter,
                        new OBSListing.AcceptAllButSelfAndS3nDirs(path)));
                endTime = System.currentTimeMillis();
                if (getMetricSwitch()) {
                    BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(null,
                        BasicMetricsConsumer.MetricRecord.LIST_LOCATED_STS, true, endTime - startTime);
                    OBSCommonUtils.setMetricsInfo(this, record);
                }
                return locatedFileStatusRemoteList;
            }
        } catch (ObsException e) {
            endTime = System.currentTimeMillis();
            if (getMetricSwitch()) {
                BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(null,
                    BasicMetricsConsumer.MetricRecord.LIST_LOCATED_STS, false, endTime - startTime);
                OBSCommonUtils.setMetricsInfo(this, record);
            }
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
}
