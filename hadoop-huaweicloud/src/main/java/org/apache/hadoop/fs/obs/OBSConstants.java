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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.obs.input.OBSInputStream;

/**
 * All constants used by {@link OBSFileSystem}.
 *
 * <p>Some of the strings are marked as {@code Unstable}. This means that they
 * may be unsupported in future; at which point they will be marked as
 * deprecated and simply ignored.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class OBSConstants {
    /**
     * Minimum multipart size which OBS supports.
     */
    public static final int MULTIPART_MIN_SIZE = 1024 * 1024;

    /**
     * OBS access key.
     */
    public static final String ACCESS_KEY = "fs.obs.access.key";

    /**
     * OBS secret key.
     */
    public static final String SECRET_KEY = "fs.obs.secret.key";

    /**
     * OBS credentials provider.
     */
    static final String OBS_CREDENTIALS_PROVIDER = "fs.obs.credentials.provider";

    /**
     * OBS metrics consumer.
     */
    static final String OBS_METRICS_CONSUMER = "fs.obs.metrics.consumer";

    /**
     * Default value of {@link #OBS_METRICS_CONSUMER}.
     */
    static final Class<? extends BasicMetricsConsumer> DEFAULT_OBS_METRICS_CONSUMER = DefaultMetricsConsumer.class;

    /**
     * OBS client security provider.
     */
    static final String OBS_SECURITY_PROVIDER = "fs.obs.security.provider";

    /**
     * Extra set of security credentials which will be prepended to that set in
     * {@code "hadoop.security.credential.provider.path"}. This extra option
     * allows for per-bucket overrides.
     */
    static final String OBS_SECURITY_CREDENTIAL_PROVIDER_PATH = "fs.obs.security.credential.provider.path";

    /**
     * Switch for whether need to verify buffer dir accessibility.
     */
    static final String VERIFY_BUFFER_DIR_ACCESSIBLE_ENABLE = "fs.obs.bufferdir.verify.enable";

    /**
     * Session token for when using TemporaryOBSCredentialsProvider.
     */
    static final String SESSION_TOKEN = "fs.obs.session.token";

    /**
     * Maximum number of simultaneous connections to obs.
     */
    static final String MAXIMUM_CONNECTIONS = "fs.obs.connection.maximum";

    /**
     * Default value of {@link #MAXIMUM_CONNECTIONS}.
     */
    static final int DEFAULT_MAXIMUM_CONNECTIONS = 1000;

    /**
     * Connect to obs over ssl.
     */
    static final String SECURE_CONNECTIONS = "fs.obs.connection.ssl.enabled";

    /**
     * Default value of {@link #SECURE_CONNECTIONS}.
     */
    static final boolean DEFAULT_SECURE_CONNECTIONS = false;

    /**
     * Use a custom endpoint.
     */
    public static final String ENDPOINT = "fs.obs.endpoint";

    /**
     * Host for connecting to OBS through proxy server.
     */
    static final String PROXY_HOST = "fs.obs.proxy.host";

    /**
     * Port for connecting to OBS through proxy server.
     */
    static final String PROXY_PORT = "fs.obs.proxy.port";

    /**
     * User name for connecting to OBS through proxy server.
     */
    static final String PROXY_USERNAME = "fs.obs.proxy.username";

    /**
     * Password for connecting to OBS through proxy server.
     */
    static final String PROXY_PASSWORD = "fs.obs.proxy.password";

    /**
     * String constant for http schema.
     */
    static final String HTTP_PREFIX = "http://";

    /**
     * String constant for https schema.
     */
    static final String HTTPS_PREFIX = "https://";

    /**
     * Default port for HTTPS.
     */
    static final int DEFAULT_HTTPS_PORT = 443;

    /**
     * Default port for HTTP.
     */
    static final int DEFAULT_HTTP_PORT = 80;

    /**
     * Number of times we should retry errors.
     */
    static final String MAX_ERROR_RETRIES = "fs.obs.attempts.maximum";

    /**
     * Default value of {@link #MAX_ERROR_RETRIES}.
     */
    static final int DEFAULT_MAX_ERROR_RETRIES = 3;

    /**
     * Seconds until we give up trying to establish a connection to obs.
     */
    static final String ESTABLISH_TIMEOUT = "fs.obs.connection.establish.timeout";

    /**
     * Default value of {@link #ESTABLISH_TIMEOUT}.
     */
    static final int DEFAULT_ESTABLISH_TIMEOUT = 5000;

    /**
     * Seconds until we give up on a connection to obs.
     */
    static final String SOCKET_TIMEOUT = "fs.obs.connection.timeout";

    /**
     * Default value of {@link #SOCKET_TIMEOUT}.
     */
    static final int DEFAULT_SOCKET_TIMEOUT = 120000;

    /**
     * Socket send buffer to be used in OBS SDK.
     */
    static final String SOCKET_SEND_BUFFER = "fs.obs.socket.send.buffer";

    /**
     * Default value of {@link #SOCKET_SEND_BUFFER}.
     */
    static final int DEFAULT_SOCKET_SEND_BUFFER = 256 * 1024;

    /**
     * Socket receive buffer to be used in OBS SDK.
     */
    static final String SOCKET_RECV_BUFFER = "fs.obs.socket.recv.buffer";

    /**
     * Default value of {@link #SOCKET_RECV_BUFFER}.
     */
    static final int DEFAULT_SOCKET_RECV_BUFFER = 256 * 1024;

    /**
     * Number of records to get while paging through a directory listing.
     */
    static final String MAX_PAGING_KEYS = "fs.obs.paging.maximum";

    /**
     * Default value of {@link #MAX_PAGING_KEYS}.
     */
    static final int DEFAULT_MAX_PAGING_KEYS = 1000;

    /**
     * Maximum number of threads to allow in the pool used by TransferManager.
     */
    static final String MAX_THREADS = "fs.obs.threads.max";

    /**
     * Default value of {@link #MAX_THREADS}.
     */
    static final int DEFAULT_MAX_THREADS = 20;

    /**
     * Maximum number of tasks cached if all threads are already uploading.
     */
    static final String MAX_TOTAL_TASKS = "fs.obs.max.total.tasks";

    /**
     * Default value of {@link #MAX_TOTAL_TASKS}.
     */
    static final int DEFAULT_MAX_TOTAL_TASKS = 20;

    /**
     * Max number of copy threads.
     */
    static final String MAX_COPY_THREADS = "fs.obs.copy.threads.max";

    /**
     * Default value of {@link #MAX_COPY_THREADS}.
     */
    static final int DEFAULT_MAX_COPY_THREADS = 40;

    /**
     * Max number of delete threads.
     */
    static final String MAX_DELETE_THREADS = "fs.obs.delete.threads.max";

    /**
     * Default value of {@link #MAX_DELETE_THREADS}.
     */
    static final int DEFAULT_MAX_DELETE_THREADS = 20;

    /**
     * Unused option: maintained for compile-time compatibility. If set, a
     * warning is logged in OBS during init.
     */
    @Deprecated
    static final String CORE_THREADS = "fs.obs.threads.core";

    /**
     * The time that an idle thread waits before terminating.
     */
    static final String KEEPALIVE_TIME = "fs.obs.threads.keepalivetime";

    /**
     * Default value of {@link #KEEPALIVE_TIME}.
     */
    static final int DEFAULT_KEEPALIVE_TIME = 60;

    /**
     * Size of each of or multipart pieces in bytes.
     */
    public static final String MULTIPART_SIZE = "fs.obs.multipart.size";

    /**
     * Default value of {@link #MULTIPART_SIZE}.
     */
    static final long DEFAULT_MULTIPART_SIZE = 104857600; // 100 MB

    /**
     * Enable multi-object delete calls.
     */
    static final String ENABLE_MULTI_DELETE = "fs.obs.multiobjectdelete.enable";

    /**
     * Max number of objects in one multi-object delete call. This option takes
     * effect only when the option 'ENABLE_MULTI_DELETE' is set to 'true'.
     */
    static final String MULTI_DELETE_MAX_NUMBER = "fs.obs.multiobjectdelete.maximum";

    /**
     * Default value of {@link #MULTI_DELETE_MAX_NUMBER}.
     */
    static final int DEFAULT_MULTI_DELETE_MAX_NUMBER = 1000;

    /**
     * Minimum number of objects in one multi-object delete call.
     */
    static final String MULTI_DELETE_THRESHOLD = "fs.obs.multiobjectdelete.threshold";

    /**
     * Default value of {@link #MULTI_DELETE_THRESHOLD}.
     */
    static final int MULTI_DELETE_DEFAULT_THRESHOLD = 3;

    /**
     * Comma separated list of directories.
     */
    static final String BUFFER_DIR = "fs.obs.buffer.dir";

    /**
     * Switch to the fast block-by-block upload mechanism.
     */
    public static final String FAST_UPLOAD = "fs.obs.fast.upload";

    /**
     * What buffer to use. Default is {@link #FAST_UPLOAD_BUFFER_DISK} Value:
     * {@value}
     */
    @InterfaceStability.Unstable
    public static final String FAST_UPLOAD_BUFFER = "fs.obs.fast.upload.buffer";

    /**
     * Buffer blocks to disk: {@value}. Capacity is limited to available disk
     * space.
     */
    @InterfaceStability.Unstable
    static final String FAST_UPLOAD_BUFFER_DISK = "disk";

    /**
     * Use an in-memory array. Fast but will run of heap rapidly: {@value}.
     */
    @InterfaceStability.Unstable
    public static final String FAST_UPLOAD_BUFFER_ARRAY = "array";

    /**
     * Use a byte buffer. May be more memory efficient than the {@link
     * #FAST_UPLOAD_BUFFER_ARRAY}: {@value}.
     */
    @InterfaceStability.Unstable
    static final String FAST_UPLOAD_BYTEBUFFER = "bytebuffer";

    /**
     * Maximum number of blocks a single output stream can have active
     * (uploading, or queued to the central FileSystem instance's pool of queued
     * operations. )This stops a single stream overloading the shared thread
     * pool. {@value}
     *
     * <p>Default is {@link #DEFAULT_FAST_UPLOAD_ACTIVE_BLOCKS}
     */
    @InterfaceStability.Unstable
    static final String FAST_UPLOAD_ACTIVE_BLOCKS = "fs.obs.fast.upload.active.blocks";

    /**
     * Limit of queued block upload operations before writes block. Value:
     * {@value}
     */
    @InterfaceStability.Unstable
    static final int DEFAULT_FAST_UPLOAD_ACTIVE_BLOCKS = 4;

    /**
     * Canned acl options: Private | PublicRead | PublicReadWrite |
     * AuthenticatedRead | LogDeliveryWrite | BucketOwnerRead |
     * BucketOwnerFullControl.
     */
    static final String CANNED_ACL = "fs.obs.acl.default";

    /**
     * Default value of {@link #CANNED_ACL}.
     */
    static final String DEFAULT_CANNED_ACL = "";

    /**
     * Should we try to purge old multipart uploads when starting up.
     */
    static final String PURGE_EXISTING_MULTIPART = "fs.obs.multipart.purge";

    /**
     * Default value of {@link #PURGE_EXISTING_MULTIPART}.
     */
    static final boolean DEFAULT_PURGE_EXISTING_MULTIPART = false;

    /**
     * Purge any multipart uploads older than this number of seconds.
     */
    static final String PURGE_EXISTING_MULTIPART_AGE = "fs.obs.multipart.purge.age";

    /**
     * Default value of {@link #PURGE_EXISTING_MULTIPART_AGE}.
     */
    static final long DEFAULT_PURGE_EXISTING_MULTIPART_AGE = 86400;

    /**
     * OBS folder suffix.
     */
    static final String OBS_FOLDER_SUFFIX = "_$folder$";

    /**
     * Block size for
     * {@link org.apache.hadoop.fs.FileSystem#getDefaultBlockSize()}.
     */
    static final String FS_OBS_BLOCK_SIZE = "fs.obs.block.size";

    /**
     * Default value of {@link #FS_OBS_BLOCK_SIZE}.
     */
    static final int DEFAULT_FS_OBS_BLOCK_SIZE = 128 * 1024 * 1024;

    /**
     * OBS scheme.
     */
    static final String OBS_SCHEME = "obs";

    /**
     * Prefix for all OBS properties: {@value}.
     */
    static final String FS_OBS_PREFIX = "fs.obs.";

    /**
     * Prefix for OBS bucket-specific properties: {@value}.
     */
    static final String FS_OBS_BUCKET_PREFIX = "fs.obs.bucket.";

    /**
     * OBS default port.
     */
    static final int OBS_DEFAULT_PORT = -1;

    /**
     * User agent prefix.
     */
    static final String USER_AGENT_PREFIX = "fs.obs.user.agent.prefix";

    public static final String DELEGATION_TOKEN_ONLY  = "fs.obs.delegation.token.only";

    public static final boolean DEFAULT_DELEGATION_TOKEN_ONLY  = false;

    public static final String DELEGATION_TOKEN_PROVIDERS  = "fs.obs.delegation.token.providers";

    public static final String DEFAULT_DELEGATION_TOKEN_PROVIDERS  = "";
    /**
     * what read policy to use. Default is {@link #READAHEAD_POLICY_PRIMARY} Value:
     * {@value}
     */
    @InterfaceStability.Unstable
    public static final String READAHEAD_POLICY = "fs.obs.readahead.policy";

    @InterfaceStability.Unstable
    public static final String READAHEAD_POLICY_PRIMARY = "primary";

    @InterfaceStability.Unstable
    public static final String READAHEAD_POLICY_ADVANCE = "advance";

    @InterfaceStability.Unstable
    public static final String READAHEAD_POLICY_MEMARTSCC = "memArtsCC";

    public static final String MEMARTSCC_LOCALITY_ENABLE = "fs.obs.memartscc.locality.enable";

    public static final String CACHE_CONFIG_PREFIX = "fs.obs.cache.config.prefix";

    public static final String DEFAULT_CACHE_CONFIG_PREFIX = "fs.obs.memartscc.config";

    public static final boolean DEFAULT_MEMARTSCC_LOCALITY_ENABLE = false;

    /**
     * Read ahead buffer size to prevent connection re-establishments.
     */
    public static final String READAHEAD_RANGE = "fs.obs.readahead.range";

    /**
     * Default value of {@link #READAHEAD_RANGE}.
     */
    public static final long DEFAULT_READAHEAD_RANGE = 1024 * 1024;

    /**
     * the prefetch range sent to memartscc
     */
    public static final String MEMARTSCC_READAHEAD_RANGE = "fs.obs.memartscc.readahead.range";

    public static final long DEFAULT_MEMARTSCC_READAHEAD_RANGE = 8 * 1024 * 1024;

    public static final String MEMARTSCC_BUFFER_SIZE = "fs.obs.memartscc.buffer.size";

    public static final int DEFAULT_MEMARTSCC_BUFFER_SIZE = 8192;

    public static final String MEMARTSCC_DIRECTBUFFER_SIZE = "fs.obs.memartscc.directbuffer.size";

    public static final int DEFAULT_MEMARTSCC_DIRECTBUFFER_SIZE = 1024 * 1024;

    public static final String MEMARTSCC_AKSK_REFRESH_INTERVAL = "fs.obs.memartscc.aksk.refresh.interval";

    public static final int DEFAULT_MEMARTSCC_AKSK_REFRESH_INTERVAL = 60; // 60 sec

    public static final String MEMARTSCC_CACHE_IMPL = "fs.obs.memartscc.cache.impl";

    public static final String READAHEAD_MAX_NUM = "fs.obs.readahead.max.number";

    public static final int DEFAULT_READAHEAD_MAX_NUM = 4;

    public static final String MEMARTSCC_INPUTSTREAM_BUFFER_TYPE = "fs.obs.memartscc.inputstream.buffer.type";

    public static final String MEMARTSCC_INPUTSTREAM_BUFFER_TYPE_BIND = "bind";

    public static final String MEMARTSCC_INPUTSTREAM_BUFFER_TYPE_POOL = "pool";

    public static final String MEMARTSCC_INPUTSTREAM_BUFFER_POOL_MAX_SIZE = "fs.obs.memartscc.inputstream.buffer.pool.maxsize";

    public static final int MEMARTSCC_INPUTSTREAM_BUFFER_POOL_DEFAULT_MAX_SIZE = 128;

    public static final String MEMARTSCC_INPUTSTREAM_BUFFER_BORROW_TIMEOUT = "fs.obs.memartscc.inputstream.buffer.poll.timeout";

    public static final int MEMARTSCC_INPUTSTREAM_BUFFER_BORROW_DEFAULT_TIMEOUT = 5000; // ms

    public static final String MEMARTSCC_PYSPARK_OPTIMIZED = "fs.obs.memartscc.pyspark.optimized";

    public static final boolean DEFAULT_MEMARTSCC_PYSPARK_OPTIMIZED = true;

    public static final String MEMARTSCC_TRAFFIC_REPORT_ENABLE = "fs.obs.memartscc.inputstream.statistics.report.enable";

    public static final boolean DEFAULT_MEMARTSCC_TRAFFIC_REPORT_ENABLE = false;

    /**
     * The interval of reporting traffic statistics to CC SDK, unit: seconds
     */
    public static final String MEMARTSCC_TRAFFIC_REPORT_INTERVAL = "fs.obs.memartscc.inputstream.statistics.report.interval";

    public static final long MEMARTSCC_TRAFFIC_REPORT_DEFAULT_INTERVAL = 30;

    /*
    * memartscc duplications num, set to 1 for now
    * */
    public static final int MAX_DUPLICATION_NUM = 1;

    /*
     * memartscc errorcode getShardInfo success
     * */
    public static final int GET_SHARD_INFO_SUCCESS = 0;

    /**
     * Flag indicating if
     * {@link OBSInputStream#read(long, byte[], int, int)}
     * will use the implementation of
     * {@link org.apache.hadoop.fs.FSInputStream#read(long,
     * byte[], int, int)}.
     */
    public static final String READAHEAD_TRANSFORM_ENABLE = "fs.obs.read.transform.enable";

    /**
     * obs file system permission mode settings, only take effect on posix file system.
     */
    public static final String PERMISSIONS_MODE = "fs.obs.permissions.mode";

    /**
     * default permission mode, doesn't support permissions.
     */
    public static final String DEFAULT_PERMISSIONS_MODE = "none";

    /**
     * disguise permission mode, support file owner, group, permission attribute.
     */
    public static final String PERMISSIONS_MODE_DISGUISE = "disguise";

    /**
     * OBS client factory implementation class.
     */
    @InterfaceAudience.Private
    @InterfaceStability.Unstable
    static final String OBS_CLIENT_FACTORY_IMPL = "fs.obs.client.factory.impl";

    /**
     * Default value of {@link #OBS_CLIENT_FACTORY_IMPL}.
     */
    @InterfaceAudience.Private
    @InterfaceStability.Unstable
    static final Class<? extends OBSClientFactory> DEFAULT_OBS_CLIENT_FACTORY_IMPL = DefaultOBSClientFactory.class;

    /**
     * Maximum number of partitions in a multipart upload: {@value}.
     */
    @InterfaceAudience.Private
    static final int MAX_MULTIPART_COUNT = 10000;

    // OBS Client configuration

    /**
     * Idle connection time.
     */
    static final String IDLE_CONNECTION_TIME = "fs.obs.idle.connection.time";

    /**
     * Default value of {@link #IDLE_CONNECTION_TIME}.
     */
    static final int DEFAULT_IDLE_CONNECTION_TIME = 30000;

    /**
     * Maximum number of idle connections.
     */
    static final String MAX_IDLE_CONNECTIONS = "fs.obs.max.idle.connections";

    /**
     * Default value of {@link #MAX_IDLE_CONNECTIONS}.
     */
    static final int DEFAULT_MAX_IDLE_CONNECTIONS = 1000;

    /**
     * Keep alive.
     */
    static final String KEEP_ALIVE = "fs.obs.keep.alive";

    /**
     * Default value of {@link #KEEP_ALIVE}.
     */
    static final boolean DEFAULT_KEEP_ALIVE = true;

    /**
     * Validate certificate.
     */
    static final String VALIDATE_CERTIFICATE = "fs.obs.validate.certificate";

    /**
     * Default value of {@link #VALIDATE_CERTIFICATE}.
     */
    static final boolean DEFAULT_VALIDATE_CERTIFICATE = false;

    /**
     * Verify response content type.
     */
    static final String VERIFY_RESPONSE_CONTENT_TYPE = "fs.obs.verify.response.content.type";

    /**
     * Default value of {@link #VERIFY_RESPONSE_CONTENT_TYPE}.
     */
    static final boolean DEFAULT_VERIFY_RESPONSE_CONTENT_TYPE = true;

    /**
     * UploadStreamRetryBufferSize.
     */
    static final String UPLOAD_STREAM_RETRY_SIZE = "fs.obs.upload.stream.retry.buffer.size";

    /**
     * Default value of {@link #UPLOAD_STREAM_RETRY_SIZE}.
     */
    static final int DEFAULT_UPLOAD_STREAM_RETRY_SIZE = 512 * 1024;

    /**
     * Read buffer size.
     */
    static final String READ_BUFFER_SIZE = "fs.obs.read.buffer.size";

    /**
     * Default value of {@link #READ_BUFFER_SIZE}.
     */
    static final int DEFAULT_READ_BUFFER_SIZE = 256 * 1024;

    /**
     * Write buffer size.
     */
    static final String WRITE_BUFFER_SIZE = "fs.obs.write.buffer.size";

    /**
     * Default value of {@link #WRITE_BUFFER_SIZE}.
     */
    static final int DEFAULT_WRITE_BUFFER_SIZE = 256 * 1024;

    /**
     * Canonical name.
     */
    static final String CNAME = "fs.obs.cname";

    /**
     * Default value of {@link #CNAME}.
     */
    static final boolean DEFAULT_CNAME = false;

    /**
     * Strict host name verification.
     */
    static final String STRICT_HOSTNAME_VERIFICATION = "fs.obs.strict.hostname.verification";

    /**
     * Default value of {@link #STRICT_HOSTNAME_VERIFICATION}.
     */
    static final boolean DEFAULT_STRICT_HOSTNAME_VERIFICATION = false;

    /**
     * Size of object copy part pieces in bytes.
     */
    static final String COPY_PART_SIZE = "fs.obs.copypart.size";

    /**
     * Maximum value of {@link #COPY_PART_SIZE}.
     */
    static final long MAX_COPY_PART_SIZE = 5368709120L; // 5GB

    /**
     * Default value of {@link #COPY_PART_SIZE}.
     */
    static final long DEFAULT_COPY_PART_SIZE = 104857600L; // 100MB

    /**
     * Maximum number of copy part threads.
     */
    static final String MAX_COPY_PART_THREADS = "fs.obs.copypart.threads.max";

    /**
     * Default value of {@link #MAX_COPY_PART_THREADS}.
     */
    static final int DEFAULT_MAX_COPY_PART_THREADS = 40;

    /**
     * Number of core list threads.
     */
    static final String CORE_LIST_THREADS = "fs.obs.list.threads.core";

    /**
     * Default value of {@link #CORE_LIST_THREADS}.
     */
    static final int DEFAULT_CORE_LIST_THREADS = 30;

    /**
     * Maximum number of list threads.
     */
    static final String MAX_LIST_THREADS = "fs.obs.list.threads.max";

    /**
     * Default value of {@link #MAX_LIST_THREADS}.
     */
    static final int DEFAULT_MAX_LIST_THREADS = 60;

    /**
     * Capacity of list work queue.
     */
    static final String LIST_WORK_QUEUE_CAPACITY = "fs.obs.list.workqueue.capacity";

    /**
     * Default value of {@link #LIST_WORK_QUEUE_CAPACITY}.
     */
    static final int DEFAULT_LIST_WORK_QUEUE_CAPACITY = 1024;

    /**
     * List parallel factor.
     */
    static final String LIST_PARALLEL_FACTOR = "fs.obs.list.parallel.factor";

    /**
     * Default value of {@link #LIST_PARALLEL_FACTOR}.
     */
    static final int DEFAULT_LIST_PARALLEL_FACTOR = 30;

    /**
     * Multi list contentsummary parallel factor
     */
    static final String MULTILISTCS_PARALLEL_FACTOR = "fs.obs.multilistcs.parallel.factor";

    /**
     * Default value of {@link #MULTILISTCS_PARALLEL_FACTOR}.
     */
    static final int DEFAULT_MULTILISTCS_PARALLEL_FACTOR = 30;

    /**
     * Switch for the fast delete.
     */
    static final String FAST_DELETE_ENABLE = "fs.obs.trash.enable";

    /**
     * Default trash : false.
     */
    static final boolean DEFAULT_FAST_DELETE_ENABLE = false;

    /**
     * The fast delete recycle directory.
     */
    static final String FAST_DELETE_DIR = "fs.obs.trash.dir";

    /**
     * Enable obs content summary or not.
     */
    static final String OBS_CONTENT_SUMMARY_ENABLE = "fs.obs.content.summary.enable";

    static final String OBS_CONTENT_SUMMARY_VERSION = "fs.obs.content.summary.version";

    static final String OBS_CONTENT_SUMMARY_VERSION_V1 = "1";

    static final String OBS_CONTENT_SUMMARY_VERSION_V2 = "2";

    static final int OBS_CONTENT_SUMMARY_FALLBACK_THRESHOLD = 1000;

    /**
     * Enable obs client dfs list or not.
     */
    static final String OBS_CLIENT_DFS_LIST_ENABLE = "fs.obs.client.dfs.list.enable";

    /**
     * Encryption type is sse-kms or sse-c.
     */
    static final String SSE_TYPE = "fs.obs.server-side-encryption-type";

    /**
     * Kms key id for sse-kms, while key base64 encoded content for sse-c.
     */
    static final String SSE_KEY = "fs.obs.server-side-encryption-key";

    /**
     * Array first block size.
     */
    static final String FAST_UPLOAD_BUFFER_ARRAY_FIRST_BLOCK_SIZE = "fs.obs.fast.upload.array.first.buffer";

    /**
     * The fast upload buffer array first block default size.
     */
    public static final int FAST_UPLOAD_BUFFER_ARRAY_FIRST_BLOCK_SIZE_DEFAULT = 1024 * 1024;

    /**
     * Auth Type Negotiation Enable Switch.
     */
    static final String SDK_AUTH_TYPE_NEGOTIATION_ENABLE = "fs.obs.authtype.negotiation.enable";

    /**
     * Default value of {@link #SDK_AUTH_TYPE_NEGOTIATION_ENABLE}.
     */
    static final boolean DEFAULT_SDK_AUTH_TYPE_NEGOTIATION_ENABLE = false;

    /**
     * Okhttp retryOnConnectionFailure switch.
     */
    static final String SDK_RETRY_ON_CONNECTION_FAILURE_ENABLE = "fs.obs.connection.retry.enable";

    /**
     * Default value of {@link #SDK_RETRY_ON_CONNECTION_FAILURE_ENABLE}.
     */
    static final boolean DEFAULT_SDK_RETRY_ON_CONNECTION_FAILURE_ENABLE = true;

    /**
     * Sdk max retry times on unexpected end of stream. exception, default: -1,
     * don't retry
     */
    static final String SDK_RETRY_TIMES_ON_UNEXPECTED_END_EXCEPTION = "fs.obs.unexpectedend.retrytime";

    /**
     * Default value of {@link #SDK_RETRY_TIMES_ON_UNEXPECTED_END_EXCEPTION}.
     */
    static final int DEFAULT_SDK_RETRY_TIMES_ON_UNEXPECTED_END_EXCEPTION = -1;

    /**
     * Maximum sdk connection retry times, default : 2000.
     */
    static final int DEFAULT_MAX_SDK_CONNECTION_RETRY_TIMES = 2000;

    /**
     * Whether to implement getCanonicalServiceName switch.
     */
    static final String GET_CANONICAL_SERVICE_NAME_ENABLE = "fs.obs.getcanonicalservicename.enable";

    /**
     * Default value of {@link #GET_CANONICAL_SERVICE_NAME_ENABLE}.
     */
    static final boolean DEFAULT_GET_CANONICAL_SERVICE_NAME_ENABLE = false;

    static final String RETRY_MAXTIME = "fs.obs.retry.maxtime";

    static final long DEFAULT_RETRY_MAXTIME = 180000;

    static final String RETRY_SLEEP_BASETIME = "fs.obs.retry.sleep.basetime";

    static final long DEFAULT_RETRY_SLEEP_BASETIME = 50;

    static final String RETRY_SLEEP_MAXTIME = "fs.obs.retry.sleep.maxtime";

    static final long DEFAULT_RETRY_SLEEP_MAXTIME = 30000;


    static final String RETRY_QOS_MAXTIME = "fs.obs.retry.qos.maxtime";

    static final long DEFAULT_RETRY_QOS_MAXTIME = 180000;

    static final String RETRY_QOS_SLEEP_BASETIME = "fs.obs.retry.qos.sleep.basetime";

    static final long DEFAULT_RETRY_QOS_SLEEP_BASETIME = 1000;

    static final String RETRY_QOS_SLEEP_MAXTIME = "fs.obs.retry.qos.sleep.maxtime";

    static final long DEFAULT_RETRY_QOS_SLEEP_MAXTIME = 30000;

    public static final String RETRY_LIMIT = "fs.obs.retry.limit";

    public static final int DEFAULT_RETRY_LIMIT = 7;

    public static final String RETRY_QOS_LIMIT = "fs.obs.retry.qos.limit";

    public static final int DEFAULT_RETRY_QOS_LIMIT = 7;

    /**
     * File visibility after create interface switch.
     */
    static final String FILE_VISIBILITY_AFTER_CREATE_ENABLE =
        "fs.obs.file.visibility.enable";

    /**
     * Default value of {@link #FILE_VISIBILITY_AFTER_CREATE_ENABLE}.
     */
    static final boolean DEFAULT_FILE_VISIBILITY_AFTER_CREATE_ENABLE = false;

    public static final String AUTHORIZER_PROVIDER = "fs.obs.authorize.provider";

    public static final String  AUTHORIZE_FAIL_FALLBACK = "fs.obs.authorize.fail.fallback";
    public static final boolean DEFAULT_AUTHORIZE_FAIL_FALLBACK = false;

    public static final String  AUTHORIZE_EXCEPTION_FALLBACK = "fs.obs.authorize.exception.fallback";
    public static final boolean DEFAULT_AUTHORIZE_EXCEPTION_FALLBACK = true;

    /**
     * Second to millisecond factor.
     */
    static final int SEC2MILLISEC_FACTOR = 1000;

    static final String METRICS_COUNT = "fs.obs.metrics.count";

    /**
     * When it takes more than one second, print the log.
     */
    static final int DEFAULT_METRICS_COUNT = 1;

    static final String METRICS_SWITCH = "fs.obs.metrics.switch";

    static final boolean DEFAULT_METRICS_SWITCH = false;

    /**
     * OBSBlockOutputStream implement the Syncable interface with its full semantic,
     * but this will lead to low performance in some scenario, for detail see [BUG2021092400077].
     */
    static final String OUTPUT_STREAM_HFLUSH_POLICY = "fs.obs.outputstream.hflush.policy";

    static final String OUTPUT_STREAM_HFLUSH_POLICY_SYNC = "sync"; // use original policy

    static final String OUTPUT_STREAM_HFLUSH_POLICY_FLUSH = "flush"; // downgrade hflush/hsync to the buffer's flush

    static final String OUTPUT_STREAM_HFLUSH_POLICY_EMPTY = "empty"; // downgrade hflush/hsync to empty func, which means calling hflush/hsync will do nothing

    static final String OUTPUT_STREAM_ATTACH_MD5 = "fs.obs.outputstream.attach.md5";

    static final Boolean DEFAULT_OUTPUT_STREAM_ATTACH_MD5 = false;

    /**
     * Use which type to validate consistency of uploaded block data. Default value is {@link #FAST_UPLOAD_CHECKSUM_TYPE_NONE}.
     * Normally replace {@link #OUTPUT_STREAM_ATTACH_MD5}.
     * Recommend {@link #FAST_UPLOAD_CHECKSUM_TYPE_SHA256} for more secure.
     */
    static final String FAST_UPLOAD_CHECKSUM_TYPE = "fs.obs.fast.upload.checksum.type";

    static final String FAST_UPLOAD_CHECKSUM_TYPE_NONE = "none";

    static final String FAST_UPLOAD_CHECKSUM_TYPE_MD5 = "md5";

    static final String FAST_UPLOAD_CHECKSUM_TYPE_SHA256 = "sha256";

    static final String OUTPUT_STREAM_DISK_FORCE_FLUSH = "fs.obs.outputstream.disk.force.flush";

    static final Boolean DEFAULT_OUTPUT_STREAM_DISK_FORCE_FLUSH = true;

    static final String FAST_DELETE_VERSION = "fs.obs.fast.delete.version";

    static final String FAST_DELETE_VERSION_V1 = "1";

    static final String FAST_DELETE_VERSION_V2 = "2";

    static final String FAST_DELETE_VERSION_V2_CHECKPOINT_FORMAT = "yyyyMMddHH";

    /**
     * Determines the HDFS trash behavior. Default value is {@link #HDFS_TRASH_VERSION_V1}.
     */
    static final String HDFS_TRASH_VERSION = "fs.obs.hdfs.trash.version";

    static final String HDFS_TRASH_VERSION_V1 = "1";

    static final String HDFS_TRASH_VERSION_V2 = "2";

    static final String HDFS_TRASH_PREFIX = "fs.obs.hdfs.trash.prefix";

    static final String DEFAULT_HDFS_TRASH_PREFIX = "/user/.Trash";

    private OBSConstants() {
    }
}
