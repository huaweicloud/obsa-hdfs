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

/**
 * All the constants used with the {@link OBSFileSystem}.
 *
 * <p>Some of the strings are marked as {@code Unstable}. This means that they may be unsupported in
 * future; at which point they will be marked as deprecated and simply ignored.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class Constants {

  /** The minimum multipart size which OBS supports. */
  public static final int MULTIPART_MIN_SIZE = 5 * 1024 * 1024;
  // OBS access key
  public static final String ACCESS_KEY = "fs.obs.access.key";
  // OBS secret key
  public static final String SECRET_KEY = "fs.obs.secret.key";
  // obs credentials provider
  public static final String OBS_CREDENTIALS_PROVIDER = "fs.obs.credentials.provider";
  // obs client security provider
  public static final String OBS_SECURITY_PROVIDER = "fs.obs.security.provider";
  /**
   * Extra set of security credentials which will be prepended to that set in {@code
   * "hadoop.security.credential.provider.path"}. This extra option allows for per-bucket overrides.
   */
  public static final String OBS_SECURITY_CREDENTIAL_PROVIDER_PATH =
      "fs.obs.security.credential.provider.path";
  // session token for when using TemporaryOBSCredentialsProvider
  public static final String SESSION_TOKEN = "fs.obs.session.token";
  // number of simultaneous connections to obs
  public static final String MAXIMUM_CONNECTIONS = "fs.obs.connection.maximum";
  public static final int DEFAULT_MAXIMUM_CONNECTIONS = 1000;
  // connect to obs over ssl?
  public static final String SECURE_CONNECTIONS = "fs.obs.connection.ssl.enabled";
  public static final boolean DEFAULT_SECURE_CONNECTIONS = false;
  // use a custom endpoint?
  public static final String ENDPOINT = "fs.obs.endpoint";
  // connect to obs through a proxy server?
  public static final String PROXY_HOST = "fs.obs.proxy.host";
  public static final String PROXY_PORT = "fs.obs.proxy.port";
  public static final String PROXY_USERNAME = "fs.obs.proxy.username";
  public static final String PROXY_PASSWORD = "fs.obs.proxy.password";
  // number of times we should retry errors
  public static final String MAX_ERROR_RETRIES = "fs.obs.attempts.maximum";
  public static final int DEFAULT_MAX_ERROR_RETRIES = 3;
  // seconds until we give up trying to establish a connection to obs
  public static final String ESTABLISH_TIMEOUT = "fs.obs.connection.establish.timeout";
  public static final int DEFAULT_ESTABLISH_TIMEOUT = 60000;
  // seconds until we give up on a connection to obs
  public static final String SOCKET_TIMEOUT = "fs.obs.connection.timeout";
  public static final int DEFAULT_SOCKET_TIMEOUT = 60000;
  // socket send buffer to be used in OBS SDK
  public static final String SOCKET_SEND_BUFFER = "fs.obs.socket.send.buffer";
  public static final int DEFAULT_SOCKET_SEND_BUFFER = 64 * 1024;
  // socket send buffer to be used in OBS SDK
  public static final String SOCKET_RECV_BUFFER = "fs.obs.socket.recv.buffer";
  public static final int DEFAULT_SOCKET_RECV_BUFFER = 64 * 1024;
  // number of records to get while paging through a directory listing
  public static final String MAX_PAGING_KEYS = "fs.obs.paging.maximum";
  public static final int DEFAULT_MAX_PAGING_KEYS = 1000;
  // the maximum number of threads to allow in the pool used by TransferManager
  public static final String MAX_THREADS = "fs.obs.threads.max";
  public static final int DEFAULT_MAX_THREADS = 20;
  // the maximum number of tasks cached if all threads are already uploading
  public static final String MAX_TOTAL_TASKS = "fs.obs.max.total.tasks";
  public static final int DEFAULT_MAX_TOTAL_TASKS = 20;

//  public static final String CORE_COPY_THREADS = "fs.obs.copy.threads.core";
//  public static final int DEFAULT_CORE_COPY_THREADS = 20;
  public static final String MAX_COPY_THREADS = "fs.obs.copy.threads.max";
  public static final int DEFAULT_MAX_COPY_THREADS = 40;

//  public static final String CORE_DELETE_THREADS = "fs.obs.delete.threads.core";
//  public static final int DEFAULT_CORE_DELETE_THREADS = 10;
  public static final String MAX_DELETE_THREADS = "fs.obs.delete.threads.max";
  public static final int DEFAULT_MAX_DELETE_THREADS = 20;

  //Read thread configuration for read-ahead input stream
  public static final String MAX_READ_THREADS = "fs.obs.threads.read.max";
  public static final int DEFAULT_MAX_READ_THREADS = 20;
//  public static final String CORE_READ_THREADS = "fs.obs.threads.read.core";
//  public static final int DEFAULT_CORE_READ_THREADS = 5;
  // Use read-ahead input stream
  public static final String READAHEAD_INPUTSTREAM_ENABLED = "fs.obs.readahead.inputstream.enabled";
  public static final boolean READAHEAD_INPUTSTREAM_ENABLED_DEFAULT = false;
  public static final String BUFFER_PART_SIZE = "fs.obs.buffer.part.size";
  public static final int DEFAULT_BUFFER_PART_SIZE = 64 * 1024;
  public static final String BUFFER_MAX_RANGE = "fs.obs.buffer.max.range";
  public static final int DEFAULT_BUFFER_MAX_RANGE = 20 * 1024 * 1024;
  // unused option: maintained for compile-time compatibility.
  // if set, a warning is logged in OBS during init
  @Deprecated public static final String CORE_THREADS = "fs.obs.threads.core";
  // the time an idle thread waits before terminating
  public static final String KEEPALIVE_TIME = "fs.obs.threads.keepalivetime";
  public static final int DEFAULT_KEEPALIVE_TIME = 60;
  // size of each of or multipart pieces in bytes
  public static final String MULTIPART_SIZE = "fs.obs.multipart.size";
  public static final long DEFAULT_MULTIPART_SIZE = 104857600; // 100 MB
  // minimum size in bytes before we start a multipart uploads or copy
  public static final String MIN_MULTIPART_THRESHOLD = "fs.obs.multipart.threshold";
  public static final long DEFAULT_MIN_MULTIPART_THRESHOLD = Integer.MAX_VALUE;
  // enable multiobject-delete calls?
  public static final String ENABLE_MULTI_DELETE = "fs.obs.multiobjectdelete.enable";
  // max number of objects in one multiobject-delete call.
  // this option takes effect only when the option 'ENABLE_MULTI_DELETE' is set to 'true'.
  public static final String MULTI_DELETE_MAX_NUMBER = "fs.obs.multiobjectdelete.maximum";
  public static final int MULTI_DELETE_DEFAULT_NUMBER = 1000;
  // delete recursively or not.
  public static final String MULTI_DELETE_RECURSION = "fs.obs.multiobjectdelete.recursion";
  // support to rename a folder to an empty folder or not.
  public static final String RENAME_TO_EMPTY_FOLDER = "fs.obs.rename.to_empty_folder";
  // comma separated list of directories
  public static final String BUFFER_DIR = "fs.obs.buffer.dir";
  // switch to the fast block-by-block upload mechanism
  public static final String FAST_UPLOAD = "fs.obs.fast.upload";
  public static final boolean DEFAULT_FAST_UPLOAD = true;
  /** What buffer to use. Default is {@link #FAST_UPLOAD_BUFFER_DISK} Value: {@value} */
  @InterfaceStability.Unstable
  public static final String FAST_UPLOAD_BUFFER = "fs.obs.fast.upload.buffer";

  // initial size of memory buffer for a fast upload
  // @Deprecated
  // public static final String FAST_BUFFER_SIZE = "fs.obs.fast.buffer.size";
  // public static final int DEFAULT_FAST_BUFFER_SIZE = 1048576; //1MB
  /** Buffer blocks to disk: {@value}. Capacity is limited to available disk space. */
  @InterfaceStability.Unstable public static final String FAST_UPLOAD_BUFFER_DISK = "disk";
  /** Use an in-memory array. Fast but will run of heap rapidly: {@value}. */
  @InterfaceStability.Unstable public static final String FAST_UPLOAD_BUFFER_ARRAY = "array";
  /**
   * Use a byte buffer. May be more memory efficient than the {@link #FAST_UPLOAD_BUFFER_ARRAY}:
   * {@value}.
   */
  @InterfaceStability.Unstable public static final String FAST_UPLOAD_BYTEBUFFER = "bytebuffer";
  /** Default buffer option: {@value}. */
  @InterfaceStability.Unstable
  public static final String DEFAULT_FAST_UPLOAD_BUFFER = FAST_UPLOAD_BUFFER_DISK;
  /**
   * Maximum Number of blocks a single output stream can have active (uploading, or queued to the
   * central FileSystem instance's pool of queued operations. This stops a single stream overloading
   * the shared thread pool. {@value}
   *
   * <p>Default is {@link #DEFAULT_FAST_UPLOAD_ACTIVE_BLOCKS}
   */
  @InterfaceStability.Unstable
  public static final String FAST_UPLOAD_ACTIVE_BLOCKS = "fs.obs.fast.upload.active.blocks";
  /** Limit of queued block upload operations before writes block. Value: {@value} */
  @InterfaceStability.Unstable public static final int DEFAULT_FAST_UPLOAD_ACTIVE_BLOCKS = 4;
  // Private | PublicRead | PublicReadWrite | AuthenticatedRead |
  // LogDeliveryWrite | BucketOwnerRead | BucketOwnerFullControl
  public static final String CANNED_ACL = "fs.obs.acl.default";
  public static final String DEFAULT_CANNED_ACL = "";
  // should we try to purge old multipart uploads when starting up
  public static final String PURGE_EXISTING_MULTIPART = "fs.obs.multipart.purge";
  public static final boolean DEFAULT_PURGE_EXISTING_MULTIPART = false;
  // purge any multipart uploads older than this number of seconds
  public static final String PURGE_EXISTING_MULTIPART_AGE = "fs.obs.multipart.purge.age";
  public static final long DEFAULT_PURGE_EXISTING_MULTIPART_AGE = 86400;
  public static final String OBS_FOLDER_SUFFIX = "_$folder$";
  public static final String FS_OBS_BLOCK_SIZE = "fs.obs.block.size";
  public static final String FS_OBS = "obs";
  /** Prefix for all OBS properties: {@value}. */
  public static final String FS_OBS_PREFIX = "fs.obs.";
  /** Prefix for OBS bucket-specific properties: {@value}. */
  public static final String FS_OBS_BUCKET_PREFIX = "fs.obs.bucket.";

  public static final int OBS_DEFAULT_PORT = -1;
  public static final String USER_AGENT_PREFIX = "fs.obs.user.agent.prefix";
  /** read ahead buffer size to prevent connection re-establishments. */
  public static final String READAHEAD_RANGE = "fs.obs.readahead.range";

  public static final long DEFAULT_READAHEAD_RANGE = 64 * 1024;
  /**
   * Which input strategy to use for buffering, seeking and similar when reading data. Value:
   * {@value}
   */
  @InterfaceStability.Unstable
  public static final String INPUT_FADVISE = "fs.obs.experimental.input.fadvise";
  /** General input. Some seeks, some reads. Value: {@value} */
  @InterfaceStability.Unstable public static final String INPUT_FADV_NORMAL = "normal";
  /** Optimized for sequential access. Value: {@value} */
  @InterfaceStability.Unstable public static final String INPUT_FADV_SEQUENTIAL = "sequential";
  /**
   * Optimized purely for random seek+read/positionedRead operations; The performance of sequential
   * IO may be reduced in exchange for more efficient {@code seek()} operations. Value: {@value}
   */
  @InterfaceStability.Unstable public static final String INPUT_FADV_RANDOM = "random";

  @InterfaceAudience.Private @InterfaceStability.Unstable
  public static final String OBS_CLIENT_FACTORY_IMPL = "fs.obs.s3.client.factory.impl";

  @InterfaceAudience.Private @InterfaceStability.Unstable
  public static final Class<? extends ObsClientFactory> DEFAULT_OBS_CLIENT_FACTORY_IMPL =
      ObsClientFactory.DefaultObsClientFactory.class;
  /** Maximum number of partitions in a multipart upload: {@value}. */
  @InterfaceAudience.Private public static final int MAX_MULTIPART_COUNT = 10000;
  /** OBS Client configuration */
  // idleConnectionTime
  public static final String IDLE_CONNECTION_TIME = "fs.obs.idle.connection.time";

  public static final int DEFAULT_IDLE_CONNECTION_TIME = 30000;
  // maxIdleConnections
  public static final String MAX_IDLE_CONNECTIONS = "fs.obs.max.idle.connections";
  public static final int DEFAULT_MAX_IDLE_CONNECTIONS = 10;
  // keepAlive
  public static final String KEEP_ALIVE = "fs.obs.keep.alive";
  public static final boolean DEFAULT_KEEP_ALIVE = true;
  // verifyResponseContentType
  public static final String VALIDATE_CERTIFICATE = "fs.obs.validate.certificate";
  public static final boolean DEFAULT_VALIDATE_CERTIFICATE = false;
  // verifyResponseContentType
  public static final String VERIFY_RESPONSE_CONTENT_TYPE = "fs.obs.verify.response.content.type";
  public static final boolean DEFAULT_VERIFY_RESPONSE_CONTENT_TYPE = true;
  // uploadStreamRetryBufferSize
  public static final String UPLOAD_STREAM_RETRY_SIZE = "fs.obs.upload.stream.retry.buffer.size";
  public static final int DEFAULT_UPLOAD_STREAM_RETRY_SIZE = 512 * 1024;
  // readBufferSize
  public static final String READ_BUFFER_SIZE = "fs.obs.read.buffer.size";
  public static final int DEFAULT_READ_BUFFER_SIZE = 8192;
  // writeBufferSize
  public static final String WRITE_BUFFER_SIZE = "fs.obs.write.buffer.size";
  public static final int DEFAULT_WRITE_BUFFER_SIZE = 8192;
  // cname
  public static final String CNAME = "fs.obs.cname";
  public static final boolean DEFAULT_CNAME = false;
  // isStrictHostnameVerification
  public static final String STRICT_HOSTNAME_VERIFICATION = "fs.obs.strict.hostname.verification";
  public static final boolean DEFAULT_STRICT_HOSTNAME_VERIFICATION = false;
  // test Path
  public static final String PATH_LOCAL_TEST = "fs.obs.test.local.path";
  // size of object copypart pieces in bytes
  public static final String COPY_PART_SIZE = "fs.obs.copypart.size";
  public static final long MAX_COPY_PART_SIZE = 5368709120L; // 5GB
  public static final long DEFAULT_COPY_PART_SIZE = 104857600L; // 100MB

//  public static final String CORE_COPY_PART_THREADS =  "fs.obs.copypart.threads.core";
//  public static final int DEFAULT_CORE_COPY_PART_THREADS = 20;
  public static final String MAX_COPY_PART_THREADS = "fs.obs.copypart.threads.max";
  public static final int DEFAULT_MAX_COPY_PART_THREADS = 40;
  // switch to the fast delete
  public static final String TRASH_ENALBLE = "fs.obs.trash.enable";
  public static final boolean DEFAULT_TRASH = false;
  // The fast delete recycle directory
  public static final String TRASH_DIR = "fs.obs.trash.dir";

  // encryption type is sse-kms or sse-c
  public static final String SSE_TYPE = "fs.obs.server-side-encryption-type";
  // kms key id for sse-kms, while key base64 encoded content for sse-c
  public static final String SSE_KEY = "fs.obs.server-side-encryption-key";

  private Constants() {}
}
