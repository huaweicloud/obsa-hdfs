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
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.SettableFuture;
import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;
import com.obs.services.model.*;
import com.obs.services.model.fs.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.fs.obs.Constants.*;
import static org.apache.hadoop.fs.obs.Listing.ACCEPT_ALL;
import static org.apache.hadoop.fs.obs.OBSUtils.*;

/**
 * The core OBS Filesystem implementation.
 *
 * <p>This subclass is marked as private as code should not be creating it directly; use {@link
 * FileSystem#get(Configuration)} and variants to create one.
 *
 * <p>If cast to {@code OBSFileSystem}, extra methods and features may be accessed. Consider those
 * private and unstable.
 *
 * <p>Because it prints some of the state of the instrumentation, the output of {@link #toString()}
 * must also be considered unstable.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class OBSFileSystem extends FileSystem {
  /** Default blocksize as used in blocksize and FS status queries. */
  public static final int DEFAULT_BLOCKSIZE = 32 * 1024 * 1024;

  public static final Logger LOG = LoggerFactory.getLogger(OBSFileSystem.class);
  private static final Logger PROGRESS =
      LoggerFactory.getLogger("org.apache.hadoop.fs.obs.OBSFileSystem.Progress");
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private URI uri;
  private Path workingDir;
  private String username;
  private ObsClient obs;
  private boolean enablePosix = false;
  private boolean enableMultiObjectsDeleteRecursion = true;
  private boolean renameSupportEmptyDestinationFolder = true;
  private String bucket;
  private int maxKeys;
  private Listing listing;
  private OBSWriteOperationHelper writeHelper;
  private long partSize;
  private boolean enableMultiObjectsDelete;
  private ListeningExecutorService boundedThreadPool;
  private ThreadPoolExecutor boundedCopyThreadPool;
  private ThreadPoolExecutor boundedDeleteThreadPool;
  private ThreadPoolExecutor unboundedReadThreadPool;
  private ThreadPoolExecutor unboundedThreadPool;
  private ThreadPoolExecutor boundedCopyPartThreadPool;
  private long multiPartThreshold;
  private LocalDirAllocator directoryAllocator;
  private String serverSideEncryptionAlgorithm;
  private long readAhead;
  private OBSInputPolicy inputPolicy;
  // The maximum number of entries that can be deleted in any call to obs
  private int MAX_ENTRIES_TO_DELETE;
  private boolean blockUploadEnabled;
  private String blockOutputBuffer;
  private OBSDataBlocks.BlockFactory blockFactory;
  private int blockOutputActiveBlocks;

  private int bufferPartSize;
  private long bufferMaxRange;

  private boolean readaheadInputStreamEnabled = false;
  private long copyPartSize;
  private int maxCopyPartThreads;
  private int coreCopyPartThreads;
  private long keepAliveTime;
  private boolean enableTrash = false;
  private String trashDir;
  private AccessControlList cannedACL;
  private SseWrapper sse;
  private static final int MAX_RETRY_TIME = 3;
  private static final int DELAY_TIME = 10;

  /**
   * Get the depth of an absolute path, that is the number of '/' in the path
   *
   * @param key object key
   * @return depth
   */
  private static int fsGetObjectKeyDepth(String key) {
    int depth = 0;
    for (int idx = key.indexOf('/'); idx >= 0; idx = key.indexOf('/', idx + 1)) {
      depth++;
    }
    return (key.endsWith("/") ? depth - 1 : depth);
  }

  /**
   * Used to judge that an object is a file or folder
   *
   * @param attr posix object attribute
   * @return is posix folder
   */
  public static boolean fsIsFolder(ObsFSAttribute attr) {
    final int S_IFDIR = 0040000;
    int mode = attr.getMode();
    // object mode is -1 when the object is migrated from object bucket to posix bucket.
    // -1 is a file, not folder.
    if (mode < 0) {
      return false;
    }

    return ((mode & S_IFDIR) != 0);
  }

  /**
   * Called after a new FileSystem instance is constructed.
   *
   * @param name a uri whose authority section names the host, port, etc. for this FileSystem
   * @param originalConf the configuration to use for the FS. The bucket-specific options are
   *     patched over the base ones before any use is made of the config.
   */
  public void initialize(URI name, Configuration originalConf) throws IOException {
    // uri = OBSLoginHelper.buildFSURI(name);
    // get the host; this is guaranteed to be non-null, non-empty
    // bucket = name.getHost();
	uri = java.net.URI.create(name.getScheme() + "://" + name.getAuthority());
    bucket = name.getAuthority();
    // clone the configuration into one with propagated bucket options
    Configuration conf = propagateBucketOptions(originalConf, bucket);
    patchSecurityCredentialProviders(conf);
    super.initialize(name, conf);
    setConf(conf);
    try {

      // Username is the current user at the time the FS was instantiated.
      username = UserGroupInformation.getCurrentUser().getShortUserName();
      workingDir = new Path("/user", username).makeQualified(this.uri, this.getWorkingDirectory());

      Class<? extends ObsClientFactory> obsClientFactoryClass =
          conf.getClass(
              OBS_CLIENT_FACTORY_IMPL, DEFAULT_OBS_CLIENT_FACTORY_IMPL, ObsClientFactory.class);
      obs = ReflectionUtils.newInstance(obsClientFactoryClass, conf).createObsClient(name);
      sse = new SseWrapper(conf);

      maxKeys = intOption(conf, MAX_PAGING_KEYS, DEFAULT_MAX_PAGING_KEYS, 1);
      listing = new Listing(this);
      partSize = getMultipartSizeProperty(conf, MULTIPART_SIZE, DEFAULT_MULTIPART_SIZE);
      multiPartThreshold =
          getMultipartSizeProperty(conf, MIN_MULTIPART_THRESHOLD, DEFAULT_MIN_MULTIPART_THRESHOLD);

      // check but do not store the block size
      longBytesOption(conf, FS_OBS_BLOCK_SIZE, DEFAULT_BLOCKSIZE, 1);
      enableMultiObjectsDelete = conf.getBoolean(ENABLE_MULTI_DELETE, true);
      MAX_ENTRIES_TO_DELETE = conf.getInt(MULTI_DELETE_MAX_NUMBER, MULTI_DELETE_DEFAULT_NUMBER);
      enableMultiObjectsDeleteRecursion = conf.getBoolean(MULTI_DELETE_RECURSION, true);
      renameSupportEmptyDestinationFolder = conf.getBoolean(RENAME_TO_EMPTY_FOLDER, true);

      readAhead = longBytesOption(conf, READAHEAD_RANGE, DEFAULT_READAHEAD_RANGE, 0);

      int maxThreads = conf.getInt(MAX_THREADS, DEFAULT_MAX_THREADS);
      if (maxThreads < 2) {
        LOG.warn(MAX_THREADS + " must be at least 2: forcing to 2.");
        maxThreads = 2;
      }

      //int coreCopyThreads = conf.getInt(CORE_COPY_THREADS, DEFAULT_CORE_COPY_THREADS);
      int maxCopyThreads = conf.getInt(MAX_COPY_THREADS, DEFAULT_MAX_COPY_THREADS);
      if (maxCopyThreads < 2) {
            LOG.warn(MAX_COPY_THREADS + " must be at least 2: forcing to 2.");
          maxCopyThreads = 2;
      }
      int coreCopyThreads = new Double(Math.ceil(maxCopyThreads/2.0)).intValue();

      //int coreDeleteThreads = conf.getInt(CORE_DELETE_THREADS, DEFAULT_CORE_DELETE_THREADS);
      int maxDeleteThreads = conf.getInt(MAX_DELETE_THREADS, DEFAULT_MAX_DELETE_THREADS);
      if (maxDeleteThreads < 2) {
            LOG.warn(MAX_DELETE_THREADS + " must be at least 2: forcing to 2.");
          maxDeleteThreads = 2;
      }
      int coreDeleteThreads = new Double(Math.ceil(maxDeleteThreads/2.0)).intValue();
      int maxReadThreads = conf.getInt(MAX_READ_THREADS, DEFAULT_MAX_READ_THREADS);
      //int coreReadThreads = conf.getInt(CORE_READ_THREADS, DEFAULT_CORE_READ_THREADS);
      if (maxReadThreads < 2) {
        LOG.warn(MAX_READ_THREADS + " must be at least 2: forcing to 2.");
        maxReadThreads = 2;
      }
      int coreReadThreads = new Double(Math.ceil(maxReadThreads/2.0)).intValue();
      bufferPartSize = intOption(conf, BUFFER_PART_SIZE, DEFAULT_BUFFER_PART_SIZE, 1 * 1024);
      bufferMaxRange = intOption(conf, BUFFER_MAX_RANGE, DEFAULT_BUFFER_MAX_RANGE, 1 * 1024);

      int totalTasks = intOption(conf, MAX_TOTAL_TASKS, DEFAULT_MAX_TOTAL_TASKS, 1);
      keepAliveTime = longOption(conf, KEEPALIVE_TIME, DEFAULT_KEEPALIVE_TIME, 0);

      copyPartSize = longOption(conf, COPY_PART_SIZE, DEFAULT_COPY_PART_SIZE, 0);
      if (copyPartSize > MAX_COPY_PART_SIZE) {
        LOG.warn("obs: {} capped to ~5GB (maximum allowed part size with current output mechanism)", COPY_PART_SIZE);
        copyPartSize = MAX_COPY_PART_SIZE;
      }
      //coreCopyPartThreads = conf.getInt(CORE_COPY_PART_THREADS, DEFAULT_CORE_COPY_PART_THREADS);
      maxCopyPartThreads = conf.getInt(MAX_COPY_PART_THREADS, DEFAULT_MAX_COPY_PART_THREADS);
      if (maxCopyPartThreads < 2) {
        LOG.warn(MAX_COPY_PART_THREADS + " must be at least 2: forcing to 2.");
        maxReadThreads = 2;
      }
      coreCopyPartThreads = new Double(Math.ceil(maxCopyPartThreads/2.0)).intValue();
      boundedThreadPool =
          BlockingThreadPoolExecutorService.newInstance(
              maxThreads,
              maxThreads + totalTasks,
              keepAliveTime,
              TimeUnit.SECONDS,
              "obs-transfer-shared");

      boundedCopyThreadPool =
          new ThreadPoolExecutor(
              coreCopyThreads,
              maxCopyThreads,
              keepAliveTime,
              TimeUnit.SECONDS,
              new LinkedBlockingQueue<Runnable>(),
              BlockingThreadPoolExecutorService.newDaemonThreadFactory("obs-copy-transfer-shared"));
      boundedCopyThreadPool.allowCoreThreadTimeOut(true);
      boundedDeleteThreadPool =
          new ThreadPoolExecutor(
              coreDeleteThreads,
              maxDeleteThreads,
              keepAliveTime,
              TimeUnit.SECONDS,
              new LinkedBlockingQueue<Runnable>(),
              BlockingThreadPoolExecutorService.newDaemonThreadFactory(
                  "obs-delete-transfer-shared"));
      boundedDeleteThreadPool.allowCoreThreadTimeOut(true);
      unboundedThreadPool =
          new ThreadPoolExecutor(
              maxThreads,
              Integer.MAX_VALUE,
              keepAliveTime,
              TimeUnit.SECONDS,
              new LinkedBlockingQueue<Runnable>(),
              BlockingThreadPoolExecutorService.newDaemonThreadFactory("obs-transfer-unbounded"));
      unboundedThreadPool.allowCoreThreadTimeOut(true);
      readaheadInputStreamEnabled =
          conf.getBoolean(READAHEAD_INPUTSTREAM_ENABLED, READAHEAD_INPUTSTREAM_ENABLED_DEFAULT);
      if (readaheadInputStreamEnabled) {
        unboundedReadThreadPool =
            new ThreadPoolExecutor(
                coreReadThreads,
                maxReadThreads,
                keepAliveTime,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(),
                BlockingThreadPoolExecutorService.newDaemonThreadFactory(
                    "obs-transfer-read-unbounded"));
        unboundedReadThreadPool.allowCoreThreadTimeOut(true);
      }

      boundedCopyPartThreadPool =
          new ThreadPoolExecutor(
              coreCopyPartThreads,
              maxCopyPartThreads,
              keepAliveTime,
              TimeUnit.SECONDS,
              new LinkedBlockingQueue<Runnable>(),
              BlockingThreadPoolExecutorService.newDaemonThreadFactory(
                  "obs-copy-part-transfer-shared"));
      boundedCopyPartThreadPool.allowCoreThreadTimeOut(true);
      writeHelper = new OBSWriteOperationHelper(this, getConf());

      initCannedAcls(conf);

      verifyBucketExists();

      getBucketFsStatus();

      initMultipartUploads(conf);

      inputPolicy = OBSInputPolicy.getPolicy(conf.getTrimmed(INPUT_FADVISE, INPUT_FADV_NORMAL));

      blockUploadEnabled = conf.getBoolean(FAST_UPLOAD, DEFAULT_FAST_UPLOAD);

      if (!blockUploadEnabled) {
        LOG.warn("The \"slow\" output stream is no longer supported");
      }
      blockOutputBuffer = conf.getTrimmed(FAST_UPLOAD_BUFFER, DEFAULT_FAST_UPLOAD_BUFFER);
      partSize = ensureOutputParameterInRange(MULTIPART_SIZE, partSize);
      blockFactory = OBSDataBlocks.createFactory(this, blockOutputBuffer);
      blockOutputActiveBlocks =
          intOption(conf, FAST_UPLOAD_ACTIVE_BLOCKS, DEFAULT_FAST_UPLOAD_ACTIVE_BLOCKS, 1);
      LOG.debug(
          "Using OBSBlockOutputStream with buffer = {}; block={};" + " queue limit={}",
          blockOutputBuffer,
          partSize,
          blockOutputActiveBlocks);

      enableTrash = conf.getBoolean(TRASH_ENALBLE, DEFAULT_TRASH);
      if (enableTrash) {
        if (!isFsBucket()) {
          String errorMsg = String.format("The bucket [%s] is not posix. not supported for trash.", bucket);
          LOG.warn(errorMsg);
          enableTrash = false;
          trashDir = null;
        } else {
          trashDir = conf.get(TRASH_DIR);
          if (StringUtils.isEmpty(trashDir)) {
            String errorMsg =
                    String.format(
                            "The trash feature(fs.obs.trash.enable) is enabled,"
                                    + " but the configuration(fs.obs.trash.dir [%s]) is empty.",
                            trashDir);
            LOG.error(errorMsg);
            throw new ObsException(errorMsg);
          }
          trashDir = maybeAddBeginningSlash(trashDir);
          trashDir = maybeAddTrailingSlash(trashDir);
        }
      }
    } catch (ObsException e) {
      throw translateException("initializing ", new Path(name), e);
    }
  }

  /**
   * Verify that the bucket exists. This does not check permissions, not even read access.
   *
   * @throws FileNotFoundException the bucket is absent
   * @throws IOException any other problem talking to OBS
   */
  protected void verifyBucketExists() throws FileNotFoundException, IOException {
    try {
      if (!obs.headBucket(bucket)) {
        throw new FileNotFoundException("Bucket " + bucket + " does not exist");
      }
    } catch (ObsException e) {
      throw translateException("doesBucketExist", bucket, e);
    }
  }

  /**
   * Get the fs status of the bucket.
   *
   * @throws FileNotFoundException the bucket is absent
   * @throws IOException any other problem talking to OBS
   */
  public boolean getBucketFsStatus(String bucketName) throws FileNotFoundException, IOException {
    try {
      GetBucketFSStatusRequest getBucketFsStatusRequest = new GetBucketFSStatusRequest();
      getBucketFsStatusRequest.setBucketName(bucketName);
      GetBucketFSStatusResult getBucketFsStatusResult =
          obs.getBucketFSStatus(getBucketFsStatusRequest);
      FSStatusEnum fsStatus = getBucketFsStatusResult.getStatus();
      return ((fsStatus != null) && (fsStatus == FSStatusEnum.ENABLED));
    } catch (ObsException e) {
      LOG.error(e.toString());
      throw translateException("getBucketFsStatus", bucket, e);
    }
  }

  /**
   * jungle bucket is posix
   *
   * @throws IOException bucket status exception
   */
  private void getBucketFsStatus() throws IOException {
    enablePosix = getBucketFsStatus(bucket);
  }

  /**
   * get bucket is posix
   *
   * @return is posix bucket
   */
  public boolean isFsBucket() {
    return enablePosix;
  }

  /**
   * initialize bucket acl for upload, write operation
   *
   * @param conf the configuration to use for the FS.
   */
  private void initCannedAcls(Configuration conf) {
    // No canned acl in obs
    String cannedACLName = conf.get(CANNED_ACL, DEFAULT_CANNED_ACL);
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
   * get the bucket acl of user setting
   *
   * @return bucket acl {@link AccessControlList}
   */
  AccessControlList getCannedACL() {
    return cannedACL;
  }

  /**
   * initialize multi-part upload, purge larger than the value of PURGE_EXISTING_MULTIPART_AGE
   *
   * @param conf the configuration to use for the FS.
   * @throws IOException
   */
  private void initMultipartUploads(Configuration conf) throws IOException {
    boolean purgeExistingMultipart =
        conf.getBoolean(PURGE_EXISTING_MULTIPART, DEFAULT_PURGE_EXISTING_MULTIPART);
    long purgeExistingMultipartAge =
        longOption(conf, PURGE_EXISTING_MULTIPART_AGE, DEFAULT_PURGE_EXISTING_MULTIPART_AGE, 0);

    if (purgeExistingMultipart) {
      final Date purgeBefore = new Date(new Date().getTime() - purgeExistingMultipartAge * 1000);

      try {
        ListMultipartUploadsRequest request = new ListMultipartUploadsRequest(bucket);
        while (true) {
          // List + purge
          MultipartUploadListing uploadListing = obs.listMultipartUploads(request);
          for (MultipartUpload upload : uploadListing.getMultipartTaskList()) {
            if (upload.getInitiatedDate().compareTo(purgeBefore) < 0) {
              obs.abortMultipartUpload(
                  new AbortMultipartUploadRequest(
                      bucket, upload.getObjectKey(), upload.getUploadId()));
            }
          }
          if (!uploadListing.isTruncated()) {
            break;
          }
          request.setUploadIdMarker(uploadListing.getNextUploadIdMarker());
          request.setKeyMarker(uploadListing.getNextKeyMarker());
        }
      } catch (ObsException e) {
        if (e.getResponseCode() == 403) {
          LOG.debug(
              "Failed to purging multipart uploads against {}," + " FS may be read only",
              bucket,
              e);
        } else {
          throw translateException("purging multipart uploads", bucket, e);
        }
      }
    }
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

  /** @return Returns a URI whose scheme and authority identify this FileSystem. */
  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public int getDefaultPort() {
    return Constants.OBS_DEFAULT_PORT;
  }

  /**
   * Returns the OBS client used by this filesystem.
   *
   * @return ObsClient
   */
  @VisibleForTesting
  public ObsClient getObsClient() {
    return obs;
  }

  /**
   * Returns the read ahead range value used by this filesystem
   *
   * @return
   */
  @VisibleForTesting
  long getReadAheadRange() {
    return readAhead;
  }

  /**
   * Get the input policy for this FS instance.
   *
   * @return the input policy
   */
  @InterfaceStability.Unstable
  public OBSInputPolicy getInputPolicy() {
    return inputPolicy;
  }

  /**
   * Change the input policy for this FS.
   *
   * @param inputPolicy new policy
   */
  @InterfaceStability.Unstable
  public void setInputPolicy(OBSInputPolicy inputPolicy) {
    Objects.requireNonNull(inputPolicy, "Null inputStrategy");
    LOG.debug("Setting input strategy: {}", inputPolicy);
    this.inputPolicy = inputPolicy;
  }

  /**
   * Demand create the directory allocator, then create a temporary file. {@link
   * LocalDirAllocator#createTmpFileForWrite(String, long, Configuration)}.
   *
   * @param pathStr prefix for the temporary file
   * @param size the size of the file that is going to be written
   * @param conf the Configuration object
   * @return a unique temporary file
   * @throws IOException IO problems
   */
  synchronized File createTmpFileForWrite(String pathStr, long size, Configuration conf)
      throws IOException {
    if (directoryAllocator == null) {
      String bufferDir = conf.get(BUFFER_DIR) != null ? BUFFER_DIR : "hadoop.tmp.dir";
      directoryAllocator = new LocalDirAllocator(bufferDir);
    }
    return directoryAllocator.createTmpFileForWrite(pathStr, size, conf);
  }

  /**
   * Get the bucket of this filesystem.
   *
   * @return the bucket
   */
  public String getBucket() {
    return bucket;
  }

  /**
   * Turns a path (relative or otherwise) into an OBS key.
   *
   * @param path input path, may be relative to the working dir
   * @return a key excluding the leading "/", or, if it is the root path, ""
   */
  private String pathToKey(Path path) {
    if (!path.isAbsolute()) {
      path = new Path(workingDir, path);
    }

    if (path.toUri().getScheme() != null && path.toUri().getPath().isEmpty()) {
      return "";
    }

    return path.toUri().getPath().substring(1);
  }

  /**
   * Turns a path (relative or otherwise) into an OBS key, adding a trailing "/" if the path is not
   * the root <i>and</i> does not already have a "/" at the end.
   *
   * @param key obs key or ""
   * @return the with a trailing "/", or, if it is the root key, "",
   */
  private String maybeAddTrailingSlash(String key) {
    if (!StringUtils.isEmpty(key) && !key.endsWith("/")) {
      return key + '/';
    } else {
      return key;
    }
  }

  /**
   * Convert a path back to a key.
   *
   * @param key input key
   * @return the path from this key
   */
  private Path keyToPath(String key) {
    return new Path("/" + key);
  }

  /**
   * Convert a key to a fully qualified path.
   *
   * @param key input key
   * @return the fully qualified path including URI scheme and bucket name.
   */
  Path keyToQualifiedPath(String key) {
    return qualify(keyToPath(key));
  }

  /**
   * Qualify a path.
   *
   * @param path path to qualify
   * @return a qualified path.
   */
  private Path qualify(Path path) {
    return path.makeQualified(uri, workingDir);
  }

  /**
   * Check that a Path belongs to this FileSystem. Unlike the superclass, this version does not look
   * at authority, only hostnames.
   *
   * @param path to check
   * @throws IllegalArgumentException if there is an FS mismatch
   */
  @Override
  public void checkPath(Path path) {
    OBSLoginHelper.checkPath(getConf(), getUri(), path, getDefaultPort());
  }

  @Override
  protected URI canonicalizeUri(URI rawUri) {
    return OBSLoginHelper.canonicalizeUri(rawUri, getDefaultPort());
  }

  /**
   * Opens an FSDataInputStream at the indicated Path.
   *
   * @param f the file name to open
   * @param bufferSize the size of the buffer to be used.
   */
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    LOG.debug("Opening '{}' for reading.", f);
    final FileStatus fileStatus = getFileStatus(f);
    if (fileStatus.isDirectory()) {
      throw new FileNotFoundException("Can't open " + f + " because it is a directory");
    }

    if (readaheadInputStreamEnabled) {
      return new FSDataInputStream(
          new OBSReadaheadInputStream(
              bucket,
              pathToKey(f),
              fileStatus.getLen(),
              obs,
              statistics,
              readAhead,
              inputPolicy,
              unboundedReadThreadPool,
              bufferPartSize,
              bufferMaxRange,
              this));
    }

    return new FSDataInputStream(
        new OBSInputStream(
            bucket, pathToKey(f), fileStatus.getLen(), obs, statistics, readAhead, inputPolicy, this));
  }

  /**
   * Create an FSDataOutputStream at the indicated Path with write-progress reporting.
   *
   * @param f the file name to open
   * @param permission the permission to set.
   * @param overwrite if a file with this name already exists, then if true, the file will be
   *     overwritten, and if false an error will be thrown.
   * @param bufferSize the size of the buffer to be used.
   * @param replication required block replication for the file.
   * @param blockSize the requested block size.
   * @param progress the progress reporter.
   * @throws IOException in the event of IO related errors.
   * @see #setPermission(Path, FsPermission)
   */
  @Override
  @SuppressWarnings("IOResourceOpenedButNotSafelyClosed")
  public FSDataOutputStream create(
      Path f,
      FsPermission permission,
      boolean overwrite,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress)
      throws IOException {
    String key = pathToKey(f);
    OBSFileStatus status = null;
    try {
      // get the status or throw an exception
      status = getFileStatus(f);

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
    }
    return new FSDataOutputStream(
        new OBSBlockOutputStream(
            this,
            key,
            new SemaphoredDelegatingExecutor(boundedThreadPool, blockOutputActiveBlocks, true),
            progress,
            partSize,
            blockFactory,
            getWriteOperationHelper(),
            false),
        null);
  }

  @InterfaceAudience.Private
  private OBSWriteOperationHelper getWriteOperationHelper() {
    return writeHelper;
  }

  /**
   * Create an FSDataOutputStream at the indicated Path with write-progress reporting.
   *
   * @param f the file name to open
   * @param permission permission of
   * @param flags {@link CreateFlag}s to use for this stream.
   * @param bufferSize the size of the buffer to be used.
   * @param replication required block replication for the file.
   * @param blockSize block size
   * @param progress progress
   * @param checksumOpt check sum option
   * @throws IOException io exception
   * @see #setPermission(Path, FsPermission)
   */
  public FSDataOutputStream create(
      Path f,
      FsPermission permission,
      EnumSet<CreateFlag> flags,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress,
      ChecksumOpt checksumOpt)
      throws IOException {
    LOG.debug("create: Creating new file {}, flags:{}, isFsBucket:{}", f, flags, isFsBucket());
    if (null != flags && flags.contains(CreateFlag.APPEND)) {
      if (!isFsBucket()) {
        throw new UnsupportedOperationException(
            "non-posix bucket. Append is not supported by OBSFileSystem");
      }
      String key = pathToKey(f);
      OBSFileStatus status = null;
      try {
        // get the status or throw an FNFE
        status = getFileStatus(f);

        // if the thread reaches here, there is something at the path
        if (status.isDirectory()) {
          // path references a directory: automatic error
          throw new FileAlreadyExistsException(f + " is a directory");
        }
      } catch (FileNotFoundException e) {
        LOG.debug("FileNotFoundException, create: Creating new file {}", f);
      }

      return new FSDataOutputStream(
          new OBSBlockOutputStream(
              this,
              key,
              new SemaphoredDelegatingExecutor(boundedThreadPool, blockOutputActiveBlocks, true),
              progress,
              partSize,
              blockFactory,
              getWriteOperationHelper(),
              true),
          null);
    } else {
      return create(
          f,
          permission,
          flags == null || flags.contains(CreateFlag.OVERWRITE),
          bufferSize,
          replication,
          blockSize,
          progress);
    }
  }

  /**
   * {@inheritDoc}
   *
   * @throws FileNotFoundException if the parent directory is not present -or is not a directory.
   */
  @Override
  public FSDataOutputStream createNonRecursive(
      Path path,
      FsPermission permission,
      EnumSet<CreateFlag> flags,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress)
      throws IOException {
    Path parent = path.getParent();
    if (parent != null) {
      // expect this to raise an exception if there is no parent
      if (!getFileStatus(parent).isDirectory()) {
        throw new FileAlreadyExistsException("Not a directory: " + parent);
      }
    }
    return create(
        path,
        permission,
        flags.contains(CreateFlag.OVERWRITE),
        bufferSize,
        replication,
        blockSize,
        progress);
  }

  /**
   * Append to an existing file (optional operation).
   *
   * @param f the existing file to be appended.
   * @param bufferSize the size of the buffer to be used.
   * @param progress for reporting progress if it is not null.
   * @throws IOException indicating that append is not supported.
   */
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
      throws IOException {
    if (!isFsBucket()) {
      throw new UnsupportedOperationException(
          "non-posix bucket. Append is not supported " + "by OBSFileSystem");
    }
    LOG.debug("append: Append file {}.", f);
    String key = pathToKey(f);

    // get the status or throw an FNFE
    OBSFileStatus status = getFileStatus(f);

    // if the thread reaches here, there is something at the path
    if (status.isDirectory()) {
      // path references a directory: automatic error
      throw new FileAlreadyExistsException(f + " is a directory");
    }

    return new FSDataOutputStream(
        new OBSBlockOutputStream(
            this,
            key,
            new SemaphoredDelegatingExecutor(boundedThreadPool, blockOutputActiveBlocks, true),
            progress,
            partSize,
            blockFactory,
            getWriteOperationHelper(),
            true),
        null);
  }

  /**
   * Renames Path src to Path dst. Can take place on local fs or remote DFS.
   *
   * <p>Warning: OBS does not support renames. This method does a copy which can take OBS some time
   * to execute with large files and directories. Since there is no Progressable passed in, this can
   * time out jobs.
   *
   * <p>Note: This implementation differs with other OBS drivers. Specifically:
   *
   * <pre>
   *       Fails if src is a file and dst is a directory.
   *       Fails if src is a directory and dst is a file.
   *       Fails if the parent of dst does not exist or is a file.
   *       Fails if dst is a directory that is not empty.
   * </pre>
   *
   * @param src path to be renamed
   * @param dst new path after rename
   * @return true if rename is successful
   * @throws IOException on IO failure
   */
  public boolean rename(Path src, Path dst) throws IOException {
    LOG.info("Rename path {} to {}", src, dst);
    try {
      return innerRename(src, dst);
    } catch (ObsException e) {
      throw translateException("rename(" + src + ", " + dst + ")", src, e);
    } catch (RenameFailedException e) {
      LOG.error(e.getMessage());
      return e.getExitCode();
    } catch (FileNotFoundException e) {
      LOG.error(e.toString());
      return false;
    } finally {
      LOG.info("Rename path {} to {} finished.", src, dst);
    }
  }

  /**
   * The inner rename operation. See {@link #rename(Path, Path)} for the description of the
   * operation. This operation throws an exception on any failure which needs to be reported and
   * downgraded to a failure. That is: if a rename
   *
   * @param src path to be renamed
   * @param dst new path after rename
   * @throws RenameFailedException if some criteria for a state changing rename was not met. This
   *     means work didn't happen; it's not something which is reported upstream to the FileSystem
   *     APIs, for which the semantics of "false" are pretty vague.
   * @throws FileNotFoundException there's no source file.
   * @throws IOException on IO failure.
   * @throws ObsException on failures inside the OBS SDK
   */
  private boolean innerRename(Path src, Path dst)
      throws RenameFailedException, FileNotFoundException, IOException, ObsException {
    String srcKey = pathToKey(src);
    String dstKey = pathToKey(dst);

    if (srcKey.isEmpty()) {
      throw new RenameFailedException(src, dst, "source is root directory");
    }
    if (dstKey.isEmpty()) {
      throw new RenameFailedException(src, dst, "dest is root directory");
    }

    // get the source file status; this raises a FNFE if there is no source
    // file.
    OBSFileStatus srcStatus = getFileStatus(src);

    if (srcKey.equals(dstKey)) {
      LOG.error("rename: src and dest refer to the same file or directory: {}", dst);
      throw new RenameFailedException(
              src, dst, "source and dest refer to the same file or directory")
          .withExitCode(srcStatus.isFile());
    }

    OBSFileStatus dstStatus = null;
    boolean dstFolderExisted = false;
    try {
      dstStatus = getFileStatus(dst);
      // if there is no destination entry, an exception is raised.
      // hence this code sequence can assume that there is something
      // at the end of the path; the only detail being what it is and
      // whether or not it can be the destination of the rename.
      if (srcStatus.isDirectory()) {
        if (dstStatus.isFile()) {
          throw new RenameFailedException(src, dst, "source is a directory and dest is a file")
              .withExitCode(srcStatus.isFile());
        } else if (!renameSupportEmptyDestinationFolder) {
          throw new RenameFailedException(src, dst, "destination is an existed directory")
              .withExitCode(false);
        } else if (!dstStatus.isEmptyDirectory()) {
          throw new RenameFailedException(src, dst, "Destination is a non-empty directory")
              .withExitCode(false);
        }
        dstFolderExisted = true;
        // at this point the destination is an empty directory
      } else {
        // source is a file. The destination must be a directory,
        // empty or not
        if (dstStatus.isFile()) {
          throw new RenameFailedException(src, dst, "Cannot rename onto an existing file")
              .withExitCode(false);
        }
        dstFolderExisted = true;
      }

    } catch (FileNotFoundException e) {
      LOG.debug("rename: destination path {} not found", dst);

      if (enablePosix && (!srcStatus.isDirectory()) && dstKey.endsWith("/")) {
        throw new RenameFailedException(
            src, dst, "source is a file but destination directory is not existed");
      }

      // Parent must exist
      Path parent = dst.getParent();
      if (!pathToKey(parent).isEmpty()) {
        try {
          OBSFileStatus dstParentStatus = getFileStatus(dst.getParent());
          if (!dstParentStatus.isDirectory()) {
            throw new RenameFailedException(src, dst, "destination parent is not a directory");
          }
        } catch (FileNotFoundException e2) {
          throw new RenameFailedException(src, dst, "destination has no parent ");
        }
      }
    }

    // Ok! Time to start
    if (srcStatus.isFile()) {
      LOG.debug("rename: renaming file {} to {}", src, dst);
      if (dstStatus != null && dstStatus.isDirectory()) {
        String newDstKey = maybeAddTrailingSlash(dstKey);
        String filename = srcKey.substring(pathToKey(src.getParent()).length() + 1);
        newDstKey = newDstKey + filename;
        dstKey = newDstKey;
      }

      renameFile(srcKey, dstKey, srcStatus);
    } else {
      LOG.debug("rename: renaming directory {} to {}", src, dst);

      // This is a directory to directory copy
      dstKey = maybeAddTrailingSlash(dstKey);
      srcKey = maybeAddTrailingSlash(srcKey);

      // Verify dest is not a child of the source directory
      if (dstKey.startsWith(srcKey)) {
        throw new RenameFailedException(
            srcKey, dstKey, "cannot rename a directory to a subdirectory o fitself ");
      }

      renameFolder(srcKey, dstKey, dstFolderExisted, dstStatus);
    }

    if (src.getParent() != dst.getParent()) {
      // deleteUnnecessaryFakeDirectories(dst.getParent());
      createFakeDirectoryIfNecessary(src.getParent());
    }
    return true;
  }

  /**
   * implement rename file
   *
   * @param srcKey source object key
   * @param dstKey destination object key
   * @param srcStatus source object status
   * @throws IOException any problem with rename operation
   */
  private void renameFile(String srcKey, String dstKey, OBSFileStatus srcStatus)
      throws IOException {
    long startTime = System.nanoTime();

    if (enablePosix) {
      fsRenameFile(srcKey, dstKey);
    } else {
      copyFile(srcKey, dstKey, srcStatus.getLen());
      innerDelete(srcStatus, false);
    }

    if (LOG.isDebugEnabled()) {
      long delay = System.nanoTime() - startTime;
      LOG.debug(
          "OBSFileSystem rename: "
              + ", {src="
              + srcKey
              + ", dst="
              + dstKey
              + ", delay="
              + delay
              + "}");
    }
  }

  /**
   * implement rename folder
   *
   * @param srcKey source folder key
   * @param dstKey destination folder key
   * @param dstFolderExisted jungle destination folder is existed
   * @param dstStatus destination folder status
   * @throws IOException any problem with rename folder
   */
  private void renameFolder(
      String srcKey, String dstKey, boolean dstFolderExisted, OBSFileStatus dstStatus)
      throws IOException {
    long startTime = System.nanoTime();

    if (enablePosix) {
      fsRenameFolder(dstFolderExisted, srcKey, dstKey);
    } else {
      List<KeyAndVersion> keysToDelete = new ArrayList<>();
      if (dstStatus != null && dstStatus.isEmptyDirectory()) {
        // delete unnecessary fake directory.
        keysToDelete.add(new KeyAndVersion(dstKey));
      }

      ListObjectsRequest request = new ListObjectsRequest();
      request.setBucketName(bucket);
      request.setPrefix(srcKey);
      request.setMaxKeys(maxKeys);

      ObjectListing objects = listObjects(request);

      List<Future<DeleteObjectsResult>> deletefutures = new LinkedList<>();
      List<Future<CopyObjectResult>> copyfutures = new LinkedList<>();
      while (true) {
        for (ObsObject summary : objects.getObjects()) {
          keysToDelete.add(new KeyAndVersion(summary.getObjectKey()));
          String newDstKey = dstKey + summary.getObjectKey().substring(srcKey.length());
          // copyFile(summary.getObjectKey(), newDstKey, summary.getMetadata().getContentLength());
          copyfutures.add(
              copyFileAsync(
                  summary.getObjectKey(), newDstKey, summary.getMetadata().getContentLength()));

          if (keysToDelete.size() == MAX_ENTRIES_TO_DELETE) {
            waitAllCopyFinished(copyfutures);
            copyfutures.clear();
            deletefutures.add(removeKeysAsync(keysToDelete, true, false));
          }
        }

        if (!objects.isTruncated()) {
          if (!keysToDelete.isEmpty()) {
            waitAllCopyFinished(copyfutures);
            copyfutures.clear();
            deletefutures.add(removeKeysAsync(keysToDelete, false, false));
          }
          break;
        }
        objects = continueListObjects(objects);
      }
      try {
        for (Future deleteFuture : deletefutures) {
          deleteFuture.get();
        }
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while copying objects (delete)");
        throw new InterruptedIOException("Interrupted while copying objects (delete)");
      } catch (ExecutionException e) {
        if (e.getCause() instanceof ObsException) {
          throw (ObsException) e.getCause();
        }
        if (e.getCause() instanceof IOException) {
          throw (IOException) e.getCause();
        }
        throw new ObsException("unknown error while copying objects (delete)", e.getCause());
      }
    }

    if (LOG.isDebugEnabled()) {
      long delay = System.nanoTime() - startTime;
      LOG.debug(
          "OBSFileSystem rename: "
              + ", {src="
              + srcKey
              + ", dst="
              + dstKey
              + ", delay="
              + delay
              + "}");
    }
  }

  private void waitAllCopyFinished(List<Future<CopyObjectResult>> copyFutures)
      throws InterruptedIOException {
    try {
      for (Future copyFuture : copyFutures) {
        copyFuture.get();
      }
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while copying objects (copy)");
      // TODO Interrupted
      throw new InterruptedIOException("Interrupted while copying objects (copy)");
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }

  /**
   * Used to rename a posix file.
   *
   * @param src source object key
   * @param dst destination object key
   * @throws IOException io exception
   * @throws ObsException obs exception
   */
  private void fsRenameFile(String src, String dst) throws IOException, ObsException {
    LOG.debug("RenameFile path {} to {}", src, dst);

    try {
      final RenameRequest renameObjectRequest = new RenameRequest();
      renameObjectRequest.setBucketName(bucket);
      renameObjectRequest.setObjectKey(src);
      renameObjectRequest.setNewObjectKey(dst);
      obs.renameFile(renameObjectRequest);
      incrementWriteOperations();
    } catch (ObsException e) {
      throw translateException("renameFile(" + src + ", " + dst + ")", src, e);
    }
  }

  /**
   * Used to rename a source folder to a destination folder that is not existed before rename.
   *
   * @param src source folder key
   * @param dst destination folder key that not existed before rename
   * @throws IOException any io exception
   * @throws ObsException any obs operation exception
   */
  private void fsRenameToNewFolder(String src, String dst) throws IOException, ObsException {
    LOG.debug("RenameFolder path {} to {}", src, dst);

    try {
      RenameRequest renameObjectRequest = new RenameRequest();
      renameObjectRequest.setBucketName(bucket);
      renameObjectRequest.setObjectKey(src);
      renameObjectRequest.setNewObjectKey(dst);
      obs.renameFolder(renameObjectRequest);
      incrementWriteOperations();
    } catch (ObsException e) {
      throw translateException("renameFile(" + src + ", " + dst + ")", src, e);
    }
  }

  /**
   * Used to rename all sub objects of a source folder to an existed destination folder.
   *
   * @param srcKey source folder key
   * @param dstKey destination folder key
   * @throws IOException any problem with rename sub objects
   */
  private void fsRenameAllSubObjectsToOldFolder(String srcKey, String dstKey) throws IOException {
    // Get the first batch of son objects.
    final int maxKeyNum = maxKeys;
    ListObjectsRequest request = createListObjectsRequest(srcKey, "/", maxKeyNum);
    ObjectListing objects = listObjects(request);

    while (true) {
      // Rename sub files of current batch.
      for (ObsObject sonSrcObject : objects.getObjects()) {
        String sonSrcKey = sonSrcObject.getObjectKey();
        String sonDstKey = dstKey + sonSrcKey.substring(srcKey.length());
        if (sonSrcKey.equals(srcKey)) {
          continue;
        }
        fsRenameToNewObject(sonSrcKey, sonDstKey);
      }

      // Recursively delete sub folders of current batch.
      for (String sonSrcKey : objects.getCommonPrefixes()) {
        String sonDstKey = dstKey + sonSrcKey.substring(srcKey.length());
        if (sonSrcKey.equals(srcKey)) {
          continue;
        }
        fsRenameToNewObject(sonSrcKey, sonDstKey);
      }

      // Get the next batch of sub objects.
      if (!objects.isTruncated()) {
        // There is no sub object remaining.
        break;
      }
      objects = continueListObjects(objects);
    }
  }

  /**
   * delete obs key started '/'
   *
   * @param key object key
   * @return new key
   */
  private String maybeDeleteBeginningSlash(String key) {
    return (!StringUtils.isEmpty(key) && key.startsWith("/")) ? key.substring(1) : key;
  }

  /**
   * add obs key started '/'
   *
   * @param key object key
   * @return new key
   */
  private String maybeAddBeginningSlash(String key) {
    return (!StringUtils.isEmpty(key) && !key.startsWith("/")) ? ("/" + key) : key;
  }

  /**
   * Used to rename a source object to a destination object which is not existed before rename
   *
   * @param srcKey source object key
   * @param dstKey destination object key
   * @throws IOException io exception
   */
  private void fsRenameToNewObject(String srcKey, String dstKey) throws IOException {
    srcKey = maybeDeleteBeginningSlash(srcKey);
    dstKey = maybeDeleteBeginningSlash(dstKey);
    if (srcKey.endsWith("/")) {
      // Rename folder.
      fsRenameToNewFolder(srcKey, dstKey);
    } else {
      // Rename file.
      fsRenameFile(srcKey, dstKey);
    }
  }

  /**
   * Used to : 1) rename a source folder to a destination folder which is not existed before rename,
   * 2) rename all sub objects of a source folder to a destination folder which is already existed
   * before rename and delete the source folder at last. Comment: Both the source folder and the
   * destination folder should be end with '/'.
   *
   * @param dstFolderIsExisted destination folder is existed
   * @param srcKey source folder key
   * @param dstKey destination folder key
   * @throws IOException any problem with rename folder
   */
  private void fsRenameFolder(final boolean dstFolderIsExisted, String srcKey, String dstKey)
      throws IOException {
    LOG.debug(
        "RenameFolder path {} to {}, dstFolderIsExisted={}", srcKey, dstKey, dstFolderIsExisted);
    srcKey = maybeDeleteBeginningSlash(srcKey);
    dstKey = maybeDeleteBeginningSlash(dstKey);
    if (!dstFolderIsExisted) {
      // Rename the source folder to the destination foldeDestination is a non-empty directoryr that
      // is not existed.
      fsRenameToNewFolder(srcKey, dstKey);
    } else {
      // Rename all sub objects of the source folder to the destination folder.
      fsRenameAllSubObjectsToOldFolder(srcKey, dstKey);
      // Delete the source folder which has become empty.
      deleteObject(srcKey, true);
    }
  }

  /**
   * Low-level call to get at the object metadata.
   *
   * @param path path to the object
   * @return metadata
   * @throws IOException IO and object access problems.
   */
  @VisibleForTesting
  public ObjectMetadata getObjectMetadata(Path path) throws IOException {
    return getObjectMetadata(pathToKey(path));
  }

  /**
   * Request object metadata; increments counters in the process.
   *
   * @param key key
   * @return the metadata
   */
  protected ObjectMetadata getObjectMetadata(String key) {
    GetObjectMetadataRequest request = new GetObjectMetadataRequest();
    request.setBucketName(bucket);
    request.setObjectKey(key);
    if (sse.isSseCEnable()) {
      request.setSseCHeader(sse.getSseCHeader());
    }
    ObjectMetadata meta = obs.getObjectMetadata(request);
    incrementReadOperations();
    return meta;
  }

  /**
   * Initiate a {@code listObjects} operation, incrementing metrics in the process.
   *
   * @param request request to initiate
   * @return the results
   */
  ObjectListing listObjects(ListObjectsRequest request) {
    incrementReadOperations();
    return obs.listObjects(request);
  }

  /**
   * List the next set of objects.
   *
   * @param objects paged result
   * @return the next result object
   */
  ObjectListing continueListObjects(ObjectListing objects) {
    String delimiter = objects.getDelimiter();
    int maxKeyNum = objects.getMaxKeys();
    // LOG.debug("delimiters: "+objects.getDelimiter());
    incrementReadOperations();
    ListObjectsRequest request = new ListObjectsRequest();
    request.setMarker(objects.getNextMarker());
    request.setBucketName(bucket);
    request.setPrefix(objects.getPrefix());
    if ((maxKeyNum > 0) && (maxKeyNum < maxKeys)) {
      request.setMaxKeys(maxKeyNum);
    } else {
      request.setMaxKeys(maxKeys);
    }
    if (delimiter != null) {
      request.setDelimiter(delimiter);
    }
    return obs.listObjects(request);
  }

  /** Increment read operations. */
  private void incrementReadOperations() {
    statistics.incrementReadOps(1);
  }

  /**
   * Increment the write operation counter. This is somewhat inaccurate, as it appears to be invoked
   * more often than needed in progress callbacks.
   */
  void incrementWriteOperations() {
    statistics.incrementWriteOps(1);
  }

  /**
   * Delete an object. Increments the {@code OBJECT_DELETE_REQUESTS} and write operation statistics.
   *
   * @param key key to blob to delete.
   */
  private void deleteObject(String key, boolean isFolder) throws InvalidRequestException {
    blockRootDelete(key);
    obs.deleteObject(bucket, key);
    incrementWriteOperations();
  }

  /**
   * Reject any request to delete an object where the key is root.
   *
   * @param key key to validate
   * @throws InvalidRequestException if the request was rejected due to a mistaken attempt to delete
   *     the root directory.
   */
  private void blockRootDelete(String key) throws InvalidRequestException {
    if (key.isEmpty() || "/".equals(key)) {
      throw new InvalidRequestException("Bucket " + bucket + " cannot be deleted");
    }
  }

  /**
   * Perform a bulk object delete operation. Increments the {@code OBJECT_DELETE_REQUESTS} and write
   * operation statistics.
   *
   * @param deleteRequest keys to delete on the obs-backend
   */
  private void deleteObjects(DeleteObjectsRequest deleteRequest) {
    obs.deleteObjects(deleteRequest);
    incrementWriteOperations();
  }

  /**
   * Create a putObject request. Adds the ACL and metadata
   *
   * @param key key of object
   * @param metadata metadata header
   * @param srcfile source file
   * @return the request
   */
  PutObjectRequest newPutObjectRequest(String key, ObjectMetadata metadata, File srcfile) {
    Preconditions.checkNotNull(srcfile);
    PutObjectRequest putObjectRequest = new PutObjectRequest(bucket, key, srcfile);
    putObjectRequest.setAcl(cannedACL);
    putObjectRequest.setMetadata(metadata);
    if (getSse().isSseCEnable()) {
      putObjectRequest.setSseCHeader(getSse().getSseCHeader());
    } else if (getSse().isSseKmsEnable()) {
      putObjectRequest.setSseKmsHeader(getSse().getSseKmsHeader());
    }
    return putObjectRequest;
  }

  /**
   * Create a {@link PutObjectRequest} request. The metadata is assumed to have been configured with
   * the size of the operation.
   *
   * @param key key of object
   * @param metadata metadata header
   * @param inputStream source data.
   * @return the request
   */
  PutObjectRequest newPutObjectRequest(
      String key, ObjectMetadata metadata, InputStream inputStream) {
    Preconditions.checkNotNull(inputStream);
    PutObjectRequest putObjectRequest = new PutObjectRequest(bucket, key, inputStream);
    putObjectRequest.setAcl(cannedACL);
    putObjectRequest.setMetadata(metadata);
    if (getSse().isSseCEnable()) {
      putObjectRequest.setSseCHeader(getSse().getSseCHeader());
    } else if (getSse().isSseKmsEnable()) {
      putObjectRequest.setSseKmsHeader(getSse().getSseKmsHeader());
    }
    return putObjectRequest;
  }

  /**
   * Create a new object metadata instance. Any standard metadata headers are added here, for
   * example: encryption.
   *
   * @return a new metadata instance
   */
  private ObjectMetadata newObjectMetadata() {
    return new ObjectMetadata();
  }

  /**
   * Create a new object metadata instance. Any standard metadata headers are added here, for
   * example: encryption.
   *
   * @param length length of data to set in header.
   * @return a new metadata instance
   */
  ObjectMetadata newObjectMetadata(long length) {
    final ObjectMetadata om = newObjectMetadata();
    if (length >= 0) {
      om.setContentLength(length);
    }
    return om;
  }

  /**
   * Start a transfer-manager managed async PUT of an object, incrementing the put requests and put
   * bytes counters. It does not update the other counters, as existing code does that as progress
   * callbacks come in. Byte length is calculated from the file length, or, if there is no file,
   * from the content length of the header. Because the operation is async, any stream supplied in
   * the request must reference data (files, buffers) which stay valid until the upload completes.
   *
   * @param putObjectRequest the request
   * @return the upload initiated
   */
  public Future putObject(final PutObjectRequest putObjectRequest) {
    long len;
    if (putObjectRequest.getFile() != null) {
      len = putObjectRequest.getFile().length();
    } else {
      len = putObjectRequest.getMetadata().getContentLength();
    }
    incrementPutStartStatistics(len);
    Future future = null;
    try {
      future =
          unboundedThreadPool.submit(
              new Callable<PutObjectResult>() {
                @Override
                public PutObjectResult call() throws ObsException {
                  return obs.putObject(putObjectRequest);
                }
              });
      return future;
    } catch (ObsException e) {
      throw e;
    }
  }

  /**
   * PUT an object directly (i.e. not via the transfer manager). Byte length is calculated from the
   * file length, or, if there is no file, from the content length of the header. <i>Important: this
   * call will close any input stream in the request.</i>
   *
   * @param putObjectRequest the request
   * @return the upload initiated
   * @throws ObsException on problems
   */
  PutObjectResult putObjectDirect(PutObjectRequest putObjectRequest) throws ObsException {
    long len;
    if (putObjectRequest.getFile() != null) {
      len = putObjectRequest.getFile().length();
    } else {
      len = putObjectRequest.getMetadata().getContentLength();
    }
    try {
      PutObjectResult result = obs.putObject(putObjectRequest);
      incrementPutCompletedStatistics(true, len);
      return result;
    } catch (ObsException e) {
      incrementPutCompletedStatistics(false, len);
      throw e;
    }
  }

  /**
   * Upload part of a multi-partition file. Increments the write and put counters. <i>Important:
   * this call does not close any input stream in the request.</i>
   *
   * @param request request
   * @return the result of the operation.
   * @throws ObsException on problems
   */
  UploadPartResult uploadPart(UploadPartRequest request) throws ObsException {
    long len = request.getPartSize();
    try {
      UploadPartResult uploadPartResult = obs.uploadPart(request);
      incrementPutCompletedStatistics(true, len);
      return uploadPartResult;
    } catch (ObsException e) {
      incrementPutCompletedStatistics(false, len);
      throw e;
    }
  }

  /**
   * At the start of a put/multipart upload operation, update the relevant counters.
   *
   * @param bytes bytes in the request.
   */
  private void incrementPutStartStatistics(long bytes) {
    LOG.debug("PUT start {} bytes", bytes);
    incrementWriteOperations();
    statistics.incrementBytesWritten(bytes);
  }

  /**
   * At the end of a put/multipart upload operation, update the relevant counters and gauges.
   *
   * @param success did the operation succeed?
   * @param bytes bytes in the request.
   */
  void incrementPutCompletedStatistics(boolean success, long bytes) {
    LOG.debug("PUT completed success={}; {} bytes", success, bytes);
    incrementWriteOperations();
    if (success) {
      statistics.incrementBytesWritten(bytes);
    }
  }

  /**
   * Callback for use in progress callbacks from put/multipart upload events. Increments those
   * statistics which are expected to be updated during the ongoing upload operation.
   *
   * @param key key to file that is being written (for logging)
   * @param bytes bytes successfully uploaded.
   */
  private void incrementPutProgressStatistics(String key, long bytes) {
    PROGRESS.debug("PUT {}: {} bytes", key, bytes);
    incrementWriteOperations();
    if (bytes > 0) {
      statistics.incrementBytesWritten(bytes);
    }
  }

  /**
   * A helper method to delete a list of keys on a obs-backend.
   *
   * @param keysToDelete collection of keys to delete on the obs-backend. if empty, no request is
   *     made of the object store.
   * @param clearKeys clears the keysToDelete-list after processing the list when set to true
   * @param deleteFakeDir indicates whether this is for deleting fake dirs
   * @throws InvalidRequestException if the request was rejected due to a mistaken attempt to delete
   *     the root directory.
   */
  private Future<DeleteObjectsResult> removeKeysAsync(
      List<KeyAndVersion> keysToDelete, boolean clearKeys, boolean deleteFakeDir)
      throws ObsException, InvalidRequestException {
    if (keysToDelete.isEmpty()) {
      // exit fast if there are no keys to delete
      Future future = SettableFuture.create();
      ((SettableFuture) future).set(null);
      return (ListenableFuture<DeleteObjectsResult>) future;
    }
    for (KeyAndVersion keyVersion : keysToDelete) {
      blockRootDelete(keyVersion.getKey());
    }
    Future<DeleteObjectsResult> future;

    if (enableMultiObjectsDelete) {
      final DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(bucket);
      deleteObjectsRequest.setKeyAndVersions(
          keysToDelete.toArray(new KeyAndVersion[keysToDelete.size()]));
      future =
          boundedDeleteThreadPool.submit(
              new Callable<DeleteObjectsResult>() {
                @Override
                public DeleteObjectsResult call() throws Exception {
                  deleteObjects(deleteObjectsRequest);
                  return null;
                }
              });
    } else {
      // Copy
      final List<KeyAndVersion> keys = new ArrayList<>(keysToDelete);
      future =
          boundedDeleteThreadPool.submit(
              new Callable<DeleteObjectsResult>() {
                @Override
                public DeleteObjectsResult call() throws Exception {
                  for (KeyAndVersion keyVersion : keys) {
                    deleteObject(keyVersion.getKey(), keyVersion.getKey().endsWith("/"));
                  }
                  keys.clear();
                  return null;
                }
              });
    }

    if (clearKeys) {
      keysToDelete.clear();
    }
    return future;
  }

  // Remove all objects indicated by a leaf key list.
  private void removeKeys(List<KeyAndVersion> keysToDelete, boolean clearKeys)
      throws InvalidRequestException {
    removeKeys(keysToDelete, clearKeys, false);
  }

  private void removeKeys(
      List<KeyAndVersion> keysToDelete, boolean clearKeys, boolean checkRootDelete)
      throws InvalidRequestException {
    if (keysToDelete.isEmpty()) {
      // exit fast if there are no keys to delete
      return;
    }

    if (checkRootDelete) {
      for (KeyAndVersion keyVersion : keysToDelete) {
        blockRootDelete(keyVersion.getKey());
      }
    }

    if (!enableMultiObjectsDelete) {
      // delete one by one.
      for (KeyAndVersion keyVersion : keysToDelete) {
        deleteObject(keyVersion.getKey(), keyVersion.getKey().endsWith("/"));
      }
    } else if (keysToDelete.size() <= MAX_ENTRIES_TO_DELETE) {
      // Only one batch.
      DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(bucket);
      deleteObjectsRequest.setKeyAndVersions(
          keysToDelete.toArray(new KeyAndVersion[keysToDelete.size()]));
      deleteObjects(deleteObjectsRequest);
    } else {
      // Multi batches.
      List<KeyAndVersion> keys = new ArrayList<KeyAndVersion>(MAX_ENTRIES_TO_DELETE);
      for (KeyAndVersion key : keysToDelete) {
        keys.add(key);
        if (keys.size() == MAX_ENTRIES_TO_DELETE) {
          // Delete one batch.
          removeKeys(keys, true, false);
        }
      }
      // Delete the last batch
      removeKeys(keys, true, false);
    }

    if (clearKeys) {
      keysToDelete.clear();
    }
  }

  /**
   * Delete a Path. This operation is at least {@code O(files)}, with added overheads to enumerate
   * the path. It is also not atomic.
   *
   * @param f the path to delete.
   * @param recursive if path is a directory and set to true, the directory is deleted else throws
   *     an exception. In case of a file the recursive can be set to either true or false.
   * @return true if delete is successful else false.
   * @throws IOException due to inability to delete a directory or file.
   */
  public boolean delete(Path f, boolean recursive) throws IOException {
    ObsException lastException = null;
    for (int retryTime = 1; retryTime <= MAX_RETRY_TIME; retryTime++) {
      try {
        return innerDelete(getFileStatus(f), recursive);
      } catch (FileNotFoundException e) {
        LOG.warn("Couldn't delete {} - does not exist", f);
        return false;
      } catch (ObsException e) {
        if (e.getResponseCode() == 409) {
          lastException = e;
          LOG.warn("Delete path failed with [{}], retry time [{}] - request id [{}] - error code [{}] - error message [{}]",
                  e.getResponseCode(), retryTime, e.getErrorRequestId(), e.getErrorCode(), e.getErrorMessage());
          try {
            Thread.sleep(DELAY_TIME);
          } catch (InterruptedException ie) {
            throw e;
          }
        } else {
          throw translateException("delete", f, e);
        }
      }
    }
    throw translateException(String.format("retry max times [%s] delete failed", MAX_RETRY_TIME), f, lastException);
  }

  /**
   * Delete an object. See {@link #delete(Path, boolean)}.
   *
   * @param status fileStatus object
   * @param recursive if path is a directory and set to true, the directory is deleted else throws
   *     an exception. In case of a file the recursive can be set to either true or false.
   * @return true if delete is successful else false.
   * @throws IOException due to inability to delete a directory or file.
   * @throws ObsException on failures inside the OBS SDK
   */
  private boolean innerDelete(OBSFileStatus status, boolean recursive)
      throws IOException, ObsException {
    LOG.info("delete: path {} - recursive {}", status.getPath(), recursive);
    if (enablePosix) {
      return fsDelete(status, recursive);
    }

    Path f = status.getPath();
    String key = pathToKey(f);

    if (status.isDirectory()) {
      LOG.debug("delete: Path is a directory: {} - recursive {}", f, recursive);
      long startTime = System.nanoTime();

      key = maybeAddTrailingSlash(key);
      if (!key.endsWith("/")) {
        key = key + "/";
      }

      boolean isEmptyDir = isFolderEmpty(key);
      if (key.equals("/")) {
        return rejectRootDirectoryDelete(isEmptyDir, recursive);
      }

      if (!recursive && !isEmptyDir) {
        throw new PathIsNotEmptyDirectoryException(f.toString());
      }

      if (isEmptyDir) {
        LOG.debug("delete: Deleting fake empty directory {} - recursive {}", f, recursive);
        deleteObject(key, true);
      } else {
        LOG.debug("delete: Deleting objects for directory prefix {} - recursive {}", f, recursive);

        String delimiter = recursive ? null : "/";
        ListObjectsRequest request = createListObjectsRequest(key, delimiter);

        ObjectListing objects = listObjects(request);
        List<KeyAndVersion> keys = new ArrayList<>(objects.getObjects().size());
        while (true) {
          for (ObsObject summary : objects.getObjects()) {
            keys.add(new KeyAndVersion(summary.getObjectKey()));
            LOG.debug("Got object to delete {}", summary.getObjectKey());

            if (keys.size() == MAX_ENTRIES_TO_DELETE) {
              removeKeys(keys, true, true);
            }
          }

          if (!objects.isTruncated()) {
            if (!keys.isEmpty()) {
              removeKeys(keys, false, true);
            }
            break;
          }
          objects = continueListObjects(objects);
        }
      }

    } else {
      LOG.debug("delete: Path is a file");
      deleteObject(key, false);
    }

    Path parent = f.getParent();
    if (parent != null) {
      createFakeDirectoryIfNecessary(parent);
    }
    return true;
  }

  // Recursively delete a folder that might be not empty.
  private boolean fsDelete(OBSFileStatus status, boolean recursive)
      throws IOException, ObsException {
    Path f = status.getPath();
    String key = pathToKey(f);

    if (!status.isDirectory()) {
      LOG.debug("delete: Path is a file");
      trashObjectIfNeed(key);
    } else {
      LOG.debug("delete: Path is a directory: {} - recursive {}", f, recursive);
      key = maybeAddTrailingSlash(key);
      boolean isEmptyDir = isFolderEmpty(key);
      if (key.equals("/")) {
        return rejectRootDirectoryDelete(isEmptyDir, recursive);
      }
      if (!recursive && !isEmptyDir) {
        LOG.warn("delete: Path is not empty: {} - recursive {}", f, recursive);
        throw new PathIsNotEmptyDirectoryException(f.toString());
      }
      if (isEmptyDir) {
        LOG.debug("delete: Deleting fake empty directory {} - recursive {}", f, recursive);
        deleteObject(key, true);
      } else {
        LOG.debug(
            "delete: Deleting objects for directory prefix {} to delete - recursive {}",
            f,
            recursive);
         trashFolderIfNeed(key, f);
      }
    }

    Path parent = f.getParent();
    if (parent != null) {
      createFakeDirectoryIfNecessary(parent);
    }
    return true;
  }

  private boolean needToTrash(String key) {
    key = maybeDeleteBeginningSlash(key);
    if (enableTrash && key.startsWith(trashDir)) {
      return false;
    }
    return enableTrash;
  }

  private void trashObjectIfNeed(String key) throws ObsException, IOException {
    if (needToTrash(key)) {
      mkTrash(key);
      StringBuilder sb = new StringBuilder(trashDir);
      sb.append(key);
      if (exists(new Path(sb.toString()))) {
        SimpleDateFormat df = new SimpleDateFormat("-yyyyMMdd-HH:mm:ss");
        sb.append(df.format(new Date()));
      }
      fsRenameToNewObject(key, sb.toString());
      LOG.debug("Moved: '" + key + "' to trash at: " + sb.toString());
    } else {
      deleteObject(key, false);
    }
  }

  private void trashFolderIfNeed(String key, Path f) throws ObsException, IOException {
    if (needToTrash(key)) {
      mkTrash(key);
      StringBuilder sb = new StringBuilder(trashDir);
      String subKey = maybeAddTrailingSlash(key);
      sb.append(subKey);
      if (exists(new Path(sb.toString()))) {
        SimpleDateFormat df = new SimpleDateFormat("-yyyyMMdd-HH:mm:ss:SSS");
        sb.insert(sb.length() - 1, df.format(new Date()));
      }

      fsRenameFolder(false, key, sb.toString());
      LOG.debug("Moved: '" + key + "' to trash at: " + sb.toString());
    } else {
      if (enableMultiObjectsDeleteRecursion) {
        fsRecursivelyDelete(key, true);
      } else {
        fsNonRecursivelyDelete(f);
      }
    }
  }

  private void mkTrash(String key) throws ObsException, IOException {
    StringBuilder sb = new StringBuilder(trashDir);
    key = maybeAddTrailingSlash(key);
    sb.append(key);
    sb.deleteCharAt(sb.length() - 1);
    sb.delete(sb.lastIndexOf("/"), sb.length());
    Path fastDeleteRecycleDirPath = new Path(sb.toString());
    // keep the parent directory of the target path exists
    if (!exists(fastDeleteRecycleDirPath)) {
      mkdirs(fastDeleteRecycleDirPath);
    }
  }

  // List all sub objects at first, delete sub objects in batch secondly.
  private void fsNonRecursivelyDelete(Path parent) throws IOException, ObsException {
    // List sub objects sorted by path depth.
    FileStatus[] arFileStatus = innerListStatus(parent, true);
    // Remove sub objects one depth by one depth to avoid that parents and children in a same batch.
    fsRemoveKeys(arFileStatus);
    // Delete parent folder that should has become empty.
    deleteObject(pathToKey(parent), true);
  }

  // Remove sub objects of each depth one by one to avoid that parents and children in a same batch.
  private void fsRemoveKeys(FileStatus[] arFileStatus)
      throws ObsException, InvalidRequestException {
    if (arFileStatus.length <= 0) {
      // exit fast if there are no keys to delete
      return;
    }

    String key = "";
    for (int i = 0; i < arFileStatus.length; i++) {
      key = pathToKey(arFileStatus[i].getPath());
      blockRootDelete(key);
    }

    fsRemoveKeysByDepth(arFileStatus);
  }

  // Batch delete sub objects one depth by one depth to avoid that parents and children in a same
  // batch.
  // A batch deletion might be split into some concurrent deletions to promote the performance, but
  // it
  // can't make sure that an object is deleted before it's children.
  private void fsRemoveKeysByDepth(FileStatus[] arFileStatus)
      throws ObsException, InvalidRequestException {
    if (arFileStatus.length <= 0) {
      // exit fast if there is no keys to delete
      return;
    }

    // Find all leaf keys in the list.
    String key = "";
    int depth = Integer.MAX_VALUE;
    List<KeyAndVersion> leafKeys = new ArrayList<KeyAndVersion>(MAX_ENTRIES_TO_DELETE);
    for (int idx = (arFileStatus.length - 1); idx >= 0; idx--) {
      if (leafKeys.size() >= MAX_ENTRIES_TO_DELETE) {
        removeKeys(leafKeys, true);
      }

      key = pathToKey(arFileStatus[idx].getPath());

      // Check file.
      if (!arFileStatus[idx].isDirectory()) {
        // A file must be a leaf.
        leafKeys.add(new KeyAndVersion(key, null));
        continue;
      }

      // Check leaf folder at current depth.
      int keyDepth = fsGetObjectKeyDepth(key);
      if (keyDepth == depth) {
        // Any key at current depth must be a leaf.
        leafKeys.add(new KeyAndVersion(key, null));
        continue;
      }
      if (keyDepth < depth) {
        // The last batch delete at current depth.
        removeKeys(leafKeys, true);
        // Go on at the upper depth.
        depth = keyDepth;
        leafKeys.add(new KeyAndVersion(key, null));
        continue;
      }
      LOG.warn("The objects list is invalid because it isn't sorted by path depth.");
      throw new ObsException("System failure");
    }

    // The last batch delete at the minimum depth of all keys.
    removeKeys(leafKeys, true);
  }

  private int fsRemoveKeysByDepth(List<KeyAndVersion> keys)
      throws ObsException, InvalidRequestException {
    if (keys.size() <= 0) {
      // exit fast if there is no keys to delete
      return 0;
    }

    // Find all leaf keys in the list.
    int filesNum = 0;
    int foldersNum = 0;
    String key = "";
    int depth = Integer.MAX_VALUE;
    List<KeyAndVersion> leafKeys = new ArrayList<KeyAndVersion>(MAX_ENTRIES_TO_DELETE);
    for (int idx = (keys.size() - 1); idx >= 0; idx--) {
      if (leafKeys.size() >= MAX_ENTRIES_TO_DELETE) {
        removeKeys(leafKeys, true);
      }

      key = keys.get(idx).getKey();

      // Check file.
      if (!key.endsWith("/")) {
        // A file must be a leaf.
        leafKeys.add(new KeyAndVersion(key, null));
        filesNum++;
        continue;
      }

      // Check leaf folder at current depth.
      int keyDepth = fsGetObjectKeyDepth(key);
      if (keyDepth == depth) {
        // Any key at current depth must be a leaf.
        leafKeys.add(new KeyAndVersion(key, null));
        foldersNum++;
        continue;
      }
      if (keyDepth < depth) {
        // The last batch delete at current depth.
        removeKeys(leafKeys, true);
        // Go on at the upper depth.
        depth = keyDepth;
        leafKeys.add(new KeyAndVersion(key, null));
        foldersNum++;
        continue;
      }
      LOG.warn("The objects list is invalid because it isn't sorted by path depth.");
      throw new ObsException("System failure");
    }

    // The last batch delete at the minimum depth of all keys.
    removeKeys(leafKeys, true);

    return filesNum + foldersNum;
  }

  // Recursively delete a folder that might be not empty.
  private int fsRecursivelyDelete(String parentKey, boolean deleteParent, int recursionScale)
      throws IOException {
    List<KeyAndVersion> folders = new ArrayList<KeyAndVersion>(MAX_ENTRIES_TO_DELETE);
    List<KeyAndVersion> files = new ArrayList<KeyAndVersion>(MAX_ENTRIES_TO_DELETE);

    // Get the first batch of son objects.
    ListObjectsRequest request = createListObjectsRequest(parentKey, null, maxKeys);
    ObjectListing objects = listObjects(request);
    int delNum = 0;

    while (true) {
      // Delete sub files of current batch.
      for (ObsObject sonObject : objects.getObjects()) {
        String sonObjectKey = sonObject.getObjectKey();

        if (sonObjectKey.length() == parentKey.length()) {
          // Ignore parent folder.
          continue;
        }

        // save or remove the son object.
        delNum += fsRemoveSonObject(sonObjectKey, files, folders);
      }

      if (folders.size() >= recursionScale) {
        // There are too many folders, need to recursively delete current deepest folders in list.
        // 1)Delete remaining files which may be sub objects of deepest folders or not.
        delNum += files.size();
        removeKeys(files, true);
        // 2)Extract deepest folders and recursively delete them.
        delNum += fsRecursivelyDeleteDeepest(folders);
      }

      // Get the next batch of sub objects.
      if (!objects.isTruncated()) {
        // There is no sub object remaining.
        break;
      }
      objects = continueListObjects(objects);
    }

    // Delete remaining files.
    delNum += files.size();
    removeKeys(files, true);

    // Delete remaining folders.
    delNum += fsRemoveKeysByDepth(folders);

    // Delete parent folder.
    if (deleteParent) {
      deleteObject(parentKey, true);
      delNum++;
    }

    return delNum;
  }

  private int fsRecursivelyDelete(String parentKey, boolean deleteParent) throws IOException {
    return fsRecursivelyDelete(parentKey, deleteParent, MAX_ENTRIES_TO_DELETE * 2);
  }

  private int fsRemoveSonObject(
      String sonObjectKey, List<KeyAndVersion> files, List<KeyAndVersion> folders)
      throws IOException {
    if (!sonObjectKey.endsWith("/")) {
      // file
      return fsRemoveFile(sonObjectKey, files);
    } else {
      // folder
      folders.add(new KeyAndVersion(sonObjectKey));
      return 0;
    }
  }

  // Delete a file.
  private int fsRemoveFile(String sonObjectKey, List<KeyAndVersion> files) throws IOException {
    files.add(new KeyAndVersion(sonObjectKey));
    if (files.size() == MAX_ENTRIES_TO_DELETE) {
      // batch delete files.
      removeKeys(files, true);
      return MAX_ENTRIES_TO_DELETE;
    }
    return 0;
  }

  // Extract deepest folders from a folders list sorted by path depth.
  private List<KeyAndVersion> fsExtractDeepestFolders(List<KeyAndVersion> folders)
      throws ObsException {
    if (folders.isEmpty()) {
      return null;
    }

    // Find the index of the first deepest folder.
    int start = folders.size() - 1;
    KeyAndVersion folder = folders.get(start);
    int deepestDepth = fsGetObjectKeyDepth(folder.getKey());
    int currDepth = 0;
    for (start--; start >= 0; start--) {
      folder = folders.get(start);
      currDepth = fsGetObjectKeyDepth(folder.getKey());
      if (currDepth == deepestDepth) {
        continue;
      } else if (currDepth < deepestDepth) {
        // Found the first deepest folder.
        start++;
        break;
      } else {
        LOG.warn("The folders list is invalid because it isn't sorted by path depth.");
        throw new ObsException("System failure");
      }
    }
    if (start < 0) {
      // All folders in list is at the same depth.
      start = 0;
    }

    // Extract deepest folders.
    int deepestFoldersNum = folders.size() - start;
    List<KeyAndVersion> deepestFolders =
        new ArrayList<>(Math.min(folders.size(), deepestFoldersNum));
    for (int i = folders.size() - 1; i >= start; i--) {
      folder = folders.get(i);
      deepestFolders.add(folder);
      folders.remove(i);
    }

    return deepestFolders;
  }

  // Extract deepest folders from a list sorted by path depth and recursively delete them.
  private int fsRecursivelyDeleteDeepest(List<KeyAndVersion> folders) throws IOException {
    int delNum = 0;

    // Extract deepest folders.
    List<KeyAndVersion> deepestFolders = fsExtractDeepestFolders(folders);

    // Recursively delete sub objects of each deepest folder one by one.
    for (KeyAndVersion folder : deepestFolders) {
      delNum += fsRecursivelyDelete(folder.getKey(), false);
    }

    // Batch delete deepest folders.
    delNum += deepestFolders.size();
    removeKeys(deepestFolders, false);

    return delNum;
  }

  /**
   * Implements the specific logic to reject root directory deletion. The caller must return the
   * result of this call, rather than attempt to continue with the delete operation: deleting root
   * directories is never allowed. This method simply implements the policy of when to return an
   * exit code versus raise an exception.
   *
   * @param recursive recursive flag from command
   * @return a return code for the operation
   * @throws PathIOException if the operation was explicitly rejected.
   */
  private boolean rejectRootDirectoryDelete(boolean isEmptyDir, boolean recursive)
      throws IOException {
    LOG.info("obs delete the {} root directory of {}", bucket, recursive);
    if (isEmptyDir) {
      return true;
    }
    if (recursive) {
      return false;
    } else {
      // reject
      throw new PathIOException(bucket, "Cannot delete root path");
    }
  }

  private void createFakeDirectoryIfNecessary(Path f) throws IOException, ObsException {
    if (enablePosix) {
      return;
    }

    String key = pathToKey(f);
    if (!key.isEmpty() && !exists(f)) {
      LOG.debug("Creating new fake directory at {}", f);
      createFakeDirectory(key);
    }
  }

  /**
   * List the statuses of the files/directories in the given path if the path is a directory.
   *
   * @param f given path
   * @return the statuses of the files/directories in the given patch
   * @throws FileNotFoundException when the path does not exist; IOException see specific
   *     implementation
   */
  public FileStatus[] listStatus(Path f) throws IOException {
    try {
      return innerListStatus(f, false);
    } catch (ObsException e) {
      throw translateException("listStatus", f, e);
    }
  }

  /**
   * List the statuses of the files/directories in the given path if the path is a directory.
   *
   * @param f given path
   * @return the statuses of the files/directories in the given patch
   * @throws FileNotFoundException when the path does not exist;
   * @throws IOException due to an IO problem.
   * @throws ObsException on failures inside the OBS SDK
   */
  private FileStatus[] innerListStatus(Path f, boolean recursive)
      throws FileNotFoundException, IOException, ObsException {
    Path path = qualify(f);
    String key = pathToKey(path);
    LOG.debug("List status for path: {}", path);

    List<FileStatus> result;
    final FileStatus fileStatus = getFileStatus(path);

    if (fileStatus.isDirectory()) {
      key = maybeAddTrailingSlash(key);
      String delimiter = recursive ? null : "/";
      ListObjectsRequest request = createListObjectsRequest(key, delimiter);
      LOG.debug("listStatus: doing listObjects for directory {} - recursive {}", f, recursive);

      Listing.FileStatusListingIterator files =
          listing.createFileStatusListingIterator(
              path, request, ACCEPT_ALL, new Listing.AcceptAllButSelfAndS3nDirs(path));
      result = new ArrayList<>(files.getBatchSize());
      while (files.hasNext()) {
        result.add(files.next());
      }
      return result.toArray(new FileStatus[result.size()]);
    } else {
      LOG.debug("Adding: rd (not a dir): {}", path);
      FileStatus[] stats = new FileStatus[1];
      stats[0] = fileStatus;
      return stats;
    }
  }

  /**
   * Create a {@code ListObjectsRequest} request against this bucket, with the maximum keys returned
   * in a query set by {@link #maxKeys}.
   *
   * @param key key for request
   * @param delimiter any delimiter
   * @return the request
   */
  private ListObjectsRequest createListObjectsRequest(String key, String delimiter) {
    return createListObjectsRequest(key, delimiter, -1);
  }

  private ListObjectsRequest createListObjectsRequest(String key, String delimiter, int maxKeyNum) {
    ListObjectsRequest request = new ListObjectsRequest();
    request.setBucketName(bucket);
    if ((maxKeyNum > 0) && (maxKeyNum < maxKeys)) {
      request.setMaxKeys(maxKeyNum);
    } else {
      request.setMaxKeys(maxKeys);
    }
    request.setPrefix(key);
    if (delimiter != null) {
      request.setDelimiter(delimiter);
    }
    return request;
  }

  /**
   * Get the current working directory for the given file system.
   *
   * @return the directory pathname
   */
  public Path getWorkingDirectory() {
    return workingDir;
  }

  /**
   * Set the current working directory for the given file system. All relative paths will be
   * resolved relative to it.
   *
   * @param newDir the current working directory.
   */
  public void setWorkingDirectory(Path newDir) {
    workingDir = newDir;
  }

  /**
   * Get the username of the FS.
   *
   * @return the short name of the user who instantiated the FS
   */
  String getUsername() {
    return username;
  }

  /**
   * Make the given path and all non-existent parents into directories. Has the semantics of Unix
   * {@code 'mkdir -p'}. Existence of the directory hierarchy is not an error.
   *
   * @param path path to create
   * @param permission to apply to f
   * @return true if a directory was created
   * @throws FileAlreadyExistsException there is a file at the path specified
   * @throws IOException other IO problems
   */
  // TODO: If we have created an empty file at /foo/bar and we then call
  // mkdirs for /foo/bar/baz/roo what happens to the empty file /foo/bar/?
  public boolean mkdirs(Path path, FsPermission permission)
      throws IOException, FileAlreadyExistsException {
    try {
      return innerMkdirs(path, permission);
    } catch (ObsException e) {
      throw translateException("innerMkdirs", path, e);
    }
  }

  /**
   * Make the given path and all non-existent parents into directories. See {@link #mkdirs(Path,
   * FsPermission)}
   *
   * @param f path to create
   * @param permission to apply to f
   * @return true if a directory was created
   * @throws FileAlreadyExistsException there is a file at the path specified
   * @throws IOException other IO problems
   * @throws ObsException on failures inside the OBS SDK
   */
  // TODO: If we have created an empty file at /foo/bar and we then call
  // mkdirs for /foo/bar/baz/roo what happens to the empty file /foo/bar/?
  private boolean innerMkdirs(Path f, FsPermission permission)
      throws IOException, FileAlreadyExistsException, ObsException {
    LOG.debug("Making directory: {}", f);
    FileStatus fileStatus;
    try {
      fileStatus = getFileStatus(f);

      if (fileStatus.isDirectory()) {
        return true;
      } else {
        throw new FileAlreadyExistsException("Path is a file: " + f);
      }
    } catch (FileNotFoundException e) {
      Path fPart = f.getParent();
      do {
        try {
          fileStatus = getFileStatus(fPart);
          if (fileStatus.isDirectory()) {
            break;
          }
          if (fileStatus.isFile()) {
            throw new FileAlreadyExistsException(
                String.format("Can't make directory for path '%s' since it is a file.", fPart));
          }
        } catch (FileNotFoundException fnfe) {
          LOG.debug("file {} not fount, but ignore.", f);
        }
        fPart = fPart.getParent();
      } while (fPart != null);

      String key = pathToKey(f);
      createFakeDirectory(key);
      return true;
    }
  }

  /**
   * Return a file status object that represents the path.
   *
   * @param f The path we want information from
   * @return a FileStatus object
   * @throws java.io.FileNotFoundException when the path does not exist;
   * @throws IOException on other problems.
   */
  public OBSFileStatus getFileStatus(final Path f) throws IOException {
    if (enablePosix) {
      return fsGetObjectStatus(f);
    }

    final Path path = qualify(f);
    String key = pathToKey(path);
    LOG.debug("Getting path status for {}  ({})", path, key);
    if (!StringUtils.isEmpty(key)) {
      try {
        ObjectMetadata meta = getObjectMetadata(key);

        if (objectRepresentsDirectory(key, meta.getContentLength())) {
          LOG.debug("Found exact file: fake directory");
          return new OBSFileStatus(true, path, username);
        } else {
          LOG.debug("Found exact file: normal file");
          return new OBSFileStatus(
              meta.getContentLength(),
              dateToLong(meta.getLastModified()),
              path,
              getDefaultBlockSize(path),
              username);
        }
      } catch (ObsException e) {
        if (e.getResponseCode() != 404) {
          throw translateException("getFileStatus", path, e);
        }
      }

      // Necessary? TODO ???
      if (!key.endsWith("/")) {
        String newKey = key + "/";
        try {
          ObjectMetadata meta = getObjectMetadata(newKey);

          if (objectRepresentsDirectory(newKey, meta.getContentLength())) {
            LOG.debug("Found file (with /): fake directory");
            return new OBSFileStatus(true, path, username);
          } else {
            LOG.debug("Found file (with /): real file? should not happen: {}", key);

            return new OBSFileStatus(
                meta.getContentLength(),
                dateToLong(meta.getLastModified()),
                path,
                getDefaultBlockSize(path),
                username);
          }
        } catch (ObsException e) {
          if (e.getResponseCode() != 404) {
            throw translateException("getFileStatus", newKey, e);
          }
        }
      }
    }

    try {
      boolean isEmpty = isFolderEmpty(key);
      return new OBSFileStatus(isEmpty, path, username);
    } catch (ObsException e) {
      if (e.getResponseCode() != 404) {
        throw translateException("getFileStatus", key, e);
      }
    }

    LOG.error("Not Found: {}", path);
    throw new FileNotFoundException("No such file or directory: " + path);
  }

  // Used to check if a folder is empty or not by counting the number of sub objects in list.
  private boolean isFolderEmpty(String key, ObjectListing objects) {
    int count = objects.getObjects().size();
    if ((count >= 2)
        || ((count == 1) && (!objects.getObjects().get(0).getObjectKey().equals(key)))) {
      // There is a sub file at least.
      return false;
    }

    count = objects.getCommonPrefixes().size();
    if ((count >= 2)
            || ((count == 1) && (!objects.getCommonPrefixes().get(0).equals(key)))) {
      // There is a sub directory at least.
      return false;
    }

    // There is no sub object.
    return true;
  }

  // Used to check if a folder is empty or not.
  private boolean isFolderEmpty(String key) throws FileNotFoundException, ObsException {
    key = maybeAddTrailingSlash(key);
    ListObjectsRequest request = new ListObjectsRequest();
    request.setBucketName(bucket);
    request.setPrefix(key);
    request.setDelimiter("/");
    request.setMaxKeys(3);
    ObjectListing objects = listObjects(request);

    if (!objects.getCommonPrefixes().isEmpty() || !objects.getObjects().isEmpty()) {
      if (isFolderEmpty(key, objects)) {
        LOG.debug("Found empty directory {}", key);
        return true;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Found path as directory (with /): {}/{}",
            objects.getCommonPrefixes().size(),
            objects.getObjects().size());

        for (ObsObject summary : objects.getObjects()) {
          LOG.debug(
              "Summary: {} {}", summary.getObjectKey(), summary.getMetadata().getContentLength());
        }
        for (String prefix : objects.getCommonPrefixes()) {
          LOG.debug("Prefix: {}", prefix);
        }
      }
      LOG.debug("Found non-empty directory {}", key);
      return false;
    } else if (key.isEmpty()) {
      LOG.debug("Found root directory");
      return true;
    } else if (enablePosix) {
      LOG.debug("Found empty directory {}", key);
      return true;
    }

    LOG.debug("Not Found: {}", key);
    throw new FileNotFoundException("No such file or directory: " + key);
  }

  // Used to get the status of a file or folder in a file-gateway bucket.
  public OBSFileStatus fsGetObjectStatus(final Path f) throws IOException {
    final Path path = qualify(f);
    String key = pathToKey(path);
    LOG.debug("Getting path status for {}  ({})", path, key);

    if (key.isEmpty()) {
      LOG.debug("Found root directory");
      boolean isEmpty = isFolderEmpty(key);
      return new OBSFileStatus(isEmpty, path, username);
    }

    try {
      final GetAttributeRequest getAttrRequest = new GetAttributeRequest(bucket, key);
      ObsFSAttribute meta = obs.getAttribute(getAttrRequest);
      incrementReadOperations();
      if (fsIsFolder(meta)) {
        LOG.debug("Found file (with /): fake directory");
        boolean isEmpty = isFolderEmpty(key);
        return new OBSFileStatus(isEmpty, path, dateToLong(meta.getLastModified()), username);
      } else {
        LOG.debug("Found file (with /): real file? should not happen: {}", key);
        return new OBSFileStatus(
            meta.getContentLength(),
            dateToLong(meta.getLastModified()),
            path,
            getDefaultBlockSize(path),
            username);
      }
    } catch (ObsException e) {
      if (e.getResponseCode() != 404) {
        throw translateException("getFileStatus", path, e);
      }
    }

    LOG.debug("Not Found: {}", path);
    throw new FileNotFoundException("No such file or directory: " + path);
  }

  /**
   * The src file is on the local disk. Add it to FS at the given dst name.
   *
   * <p>This version doesn't need to create a temporary file to calculate the md5. Sadly this
   * doesn't seem to be used by the shell cp :(
   *
   * <p>delSrc indicates if the source should be removed
   *
   * @param delSrc whether to delete the src
   * @param overwrite whether to overwrite an existing file
   * @param src path
   * @param dst path
   * @throws IOException IO problem
   * @throws FileAlreadyExistsException the destination file exists and overwrite==false
   * @throws ObsException failure in the OBS SDK
   */
  @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path src, Path dst)
      throws IOException {
    try {
      //innerCopyFromLocalFile(delSrc, overwrite, src, dst);
      super.copyFromLocalFile(delSrc, overwrite, src, dst);
    } catch (ObsException e) {
      throw translateException("copyFromLocalFile(" + src + ", " + dst + ")", src, e);
    }
  }

  /**
   * The src file is on the local disk. Add it to FS at the given dst name.
   *
   * <p>This version doesn't need to create a temporary file to calculate the md5. Sadly this
   * doesn't seem to be used by the shell cp :(
   *
   * <p>delSrc indicates if the source should be removed
   *
   * @param delSrc whether to delete the src
   * @param overwrite whether to overwrite an existing file
   * @param src path
   * @param dst path
   * @throws IOException IO problem
   * @throws FileAlreadyExistsException the destination file exists and overwrite==false
   * @throws ObsException failure in the OBS SDK
   */
  private void innerCopyFromLocalFile(boolean delSrc, boolean overwrite, Path src, Path dst)
      throws IOException, FileAlreadyExistsException, ObsException, InterruptedIOException {

    final String key = pathToKey(dst);

    if (!overwrite && exists(dst)) {
      throw new FileAlreadyExistsException(dst + " already exists");
    }
    LOG.debug("Copying local file from {} to {}", src, dst);

    // Since we have a local file, we don't need to stream into a temporary file
    LocalFileSystem local = getLocal(getConf());
    final File srcfile = local.pathToFile(src);
    InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest(bucket, key);
    request.setAcl(cannedACL);
    if (getSse().isSseCEnable()) {
      request.setSseCHeader(getSse().getSseCHeader());
    } else if (getSse().isSseKmsEnable()) {
      request.setSseKmsHeader(getSse().getSseKmsHeader());
    }
    InitiateMultipartUploadResult result = obs.initiateMultipartUpload(request);
    final String uploadId = result.getUploadId();
    long fileSize = srcfile.length();
    long partCount = calPartCount(partSize, fileSize);
    final List<PartEtag> partEtags = Collections.synchronizedList(new ArrayList<PartEtag>());
    final List<ListenableFuture<?>> partUploadFutures = new ArrayList<ListenableFuture<?>>();
    for (int i = 0; i < partCount; i++) {
      final long offset = i * partSize;
      final long currPartSize = (i + 1 == partCount) ? fileSize - offset : partSize;
      final int partNumber = i + 1;
      partUploadFutures.add(
          boundedThreadPool.submit(
              new Runnable() {
                @Override
                public void run() {
                  UploadPartRequest uploadPartRequest = new UploadPartRequest();
                  uploadPartRequest.setBucketName(bucket);
                  uploadPartRequest.setObjectKey(key);
                  uploadPartRequest.setUploadId(uploadId);
                  uploadPartRequest.setFile(srcfile);
                  uploadPartRequest.setPartSize(currPartSize);
                  uploadPartRequest.setOffset(offset);
                  uploadPartRequest.setPartNumber(partNumber);
                  if (getSse().isSseCEnable()) {
                    uploadPartRequest.setSseCHeader(getSse().getSseCHeader());
                  }
                  UploadPartResult uploadPartResult;
                  try {
                    uploadPartResult = obs.uploadPart(uploadPartRequest);
                    LOG.debug("Part#" + partNumber + " done");

                    partEtags.add(
                        new PartEtag(uploadPartResult.getEtag(), uploadPartResult.getPartNumber()));
                  } catch (ObsException e) {
                    LOG.error("Multipart copy from local file exception.", e);
                  }
                }
              }));
    }
    try {
      Futures.allAsList(partUploadFutures).get();
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while part upload objects(copyFromLocalFile)");
      throw new InterruptedIOException(
          "Interrupted while part uploading objects (copyFromLocalFile)");
    } catch (ExecutionException e) {
      LOG.error("part upload file exception: ", e);
      LOG.debug("Cancelling futures");
      for (ListenableFuture<?> future : partUploadFutures) {
        future.cancel(true);
      }
      OBSWriteOperationHelper writeHelper = new OBSWriteOperationHelper(this, getConf());
      ObsException lastException;
      String operation =
          String.format("Aborting multi-part upload for '%s', id '%s", writeHelper, uploadId);
      writeHelper.abortMultipartUpload(key, uploadId);
      throw extractException("Multi-part upload with id '" + uploadId + "' to " + key, key, e);
    }
    if (partEtags.size() != partCount) {
      LOG.error("partEtags({}) is not equals partCount({}).", partEtags.size(), partCount);
      throw new IllegalStateException(
          "Upload multiparts fail due to some parts are not finished yet");
    }
    Collections.sort(
        partEtags,
        new Comparator<PartEtag>() {
          @Override
          public int compare(PartEtag o1, PartEtag o2) {
            return o1.getPartNumber() - o2.getPartNumber();
          }
        });

    CompleteMultipartUploadRequest completeMultipartUploadRequest =
        new CompleteMultipartUploadRequest(bucket, key, uploadId, partEtags);
    int retryCount = 0;
    ObsException lastException;
    String operation =
        String.format(
            "Completing multi-part upload for key '%s'," + " id '%s' with %s partitions ",
            key, uploadId, partEtags.size());
    try {
      obs.completeMultipartUpload(completeMultipartUploadRequest);
      incrementPutCompletedStatistics(true, srcfile.length());
      if (delSrc) {
        local.delete(src, false);
      }
      return;
    } catch (ObsException e) {
      lastException = e;
    }
    incrementPutCompletedStatistics(false, srcfile.length());
    throw translateException(operation, key, lastException);
  }

  int calPartCount(long partSize, long cloudSize) {
    // get user setting of per copy part size ,default is 100MB
    // calculate the part count
    long partCount = cloudSize % partSize == 0 ? cloudSize / partSize : cloudSize / partSize + 1;
    return (int) partCount;
  }

  /**
   * Close the filesystem. This shuts down all transfers.
   *
   * @throws IOException IO problem
   */
  @Override
  public void close() throws IOException {
    LOG.info("This Filesystem closed by user, clear resource.");
    if (closed.getAndSet(true)) {
      // already closed
      return;
    }

    try {
      super.close();
    } finally {
      shutdownAllNow(
          LOG,
          boundedThreadPool,
          boundedCopyThreadPool,
          boundedDeleteThreadPool,
          unboundedReadThreadPool,
          unboundedThreadPool,
          boundedCopyPartThreadPool);
    }
  }

  private void shutdownAllNow(Logger log, ExecutorService... executors) {
    for (ExecutorService exe : executors) {
      if (exe != null) {
        try {
          if (log != null) {
            log.debug("Shutdown now {}", exe);
          }
          exe.shutdownNow();
        } catch (Exception e) {
          if (log != null && log.isDebugEnabled()) {
            log.debug("Exception in shutdown {}", exe, e);
          }
        }
      }
    }
  }

  private void shutdownAll(Logger log, ExecutorService... executors) {
    for (ExecutorService exe : executors) {
      if (exe != null) {
        try {
          if (log != null) {
            log.debug("Shutdown {}", exe);
          }
          exe.shutdown();
        } catch (Exception e) {
          if (log != null && log.isDebugEnabled()) {
            log.debug("Exception in shutdown {}", exe, e);
          }
        }
      }
    }
  }
  /** Override getCanonicalServiceName because we don't support token in obs. */
  @Override
  public String getCanonicalServiceName() {
    // Does not support Token
    return null;
  }

  private Future<CopyObjectResult> copyFileAsync(
      final String srcKey, final String dstKey, final long size) {
    return boundedCopyThreadPool.submit(
        new Callable<CopyObjectResult>() {
          @Override
          public CopyObjectResult call() throws Exception {
            copyFile(srcKey, dstKey, size);
            return null;
          }
        });
  }

  /**
   * Copy a single object in the bucket via a COPY operation.
   *
   * @param srcKey source object path
   * @param dstKey destination object path
   * @param size object size
   * @throws ObsException on failures inside the OBS SDK
   * @throws InterruptedIOException the operation was interrupted
   * @throws IOException Other IO problems
   */
  private void copyFile(final String srcKey, final String dstKey, final long size)
      throws IOException, InterruptedIOException, ObsException {
    LOG.debug("copyFile {} -> {} ", srcKey, dstKey);
    try {
      // 100MB per part
      long objectSize = size;
      if (objectSize > copyPartSize) {
        // initial copy part task
        InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest(bucket, dstKey);
        request.setAcl(cannedACL);
        if (getSse().isSseCEnable()) {
          request.setSseCHeader(getSse().getSseCHeader());
        } else if (getSse().isSseKmsEnable()) {
          request.setSseKmsHeader(getSse().getSseKmsHeader());
        }
        InitiateMultipartUploadResult result = obs.initiateMultipartUpload(request);

        final String uploadId = result.getUploadId();
        LOG.debug("Multipart copy file, uploadId: {}", uploadId);
        // count the parts
        long partCount = calPartCount(copyPartSize, objectSize);

        final List<PartEtag> partEtags =
            getCopyFilePartEtags(srcKey, dstKey, objectSize, uploadId, partCount);
        // merge the copy parts
        CompleteMultipartUploadRequest completeMultipartUploadRequest =
            new CompleteMultipartUploadRequest(bucket, dstKey, uploadId, partEtags);
        obs.completeMultipartUpload(completeMultipartUploadRequest);
      } else {
        ObjectMetadata srcom = getObjectMetadata(srcKey);
        ObjectMetadata dstom = cloneObjectMetadata(srcom);
        final CopyObjectRequest copyObjectRequest =
            new CopyObjectRequest(bucket, srcKey, bucket, dstKey);
        // TODO copyObjectRequest.setCannedAccessControlList(cannedACL);
        copyObjectRequest.setAcl(cannedACL);
        copyObjectRequest.setNewObjectMetadata(dstom);
        if (getSse().isSseCEnable()) {
          copyObjectRequest.setSseCHeader(getSse().getSseCHeader());
          copyObjectRequest.setSseCHeaderSource(getSse().getSseCHeader());
        } else if (getSse().isSseKmsEnable()) {
          copyObjectRequest.setSseKmsHeader(getSse().getSseKmsHeader());
        }
        obs.copyObject(copyObjectRequest);
      }

      incrementWriteOperations();
    } catch (ObsException e) {
      throw translateException("copyFile(" + srcKey + ", " + dstKey + ")", srcKey, e);
    }
  }

  long getCopyPartSize() {
    return copyPartSize;
  }

  List<PartEtag> getCopyFilePartEtags(
      final String srcKey,
      final String dstKey,
      long objectSize,
      final String uploadId,
      long partCount)
      throws IOException, InterruptedIOException {
    final List<PartEtag> partEtags = Collections.synchronizedList(new ArrayList<PartEtag>());
    final List<Future<?>> partCopyFutures = new ArrayList<Future<?>>();
    for (int i = 0; i < partCount; i++) {
      final long rangeStart = i * copyPartSize;
      final long rangeEnd = (i + 1 == partCount) ? objectSize - 1 : rangeStart + copyPartSize - 1;
      final int partNumber = i + 1;
      partCopyFutures.add(
          boundedCopyPartThreadPool.submit(
              new Runnable() {

                @Override
                public void run() {
                  CopyPartRequest request = new CopyPartRequest();
                  request.setUploadId(uploadId);
                  request.setSourceBucketName(bucket);
                  request.setSourceObjectKey(srcKey);
                  request.setDestinationBucketName(bucket);
                  request.setDestinationObjectKey(dstKey);
                  request.setByteRangeStart(rangeStart);
                  request.setByteRangeEnd(rangeEnd);
                  request.setPartNumber(partNumber);
                  if (getSse().isSseCEnable()) {
                    request.setSseCHeaderSource(getSse().getSseCHeader());
                    request.setSseCHeaderDestination(getSse().getSseCHeader());
                  }
                  CopyPartResult result = obs.copyPart(request);
                  partEtags.add(new PartEtag(result.getEtag(), result.getPartNumber()));
                  LOG.debug(
                      "Multipart copy file, uploadId: {}, Part#{} done.", uploadId, partNumber);
                }
              }));
    }

    // wait the tasks for completing
    try {
      for (Future partCopyFuture : partCopyFutures) {
        partCopyFuture.get();
      }
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while copying objects (copy)");
      throw new InterruptedIOException("Interrupted while copying objects (copy)");
    } catch (ExecutionException e) {
      LOG.error("Multipart copy file exception.", e);
      for (Future<?> future : partCopyFutures) {
        future.cancel(true);
      }

      obs.abortMultipartUpload(new AbortMultipartUploadRequest(bucket, dstKey, uploadId));

      throw OBSUtils.extractException(
          "Multi-part copy with id '" + uploadId + "' from " + srcKey + "to " + dstKey, dstKey, e);
    }

    if (partEtags.size() != partCount) {
      LOG.error("partEtags({}) is not equals partCount({}).", partEtags.size(), partCount);
      throw new IllegalStateException(
          "Upload multiparts fail due to some parts are not finished yet");
    }

    // Make part numbers in ascending order
    Collections.sort(
        partEtags,
        new Comparator<PartEtag>() {
          @Override
          public int compare(PartEtag o1, PartEtag o2) {
            return o1.getPartNumber() - o2.getPartNumber();
          }
        });
    return partEtags;
  }

  /**
   * Perform post-write actions.
   *
   * @param key key written to
   */
  void finishedWrite(String key) {
    LOG.debug("Finished write to {}", key);
  }

  private void createFakeDirectory(String objectName) throws ObsException, InterruptedIOException {
    if (enablePosix) {
      fsCreateFolder(objectName);
      return;
    }
    objectName = maybeAddTrailingSlash(objectName);
    createEmptyObject(objectName);
  }

  // Used to create an empty file that represents an empty directory
  private void createEmptyObject(final String objectName)
      throws ObsException, InterruptedIOException {
    final InputStream im =
        new InputStream() {
          @Override
          public int read() throws IOException {
            return -1;
          }
        };

    PutObjectRequest putObjectRequest = newPutObjectRequest(objectName, newObjectMetadata(0L), im);
    Future upload = putObject(putObjectRequest);
    try {
      upload.get();
      incrementPutCompletedStatistics(true, 0);
    } catch (InterruptedException e) {
      incrementPutCompletedStatistics(false, 0);
      throw new InterruptedIOException("Interrupted creating " + objectName);
    } catch (ExecutionException e) {
      incrementPutCompletedStatistics(false, 0);
      if (e.getCause() instanceof ObsException) {
        throw (ObsException) e.getCause();
      }
      throw new ObsException("obs exception: ", e.getCause());
    }
    incrementPutProgressStatistics(objectName, 0);
  }

  // Used to create a folder
  private void fsCreateFolder(final String objectName) throws ObsException, InterruptedIOException {
    try {
      final NewFolderRequest newFolderRequest = new NewFolderRequest(bucket, objectName);
      newFolderRequest.setAcl(cannedACL);
      long len = newFolderRequest.getObjectKey().length();
      incrementPutStartStatistics(len);
      ListenableFuture<ObsFSFolder> future = null;
      try {
        future =
            boundedThreadPool.submit(
                new Callable<ObsFSFolder>() {
                  @Override
                  public ObsFSFolder call() throws ObsException {
                    return obs.newFolder(newFolderRequest);
                  }
                });
      } catch (ObsException e) {
        throw e;
      }
      future.get();
      incrementPutCompletedStatistics(true, 0);
    } catch (InterruptedException e) {
      incrementPutCompletedStatistics(false, 0);
      throw new InterruptedIOException("Create Folder has Interrupted creating " + objectName);
    } catch (ExecutionException e) {
      incrementPutCompletedStatistics(false, 0);
      if (e.getCause() instanceof ObsException) {
        throw (ObsException) e.getCause();
      }
      throw new ObsException("Create Folder has obs exception: ", e.getCause());
    }
  }

  /**
   * Creates a copy of the passed {@link ObjectMetadata}. Does so without using the {@link
   * ObjectMetadata#clone()} method, to avoid copying unnecessary headers.
   *
   * @param source the {@link ObjectMetadata} to copy
   * @return a copy of {@link ObjectMetadata} with only relevant attributes
   */
  private ObjectMetadata cloneObjectMetadata(ObjectMetadata source) {
    // This approach may be too brittle, especially if
    // in future there are new attributes added to ObjectMetadata
    // that we do not explicitly call to set here
    ObjectMetadata ret = newObjectMetadata(source.getContentLength());

    if (source.getContentEncoding() != null) {
      ret.setContentEncoding(source.getContentEncoding());
    }
    return ret;
  }

  /**
   * Return the number of bytes that large input files should be optimally be split into to minimize
   * I/O time.
   *
   * @deprecated use {@link #getDefaultBlockSize(Path)} instead
   */
  @Deprecated
  public long getDefaultBlockSize() {
    return getConf().getLongBytes(FS_OBS_BLOCK_SIZE, DEFAULT_BLOCKSIZE);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("OBSFileSystem{");
    sb.append("uri=").append(uri);
    sb.append(", workingDir=").append(workingDir);
    sb.append(", inputPolicy=").append(inputPolicy);
    sb.append(", partSize=").append(partSize);
    sb.append(", enableMultiObjectsDelete=").append(enableMultiObjectsDelete);
    sb.append(", maxKeys=").append(maxKeys);
    if (cannedACL != null) {
      sb.append(", cannedACL=").append(cannedACL.toString());
    }
    sb.append(", readAhead=").append(readAhead);
    sb.append(", blockSize=").append(getDefaultBlockSize());
    sb.append(", multiPartThreshold=").append(multiPartThreshold);
    if (serverSideEncryptionAlgorithm != null) {
      sb.append(", serverSideEncryptionAlgorithm='")
          .append(serverSideEncryptionAlgorithm)
          .append('\'');
    }
    if (blockFactory != null) {
      sb.append(", blockFactory=").append(blockFactory);
    }
    sb.append(", boundedExecutor=").append(boundedThreadPool);
    sb.append(", unboundedExecutor=").append(unboundedThreadPool);
    sb.append(", statistics {").append(statistics).append("}");
    sb.append(", metrics {").append("}");
    sb.append('}');
    return sb.toString();
  }

  /**
   * Get the maximum key count.
   *
   * @return a value, valid after initialization
   */
  int getMaxKeys() {
    return maxKeys;
  }

  /**
   * {@inheritDoc}.
   *
   * <p>This implementation is optimized for OBS, which can do a bulk listing off all entries under
   * a path in one single operation. Thus there is no need to recursively walk the directory tree.
   *
   * <p>Instead a {@link ListObjectsRequest} is created requesting a (windowed) listing of all
   * entries under the given path. This is used to construct an {@code ObjectListingIterator}
   * instance, iteratively returning the sequence of lists of elements under the path. This is then
   * iterated over in a {@code FileStatusListingIterator}, which generates {@link OBSFileStatus}
   * instances, one per listing entry. These are then translated into {@link LocatedFileStatus}
   * instances.
   *
   * <p>This is essentially a nested and wrapped set of iterators, with some generator classes; an
   * architecture which may become less convoluted using lambda-expressions.
   *
   * @param f a path
   * @param recursive if the subdirectories need to be traversed recursively
   * @return an iterator that traverses statuses of the files/directories in the given path
   * @throws FileNotFoundException if {@code path} does not exist
   * @throws IOException if any I/O error occurred
   */
  @Override
  public RemoteIterator<LocatedFileStatus> listFiles(Path f, boolean recursive)
      throws FileNotFoundException, IOException {
    Path path = qualify(f);
    LOG.debug("listFiles({}, {})", path, recursive);
    try {
      // lookup dir triggers existence check
      final FileStatus fileStatus = getFileStatus(path);
      if (fileStatus.isFile()) {
        // simple case: File
        LOG.debug("Path is a file");
        return new Listing.SingleStatusRemoteIterator(toLocatedFileStatus(fileStatus));
      } else {
        LOG.debug("listFiles: doing listFiles of directory {} - recursive {}", path, recursive);
        // directory: do a bulk operation
        String key = maybeAddTrailingSlash(pathToKey(path));
        String delimiter = recursive ? null : "/";
        LOG.debug("Requesting all entries under {} with delimiter '{}'", key, delimiter);
        return listing.createLocatedFileStatusIterator(
            listing.createFileStatusListingIterator(
                path,
                createListObjectsRequest(key, delimiter),
                ACCEPT_ALL,
                new Listing.AcceptFilesOnly(path)));
      }
    } catch (ObsException e) {
      throw translateException("listFiles", path, e);
    }
  }

  /** Override superclass so as to add statistic collection. {@inheritDoc} */
  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f) throws IOException {
    return listLocatedStatus(f, ACCEPT_ALL);
  }

  /**
   * {@inheritDoc}.
   *
   * <p>OBS Optimized directory listing. The initial operation performs the first bulk listing;
   * extra listings will take place when all the current set of results are used up.
   *
   * @param f a path
   * @param filter a path filter
   * @return an iterator that traverses statuses of the files/directories in the given path
   * @throws FileNotFoundException if {@code path} does not exist
   * @throws IOException if any I/O error occurred
   */
  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(final Path f, final PathFilter filter)
      throws FileNotFoundException, IOException {
    Path path = qualify(f);
    LOG.debug("listLocatedStatus({}, {}", path, filter);
    try {
      // lookup dir triggers existence check
      final FileStatus fileStatus = getFileStatus(path);
      if (fileStatus.isFile()) {
        // simple case: File
        LOG.debug("Path is a file");
        return new Listing.SingleStatusRemoteIterator(
            filter.accept(path) ? toLocatedFileStatus(fileStatus) : null);
      } else {
        // directory: trigger a lookup
        String key = maybeAddTrailingSlash(pathToKey(path));
        return listing.createLocatedFileStatusIterator(
            listing.createFileStatusListingIterator(
                path,
                createListObjectsRequest(key, "/"),
                filter,
                new Listing.AcceptAllButSelfAndS3nDirs(path)));
      }
    } catch (ObsException e) {
      throw translateException("listLocatedStatus", path, e);
    }
  }

  /**
   * Build a {@link LocatedFileStatus} from a {@link FileStatus} instance.
   *
   * @param status file status
   * @return a located status with block locations set up from this FS.
   * @throws IOException IO Problems.
   */
  LocatedFileStatus toLocatedFileStatus(FileStatus status) throws IOException {
    return new LocatedFileStatus(
        status, status.isFile() ? getFileBlockLocations(status, 0, status.getLen()) : null);
  }

  /**
   * Append object
   *
   * @param appendObjectRequest append object request
   */
  public void appendObject(AppendObjectRequest appendObjectRequest) {
    long len = 0;
    if (appendObjectRequest.getFile() != null) {
      len = appendObjectRequest.getFile().length();
    }

    incrementPutStartStatistics(len);

    try {
      LOG.debug(
          "Append object, key {} position {} size {}",
          appendObjectRequest.getObjectKey(),
          appendObjectRequest.getPosition(),
          len);
      obs.appendObject(appendObjectRequest);
    } catch (ObsException e) {
      throw e;
    }
  }

  /**
   * Create a appendFile request. Adds the ACL and metadata
   *
   * @param key key of object
   * @param tmpFile temp file or input stream
   * @return the request
   * @throws IOException any problem
   */
  WriteFileRequest newAppendFileRequest(String key, Object tmpFile) throws IOException {
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(tmpFile);
    ObsFSAttribute obsFsAttribute = null;
    try {
      GetAttributeRequest getAttributeReq = new GetAttributeRequest(bucket, key);
      obsFsAttribute = obs.getAttribute(getAttributeReq);
    } catch (ObsException e) {
      throw translateException("GetAttributeRequest", key, e);
    }

    WriteFileRequest writeFileReq =
        tmpFile instanceof File
            ? newWriteFileRequestByFile(key, (File) tmpFile, obsFsAttribute.getContentLength())
            : newWriteFileRequestByInputStream(
                key, (InputStream) tmpFile, obsFsAttribute.getContentLength());
    writeFileReq.setAcl(cannedACL);
    return writeFileReq;
  }

  /**
   * create fs append request by File
   *
   * @param key object key
   * @param file file implement
   * @param position append position
   * @return posix file append request
   */
  private WriteFileRequest newWriteFileRequestByFile(String key, File file, long position) {
    return new WriteFileRequest(bucket, key, file, position);
  }
  /**
   * create fs append request by InputStream
   *
   * @param key object key
   * @param ins input stream
   * @param position append position
   * @return posix file append request
   */
  private WriteFileRequest newWriteFileRequestByInputStream(
      String key, InputStream ins, long position) {
    return new WriteFileRequest(bucket, key, ins, position);
  }

  /**
   * Append File
   *
   * @param appendFileRequest append object request
   */
  void appendFile(WriteFileRequest appendFileRequest) throws IOException {
    long len = 0;
    if (appendFileRequest.getFile() != null) {
      len = appendFileRequest.getFile().length();
    }

    incrementPutStartStatistics(len);

    try {
      LOG.debug(
          "Append file, key {} position {} size {}",
          appendFileRequest.getObjectKey(),
          appendFileRequest.getPosition(),
          len);
      obs.writeFile(appendFileRequest);
    } catch (ObsException e) {
      throw translateException("AppendFile", appendFileRequest.getObjectKey(), e);
    }
  }

  @Override
  protected void finalize() throws Throwable {
    LOG.info("This Filesystem GC-ful, clear resource.");
    shutdownAll(
            LOG,
            boundedThreadPool,
            boundedCopyThreadPool,
            boundedDeleteThreadPool,
            unboundedReadThreadPool,
            unboundedThreadPool,
            boundedCopyPartThreadPool);
  }

  public SseWrapper getSse() {
    return sse;
  }
}
