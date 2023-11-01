package org.apache.hadoop.fs.obs;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;
import com.obs.services.model.AbortMultipartUploadRequest;
import com.obs.services.model.DeleteObjectsRequest;
import com.obs.services.model.DeleteObjectsResult;
import com.obs.services.model.GetObjectMetadataRequest;
import com.obs.services.model.KeyAndVersion;
import com.obs.services.model.ListMultipartUploadsRequest;
import com.obs.services.model.ListObjectsRequest;
import com.obs.services.model.MultipartUpload;
import com.obs.services.model.MultipartUploadListing;
import com.obs.services.model.ObjectListing;
import com.obs.services.model.ObjectMetadata;
import com.obs.services.model.ObsObject;
import com.obs.services.model.PutObjectRequest;
import com.obs.services.model.PutObjectResult;
import com.obs.services.model.UploadPartRequest;
import com.obs.services.model.UploadPartResult;
import com.obs.services.model.fs.FSStatusEnum;
import com.obs.services.model.fs.GetBucketFSStatusRequest;
import com.obs.services.model.fs.WriteFileRequest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.InvalidRequestException;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.obs.security.ObsDelegationTokenManger;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.ProviderUtils;
import org.apache.hadoop.util.DiskChecker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

/**
 * Common utils
 */
//CHECKSTYLE:OFF
public final class OBSCommonUtils {
    //CHECKSTYLE:ON

    /**
     * Class logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(OBSCommonUtils.class);

    /**
     * Moved permanently response code.
     */
    static final int OTHER_CODE = -1;

    /**
     * IllegalArgument response code.
     */
    static final int IllEGALARGUMENT_CODE = 400;

    /**
     * Unauthorized response code.
     */
    static final int UNAUTHORIZED_CODE = 401;

    /**
     * Forbidden response code.
     */
    static final int FORBIDDEN_CODE = 403;

    /**
     * Not found response code.
     */
    static final int NOT_FOUND_CODE = 404;

    /**
     * Method not allowed
     */
    static final int NOT_ALLOWED_CODE = 405;

    /**
     * File conflict.
     */
    static final int CONFLICT_CODE = 409;

    /**
     * Gone response code.
     */
    static final int GONE_CODE = 410;

    /**
     * EOF response code.
     */
    static final int EOF_CODE = 416;

    /**
     * error response code.
     */
    static final int ERROR_CODE = 503;

    /**
     * qos error code
     */
    public static final String DETAIL_QOS_CODE = "GetQosTokenException";

    /**
     * qos indicator code
     */
    static final String DETAIL_QOS_INDICATOR_601 = "601";

    static final String DETAIL_QOS_INDICATOR_602 = "602";

    static OBSInvoker obsInvoker;


    /**
     * Core property for provider path. Duplicated here for consistent code
     * across Hadoop version: {@value}.
     */
    static final String CREDENTIAL_PROVIDER_PATH = "hadoop.security.credential.provider.path";

    static long retryMaxTime = OBSConstants.DEFAULT_RETRY_MAXTIME;

    static long retrySleepBaseTime = OBSConstants.DEFAULT_RETRY_SLEEP_BASETIME;

    static long retrySleepMaxTime = OBSConstants.DEFAULT_RETRY_SLEEP_MAXTIME;

    /**
     * Variable base of power function.
     */
    static final int VARIABLE_BASE_OF_POWER_FUNCTION = 2;

    /**
     * Max number of listing keys for checking folder empty.
     */
    static final int MAX_KEYS_FOR_CHECK_FOLDER_EMPTY = 3;

    /**
     * Max number of listing keys for checking folder empty.
     */
    static final int BYTE_TO_INT_MASK = 0xFF;

    private OBSCommonUtils() {
    }


    public static void init(OBSFileSystem fs, Configuration conf) {
        obsInvoker = new OBSInvoker(fs, new OBSRetryPolicy(conf), OBSInvoker.LOG_EVENT);
        OBSPosixBucketUtils.init(conf);
    }

    public static ObsDelegationTokenManger initDelegationTokenManger(OBSFileSystem fs, URI uri, Configuration conf)
        throws IOException {
        ObsDelegationTokenManger obsDelegationTokenManger = null;
        if (ObsDelegationTokenManger.hasDelegationTokenProviders(conf)) {
            LOG.debug("Initializing ObsDelegationTokenManager for {}", uri);
            obsDelegationTokenManger = new ObsDelegationTokenManger();
            obsDelegationTokenManger.initialize(fs, uri, conf);
        }
        return obsDelegationTokenManger;
    }

    public static OBSInvoker getOBSInvoker() {
        return obsInvoker;
    }

    /**
     * Set the max time in millisecond to retry on error.
     *
     * @param retryMaxTime max time in millisecond to set for retry
     */
    static void setRetryTime(long retryMaxTime, long retrySleepBaseTime, long retrySleepMaxTime) {
        if (retryMaxTime <= 0) {
            LOG.warn("Invalid time[{}] to set for retry on error.", retryMaxTime);
            retryMaxTime = OBSConstants.DEFAULT_RETRY_MAXTIME;
        }
        OBSCommonUtils.retryMaxTime = retryMaxTime;
        OBSCommonUtils.retrySleepBaseTime = retrySleepBaseTime;
        OBSCommonUtils.retrySleepMaxTime = retrySleepMaxTime;
    }

    /**
     * Get the fs status of the bucket.
     *
     * @param obs        OBS client instance
     * @param bucketName bucket name
     * @return boolean value indicating if this bucket is a posix bucket
     * @throws FileNotFoundException the bucket is absent
     * @throws IOException           any other problem talking to OBS
     */
    static boolean getBucketFsStatus(final ObsClient obs, final String bucketName)
        throws FileNotFoundException, IOException {
        GetBucketFSStatusRequest request = new GetBucketFSStatusRequest(bucketName);
        FSStatusEnum fsStatus;
        try {
            fsStatus = OBSCommonUtils.getOBSInvoker().retryByMaxTime(OBSOperateAction.getBucketFsStatus,
                request.getBucketName(), () -> {
                    return obs.getBucketFSStatus(request).getStatus();
                },true);
        } catch (FileNotFoundException e) {
            throw new FileNotFoundException("Bucket " + bucketName + " does not exist");
        }
        return FSStatusEnum.ENABLED == fsStatus;
    }

    /**
     * Whether the pathname is valid.  Currently prohibits relative paths, names
     * which contain a ":" or "//", or other non-canonical paths.
     *
     * @param src the path to prohibit
     * @return boolean value indicating whether the path is valid
     */
    static boolean isValidName(final String src) {
        // Path must be absolute.
        if (!src.startsWith(Path.SEPARATOR)) {
            return false;
        }

        // Check for ".." "." ":" "/"
        String[] components = org.apache.hadoop.util.StringUtils.split(src, '/');
        for (int i = 0; i < components.length; i++) {
            String element = components[i];
            if (element.equals(".") || element.contains(":") || element.contains("/")) {
                return false;
            }
            // ".." is allowed in path starting with /.reserved/.inodes
            if (element.equals("..")) {
                if (components.length > 4 && components[1].equals(".reserved") && components[2].equals(".inodes")) {
                    continue;
                }
                return false;
            }
            // The string may start or end with a /, but not have
            // "//" in the middle.
            if (element.isEmpty() && i != components.length - 1 && i != 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * @param flags
     * @throws UnsupportedOperationException
     */
    static void checkCreateFlag(final EnumSet<CreateFlag> flags) throws UnsupportedOperationException {
        if (flags == null) {
            return;
        }

        StringBuilder unsupportedFlags = new StringBuilder();
        boolean hasUnSupportedFlag = false;
        for (CreateFlag flag : flags) {
            if (flag != CreateFlag.CREATE && flag != CreateFlag.APPEND && flag != CreateFlag.OVERWRITE
                && flag != CreateFlag.SYNC_BLOCK) {
                unsupportedFlags.append(flag).append(",");
                hasUnSupportedFlag = true;
            }
        }

        if (hasUnSupportedFlag) {
            throw new UnsupportedOperationException(
                "create with unsupported flags: " + unsupportedFlags.substring(0, unsupportedFlags.lastIndexOf(",")));
        }
    }

    /**
     * Turns a path (relative or otherwise) into an OBS key.
     *
     * @param owner the owner OBSFileSystem instance
     * @param path  input path, may be relative to the working dir
     * @return a key excluding the leading "/", or, if it is the root path, ""
     */
    static String pathToKey(final OBSFileSystem owner, final Path path) {
        Path absolutePath = path;
        if (!path.isAbsolute()) {
            absolutePath = new Path(owner.getWorkingDirectory(), path);
        }

        if (absolutePath.toUri().getScheme() != null && absolutePath.toUri().getPath().isEmpty()) {
            return "";
        }

        return absolutePath.toUri().getPath().substring(1);
    }

    /**
     * Turns a path (relative or otherwise) into an OBS key, adding a trailing
     * "/" if the path is not the root <i>and</i> does not already have a "/" at
     * the end.
     *
     * @param key obs key or ""
     * @return the with a trailing "/", or, if it is the root key, "",
     */
    static String maybeAddTrailingSlash(final String key) {
        if (!isStringEmpty(key) && !key.endsWith("/")) {
            return key + '/';
        } else {
            return key;
        }
    }

    /**
     * Convert a key back to a Path.
     *
     * @param key input key
     * @return the path from this key
     */
    static Path keyToPath(final String key) {
        return new Path("/" + key);
    }

    /**
     * Convert a key to a fully qualified path.
     *
     * @param owner the owner OBSFileSystem instance
     * @param key   input key
     * @return the fully qualified path including URI scheme and bucket name.
     */
    static Path keyToQualifiedPath(final OBSFileSystem owner, final String key) {
        return qualify(owner, keyToPath(key));
    }

    /**
     * Qualify a path.
     *
     * @param owner the owner OBSFileSystem instance
     * @param path  path to qualify
     * @return a qualified path.
     */
    static Path qualify(final OBSFileSystem owner, final Path path) {
        return path.makeQualified(owner.getUri(), owner.getWorkingDirectory());
    }

    /**
     * Delete obs key started '/'.
     *
     * @param key object key
     * @return new key
     */
    static String maybeDeleteBeginningSlash(final String key) {
        return !isStringEmpty(key) && key.startsWith("/") ? key.substring(1) : key;
    }

    /**
     * Add obs key started '/'.
     *
     * @param key object key
     * @return new key
     */
    static String maybeAddBeginningSlash(final String key) {
        return !isStringEmpty(key) && !key.startsWith("/") ? "/" + key : key;
    }

    /**
     * Translate an exception raised in an operation into an IOException. HTTP
     * error codes are examined and can be used to build a more specific
     * response.
     *
     * @param operation operation
     * @param path      path operated on (may be null)
     * @param exception obs exception raised
     * @return an IOE which wraps the caught exception.
     */
    public static IOException translateException(final String operation, final String path,
        final ObsException exception) {
        String headerErrorCode = null;
        Map<String, String> responseHeaders = exception.getResponseHeaders();
        if (responseHeaders != null) {
            for (Map.Entry<String, String> entry : responseHeaders.entrySet()) {
                if (entry.getKey().equals("error-code")) {
                    headerErrorCode = entry.getValue();
                    break;
                }
            }
        }
        String indicatorCode = exception.getErrorIndicator();
        String bodyErrorCode = exception.getErrorCode();
        String errorCode = bodyErrorCode != null ? bodyErrorCode :
            headerErrorCode != null ? headerErrorCode : indicatorCode;

        String message = String.format(Locale.ROOT,"%s%s: ResponseCode[%d],ErrorCode[%s],ErrorMessage[%s],RequestId[%s]",
            operation, path == null || path.length() == 0 ?  "" : " on " + path, exception.getResponseCode(),
            errorCode, exception.getErrorMessage(),exception.getErrorRequestId());

        IOException ioe;

        int status = exception.getResponseCode();
        switch (status) {
            case IllEGALARGUMENT_CODE:
                OBSIllegalArgumentException illegalArgumentException = new OBSIllegalArgumentException(message);
                illegalArgumentException.initCause(exception);
                illegalArgumentException.setErrCode(errorCode);
                ioe = illegalArgumentException;
                break;
            case UNAUTHORIZED_CODE:
            case FORBIDDEN_CODE:
                ioe = new AccessControlException(message);
                ioe.initCause(exception);
                break;
            case NOT_FOUND_CODE:
            case GONE_CODE:
                ioe = new FileNotFoundException(message);
                ioe.initCause(exception);
                break;
            case NOT_ALLOWED_CODE:
                OBSMethodNotAllowedException methodNotAllowedException = new OBSMethodNotAllowedException(message);
                methodNotAllowedException.initCause(exception);
                methodNotAllowedException.setErrCode(errorCode);
                ioe = methodNotAllowedException;
                break;
            case CONFLICT_CODE:
                OBSFileConflictException fileConflictException = new OBSFileConflictException(message);
                fileConflictException.initCause(exception);
                fileConflictException.setErrCode(errorCode);
                ioe = fileConflictException;
                break;
            case EOF_CODE:
                ioe = new EOFException(message);
                ioe.initCause(exception);
                break;
            default:
                if (ERROR_CODE == status && (
                    DETAIL_QOS_CODE.equals(errorCode) ||
                        DETAIL_QOS_INDICATOR_601.equals(errorCode) ||
                        DETAIL_QOS_INDICATOR_602.equals(errorCode))) {
                    OBSQosException qosException = new OBSQosException(message, exception);
                    qosException.setErrCode(errorCode);
                    ioe = qosException;
                } else {
                    OBSIOException ioException = new OBSIOException(message, exception);
                    ioException.setErrCode(errorCode);
                    ioe = ioException;
                }
                break;
        }
        return ioe;
    }

    /**
     * Reject any request to delete an object where the key is root.
     *
     * @param bucket bucket name
     * @param key    key to validate
     * @throws InvalidRequestException if the request was rejected due to a
     *                                 mistaken attempt to delete the root
     *                                 directory.
     */
    static void blockRootDelete(final String bucket, final String key) throws InvalidRequestException {
        if (key.isEmpty() || "/".equals(key)) {
            throw new InvalidRequestException("Bucket " + bucket + " cannot be deleted");
        }
    }

    /**
     * Delete an object. Increments the {@code OBJECT_DELETE_REQUESTS} and write
     * operation statistics.
     *
     * @param owner the owner OBSFileSystem instance
     * @param key   key to blob to delete.
     * @throws IOException on any failure to delete object
     */
    static void deleteObject(final OBSFileSystem owner, final String key) throws IOException {
        blockRootDelete(owner.getBucket(), key);
        OBSCommonUtils.getOBSInvoker().retryByMaxTime(OBSOperateAction.delete, key, () -> {
            owner.getObsClient().deleteObject(owner.getBucket(), key);
            return null;
        }, true);
        owner.getSchemeStatistics().incrementWriteOps(1);
    }

    /**
     * Perform a bulk object delete operation. Increments the {@code
     * OBJECT_DELETE_REQUESTS} and write operation statistics.
     *
     * @param owner         the owner OBSFileSystem instance
     * @param deleteRequest keys to delete on the obs-backend
     * @throws IOException on any failure to delete objects
     */
    static void deleteObjects(final OBSFileSystem owner, final DeleteObjectsRequest deleteRequest) throws IOException {
        DeleteObjectsResult result;
        deleteRequest.setQuiet(true);
        try {
            result = owner.getObsClient().deleteObjects(deleteRequest);
            owner.getSchemeStatistics().incrementWriteOps(1);
        } catch (ObsException e) {
            LOG.warn("bulk delete objects failed: request [{}], response code [{}], error code [{}], "
                    + "error message [{}], request id [{}]", deleteRequest, e.getResponseCode(), e.getErrorCode(),
                e.getErrorMessage(), e.getErrorRequestId());
            for (KeyAndVersion keyAndVersion : deleteRequest.getKeyAndVersionsList()) {
                deleteObject(owner, keyAndVersion.getKey());
            }
            return;
        }

        // delete one by one if there is errors
        if (result != null) {
            List<DeleteObjectsResult.ErrorResult> errorResults = result.getErrorResults();
            if (!errorResults.isEmpty()) {
                LOG.warn("bulk delete {} objects: {} failed, request id [{}].begin to delete one by one.detail info "
                        + "example:key[{}],error code [{}],error message [{}]",
                    deleteRequest.getKeyAndVersionsList().size(), errorResults.size(), result.getRequestId(),
                    errorResults.get(0).getObjectKey(), errorResults.get(0).getErrorCode(),
                    errorResults.get(0).getMessage());
                for (DeleteObjectsResult.ErrorResult errorResult : errorResults) {
                    deleteObject(owner, errorResult.getObjectKey());
                }
            }
        }
    }

    /**
     * Create a putObject request. Adds the ACL and metadata
     *
     * @param owner    the owner OBSFileSystem instance
     * @param key      key of object
     * @param metadata metadata header
     * @param srcfile  source file
     * @return the request
     */
    static PutObjectRequest newPutObjectRequest(final OBSFileSystem owner, final String key,
        final ObjectMetadata metadata, final File srcfile) throws FileNotFoundException {
        Preconditions.checkNotNull(srcfile);
        PutObjectRequest putObjectRequest = new PutObjectRequest(owner.getBucket(), key);
        putObjectRequest.setInput(new FileInputStream(srcfile));
        putObjectRequest.setAutoClose(false);
        putObjectRequest.setAcl(owner.getCannedACL());
        putObjectRequest.setMetadata(metadata);
        if (owner.getSse().isSseCEnable()) {
            putObjectRequest.setSseCHeader(owner.getSse().getSseCHeader());
        } else if (owner.getSse().isSseKmsEnable()) {
            putObjectRequest.setSseKmsHeader(owner.getSse().getSseKmsHeader());
        }
        return putObjectRequest;
    }

    /**
     * Create a {@link PutObjectRequest} request. The metadata is assumed to
     * have been configured with the size of the operation.
     *
     * @param owner       the owner OBSFileSystem instance
     * @param key         key of object
     * @param metadata    metadata header
     * @param inputStream source data.
     * @return the request
     */
    static PutObjectRequest newPutObjectRequest(final OBSFileSystem owner, final String key,
        final ObjectMetadata metadata, final InputStream inputStream) {
        Preconditions.checkNotNull(inputStream);
        PutObjectRequest putObjectRequest = new PutObjectRequest(owner.getBucket(), key, inputStream);
        putObjectRequest.setAcl(owner.getCannedACL());
        putObjectRequest.setMetadata(metadata);
        if (owner.getSse().isSseCEnable()) {
            putObjectRequest.setSseCHeader(owner.getSse().getSseCHeader());
        } else if (owner.getSse().isSseKmsEnable()) {
            putObjectRequest.setSseKmsHeader(owner.getSse().getSseKmsHeader());
        }
        return putObjectRequest;
    }

    /**
     * PUT an object directly (i.e. not via the transfer manager). Byte length
     * is calculated from the file length, or, if there is no file, from the
     * content length of the header. <i>Important: this call will close any
     * input stream in the request.</i>
     *
     * @param owner            the owner OBSFileSystem instance
     * @param putObjectRequest the request
     * @return the upload initiated
     * @throws ObsException on problems
     */
    static PutObjectResult putObjectDirect(final OBSFileSystem owner, final PutObjectRequest putObjectRequest)
        throws IOException {
        PutObjectResult result = OBSCommonUtils.getOBSInvoker().retryByMaxTime(OBSOperateAction.putObject,
            putObjectRequest.getObjectKey(), () -> {
            if (putObjectRequest.getInput() instanceof FileInputStream) {
                ((FileInputStream) putObjectRequest.getInput()).getChannel().position(0);
            }
            return owner.getObsClient().putObject(putObjectRequest);
        },true);

        long len = putObjectRequest.getFile() != null ? putObjectRequest.getFile().length() :
            putObjectRequest.getMetadata().getContentLength();
        owner.getSchemeStatistics().incrementWriteOps(1);
        owner.getSchemeStatistics().incrementBytesWritten(len);
        return result;
    }

    /**
     * Upload part of a multi-partition file. Increments the write and put
     * counters. <i>Important: this call does not close any input stream in the
     * request.</i>
     *
     * @param owner   the owner OBSFileSystem instance
     * @param request request
     * @return the result of the operation.
     * @throws ObsException on problems
     */
    static UploadPartResult uploadPart(final OBSFileSystem owner, final UploadPartRequest request) throws IOException {
        UploadPartResult uploadPartResult  = OBSCommonUtils.getOBSInvoker().retryByMaxTime(OBSOperateAction.uploadPart,
            request.getObjectKey(), () -> {
            if (request.getInput() instanceof FileInputStream) {
                ((FileInputStream) request.getInput()).getChannel().position(0);
            }
            return owner.getObsClient().uploadPart(request);
        },true);
        owner.getSchemeStatistics().incrementWriteOps(1);
        owner.getSchemeStatistics().incrementBytesWritten(request.getPartSize());
        return uploadPartResult;
    }

    static void removeKeys(final OBSFileSystem owner, final List<KeyAndVersion> keysToDelete, final boolean clearKeys,
        final boolean checkRootDelete) throws IOException {
        if (keysToDelete.isEmpty()) {
            // exit fast if there are no keys to delete
            return;
        }

        if (checkRootDelete) {
            for (KeyAndVersion keyVersion : keysToDelete) {
                blockRootDelete(owner.getBucket(), keyVersion.getKey());
            }
        }

        if (!owner.isEnableMultiObjectDelete() || keysToDelete.size() < owner.getMultiDeleteThreshold()) {
            // delete one by one.
            for (KeyAndVersion keyVersion : keysToDelete) {
                deleteObject(owner, keyVersion.getKey());
            }
        } else if (keysToDelete.size() <= owner.getMaxEntriesToDelete()) {
            // Only one batch.
            DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(owner.getBucket());
            deleteObjectsRequest.setKeyAndVersions(keysToDelete.toArray(new KeyAndVersion[0]));
            deleteObjects(owner, deleteObjectsRequest);
        } else {
            // Multi batches.
            List<KeyAndVersion> keys = new ArrayList<>(owner.getMaxEntriesToDelete());
            for (KeyAndVersion key : keysToDelete) {
                keys.add(key);
                if (keys.size() == owner.getMaxEntriesToDelete()) {
                    // Delete one batch.
                    removeKeys(owner, keys, true, false);
                }
            }
            // Delete the last batch
            removeKeys(owner, keys, true, false);
        }

        if (clearKeys) {
            keysToDelete.clear();
        }
    }

    /**
     * Translate an exception raised in an operation into an IOException. The
     * specific type of IOException depends on the class of {@link ObsException}
     * passed in, and any status codes included in the operation. That is: HTTP
     * error codes are examined and can be used to build a more specific
     * response.
     *
     * @param operation operation
     * @param path      path operated on (must not be null)
     * @param exception obs exception raised
     * @return an IOE which wraps the caught exception.
     */
    static IOException translateException(final String operation, final Path path, final ObsException exception) {
        return translateException(operation, path.toString(), exception);
    }

    /**
     * List the statuses of the files/directories in the given path if the path
     * is a directory.
     *
     * @param owner     the owner OBSFileSystem instance
     * @param f         given path
     * @param recursive flag indicating if list is recursive
     * @return the statuses of the files/directories in the given patch
     * @throws FileNotFoundException when the path does not exist;
     * @throws IOException           due to an IO problem.
     * @throws ObsException          on failures inside the OBS SDK
     */
    static FileStatus[] listStatus(final OBSFileSystem owner, final Path f, final boolean recursive)
        throws FileNotFoundException, IOException, ObsException {
        Path path = qualify(owner, f);
        String key = pathToKey(owner, path);

        List<FileStatus> result;
        final FileStatus fileStatus;
        try {
            fileStatus = getFileStatusWithRetry(owner, path);
        } catch (OBSFileConflictException e) {
            throw new AccessControlException(e);
        }

        if (fileStatus.isDirectory()) {
            key = maybeAddTrailingSlash(key);
            String delimiter = recursive ? null : "/";
            ListObjectsRequest request = createListObjectsRequest(owner, key, delimiter);
            LOG.debug("listStatus: doing listObjects for directory {} - recursive {}", f, recursive);

            OBSListing.FileStatusListingIterator files = owner.getObsListing()
                .createFileStatusListingIterator(path, request, OBSListing.ACCEPT_ALL,
                    new OBSListing.AcceptAllButSelfAndOBSDirs(path));
            result = new ArrayList<>(files.getBatchSize());
            while (files.hasNext()) {
                result.add(files.next());
            }

            return result.toArray(new FileStatus[0]);
        } else {
            LOG.debug("Adding: rd (not a dir): {}", path);
            FileStatus[] stats = new FileStatus[1];
            stats[0] = fileStatus;
            return stats;
        }
    }

    /**
     * Create a {@code ListObjectsRequest} request against this bucket.
     *
     * @param owner     the owner OBSFileSystem instance
     * @param key       key for request
     * @param delimiter any delimiter
     * @return the request
     */
    static ListObjectsRequest createListObjectsRequest(final OBSFileSystem owner, final String key,
        final String delimiter) {
        return createListObjectsRequest(owner, key, delimiter, -1);
    }

    static ListObjectsRequest createListObjectsRequest(final OBSFileSystem owner, final String key,
        final String delimiter, final int maxKeyNum) {
        ListObjectsRequest request = new ListObjectsRequest();
        request.setBucketName(owner.getBucket());
        if (maxKeyNum > 0 && maxKeyNum < owner.getMaxKeys()) {
            request.setMaxKeys(maxKeyNum);
        } else {
            request.setMaxKeys(owner.getMaxKeys());
        }
        request.setPrefix(key);
        if (delimiter != null) {
            request.setDelimiter(delimiter);
        }
        return request;
    }

    /**
     * Implements the specific logic to reject root directory deletion. The
     * caller must return the result of this call, rather than attempt to
     * continue with the delete operation: deleting root directories is never
     * allowed. This method simply implements the policy of when to return an
     * exit code versus raise an exception.
     *
     * @param bucket     bucket name
     * @param isEmptyDir flag indicating if the directory is empty
     * @param recursive  recursive flag from command
     * @return a return code for the operation
     * @throws PathIsNotEmptyDirectoryException if the operation was explicitly
     *                                          rejected.
     */
    static boolean rejectRootDirectoryDelete(final String bucket, final boolean isEmptyDir, final boolean recursive)
        throws PathIsNotEmptyDirectoryException {
        LOG.info("obs delete the {} root directory of {}", bucket, recursive);
        if (isEmptyDir) {
            return true;
        }
        if (recursive) {
            return false;
        } else {
            // reject
            throw new PathIsNotEmptyDirectoryException(bucket);
        }
    }

    /**
     * Make the given path and all non-existent parents into directories.
     *
     * @param owner the owner OBSFileSystem instance
     * @param path  path to create
     * @return true if a directory was created
     * @throws FileAlreadyExistsException there is a file at the path specified
     * @throws IOException                other IO problems
     * @throws ObsException               on failures inside the OBS SDK
     */
    static boolean mkdirs(final OBSFileSystem owner, final Path path)
        throws IOException, FileAlreadyExistsException, ObsException {
        LOG.debug("Making directory: {}", path);
        FileStatus fileStatus;
        try {
            fileStatus = getFileStatusWithRetry(owner, path);

            if (fileStatus.isDirectory()) {
                return true;
            } else {
                throw new FileAlreadyExistsException("Path is a file: " + path);
            }
        } catch (FileNotFoundException e) {
            String key = pathToKey(owner, path);
            if (owner.isFsBucket()) {
                try {
                    OBSPosixBucketUtils.fsCreateFolder(owner, key);
                } catch (OBSFileConflictException e1) {
                    throw new ParentNotDirectoryException(e1.getMessage());
                }
            } else {
                Path fPart = path.getParent();
                do {
                    try {
                        fileStatus = getFileStatusWithRetry(owner, fPart);
                        if (fileStatus.isDirectory()) {
                            break;
                        }
                        if (fileStatus.isFile()) {
                            throw new FileAlreadyExistsException(
                                String.format("Can't make directory for path '%s'" + " since it is a file.", fPart));
                        }
                    } catch (FileNotFoundException fnfe) {
                        LOG.debug("file {} not fount, but ignore.", path);
                    } catch (OBSFileConflictException fce) {
                        throw new ParentNotDirectoryException(fce.getMessage());
                    }
                    fPart = fPart.getParent();
                } while (fPart != null);

                OBSObjectBucketUtils.createFakeDirectory(owner, key);
            }
            return true;
        } catch (OBSFileConflictException e) {
            throw new ParentNotDirectoryException(e.getMessage());
        }
    }

    /**
     * Initiate a {@code listObjects} operation, incrementing metrics in the
     * process.
     *
     * @param owner   the owner OBSFileSystem instance
     * @param request request to initiate
     * @return the results
     * @throws IOException on any failure to list objects
     */
    static ObjectListing listObjects(final OBSFileSystem owner, final ListObjectsRequest request) throws IOException {
        if (request.getDelimiter() == null && request.getMarker() == null && owner.isFsBucket()
            && owner.isObsClientDFSListEnable()) {
            return OBSFsDFSListing.fsDFSListObjects(owner, request);
        }

        return commonListObjects(owner, request);
    }

    static ObjectListing commonListObjects(final OBSFileSystem owner, final ListObjectsRequest request)
        throws IOException {
        ObjectListing listObjects = OBSCommonUtils.getOBSInvoker().retryByMaxTime(OBSOperateAction.listObjects, request.getPrefix(), () -> {
            return owner.getObsClient().listObjects(request);
        },true);
        owner.getSchemeStatistics().incrementReadOps(1);
        return listObjects;
    }

    /**
     * List the next set of objects.
     *
     * @param owner   the owner OBSFileSystem instance
     * @param objects paged result
     * @return the next result object
     * @throws IOException on any failure to list the next set of objects
     */
    static ObjectListing continueListObjects(final OBSFileSystem owner, final ObjectListing objects)
        throws IOException {
        if (objects.getDelimiter() == null && owner.isFsBucket() && owner.isObsClientDFSListEnable()) {
            return OBSFsDFSListing.fsDFSContinueListObjects(owner, (OBSFsDFSListing) objects);
        }

        return commonContinueListObjects(owner, objects);
    }

    private static ObjectListing commonContinueListObjects(final OBSFileSystem owner, final ObjectListing objects)
        throws IOException {
        String delimiter = objects.getDelimiter();
        int maxKeyNum = objects.getMaxKeys();
        ListObjectsRequest request = new ListObjectsRequest();
        request.setMarker(objects.getNextMarker());
        request.setBucketName(owner.getBucket());
        request.setPrefix(objects.getPrefix());
        if (maxKeyNum > 0 && maxKeyNum < owner.getMaxKeys()) {
            request.setMaxKeys(maxKeyNum);
        } else {
            request.setMaxKeys(owner.getMaxKeys());
        }
        if (delimiter != null) {
            request.setDelimiter(delimiter);
        }
        return commonContinueListObjects(owner, request);
    }

    static ObjectListing commonContinueListObjects(final OBSFileSystem owner, final ListObjectsRequest request)
        throws IOException {
        ObjectListing listObjects = OBSCommonUtils.getOBSInvoker().retryByMaxTime(OBSOperateAction.listObjects, request.getPrefix(), () -> {
            return owner.getObsClient().listObjects(request);
        },true);
        owner.getSchemeStatistics().incrementReadOps(1);
        return listObjects;
    }

    /**
     * Predicate: does the object represent a directory?.
     *
     * @param name object name
     * @param size object size
     * @return true if it meets the criteria for being an object
     */
    public static boolean objectRepresentsDirectory(final String name, final long size) {
        return !name.isEmpty() && name.charAt(name.length() - 1) == '/' && size == 0L;
    }

    /**
     * Date to long conversion. Handles null Dates that can be returned by OBS
     * by returning 0
     *
     * @param date date from OBS query
     * @return timestamp of the object
     */
    public static long dateToLong(final Date date) {
        if (date == null) {
            return 0L;
        }

        return date.getTime() / OBSConstants.SEC2MILLISEC_FACTOR * OBSConstants.SEC2MILLISEC_FACTOR;
    }

    // Used to check if a folder is empty or not.
    static boolean isFolderEmpty(final OBSFileSystem owner, final String key) throws IOException {
        return OBSCommonUtils.getOBSInvoker().retryByMaxTime(OBSOperateAction.isFolderEmpty, key, () -> {
            return innerIsFolderEmpty(owner, key);
        },true);
    }

    // Used to check if a folder is empty or not by counting the number of
    // sub objects in list.
    private static boolean innerIsFolderEmptyDepth(final String key, final ObjectListing objects) {
        int count = objects.getObjects().size();
        if (count >= 2) {
            return false;
        } else if (count == 1 && !objects.getObjects().get(0).getObjectKey().equals(key)) {
            return false;
        }

        count = objects.getCommonPrefixes().size();
        if (count >= 2) {
            return false;
        } else {
            return count != 1 || objects.getCommonPrefixes().get(0).equals(key);
        }
    }

    // Used to check if a folder is empty or not.
    static boolean innerIsFolderEmpty(final OBSFileSystem owner, final String key)
        throws FileNotFoundException, ObsException {
        String obsKey = maybeAddTrailingSlash(key);
        ListObjectsRequest request = new ListObjectsRequest();
        request.setBucketName(owner.getBucket());
        request.setPrefix(obsKey);
        request.setDelimiter("/");
        request.setMaxKeys(MAX_KEYS_FOR_CHECK_FOLDER_EMPTY);
        owner.getSchemeStatistics().incrementReadOps(1);
        ObjectListing objects = owner.getObsClient().listObjects(request);

        if (!objects.getCommonPrefixes().isEmpty() || !objects.getObjects().isEmpty()) {
            if (innerIsFolderEmptyDepth(obsKey, objects)) {
                LOG.debug("Found empty directory {}", obsKey);
                return true;
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Found path as directory (with /): {}/{}", objects.getCommonPrefixes().size(),
                    objects.getObjects().size());

                for (ObsObject summary : objects.getObjects()) {
                    LOG.debug("Summary: {} {}", summary.getObjectKey(), summary.getMetadata().getContentLength());
                }
                for (String prefix : objects.getCommonPrefixes()) {
                    LOG.debug("Prefix: {}", prefix);
                }
            }
            LOG.debug("Found non-empty directory {}", obsKey);
            return false;
        } else if (obsKey.isEmpty()) {
            LOG.debug("Found root directory");
            return true;
        } else if (owner.isFsBucket()) {
            LOG.debug("Found empty directory {}", obsKey);
            return true;
        }

        LOG.debug("Not Found: {}", obsKey);
        throw new FileNotFoundException("No such file or directory: " + obsKey);
    }

    /**
     * Build a {@link LocatedFileStatus} from a {@link FileStatus} instance.
     *
     * @param owner  the owner OBSFileSystem instance
     * @param status file status
     * @return a located status with block locations set up from this FS.
     * @throws IOException IO Problems.
     */
    static LocatedFileStatus toLocatedFileStatus(final OBSFileSystem owner, final FileStatus status)
        throws IOException {
        return new LocatedFileStatus(status,
            status.isFile() ? owner.getFileBlockLocations(status, 0, status.getLen()) : null);
    }

    /**
     * Create a appendFile request. Adds the ACL and metadata
     *
     * @param owner          the owner OBSFileSystem instance
     * @param key            key of object
     * @param tmpFile        temp file or input stream
     * @param recordPosition client record next append position
     * @return the request
     * @throws IOException any problem
     */
    static WriteFileRequest newAppendFileRequest(final OBSFileSystem owner, final String key, final long recordPosition,
        final File tmpFile, final FileStatus fileStatus) throws IOException {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(tmpFile);

        long appendPosition = Math.max(recordPosition, fileStatus.getLen());
        if (recordPosition != fileStatus.getLen()) {
            LOG.warn("append url[{}] position[{}], file contentLength[{}] not" + " equal to recordPosition[{}].", key,
                appendPosition, fileStatus.getLen(), recordPosition);
        }
        WriteFileRequest writeFileReq = new WriteFileRequest(owner.getBucket(), key);
        writeFileReq.setInput(new FileInputStream(tmpFile));
        writeFileReq.setPosition(appendPosition);
        writeFileReq.setAutoClose(false);
        writeFileReq.setAcl(owner.getCannedACL());
        return writeFileReq;
    }

    /**
     * Create a appendFile request. Adds the ACL and metadata
     *
     * @param owner          the owner OBSFileSystem instance
     * @param key            key of object
     * @param inputStream    temp file or input stream
     * @param recordPosition client record next append position
     * @return the request
     * @throws IOException any problem
     */
    static WriteFileRequest newAppendFileRequest(final OBSFileSystem owner, final String key, final long recordPosition,
        final InputStream inputStream, final FileStatus fileStatus) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(inputStream);
        Preconditions.checkNotNull(fileStatus);

        long appendPosition = Math.max(recordPosition, fileStatus.getLen());
        if (recordPosition != fileStatus.getLen()) {
            LOG.warn("append url[{}] position[{}], file contentLength[{}] not" + " equal to recordPosition[{}].", key,
                appendPosition, fileStatus.getLen(), recordPosition);
        }
        WriteFileRequest writeFileReq = new WriteFileRequest(owner.getBucket(), key, inputStream, appendPosition);
        writeFileReq.setAcl(owner.getCannedACL());
        return writeFileReq;
    }

    /**
     * Append File.
     *
     * @param owner             the owner OBSFileSystem instance
     * @param appendFileRequest append object request
     * @throws IOException on any failure to append file
     */
    static void appendFile(final OBSFileSystem owner, final WriteFileRequest appendFileRequest) throws IOException {
        OBSCommonUtils.getOBSInvoker().retryByMaxTime(OBSOperateAction.appendFile, appendFileRequest.getObjectKey(), () -> {
            if (appendFileRequest.getInput() instanceof FileInputStream) {
                ((FileInputStream) appendFileRequest.getInput()).getChannel().position(0);
            }
            return owner.getObsClient().writeFile(appendFileRequest);
        },true);

        long len = appendFileRequest.getFile() != null ? appendFileRequest.getFile().length() : 0;
        owner.getSchemeStatistics().incrementWriteOps(1);
        owner.getSchemeStatistics().incrementBytesWritten(len);
    }

    /**
     * Close the Closeable objects and <b>ignore</b> any Exception or null
     * pointers. (This is the SLF4J equivalent of that in {@code IOUtils}).
     *
     * @param closeables the objects to close
     */
    public static void closeAll(final java.io.Closeable... closeables) {
        for (java.io.Closeable c : closeables) {
            if (c != null) {
                try {
                    if (LOG != null) {
                        LOG.debug("Closing {}", c);
                    }
                    c.close();
                } catch (Exception e) {
                    if (LOG != null && LOG.isDebugEnabled()) {
                        LOG.debug("Exception in closing {}", c, e);
                    }
                }
            }
        }
    }

    /**
     * Extract an exception from a failed future, and convert to an IOE.
     *
     * @param operation operation which failed
     * @param path      path operated on (may be null)
     * @param ee        execution exception
     * @return an IOE which can be thrown
     */
    static IOException extractException(final String operation, final String path, final ExecutionException ee) {
        IOException ioe;
        Throwable cause = ee.getCause();
        if (cause instanceof ObsException) {
            ioe = translateException(operation, path, (ObsException) cause);
        } else if (cause instanceof IOException) {
            ioe = (IOException) cause;
        } else {
            ioe = new IOException(operation + " failed: " + cause, cause);
        }
        return ioe;
    }

    /**
     * Create a files status instance from a listing.
     *
     * @param keyPath   path to entry
     * @param summary   summary from OBS
     * @param blockSize block size to declare.
     * @param owner     owner of the file
     * @return a status entry
     * @throws IOException If an I/O error occurred
     */
    static OBSFileStatus createFileStatus(final Path keyPath, final ObsObject summary, final long blockSize,
        final OBSFileSystem owner) throws IOException {
        OBSFileStatus status = null;
        if (objectRepresentsDirectory(summary.getObjectKey(), summary.getMetadata().getContentLength())) {
            long lastModified = summary.getMetadata().getLastModified() == null
                ? System.currentTimeMillis()
                : OBSCommonUtils.dateToLong(summary.getMetadata().getLastModified());
            status = new OBSFileStatus(keyPath, lastModified, owner.getShortUserName());
        } else {
            status = new OBSFileStatus(summary.getMetadata().getContentLength(),
                dateToLong(summary.getMetadata().getLastModified()), keyPath, blockSize, owner.getShortUserName());
        }

        if (owner.supportDisguisePermissionsMode()) {
            OBSCommonUtils.setAccessControlAttrForFileStatus(owner, status,
                getObjectMetadata(owner, summary.getObjectKey()));
        }
        return status;
    }

    /**
     * Return the access key and secret for OBS API use. Credentials may exist
     * in configuration, within credential providers or indicated in the
     * UserInfo of the name URI param.
     *
     * @param name the URI for which we need the access keys.
     * @param conf the Configuration object to interrogate for keys.
     * @return OBSAccessKeys
     * @throws IOException problems retrieving passwords from KMS.
     */
    static OBSLoginHelper.Login getOBSAccessKeys(final URI name, final Configuration conf) throws IOException {
        OBSLoginHelper.Login login = OBSLoginHelper.extractLoginDetailsWithWarnings(name);
        Configuration c = ProviderUtils.excludeIncompatibleCredentialProviders(conf, OBSFileSystem.class);
        String accessKey = getPassword(c, OBSConstants.ACCESS_KEY, login.getUser());
        String secretKey = getPassword(c, OBSConstants.SECRET_KEY, login.getPassword());
        String sessionToken = getPassword(c, OBSConstants.SESSION_TOKEN, login.getToken());
        return new OBSLoginHelper.Login(accessKey, secretKey, sessionToken);
    }

    /**
     * Get a password from a configuration, or, if a value is passed in, pick
     * that up instead.
     *
     * @param conf configuration
     * @param key  key to look up
     * @param val  current value: if non empty this is used instead of querying
     *             the configuration.
     * @return a password or "".
     * @throws IOException on any problem
     */
    private static String getPassword(final Configuration conf, final String key, final String val) throws IOException {
        return isStringEmpty(val) ? lookupPassword(conf, key) : val;
    }

    private static String lookupPassword(final Configuration conf, final String key) throws IOException {
        try {
            final char[] pass = conf.getPassword(key);
            return pass != null ? new String(pass).trim() : "";
        } catch (IOException ioe) {
            throw new IOException("Cannot find password option " + key, ioe);
        }
    }

    /**
     * String information about a summary entry for debug messages.
     *
     * @param summary summary object
     * @return string value
     */
    static String stringify(final ObsObject summary) {
        return summary.getObjectKey() + " size=" + summary.getMetadata().getContentLength();
    }

    /**
     * Get a integer not smaller than the minimum allowed value.
     */
    public static int intOption(final Configuration conf, final String key, final int defVal, final int min) {
        int v = conf.getInt(key, defVal);
        Preconditions.checkArgument(v >= min,
            String.format(Locale.ROOT, "Value of %s: %d is below the minimum value %d", key, v, min));
        LOG.debug("Value of {} is {}", key, v);
        return v;
    }

    /**
     * Get a long not smaller than the minimum allowed value.
     */
    static long longOption(final Configuration conf, final String key, final long defVal, final long min) {
        long v = conf.getLong(key, defVal);
        Preconditions.checkArgument(v >= min,
            String.format(Locale.ROOT, "Value of %s: %d is below the minimum value %d", key, v, min));
        LOG.debug("Value of {} is {}", key, v);
        return v;
    }

    /**
     * Get a long not smaller than the minimum allowed value, supporting
     * memory prefixes K,M,G,T,P.
     */
    public static long longBytesOption(final Configuration conf, final String key, final long defVal, final long min) {
        long v = conf.getLongBytes(key, defVal);
        Preconditions.checkArgument(v >= min,
            String.format(Locale.ROOT, "Value of %s: %d is below the minimum value %d", key, v, min));
        LOG.debug("Value of {} is {}", key, v);
        return v;
    }

    /**
     * Get a size property from the configuration: this property must be at
     * least equal to {@link OBSConstants#MULTIPART_MIN_SIZE}. If it is too
     * small, it is rounded up to that minimum, and a warning printed.
     *
     * @param conf     configuration
     * @param property property name
     * @param defVal   default value
     * @return the value, guaranteed to be above the minimum size
     */
    public static long getMultipartSizeProperty(final Configuration conf, final String property, final long defVal) {
        long partSize = conf.getLongBytes(property, defVal);
        if (partSize < OBSConstants.MULTIPART_MIN_SIZE) {
            LOG.warn("{} must be at least 5 MB; configured value is {}", property, partSize);
            partSize = OBSConstants.MULTIPART_MIN_SIZE;
        }
        return partSize;
    }

    /**
     * Ensure that the long value is in the range of an integer.
     *
     * @param name property name for error messages
     * @param size original size
     * @return the size, guaranteed to be less than or equal to the max value of
     * an integer.
     */
    static int ensureOutputParameterInRange(final String name, final long size) {
        if (size > Integer.MAX_VALUE) {
            LOG.warn("obs: {} capped to ~2.14GB" + " (maximum allowed size with current output mechanism)", name);
            return Integer.MAX_VALUE;
        } else {
            return (int) size;
        }
    }

    /**
     * Propagates bucket-specific settings into generic OBS configuration keys.
     * This is done by propagating the values of the form {@code
     * fs.obs.bucket.${bucket}.key} to {@code fs.obs.key}, for all values of
     * "key" other than a small set of unmodifiable values.
     *
     * <p>The source of the updated property is set to the key name of the
     * bucket property, to aid in diagnostics of where things came from.
     *
     * <p>Returns a new configuration. Why the clone? You can use the same conf
     * for different filesystems, and the original values are not updated.
     *
     * <p>The {@code fs.obs.impl} property cannot be set, nor can any with the
     * prefix {@code fs.obs.bucket}.
     *
     * <p>This method does not propagate security provider path information
     * from the OBS property into the Hadoop common provider: callers must call
     * {@link #patchSecurityCredentialProviders(Configuration)} explicitly.
     *
     * @param source Source Configuration object.
     * @param bucket bucket name. Must not be empty.
     * @return a (potentially) patched clone of the original.
     */
    static Configuration propagateBucketOptions(final Configuration source, final String bucket) {

        Preconditions.checkArgument(isStringNotEmpty(bucket), "bucket");
        final String bucketPrefix = OBSConstants.FS_OBS_BUCKET_PREFIX + bucket + '.';
        LOG.debug("Propagating entries under {}", bucketPrefix);
        final Configuration dest = new Configuration(source);
        for (Map.Entry<String, String> entry : source) {
            final String key = entry.getKey();
            final String value = entry.getValue();
            if (!key.startsWith(bucketPrefix) || bucketPrefix.equals(key)) {
                continue;
            }
            final String stripped = key.substring(bucketPrefix.length());
            if (stripped.startsWith("bucket.") || "impl".equals(stripped)) {
                LOG.debug("Ignoring bucket option {}", key);
            } else {
                final String generic = OBSConstants.FS_OBS_PREFIX + stripped;
                LOG.debug("Updating {}", generic);
                dest.set(generic, value, key);
            }
        }
        return dest;
    }

    /**
     * Patch the security credential provider information in {@link
     * #CREDENTIAL_PROVIDER_PATH} with the providers listed in {@link
     * OBSConstants#OBS_SECURITY_CREDENTIAL_PROVIDER_PATH}.
     *
     * <p>This allows different buckets to use different credential files.
     *
     * @param conf configuration to patch
     */
    static void patchSecurityCredentialProviders(final Configuration conf) {
        Collection<String> customCredentials = conf.getStringCollection(
            OBSConstants.OBS_SECURITY_CREDENTIAL_PROVIDER_PATH);
        Collection<String> hadoopCredentials = conf.getStringCollection(CREDENTIAL_PROVIDER_PATH);
        if (!customCredentials.isEmpty()) {
            List<String> all = Lists.newArrayList(customCredentials);
            all.addAll(hadoopCredentials);
            String joined = String.join(",", all);
            LOG.debug("Setting {} to {}", CREDENTIAL_PROVIDER_PATH, joined);
            conf.set(CREDENTIAL_PROVIDER_PATH, joined,
                "patch of " + OBSConstants.OBS_SECURITY_CREDENTIAL_PROVIDER_PATH);
        }
    }

    /**
     * Verify whether the write buffer directory is accessible.
     *
     * @param conf the configuration file used by filesystem
     * @throws IOException write buffer directory is not accessible
     */
    static void verifyBufferDirAccessible(final Configuration conf) throws IOException {
        String bufferDirKey = conf.get(OBSConstants.BUFFER_DIR) != null ? OBSConstants.BUFFER_DIR : "hadoop.tmp.dir";
        String bufferDirs = conf.get(bufferDirKey);
        String[] dirStrings = org.apache.hadoop.util.StringUtils.getTrimmedStrings(bufferDirs);

        if (dirStrings.length < 1) {
            throw new AccessControlException(
                "There is no write buffer dir " + "for " + bufferDirKey + ", user: " + System.getProperty("user.name"));
        }
        LocalFileSystem localFs = org.apache.hadoop.fs.FileSystem.getLocal(conf);
        for (String dir : dirStrings) {
            Path tmpDir = new Path(dir);
            if (localFs.mkdirs(tmpDir) || localFs.exists(tmpDir)) {
                try {
                    File tmpFile = tmpDir.isAbsolute()
                        ? new File(localFs.makeQualified(tmpDir).toUri())
                        : new File(dir);
                    DiskChecker.checkDir(tmpFile);
                } catch (DiskChecker.DiskErrorException e) {
                    throw new AccessControlException("user: " + System.getProperty("user.name") + ", " + e);
                }
            }
        }
    }

    /**
     * initialize multi-part upload, purge larger than the value of
     * PURGE_EXISTING_MULTIPART_AGE.
     *
     * @param owner the owner OBSFileSystem instance
     * @param conf  the configuration to use for the FS
     * @throws IOException on any failure to initialize multipart upload
     */
    static void initMultipartUploads(final OBSFileSystem owner, final Configuration conf) throws IOException {
        boolean purgeExistingMultipart = conf.getBoolean(OBSConstants.PURGE_EXISTING_MULTIPART,
            OBSConstants.DEFAULT_PURGE_EXISTING_MULTIPART);
        long purgeExistingMultipartAge = longOption(conf, OBSConstants.PURGE_EXISTING_MULTIPART_AGE,
            OBSConstants.DEFAULT_PURGE_EXISTING_MULTIPART_AGE, 0);

        if (!purgeExistingMultipart) {
            return;
        }

        final Date purgeBefore = new Date(new Date().getTime() - purgeExistingMultipartAge * 1000);

        try {
            ListMultipartUploadsRequest request = new ListMultipartUploadsRequest(owner.getBucket());
            while (true) {
                // List + purge
                MultipartUploadListing uploadListing = owner.getObsClient().listMultipartUploads(request);
                for (MultipartUpload upload : uploadListing.getMultipartTaskList()) {
                    if (upload.getInitiatedDate().compareTo(purgeBefore) < 0) {
                        owner.getObsClient()
                            .abortMultipartUpload(
                                new AbortMultipartUploadRequest(owner.getBucket(), upload.getObjectKey(),
                                    upload.getUploadId()));
                    }
                }
                if (!uploadListing.isTruncated()) {
                    break;
                }
                request.setUploadIdMarker(uploadListing.getNextUploadIdMarker());
                request.setKeyMarker(uploadListing.getNextKeyMarker());
            }
        } catch (ObsException e) {
            if (e.getResponseCode() == FORBIDDEN_CODE) {
                LOG.debug("Failed to purging multipart uploads against {}," + " FS may be read only", owner.getBucket(),
                    e);
            } else {
                throw translateException("purging multipart uploads", owner.getBucket(), e);
            }
        }
    }

    static void shutdownAll(final ExecutorService... executors) {
        for (ExecutorService exe : executors) {
            if (exe != null) {
                try {
                    if (LOG != null) {
                        LOG.debug("Shutdown {}", exe);
                    }
                    exe.shutdown();
                } catch (Exception e) {
                    if (LOG != null && LOG.isDebugEnabled()) {
                        LOG.debug("Exception in shutdown {}", exe, e);
                    }
                }
            }
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
    static OBSFileStatus getFileStatusWithRetry(final OBSFileSystem owner, final Path f)
        throws FileNotFoundException, IOException {
        return OBSCommonUtils.getOBSInvoker().retryByMaxTime(OBSOperateAction.getFileStatus, OBSCommonUtils.pathToKey(owner, f), () -> {
            return owner.innerGetFileStatus(f);
        },true);
    }

    public static long getSleepTimeInMs(final int retryCount) {
        long sleepTime = OBSCommonUtils.retrySleepBaseTime * (long) ((int) Math.pow(
            OBSCommonUtils.VARIABLE_BASE_OF_POWER_FUNCTION, retryCount));
        return sleepTime > OBSCommonUtils.retrySleepMaxTime ? OBSCommonUtils.retrySleepMaxTime : sleepTime;
    }

    public static void setMetricsAbnormalInfo(OBSFileSystem fs, OBSOperateAction opName, Exception exception) {
        long startTime = System.currentTimeMillis();
        if (fs.getMetricSwitch()) {
            BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(opName, exception, BasicMetricsConsumer.MetricKind.abnormal);
            fs.getMetricsConsumer().putMetrics(record);
            long endTime = System.currentTimeMillis();

            long costTime = (endTime - startTime) / 1000;
            if (costTime >= fs.getInvokeCountThreshold() && !(fs.getMetricsConsumer() instanceof DefaultMetricsConsumer)) {
                LOG.warn("putMetrics cosetTime too much：exception: {},opName: {} " + "costTime: {}", record.getExceptionIns().getMessage(),
                        record.getObsOperateAction(), costTime);
            }
        }
    }

    public static void setMetricsNormalInfo(OBSFileSystem fs, OBSOperateAction opName, long start) {
        long startTime = System.currentTimeMillis();
        if (fs.getMetricSwitch()) {
            BasicMetricsConsumer.MetricRecord record = new BasicMetricsConsumer.MetricRecord(
                     opName, System.currentTimeMillis() - start, BasicMetricsConsumer.MetricKind.normal);
            fs.getMetricsConsumer().putMetrics(record);
            long endTime = System.currentTimeMillis();

            long costTime = (endTime - startTime) / 1000;
            if (costTime >= fs.getInvokeCountThreshold() && !(fs.getMetricsConsumer() instanceof DefaultMetricsConsumer)) {
                LOG.warn("putMetrics cosetTime too much：opName: {} " + "costTime: {}", record.getObsOperateAction(), costTime);
            }
        }
    }

    public static void putQosMetric(OBSFileSystem fs, OBSOperateAction action, IOException e) {
        if (e instanceof OBSQosException) {
            OBSCommonUtils.setMetricsAbnormalInfo(fs, action, e);
        }
    }

    public static boolean isStringEmpty(String str)  {
        return str == null || str.length() == 0;
    }

    public static boolean isStringNotEmpty(String str) {
        return !isStringEmpty(str);
    }

    public static boolean stringEqualsIgnoreCase(String str1, String str2) {
        return str1 == null ? str2 == null : str1.equalsIgnoreCase(str2);
    }

    public static String toHex(byte[] bytes) {
        StringBuilder result = new StringBuilder();
        for (byte b : bytes) {
            String hex = Integer.toHexString(b & 0xFF);
            if (hex.length() == 1) {
                result.append('0');
            }

            result.append(hex);
        }
        return result.toString();
    }

    static Map<String, Object> getObjectMetadata(final OBSFileSystem fs, final String key) throws IOException {
        GetObjectMetadataRequest req = new GetObjectMetadataRequest(fs.getBucket(), key);
        ObjectMetadata metadata = getOBSInvoker().retryByMaxTime(OBSOperateAction.getObjectMetadata, key,
            () -> fs.getObsClient().getObjectMetadata(req), true);
        return metadata.getAllMetadata();
    }

    static void setAccessControlAttrForFileStatus(final OBSFileSystem fs, final OBSFileStatus status,
        final Map<String, Object> objMeta) {
        FsPermission fsPermission = null;
        try {
            fsPermission = Optional.ofNullable(objMeta.get("permission")).map(Object::toString)
                .map(Short::valueOf).map(FsPermission::new).orElse(null);
        } catch (NumberFormatException e) {
            LOG.debug("File {} permission is invalid, use default permission.", status.getPath());
        }

        status.setAccessControlAttr(
            Optional.ofNullable(objMeta.get("user")).map(Object::toString).orElse(fs.getShortUserName()),
            Optional.ofNullable(objMeta.get("group")).map(Object::toString).orElse(fs.getShortUserName()),
            fsPermission
        );
    }
}
