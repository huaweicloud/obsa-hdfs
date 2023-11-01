package org.apache.hadoop.fs.obs;

import com.obs.services.exception.ObsException;
import com.obs.services.model.KeyAndVersion;
import com.obs.services.model.ListObjectsRequest;
import com.obs.services.model.ObjectListing;
import com.obs.services.model.ObsObject;
import com.obs.services.model.SetObjectMetadataRequest;
import com.obs.services.model.fs.ContentSummaryFsRequest;
import com.obs.services.model.fs.ContentSummaryFsResult;
import com.obs.services.model.fs.DirContentSummary;
import com.obs.services.model.fs.DirSummary;
import com.obs.services.model.fs.GetAttributeRequest;
import com.obs.services.model.fs.NewFolderRequest;
import com.obs.services.model.fs.ObsFSAttribute;
import com.obs.services.model.fs.RenameRequest;
import com.obs.services.model.fs.ListContentSummaryFsRequest;
import com.obs.services.model.fs.ListContentSummaryFsResult;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.security.AccessControlException;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * Posix bucket specific utils
 */
final class OBSPosixBucketUtils {
    private static final Logger LOG = LoggerFactory.getLogger(OBSPosixBucketUtils.class);

    private static RetryPolicyWithMaxTime retryPolicy;
    private static RetryPolicyWithMaxTime obsRetryPolicy;

    private OBSPosixBucketUtils() {
    }

    public static void init(Configuration conf) {
        retryPolicy = new OBSRetryPolicy.ExponentialBackoffRetryPolicy(
                conf.getInt(OBSConstants.RETRY_LIMIT, OBSConstants.DEFAULT_RETRY_LIMIT),
                conf.getLong(OBSConstants.RETRY_MAXTIME, OBSConstants.DEFAULT_RETRY_MAXTIME),
                conf.getLong(OBSConstants.RETRY_SLEEP_BASETIME, OBSConstants.DEFAULT_RETRY_SLEEP_BASETIME),
                conf.getLong(OBSConstants.RETRY_SLEEP_MAXTIME, OBSConstants.DEFAULT_RETRY_SLEEP_MAXTIME));
        obsRetryPolicy = new OBSRetryPolicy(conf);
    }

    /**
     * Used to judge that an object is a file or folder.
     *
     * @param attr posix object attribute
     * @return is posix folder
     */
    static boolean fsIsFolder(final ObsFSAttribute attr) {
        final int ifDir = 0x004000;
        int mode = attr.getMode();
        // object mode is -1 when the object is migrated from
        // object bucket to posix bucket.
        // -1 is a file, not folder.
        if (mode < 0) {
            return false;
        }

        return (mode & ifDir) != 0;
    }

    /**
     * The inner rename operation based on Posix bucket.
     *
     * @param owner OBS File System instance
     * @param src   source path to be renamed from
     * @param dst   destination path to be renamed to
     * @return boolean
     * @throws OBSRenameFailedException if some criteria for a state changing
     *                               rename was not met. This means work didn't
     *                               happen; it's not something which is
     *                               reported upstream to the FileSystem APIs,
     *                               for which the semantics of "false" are
     *                               pretty vague.
     * @throws IOException           on IO failure.
     */
    static boolean renameBasedOnPosix(final OBSFileSystem owner, final Path src, final Path dst) throws IOException {
        Path dstPath = dst;
        String srcKey = OBSCommonUtils.pathToKey(owner, src);
        String dstKey = OBSCommonUtils.pathToKey(owner, dstPath);

        if (srcKey.isEmpty()) {
            LOG.error("rename: src [{}] is root directory", src);
            return false;
        }

        try {
            FileStatus dstStatus = OBSCommonUtils.getFileStatusWithRetry(owner, dstPath);
            if (dstStatus.isDirectory()) {
                String newDstString = OBSCommonUtils.maybeAddTrailingSlash(dstPath.toString());
                String filename = srcKey.substring(OBSCommonUtils.pathToKey(owner, src.getParent()).length() + 1);
                dstPath = new Path(newDstString + filename);
                dstKey = OBSCommonUtils.pathToKey(owner, dstPath);
                LOG.debug("rename: dest is an existing directory and will be " + "changed to [{}]", dstPath);

                if (owner.exists(dstPath)) {
                    LOG.error("rename: failed to rename " + src + " to " + dstPath + " because destination exists");
                    return false;
                }
            } else {
                if (srcKey.equals(dstKey)) {
                    LOG.warn("rename: src and dest refer to the same " + "file or directory: {}", dstPath);
                    return true;
                } else {
                    LOG.error("rename: failed to rename " + src + " to " + dstPath + " because destination exists");
                    return false;
                }
            }
        } catch (FileNotFoundException e) {
            // if destination does not exist, do not change the
            // destination key, and just do rename.
            LOG.debug("rename: dest [{}] does not exist", dstPath);
        } catch (OBSFileConflictException e) {
            throw new ParentNotDirectoryException(e.getMessage());
        }

        if (dstKey.startsWith(srcKey) && (dstKey.equals(srcKey)
            || dstKey.charAt(srcKey.length()) == Path.SEPARATOR_CHAR)) {
            LOG.error("rename: dest [{}] cannot be a descendant of src [{}]", dstPath, src);
            return false;
        }

        return innerFsRenameWithRetry(owner, src, dstPath, srcKey, dstKey);
    }

    static boolean innerFsRenameWithRetry(final OBSFileSystem owner, final Path src, final Path dst,
        final String srcKey, final String dstKey) throws IOException {
        String newSrcKey = srcKey;
        String newDstKey = dstKey;
        int retries = 0;
        long startTime = System.currentTimeMillis();
        while (true) {
            boolean isRegularDirPath = newSrcKey.endsWith("/") && newDstKey.endsWith("/");
            try {
                LOG.debug("rename: {}-st rename from [{}] to [{}] ...", retries, newSrcKey, newDstKey);
                innerFsRenameFile(owner, newSrcKey, newDstKey);
                return true;
            } catch (FileNotFoundException e) {
                if (owner.exists(dst)) {
                    LOG.debug("file not found when rename. rename: successfully {}-st rename src [{}] " + "to dest [{}] with SDK retry", retries,
                        src, dst);
                    return true;
                } else {
                    LOG.error("file not found when rename. rename: failed {}-st rename src [{}] to dest [{}]", retries, src, dst);
                    return false;
                }
            } catch (AccessControlException e) {
                if (isRegularDirPath) {
                    throw e;
                }
                try {
                    FileStatus srcFileStatus = OBSCommonUtils.getFileStatusWithRetry(owner, src);
                    if (srcFileStatus.isDirectory()) {
                        newSrcKey = OBSCommonUtils.maybeAddTrailingSlash(newSrcKey);
                        newDstKey = OBSCommonUtils.maybeAddTrailingSlash(newDstKey);
                        continue;
                    } else {
                        throw e;
                    }
                } catch (OBSFileConflictException e1) {
                    throw new AccessControlException(e);
                }
            } catch (IOException e) {
                try {
                    OBSCommonUtils.getFileStatusWithRetry(owner, src);
                } catch (OBSFileConflictException e1) {
                    throw new AccessControlException(e);
                }
                if (owner.exists(dst) && owner.exists(src)) {
                    LOG.warn("rename: failed {}-st rename src [{}] to " + "dest [{}] with SDK retry", retries, src,
                        dst, e);
                    return false;
                }

                OBSCommonUtils.putQosMetric(owner, OBSOperateAction.rename, e);

                RetryPolicy.RetryAction rc;
                try {
                    rc = obsRetryPolicy.shouldRetryByMaxTime(startTime, e, retries++, 0, true);
                } catch (Exception e1) {
                    throw new IOException("unexpected exception ", e1);
                }
                if (rc.action == RetryPolicy.RetryAction.RetryDecision.FAIL) {
                    throw e;
                }
                try {
                    LOG.warn("retry #{}, {}", retries, e);
                    Thread.sleep(rc.delayMillis);
                } catch (InterruptedException e1) {
                    throw (IOException)new InterruptedIOException("interrupted").initCause(e1);
                }
            }
        }
    }

    static void innerFsRenameFile(final OBSFileSystem owner, final String srcKey, final String dstKey)
        throws IOException {
        LOG.debug("RenameFile path {} to {}", srcKey, dstKey);

        try {
            final RenameRequest renameObjectRequest = new RenameRequest();
            renameObjectRequest.setBucketName(owner.getBucket());
            renameObjectRequest.setObjectKey(srcKey);
            renameObjectRequest.setNewObjectKey(dstKey);
            owner.getObsClient().renameFile(renameObjectRequest);
            owner.getSchemeStatistics().incrementWriteOps(1);
        } catch (ObsException e) {
            throw OBSCommonUtils.translateException("renameFile(" + srcKey + ", " + dstKey + ")", srcKey, e);
        }
    }

    /**
     * Used to rename a source object to a destination object which is not
     * existed before rename.
     *
     * @param owner  OBS File System instance
     * @param srcKey source object key
     * @param dstKey destination object key
     * @throws IOException io exception
     */
    static boolean fsRenameToNewObject(final OBSFileSystem owner, final String srcKey, final String dstKey)
        throws IOException {
        String newSrcKey = srcKey;
        String newdstKey = dstKey;
        newSrcKey = OBSCommonUtils.maybeDeleteBeginningSlash(newSrcKey);
        newdstKey = OBSCommonUtils.maybeDeleteBeginningSlash(newdstKey);
        return renameBasedOnPosix(owner, OBSCommonUtils.keyToPath(newSrcKey), OBSCommonUtils.keyToPath(newdstKey));
    }

    // Delete a file.
    private static int fsRemoveFile(final OBSFileSystem owner, final String sonObjectKey,
        final List<KeyAndVersion> files) throws IOException {
        files.add(new KeyAndVersion(sonObjectKey));
        if (files.size() == owner.getMaxEntriesToDelete()) {
            // batch delete files.
            OBSCommonUtils.removeKeys(owner, files, true, false);
            return owner.getMaxEntriesToDelete();
        }
        return 0;
    }

    // Recursively delete a folder that might be not empty.
    static boolean fsDelete(final OBSFileSystem owner, final FileStatus status, final boolean recursive)
        throws IOException, ObsException {
        long startTime = System.currentTimeMillis();
        long threadId = Thread.currentThread().getId();
        Path f = status.getPath();
        String key = OBSCommonUtils.pathToKey(owner, f);

        if (!status.isDirectory()) {
            LOG.debug("delete: Path is a file");
            trashObjectIfNeed(owner, key);
        } else {
            LOG.debug("delete: Path is a directory: {} - recursive {}", f, recursive);
            key = OBSCommonUtils.maybeAddTrailingSlash(key);
            boolean isEmptyDir = OBSCommonUtils.isFolderEmpty(owner, key);
            if (key.equals("")) {
                return OBSCommonUtils.rejectRootDirectoryDelete(owner.getBucket(), isEmptyDir, recursive);
            }
            if (!recursive && !isEmptyDir) {
                LOG.warn("delete: Path is not empty: {} - recursive {}", f, recursive);
                throw new PathIsNotEmptyDirectoryException(f.toString());
            }
            if (isEmptyDir) {
                LOG.debug("delete: Deleting fake empty directory {} - recursive {}", f, recursive);
                try {
                    OBSCommonUtils.deleteObject(owner, key);
                } catch (OBSFileConflictException e) {
                    LOG.warn("delete emtryDir[{}] has conflict exception, "
                        + "will retry.", key, e);
                    trashFolderIfNeed(owner, key);
                }
            } else {
                LOG.debug("delete: Deleting objects for directory prefix {} to " + "delete - recursive {}", f,
                    recursive);
                trashFolderIfNeed(owner, key);
            }
        }

        long endTime = System.currentTimeMillis();
        LOG.debug("delete Path:{} thread:{}, timeUsedInMilliSec:{}", f, threadId, endTime - startTime);
        return true;
    }

    private static void trashObjectIfNeed(final OBSFileSystem owner, final String key)
        throws ObsException, IOException {
        if (!needToTrash(owner, key)) {
            OBSCommonUtils.deleteObject(owner, key);
            return;
        }

        mkTrash(owner, key);
        String destKeyWithNoSuffix = owner.getFastDeleteDir() + key;
        String destKey = destKeyWithNoSuffix;
        SimpleDateFormat df = new SimpleDateFormat("-yyyyMMddHHmmssSSS");
        if (owner.exists(new Path(destKey))) {
            destKey = destKeyWithNoSuffix + df.format(new Date());
        }

        LOG.debug("Moved file : '" + key + "' to trash at: " + destKey);
        int retries = 0;
        long startTime = System.currentTimeMillis();
        while (!fsRenameToNewObject(owner, key, destKey)) {
            RetryPolicy.RetryAction rc;
            try {
                rc = retryPolicy.shouldRetryByMaxTime(startTime, null, retries++, 0, true);
            } catch (Exception e1) {
                throw new IOException("unexpected exception ", e1);
            }
            if (rc.action == RetryPolicy.RetryAction.RetryDecision.FAIL) {
                throw new IOException("failed to rename " + key + " to " + destKey);
            }
            try {
                LOG.warn("retry #{}, fast delete {} to {}", retries, key, destKey);
                Thread.sleep(rc.delayMillis);
                destKey = destKeyWithNoSuffix + df.format(new Date());
            } catch (InterruptedException e1) {
                throw (IOException)new InterruptedIOException("interrupted").initCause(e1);
            }
        }
    }

    private static void trashFolderIfNeed(final OBSFileSystem owner, final String key)
        throws ObsException, IOException {
        if (!needToTrash(owner, key)) {
            fsRecursivelyDeleteDirWithRetry(owner, key, true);
            return;
        }

        mkTrash(owner, key);
        StringBuilder sb = new StringBuilder(owner.getFastDeleteDir());
        SimpleDateFormat df = new SimpleDateFormat("-yyyyMMddHHmmssSSS");
        int endIndex = key.endsWith("/") ? key.length() - 1 : key.length();
        sb.append(key, 0, endIndex);
        String destKeyWithNoSuffix = sb.toString();
        String destKey = destKeyWithNoSuffix;
        if (owner.exists(new Path(sb.toString()))) {
            destKey = destKeyWithNoSuffix + df.format(new Date());
        }

        String srcKey = OBSCommonUtils.maybeAddTrailingSlash(key);
        String dstKey = OBSCommonUtils.maybeAddTrailingSlash(destKey);

        LOG.debug("Moved folder : '" + key + "' to trash: " + destKey);
        int retries = 0;
        long startTime = System.currentTimeMillis();
        while (!fsRenameToNewObject(owner, srcKey, dstKey)) {
            RetryPolicy.RetryAction rc;
            try {
                rc = retryPolicy.shouldRetryByMaxTime(startTime, null, retries++, 0, true);
            } catch (Exception e1) {
                throw new IOException("unexpected exception ", e1);
            }
            if (rc.action == RetryPolicy.RetryAction.RetryDecision.FAIL) {
                throw new IOException("failed to rename " + key + " to " + destKey);
            }
            try {
                LOG.warn("retry #{}, fast delete {} to {}", retries, key, destKey);
                Thread.sleep(rc.delayMillis);
                destKey = destKeyWithNoSuffix + df.format(new Date());
                dstKey = OBSCommonUtils.maybeAddTrailingSlash(destKey);
            } catch (InterruptedException e1) {
                throw (IOException)new InterruptedIOException("interrupted").initCause(e1);
            }
        }
    }

    static void fsRecursivelyDeleteDirWithRetry(final OBSFileSystem owner,
        final String key, boolean deleteParent) throws IOException {
        int retries = 0;
        long startTime = System.currentTimeMillis();
        while (true) {
            try {
                fsRecursivelyDeleteDir(owner, key, deleteParent);
                return;
            } catch (OBSFileConflictException e) {
                RetryPolicy.RetryAction rc;
                try {
                    LOG.warn("retry #{}, {}", retries, e);
                    rc = retryPolicy.shouldRetryByMaxTime(startTime, e, retries++, 0, true);
                } catch (Exception e1) {
                    throw new IOException("Unexpected exception ", e1);
                }
                if (rc.action == RetryPolicy.RetryAction.RetryDecision.FAIL) {
                    throw e;
                }
                try {
                    Thread.sleep(rc.delayMillis);
                } catch (InterruptedException e1) {
                    throw (IOException)new InterruptedIOException("interrupted").initCause(e1);
                }
            }
        }
    }

    static long fsRecursivelyDeleteDir(final OBSFileSystem owner, final String parentKey, final boolean deleteParent)
        throws IOException {
        long delNum = 0;
        List<KeyAndVersion> subdirList = new ArrayList<>(owner.getMaxEntriesToDelete());
        List<KeyAndVersion> fileList = new ArrayList<>(owner.getMaxEntriesToDelete());

        ListObjectsRequest request = OBSCommonUtils.createListObjectsRequest(owner, parentKey, "/", owner.getMaxKeys());
        ObjectListing objects = OBSCommonUtils.listObjects(owner, request);
        while (true) {
            for (String commonPrefix : objects.getCommonPrefixes()) {
                if (commonPrefix.equals(parentKey)) {
                    // skip prefix itself
                    continue;
                }

                delNum += fsRemoveSubdir(owner, commonPrefix, subdirList);
            }

            for (ObsObject sonObject : objects.getObjects()) {
                String sonObjectKey = sonObject.getObjectKey();

                if (sonObjectKey.equals(parentKey)) {
                    // skip prefix itself
                    continue;
                }

                if (!sonObjectKey.endsWith("/")) {
                    delNum += fsRemoveFile(owner, sonObjectKey, fileList);
                } else {
                    delNum += fsRemoveSubdir(owner, sonObjectKey, subdirList);
                }
            }

            if (!objects.isTruncated()) {
                break;
            }

            objects = OBSCommonUtils.continueListObjects(owner, objects);
        }

        delNum += fileList.size();
        OBSCommonUtils.removeKeys(owner, fileList, true, false);

        delNum += subdirList.size();
        OBSCommonUtils.removeKeys(owner, subdirList, true, false);

        if (deleteParent) {
            OBSCommonUtils.deleteObject(owner, parentKey);
            delNum++;
        }

        return delNum;
    }

    private static boolean needToTrash(final OBSFileSystem owner, final String key) {
        String newKey = key;
        newKey = OBSCommonUtils.maybeDeleteBeginningSlash(newKey);
        if (owner.isEnableFastDelete()) {
            String trashPathKey = OBSCommonUtils.pathToKey(owner, new Path(owner.getFastDeleteDir()));
            if (newKey.startsWith(trashPathKey)) {
                return false;
            }
        }
        return owner.isEnableFastDelete();
    }

    // Delete a sub dir.
    private static int fsRemoveSubdir(final OBSFileSystem owner, final String subdirKey,
        final List<KeyAndVersion> subdirList) throws IOException {
        fsRecursivelyDeleteDirWithRetry(owner, subdirKey, false);

        subdirList.add(new KeyAndVersion(subdirKey));
        if (subdirList.size() == owner.getMaxEntriesToDelete()) {
            // batch delete subdirs.
            OBSCommonUtils.removeKeys(owner, subdirList, true, false);
            return owner.getMaxEntriesToDelete();
        }

        return 0;
    }

    private static void mkTrash(final OBSFileSystem owner, final String key) throws ObsException, IOException {
        String newKey = key;
        StringBuilder sb = new StringBuilder(owner.getFastDeleteDir());
        newKey = OBSCommonUtils.maybeAddTrailingSlash(newKey);
        sb.append(newKey);
        sb.deleteCharAt(sb.length() - 1);
        sb.delete(sb.lastIndexOf("/"), sb.length());
        Path fastDeleteRecycleDirPath = new Path(sb.toString());
        // keep the parent directory of the target path exists
        if (!owner.exists(fastDeleteRecycleDirPath)) {
            owner.mkdirs(fastDeleteRecycleDirPath);
        }
    }

    // Used to create a folder
    static void fsCreateFolder(final OBSFileSystem owner, final String objectName) throws IOException {
        String newObjectName = OBSCommonUtils.maybeAddTrailingSlash(objectName);
        final NewFolderRequest newFolderRequest = new NewFolderRequest(owner.getBucket(), newObjectName);
        newFolderRequest.setAcl(owner.getCannedACL());
        long len = newFolderRequest.getObjectKey().length();
        OBSCommonUtils.getOBSInvoker().retryByMaxTime(OBSOperateAction.createFolder, objectName, () -> {
            owner.getObsClient().newFolder(newFolderRequest);
            return null;
        },true);

        owner.getSchemeStatistics().incrementWriteOps(1);
        owner.getSchemeStatistics().incrementBytesWritten(len);
    }

    // Used to get the status of a file or folder in a file-gateway bucket.
    static OBSFileStatus innerFsGetObjectStatus(final OBSFileSystem owner, final Path f) throws IOException {
        final Path path = OBSCommonUtils.qualify(owner, f);
        String key = OBSCommonUtils.pathToKey(owner, path);
        LOG.debug("Getting path status for {}  ({})", path, key);

        if (key.isEmpty()) {
            LOG.debug("Found root directory");
            return new OBSFileStatus(path, owner.getShortUserName());
        }

        try {
            final GetAttributeRequest getAttrRequest = new GetAttributeRequest(owner.getBucket(), key);
            ObsFSAttribute meta = owner.getObsClient().getAttribute(getAttrRequest);

            owner.getSchemeStatistics().incrementReadOps(1);
            if (fsIsFolder(meta)) {
                LOG.debug("Found file (with /): fake directory");
                OBSFileStatus status = new OBSFileStatus(path, OBSCommonUtils.dateToLong(meta.getLastModified()),
                    owner.getShortUserName());
                OBSCommonUtils.setAccessControlAttrForFileStatus(owner, status, meta.getAllMetadata());
                return status;
            }

            LOG.debug("Found file (with /): real file? should not happen: {}", key);
            OBSFileStatus status = new OBSFileStatus(meta.getContentLength(),
                OBSCommonUtils.dateToLong(meta.getLastModified()), path, owner.getDefaultBlockSize(path),
                owner.getShortUserName(), meta.getEtag());
            OBSCommonUtils.setAccessControlAttrForFileStatus(owner, status, meta.getAllMetadata());
            return status;
        } catch (ObsException e) {
            throw OBSCommonUtils.translateException("getFileStatus", path, e);
        }
    }

    static ContentSummary fsGetDirectoryContentSummary(final OBSFileSystem owner, final String key) throws IOException {
        String newKey = key;
        newKey = OBSCommonUtils.maybeAddTrailingSlash(newKey);
        long[] summary = {0, 0, 1};
        LOG.debug("Summary key {}", newKey);
        ListObjectsRequest request = new ListObjectsRequest();
        request.setBucketName(owner.getBucket());
        request.setPrefix(newKey);
        request.setMaxKeys(owner.getMaxKeys());
        ObjectListing objects = OBSCommonUtils.listObjects(owner, request);
        while (true) {
            if (!objects.getCommonPrefixes().isEmpty() || !objects.getObjects().isEmpty()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Found path as directory (with /): {}/{}", objects.getCommonPrefixes().size(),
                        objects.getObjects().size());
                }
                for (String prefix : objects.getCommonPrefixes()) {
                    if (!prefix.equals(newKey)) {
                        summary[2]++;
                    }
                }

                for (ObsObject obj : objects.getObjects()) {
                    if (!obj.getObjectKey().endsWith("/")) {
                        summary[0] += obj.getMetadata().getContentLength();
                        summary[1] += 1;
                    } else if (!obj.getObjectKey().equals(newKey)) {
                        summary[2]++;
                    }
                }
            }
            if (!objects.isTruncated()) {
                break;
            }
            objects = OBSCommonUtils.continueListObjects(owner, objects);
        }
        LOG.debug(
            String.format(Locale.ROOT,"file size [%d] - file count [%d] - directory count [%d] - " + "file path [%s]", summary[0],
                summary[1], summary[2], newKey));
        return new ContentSummary.Builder().length(summary[0])
            .fileCount(summary[1])
            .directoryCount(summary[2])
            .spaceConsumed(summary[0])
            .build();
    }

    static void innerFsTruncateWithRetry(final OBSFileSystem owner, final Path f, final long newLength)
        throws IOException {
        LOG.debug("truncate {} to newLength {}", f, newLength);
        String key = OBSCommonUtils.pathToKey(owner, f);
        OBSCommonUtils.getOBSInvoker().retryByMaxTime(OBSOperateAction.truncate, key, () -> {
            owner.getObsClient().truncateObject(owner.getBucket(), key, newLength);
            return null;
        }, true);
        owner.getSchemeStatistics().incrementWriteOps(1);
    }

    static ContentSummary fsGetDirectoryContentSummaryV2(final OBSFileSystem owner, final FileStatus fileStatus)
        throws IOException {
        FsDirContentSummaryCounter summaryCounter = new FsDirContentSummaryCounter(owner, fileStatus);
        return summaryCounter.getContentSummary();
    }

    private static class FsDirContentSummaryCounter {
        public static final int MULTI_LIST_CS_MAX_DIR = 10; // multiListCS allow up to 10 dir in one batch

        public static final int MIN_RETRY_TIME = 3;

        private final OBSFileSystem owner;

        private final LinkedBlockingDeque<Node> deque;

        private final Counter counter;

        private final FileStatus fileStatus;

        private final int parallelFactor;

        public FsDirContentSummaryCounter(OBSFileSystem owner, FileStatus fileStatus) {
            this.owner = owner;
            this.deque = new LinkedBlockingDeque<>();
            this.counter = new Counter();
            this.fileStatus = fileStatus;
            this.parallelFactor = owner.getConf().getInt(OBSConstants.MULTILISTCS_PARALLEL_FACTOR,
                OBSConstants.DEFAULT_MULTILISTCS_PARALLEL_FACTOR);
        }

        /**
         * get content summary of path, path should be a dir
         *
         * @return
         */
        public ContentSummary getContentSummary() throws IOException {
            if (!fileStatus.isDirectory()) {
                throw new IllegalArgumentException("the input should be a dir");
            }
            this.counter.increase(1, 0, 0); // count root itself
            LOG.debug("counter increase (1, 0, 0) for root itself");

            Path path = fileStatus.getPath();
            String key = OBSCommonUtils.pathToKey(owner, path);
            if (path.isRoot()) {
                key = "/"; // special case of 'root'
            }
            ContentSummaryFsRequest getCSReq = new ContentSummaryFsRequest();
            getCSReq.setBucketName(this.owner.getBucket());
            getCSReq.setDirName(key);

            // 1. getCS of root dir
            try {
                ContentSummaryFsResult getCSRes = OBSCommonUtils.getOBSInvoker().retryByMaxTime(
                    OBSOperateAction.getContentSummaryFs, key, () -> this.owner.getObsClient().getContentSummaryFs(getCSReq),
                    true);
                DirSummary summary = getCSRes.getContentSummary();
                this.counter.increase(summary.getDirCount(), summary.getFileCount(), summary.getFileSize());
                LOG.debug("counter increase ({}, {}, {}) for [{}]",
                    summary.getDirCount(), summary.getFileCount(), summary.getFileSize(), key);
                if (summary.getDirCount() != 0) {
                    enqueue(new Node(key, null, summary.getInode()));
                }
            } catch (ObsException e) {
                if (e.getResponseCode() == OBSCommonUtils.NOT_ALLOWED_CODE) {
                    throw new UnsupportedOperationException("unsupported getContentSummaryFs");
                }
                throw OBSCommonUtils.translateException("getContentSummaryFs", path, e);
            }

            countExhaustive();

            return this.counter.getContentSummary();
        }

        private void countExhaustive() throws IOException {
            while (true) {
                List<Future<?>> futures = new ArrayList<>();

                // 2. try to get node for queue
                try {
                    for (int i = 0; i < parallelFactor; i++) {
                        BatchNodes nodes = tryToGetBatchNodes();
                        if (nodes.size() == 0) { // queue is empty, break out
                            break;
                        } else {
                            // 3. submit task
                            futures.add(submitTask(nodes));
                        }
                    }
                } catch (InterruptedException e) {
                    LOG.warn("getContentSummaryV2 for [{}] failed because get nodes from queue failed, {}",
                        fileStatus.getPath().toString(), e.getMessage());
                    throw new IOException(String.format(
                        "getContentSummaryV2 for [%s] failed because get nodes from queue failed",
                        fileStatus.getPath().toString()), e);
                }

                // 4. check finish
                if (futures.size() == 0) {
                    if (deque.size() != 0) {
                        continue;
                    }
                    break;
                }

                // 5. wait for batch task finished
                waitBatchTaskFinish(futures);
            }
        }

        private BatchNodes tryToGetBatchNodes() throws InterruptedException, IOException {
            BatchNodes ret = new BatchNodes();
            int size = this.deque.size();
            for (int i = 0; i < size; i++) {
                if (ret.size() == MULTI_LIST_CS_MAX_DIR) {
                    break;
                }
                Node n = this.deque.pollFirst(1, TimeUnit.SECONDS);
                if (n != null && verifyNodeRetryState(n)) {
                    ret.add(n);
                }
            }
            return ret;
        }

        // verify node should add to request
        private boolean verifyNodeRetryState(Node n) throws IOException {
            final int retryState = n.retryState();
            switch (retryState) {
                case Node.RETRY_STATE_TRIGGER:
                    return true;
                case Node.RETRY_STATE_DISCARD:
                    LOG.error("node[key={} marker={}] failed {} times, due to {}",
                        n.getPath(), n.getMarker(), n.getRetryNum(), n.getRetryMsg());
                    throw new IllegalStateException(String.format(Locale.ROOT,"node[key=%s marker=%s] failed %d times, due to %s",
                        n.getPath(), n.getMarker(), n.getRetryNum(), n.getRetryMsg()));
                case Node.RETRY_STATE_DELAY:
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ie) {
                        LOG.error("failed to retry node[key={}, marker={}], retry time[{}], due to {}",
                            n.getPath(), n.getMarker(), n.getRetryNum(), n.getRetryMsg());
                        throw new IllegalStateException(n.getRetryMsg());
                    }
                    if (!deque.offerLast(n)) { // put retry node to the end of queue
                        LOG.warn("node [{}, {}, {}, {}] enqueue failed, may be queue is full",
                            n.getPath(), n.getInode(), n.getMarker(), n.getRetryNum());
                        throw new IllegalStateException(
                            String.format(Locale.ROOT,"node [%s, %d, %s, %d] enqueue failed, may be queue is full",
                                n.getPath(), n.getInode(), n.getMarker(), n.getRetryNum()));
                    }
                    return false;

                default:
                    throw new IllegalStateException("unreachable code");
            }
        }

        private Future<?> submitTask(BatchNodes nodes) {
            return owner.getBoundedListThreadPool().submit(
                () -> {
                    List<ListContentSummaryFsRequest.DirLayer> dirs = transformToDirLayer(nodes);
                    ListContentSummaryFsRequest req = new ListContentSummaryFsRequest();
                    req.setBucketName(owner.getBucket());
                    req.setMaxKeys(owner.getMaxKeys());
                    req.setDirLayers(dirs);
                    ListContentSummaryFsResult res;
                    res = OBSCommonUtils.getOBSInvoker().retryByMaxTime(
                        OBSOperateAction.listContentSummaryFs, "",
                        () -> owner.getObsClient().listContentSummaryFs(req),
                        true);
                    handleListContentSummaryFsResult(res, nodes);
                    return "";
                }
            );
        }

        private void enqueue(Node n) {
            if (!deque.offerFirst(n)) {
                LOG.warn("node [{}, {}, {}, {}] enqueue failed, may be queue is full",
                    n.getPath(), n.getInode(), n.getMarker(), n.getRetryNum());
                throw new IllegalStateException(
                    String.format(Locale.ROOT,"node [%s, %d, %s, %d] enqueue failed, may be queue is full",
                        n.getPath(), n.getInode(), n.getMarker(), n.getRetryNum()));
            }
        }

        private List<ListContentSummaryFsRequest.DirLayer> transformToDirLayer(BatchNodes nodes) {
            List<ListContentSummaryFsRequest.DirLayer> ret = new ArrayList<>();
            for (Node node : nodes) {
                ListContentSummaryFsRequest.DirLayer dir = new ListContentSummaryFsRequest.DirLayer();
                dir.setKey(node.getPath());
                dir.setMarker(node.getMarker());
                dir.setInode(node.getInode());
                LOG.debug("transform node ({}, {}, {}, {})",
                    node.getPath(), node.getMarker(), node.getInode(), node.getRetryNum());
                ret.add(dir);
            }
            return ret;
        }

        private void handleListContentSummaryFsResult(ListContentSummaryFsResult res, BatchNodes nodes) {
            // handle errors
            List<ListContentSummaryFsResult.ErrorResult> errs = res.getErrorResults();
            if (errs != null) {
                for (ListContentSummaryFsResult.ErrorResult err : errs) {
                    LOG.debug("listContentSummary return error contents: {}, {}, {}", err.getKey(), err.getErrorCode(),
                        err.getMessage());
                    retryErrorResult(err, nodes);
                }
            }

            List<DirContentSummary> dirs = res.getDirContentSummaries();
            if (dirs != null) {
                for (DirContentSummary dir : dirs) {
                    if (dir.isTruncated()) {
                        // put self with nextMarker to the queue
                        enqueue(new Node(dir.getKey(), dir.getNextMarker(), dir.getInode()));
                    }
                    // handle subdirs
                    for (DirSummary subDir : dir.getSubDir()) {
                        counter.increase(subDir.getDirCount(), subDir.getFileCount(), subDir.getFileSize());
                        LOG.debug("counter increase ({}, {}, {}) for [{}, {}]",
                            subDir.getDirCount(), subDir.getFileCount(), subDir.getFileSize(),
                            subDir.getName(), subDir.getInode());
                        if (subDir.getDirCount() != 0) { // enqueue when dir has sub dir
                            enqueue(new Node(subDir.getName(), null, subDir.getInode()));
                        }
                    }
                }
            }
        }

        private void retryErrorResult(ListContentSummaryFsResult.ErrorResult err, BatchNodes nodes) {
            String statusCode = err.getStatusCode();
            if (statusCode == null) {
                LOG.warn("statusCode is null, {}", err);
                return;
            }
            // 408 409 429 5XX should retry
            boolean needRetry = statusCode.equals("408") || statusCode.equals("409") || statusCode.equals("429");
            needRetry = needRetry || statusCode.startsWith("5"); // 5xx

            if (needRetry) {
                for (final Node node : nodes) {
                    if (node.getPath().equals(err.getKey()) /*&& node.getMarker().equals(err.getMarker())*/) {
                        node.increaseRetryNum(
                            String.format("%s %s %s", err.getStatusCode(), err.getErrorCode(), err.getMessage()));
                        enqueue(node);
                        return;
                    }
                }
                throw new IllegalStateException(String.format("Unreachable code: could not find err[key=%s marker=] from nodes, err=%s",
                    err.getKey(), err.getMessage()));
            }
        }

        private void waitBatchTaskFinish(List<Future<?>> futures) throws IOException {
            for (Future<?> fRes : futures) {
                try {
                    fRes.get();
                } catch (InterruptedException e) {
                    LOG.warn("Interrupted while listContentSummary, {}", e.getMessage());
                    throw new InterruptedIOException(String.format("Interrupted while listContentSummary, %s", e));
                } catch (ExecutionException e) {
                    fRes.cancel(true);
                    throw OBSCommonUtils.extractException("ListContentSummary with exception", "", e);
                }
            }
        }

        private static class BatchNodes extends ArrayList<Node> {
        }

        private static class Node {
            public static final int RETRY_STATE_DISCARD = 1; // exceed retry limits, discard

            public static final int RETRY_STATE_DELAY = 2; // not reach action moment

            public static final int RETRY_STATE_TRIGGER = 3; // reach the action moment, do request immediately

            private final String path;

            private final String marker;

            private final long inode;

            private int retryNum;

            private long previousRetry;

            private long firstProcessTime;

            private String retryMsg;

            public String getRetryMsg() {
                return retryMsg;
            }

            public long getInode() {
                return inode;
            }

            public int retryState() throws IOException {
                if (retryNum == 0) {
                    return RETRY_STATE_TRIGGER;
                }
                RetryPolicy.RetryAction rc;
                try {
                    rc = retryPolicy.shouldRetryByMaxTime(firstProcessTime, null, retryNum, 0, true);
                } catch (Exception e) {
                    throw new IOException("unexpected exception", e);
                }

                if (rc.action == RetryPolicy.RetryAction.RetryDecision.FAIL) {
                    return RETRY_STATE_DISCARD;
                }

                if (rc.action == RetryPolicy.RetryAction.RetryDecision.RETRY) {
                    long delayMs = rc.delayMillis;
                    long now = System.currentTimeMillis();
                    if (previousRetry + delayMs > now) {
                        return RETRY_STATE_DELAY;
                    }
                }
                return RETRY_STATE_TRIGGER;
            }

            public int getRetryNum() {
                return retryNum;
            }

            public String getPath() {
                return path;
            }

            public String getMarker() {
                return marker;
            }

            public Node(String path, String marker, long inode) {
                this.path = OBSCommonUtils.maybeAddTrailingSlash(path);
                this.marker = marker;
                this.retryNum = 0;
                this.previousRetry = 0;
                this.inode = inode;
            }

            public void increaseRetryNum(String retryMsg) {
                LOG.debug("node[{}, {}] increase retry, msg[{}]", path, marker, retryMsg);
                this.retryNum++;
                this.previousRetry = System.currentTimeMillis();
                if (retryNum == 1) {
                    firstProcessTime = previousRetry;
                }
                this.retryMsg = retryMsg;
            }

        }

        private static class Counter {
            private volatile long dirNum;

            private volatile long fileNum;

            private volatile long size;

            synchronized void increase(long ndir, long nfile, long size) {
                this.dirNum += ndir;
                this.fileNum += nfile;
                this.size += size;
            }

            ContentSummary getContentSummary() {
                return new ContentSummary.Builder().length(this.size)
                    .fileCount(this.fileNum)
                    .directoryCount(this.dirNum)
                    .spaceConsumed(this.size)
                    .build();
            }
        }
    }

    static void fsSetOwner(final OBSFileSystem owner, final Path f, final String username, final String groupname)
        throws IOException {
        Map<String, String> userMeta = ImmutableMap.of("user", username, "group", groupname);
        addObjectMetadata(owner, f, userMeta);
    }

    static void fsSetPermission(final OBSFileSystem owner, final Path f, final FsPermission permission)
        throws IOException {
        Map<String, String> userMeta = ImmutableMap.of("permission", String.valueOf(permission.toShort()));
        addObjectMetadata(owner, f, userMeta);
    }

    private static void addObjectMetadata(final OBSFileSystem owner, final Path f, final Map<String, String> metaToAdd)
        throws IOException {
        String key = OBSCommonUtils.pathToKey(owner, f);
        SetObjectMetadataRequest req = new SetObjectMetadataRequest(owner.getBucket(), key);
        req.addAllUserMetadata(metaToAdd);

        // query existed objMeta and add it to setMetaReq to avoid existed objMeta been overwritten.
        Map<String, Object> objMeta = OBSCommonUtils.getObjectMetadata(owner, key);
        objMeta.forEach((k, v) -> req.addUserMetadata(k, Optional.of(v).map(Objects::toString)
            .orElse(null)));

        OBSCommonUtils.getOBSInvoker().retryByMaxTime(OBSOperateAction.setObjectMetadata, key, () -> {
            owner.getObsClient().setObjectMetadata(req);
            return null;
        }, true);
    }
}
