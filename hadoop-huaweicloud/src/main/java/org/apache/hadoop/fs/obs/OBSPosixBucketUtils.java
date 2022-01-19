package org.apache.hadoop.fs.obs;

import com.obs.services.exception.ObsException;
import com.obs.services.model.KeyAndVersion;
import com.obs.services.model.ListObjectsRequest;
import com.obs.services.model.ObjectListing;
import com.obs.services.model.ObsObject;
import com.obs.services.model.fs.GetAttributeRequest;
import com.obs.services.model.fs.NewFolderRequest;
import com.obs.services.model.fs.ObsFSAttribute;
import com.obs.services.model.fs.RenameRequest;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.security.AccessControlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Posix bucket specific utils for {@link OBSFileSystem}.
 */
final class OBSPosixBucketUtils {
    /**
     * Class logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(OBSPosixBucketUtils.class);

    private OBSPosixBucketUtils() {
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
     * @throws RenameFailedException if some criteria for a state changing
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
            FileStatus dstStatus = OBSCommonUtils.innerGetFileStatusWithRetry(owner, dstPath);
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
        } catch (FileConflictException e) {
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
        IOException lastException;
        long delayMs;
        int retryTime = 0;
        long startTime = System.currentTimeMillis();
        do {
            boolean isRegularDirPath = newSrcKey.endsWith("/") && newDstKey.endsWith("/");
            try {
                LOG.debug("rename: {}-st rename from [{}] to [{}] ...", retryTime, newSrcKey, newDstKey);
                innerFsRenameFile(owner, newSrcKey, newDstKey);
                return true;
            } catch (FileNotFoundException e) {
                if (owner.exists(dst)) {
                    LOG.debug("rename: successfully {}-st rename src [{}] " + "to dest [{}] with SDK retry", retryTime,
                        src, dst, e);
                    return true;
                } else {
                    LOG.error("rename: failed {}-st rename src [{}] to dest [{}]", retryTime, src, dst, e);
                    return false;
                }
            } catch (IOException e) {
                if (e instanceof AccessControlException && isRegularDirPath) {
                    throw e;
                }

                try {
                    FileStatus srcFileStatus = OBSCommonUtils.innerGetFileStatusWithRetry(owner, src);
                    if (srcFileStatus.isDirectory()) {
                        newSrcKey = OBSCommonUtils.maybeAddTrailingSlash(newSrcKey);
                        newDstKey = OBSCommonUtils.maybeAddTrailingSlash(newDstKey);
                    } else if (e instanceof AccessControlException) {
                        throw e;
                    }
                } catch (FileConflictException e1) {
                    throw new AccessControlException(e);
                }

                lastException = e;
                LOG.warn("rename: failed {}-st rename src [{}] to dest [{}]", retryTime, src, dst, e);
                if (owner.exists(dst) && owner.exists(src)) {
                    LOG.warn("rename: failed {}-st rename src [{}] to " + "dest [{}] with SDK retry", retryTime, src,
                        dst, e);
                    return false;
                }

                delayMs = OBSCommonUtils.getSleepTimeInMs(retryTime);
                retryTime++;
                if (System.currentTimeMillis() - startTime + delayMs
                    < OBSCommonUtils.MAX_TIME_IN_MILLISECONDS_TO_RETRY) {
                    try {
                        Thread.sleep(delayMs);
                    } catch (InterruptedException ie) {
                        throw e;
                    }
                }
            }
        } while (System.currentTimeMillis() - startTime <= OBSCommonUtils.MAX_TIME_IN_MILLISECONDS_TO_RETRY);

        LOG.error("rename: failed {}-st rename src [{}] to dest [{}]", retryTime, src, dst, lastException);
        throw lastException;
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
                } catch (FileConflictException e) {
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
        String destKeyWithNoSuffix = owner.getTrashDir() + key;
        String destKey = destKeyWithNoSuffix;
        SimpleDateFormat df = new SimpleDateFormat("-yyyyMMddHHmmssSSS");
        if (owner.exists(new Path(destKey))) {
            destKey = destKeyWithNoSuffix + df.format(new Date());
        }
        // add timestamp when rename failed to avoid multi clients rename sources to the same target
        long startTime = System.currentTimeMillis();
        int retryTime = 0;
        long delayMs;
        while (!fsRenameToNewObject(owner, key, destKey)) {
            LOG.debug("Move file [{}] to [{}] failed, retryTime[{}].", key,
                destKey, retryTime);

            delayMs = OBSCommonUtils.getSleepTimeInMs(retryTime);
            if (System.currentTimeMillis() - startTime + delayMs
                > OBSCommonUtils.MAX_TIME_IN_MILLISECONDS_TO_RETRY) {
                LOG.error("Failed rename file [{}] to [{}] after "
                        + "retryTime[{}].", key, destKey, retryTime);
                throw new IOException("Failed to rename " + key + " to " + destKey);
            } else {
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException ie) {
                    LOG.error("Failed rename file [{}] to [{}] after "
                            + "retryTime[{}].", key, destKey, retryTime);
                    throw new IOException("Failed to rename " + key + " to " + destKey);
                }
            }
            destKey = destKeyWithNoSuffix + df.format(new Date());
            retryTime++;
        }
        LOG.debug("Moved file : '" + key + "' to trash at: " + destKey);
    }

    private static void trashFolderIfNeed(final OBSFileSystem owner, final String key)
        throws ObsException, IOException {
        if (!needToTrash(owner, key)) {
            fsRecursivelyDeleteDirWithRetry(owner, key, true);
            return;
        }

        mkTrash(owner, key);
        StringBuilder sb = new StringBuilder(owner.getTrashDir());
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

        // add timestamp when rename failed to avoid multi clients rename sources to the same target
        long startTime = System.currentTimeMillis();
        int retryTime = 0;
        long delayMs;
        while (!fsRenameToNewObject(owner, srcKey, dstKey)) {
            LOG.debug("Move folder [{}] to [{}] failed, retryTime[{}].", key,
                destKey, retryTime);

            delayMs = OBSCommonUtils.getSleepTimeInMs(retryTime);
            if (System.currentTimeMillis() - startTime + delayMs
                > OBSCommonUtils.MAX_TIME_IN_MILLISECONDS_TO_RETRY) {
                LOG.error("Failed rename folder [{}] to [{}] after "
                        + "retryTime[{}].", key, destKey, retryTime);
                throw new IOException("Failed to rename " + key + " to " + destKey);
            } else {
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException ie) {
                    LOG.error("Failed rename folder [{}] to [{}] after "
                            + "retryTime[{}].", key, destKey, retryTime);
                    throw new IOException("Failed to rename " + key + " to " + destKey);
                }
            }
            destKey = destKeyWithNoSuffix + df.format(new Date());
            dstKey = OBSCommonUtils.maybeAddTrailingSlash(destKey);
            retryTime++;
        }
        LOG.debug("Moved folder : '" + key + "' to trash at: " + destKey);
    }

    static void fsRecursivelyDeleteDirWithRetry(final OBSFileSystem owner,
        final String key, boolean deleteParent) throws IOException {
        long startTime = System.currentTimeMillis();
        long delayMs;
        int retryTime = 0;
        while (System.currentTimeMillis() - startTime <= OBSCommonUtils.MAX_TIME_IN_MILLISECONDS_TO_RETRY) {
            try {
                long delNum = fsRecursivelyDeleteDir(owner, key, deleteParent);
                LOG.debug("Recursively delete {} files/dirs when deleting {}",
                    delNum, key);
                return;
            } catch (FileConflictException e) {
                LOG.warn("Recursively delete [{}] has conflict exception, "
                    + "retryTime[{}].", key, e);
                delayMs = OBSCommonUtils.getSleepTimeInMs(retryTime);
                retryTime++;
                if (System.currentTimeMillis() - startTime + delayMs
                    < OBSCommonUtils.MAX_TIME_IN_MILLISECONDS_TO_RETRY) {
                    try {
                        Thread.sleep(delayMs);
                    } catch (InterruptedException ie) {
                        throw e;
                    }
                }
            }
        }

        fsRecursivelyDeleteDir(owner, key, deleteParent);
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
        if (owner.isEnableTrash()) {
            String trashPathKey = OBSCommonUtils.pathToKey(owner, new Path(owner.getTrashDir()));
            if (newKey.startsWith(trashPathKey)) {
                return false;
            }
        }
        return owner.isEnableTrash();
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
        StringBuilder sb = new StringBuilder(owner.getTrashDir());
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
        IOException lastException = null;
        long delayMs;
        int retryTime = 0;
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime <= OBSCommonUtils.MAX_TIME_IN_MILLISECONDS_TO_RETRY) {
            try {
                owner.getObsClient().newFolder(newFolderRequest);
                owner.getSchemeStatistics().incrementWriteOps(1);
                owner.getSchemeStatistics().incrementBytesWritten(len);
                return;
            } catch (ObsException e) {
                LOG.debug("Failed to create folder [{}], retry time [{}], " + "exception [{}]", newObjectName,
                    retryTime, e);

                IOException ioException = OBSCommonUtils.translateException("innerFsCreateFolder", newObjectName, e);
                if (!(ioException instanceof OBSIOException)) {
                    throw ioException;
                }
                lastException = ioException;

                delayMs = OBSCommonUtils.getSleepTimeInMs(retryTime);
                retryTime++;
                if (System.currentTimeMillis() - startTime + delayMs
                    < OBSCommonUtils.MAX_TIME_IN_MILLISECONDS_TO_RETRY) {
                    try {
                        Thread.sleep(delayMs);
                    } catch (InterruptedException ie) {
                        throw ioException;
                    }
                }
            }
        }
        throw lastException;
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
                return new OBSFileStatus(path, OBSCommonUtils.dateToLong(meta.getLastModified()),
                    owner.getShortUserName());
            } else {
                LOG.debug("Found file (with /): real file? should not happen: {}", key);
                return new OBSFileStatus(meta.getContentLength(), OBSCommonUtils.dateToLong(meta.getLastModified()),
                    path, owner.getDefaultBlockSize(path), owner.getShortUserName());
            }
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
            String.format("file size [%d] - file count [%d] - directory count [%d] - " + "file path [%s]", summary[0],
                summary[1], summary[2], newKey));
        return new ContentSummary.Builder().length(summary[0])
            .fileCount(summary[1])
            .directoryCount(summary[2])
            .spaceConsumed(summary[0])
            .build();
    }

    static void innerFsTruncateWithRetry(final OBSFileSystem owner, final Path f, final long newLength)
        throws IOException {
        long delayMs;
        int retryTime = 0;
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime <= OBSCommonUtils.MAX_TIME_IN_MILLISECONDS_TO_RETRY) {
            try {
                innerFsTruncate(owner, f, newLength);
                return;
            } catch (OBSIOException e) {
                OBSFileSystem.LOG.debug(
                    "Failed to truncate [{}] to newLength" + " [{}], retry time [{}], exception [{}]", f, newLength,
                    retryTime, e);

                delayMs = OBSCommonUtils.getSleepTimeInMs(retryTime);
                retryTime++;
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException ie) {
                    throw e;
                }
            }
        }

        innerFsTruncate(owner, f, newLength);
    }

    private static void innerFsTruncate(final OBSFileSystem owner, final Path f, final long newLength)
        throws IOException {
        LOG.debug("truncate {} to newLength {}", f, newLength);

        try {
            String key = OBSCommonUtils.pathToKey(owner, f);
            owner.getObsClient().truncateObject(owner.getBucket(), key, newLength);
            owner.getSchemeStatistics().incrementWriteOps(1);
        } catch (ObsException e) {
            throw OBSCommonUtils.translateException("truncate", f, e);
        }
    }
}
