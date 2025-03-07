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
import com.google.common.util.concurrent.MoreExecutors;
import com.obs.services.exception.ObsException;
import com.obs.services.model.CompleteMultipartUploadResult;
import com.obs.services.model.PartEtag;
import com.obs.services.model.PutObjectRequest;
import com.obs.services.model.UploadPartRequest;
import com.obs.services.model.UploadPartResult;
import com.obs.services.model.fs.WriteFileRequest;
import com.sun.istack.NotNull;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Syncable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * OBS output stream based on block buffering.
 * <p>
 * Upload files/parts directly via different buffering mechanisms: including
 * memory and disk.
 *
 * <p>If the stream is closed and no update has started, then the upload is
 * instead done as a single PUT operation.
 *
 * <p>Unstable: statistics and error handling might evolve.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class OBSBlockOutputStream extends OutputStream implements Syncable {
    /**
     * Class logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(OBSBlockOutputStream.class);

    /**
     * Owner FileSystem.
     */
    private final OBSFileSystem fs;

    /**
     * Key of the object being uploaded.
     */
    private final String key;

    /**
     * Object uri.
     */
    private final String uri;

    /**
     * Length of object.
     */
    private long objectLen;

    /**
     * Size of all blocks.
     */
    private final int blockSize;

    /**
     * Callback for progress.
     */
    private final ListeningExecutorService executorService;

    /**
     * Factory for creating blocks.
     */
    private final OBSDataBlocks.BlockFactory blockFactory;

    /**
     * Preallocated byte buffer for writing single characters.
     */
    private final byte[] singleCharWrite = new byte[1];

    /**
     * Closed flag.
     */
    private volatile boolean closed = false;

    /**
     * Has exception flag.
     */
    private final AtomicBoolean hasException = new AtomicBoolean(false);

    /**
     * Has flushed flag.
     */
    private final AtomicBoolean appendAble;

    /**
     * Multipart upload details; null means none started.
     */
    private MultiPartUpload multiPartUpload;

    /**
     * Current data block. Null means none currently active.
     */
    private OBSDataBlocks.DataBlock activeBlock;

    /**
     * Count of blocks uploaded.
     */
    private long blockCount = 0;

    /**
     * Write operation helper; encapsulation of the filesystem operations.
     */
    private OBSWriteOperationHelper writeOperationHelper;

    /**
     * Flag for mocking upload part error.
     */
    private boolean mockUploadPartError = false;

    /**
     * Flag for appending remove head.
     */
    private boolean appendRemoveHead = false;
    private String appendRemoveHeadKey = "fs.obs.appendRemoveHead";

    private String hflushPolicy = OBSConstants.OUTPUT_STREAM_HFLUSH_POLICY_SYNC;

    /**
     * An OBS output stream which uploads partitions in a separate pool of
     * threads; different {@link OBSDataBlocks.BlockFactory} instances can
     * control where data is buffered.
     *
     * @param owner        OBSFilesystem
     * @param obsObjectKey OBS object to work on
     * @param objLen       object length
     * @param execService  the executor service to use to schedule work
     * @param isAppendable if append is supported
     * @throws IOException on any problem
     */
    OBSBlockOutputStream(final OBSFileSystem owner, final String obsObjectKey, final long objLen,
        final ExecutorService execService, final boolean isAppendable) throws IOException {
        this.appendAble = new AtomicBoolean(isAppendable);
        this.fs = owner;
        this.key = obsObjectKey;
        this.uri = "obs://" + this.fs.getBucket() + "/" + this.key;
        this.objectLen = objLen;
        this.blockFactory = owner.getBlockFactory();
        this.blockSize = (int) owner.getPartSize();
        this.writeOperationHelper = owner.getWriteHelper();
        Preconditions.checkArgument(owner.getPartSize() >= OBSConstants.MULTIPART_MIN_SIZE,
            "Block size is too small: %d", owner.getPartSize());
        this.executorService = MoreExecutors.listeningDecorator(execService);
        this.multiPartUpload = null;
        this.hflushPolicy = owner.getConf()
            .get(OBSConstants.OUTPUT_STREAM_HFLUSH_POLICY, OBSConstants.OUTPUT_STREAM_HFLUSH_POLICY_SYNC);
        this.appendRemoveHead = owner.getConf().getBoolean(appendRemoveHeadKey, false);
        // create that first block. This guarantees that an open + close
        // sequence writes a 0-byte entry.
        createBlockIfNeeded();
        LOG.debug("Initialized OBSBlockOutputStream for {}" + " output to {}", owner.getWriteHelper(), activeBlock);
    }

    /**
     * Demand create a destination block.
     *
     * @return the active block; null if there isn't one.
     * @throws IOException on any failure to create
     */
    private synchronized OBSDataBlocks.DataBlock createBlockIfNeeded() throws IOException {
        if (activeBlock == null) {
            blockCount++;
            if (blockCount >= OBSConstants.MAX_MULTIPART_COUNT) {
                LOG.debug("Number of partitions in stream exceeds limit for OBS: " + OBSConstants.MAX_MULTIPART_COUNT
                    + " write may fail.");
            }
            activeBlock = blockFactory.create(blockCount, this.blockSize);
        }
        return activeBlock;
    }

    synchronized OBSDataBlocks.DataBlock getActiveBlock() {
        return activeBlock;
    }

    @VisibleForTesting
    void mockPutPartError(final boolean isException) {
        this.mockUploadPartError = isException;
    }

    private synchronized boolean hasActiveBlock() {
        return activeBlock != null;
    }

    private synchronized void clearActiveBlock() {
        if (activeBlock != null) {
            LOG.debug("Clearing active block");
        }
        activeBlock = null;
    }

    private void checkStreamOpen() throws IOException {
        if (closed) {
            IOException ioe = new IOException(uri + ": " + FSExceptionMessages.STREAM_IS_CLOSED);
            OBSCommonUtils.setMetricsAbnormalInfo(fs, OBSOperateAction.write, ioe);
            throw ioe;
        }
    }

    /**
     * The flush operation does not trigger an upload; that awaits the next
     * block being full. What it does do is call {@code flush() } on the current
     * block, leaving it to choose how to react.
     *
     * @throws IOException Any IO problem.
     */
    @Override
    public synchronized void flush() throws IOException {
        fs.checkOpen();
        checkStreamOpen();
        OBSDataBlocks.DataBlock dataBlock = getActiveBlock();
        if (dataBlock != null) {
            dataBlock.flush();
        }
    }

    @Override
    public synchronized void write(final int b) throws IOException {
        fs.checkOpen();
        checkStreamOpen();
        singleCharWrite[0] = (byte) b;
        write(singleCharWrite, 0, 1);
    }

    @Override
    public synchronized void write(@NotNull final byte[] source, final int offset, final int len) throws IOException {
        fs.checkOpen();
        checkStreamOpen();
        if (hasException.get()) {
            String closeWarning = String.format("write has error. bs : pre upload obs[%s] has error.", key);
            IOException ioe = new IOException(closeWarning);
            OBSCommonUtils.setMetricsAbnormalInfo(fs, OBSOperateAction.write, ioe);
            LOG.warn(closeWarning);
            throw ioe;
        }
        OBSDataBlocks.validateWriteArgs(source, offset, len);
        if (len == 0) {
            return;
        }

        OBSDataBlocks.DataBlock block = createBlockIfNeeded();
        int written = block.write(source, offset, len);
        int remainingCapacity = block.remainingCapacity();
        try {
            innerWrite(source, offset, len, written, remainingCapacity);
        } catch (IOException e) {
            OBSCommonUtils.setMetricsAbnormalInfo(fs, OBSOperateAction.write, e);
            LOG.error("Write data for key {} of bucket {} error, error message {}", key, fs.getBucket(),
                e.getMessage());
            throw e;
        }
    }

    private synchronized void innerWrite(final byte[] source, final int offset, final int len, final int written,
        final int remainingCapacity) throws IOException {

        if (written < len) {
            // not everything was written —the block has run out
            // of capacity
            // Trigger an upload then process the remainder.
            LOG.debug("writing more data than block has capacity -triggering upload");
            if (appendAble.get()) {
                // to write a buffer then append to obs
                LOG.debug("[Append] open stream and single write size {} "
                    + "greater than buffer size {}, append buffer to obs.", len, blockSize);
                flushCurrentBlock();
            } else {
                // block output stream logic, multi-part upload
                uploadCurrentBlock();
            }
            // tail recursion is mildly expensive, but given buffer sizes
            // must be MB. it's unlikely to recurse very deeply.
            this.write(source, offset + written, len - written);
        } else {
            if (remainingCapacity == 0) {
                // the whole buffer is done, trigger an upload
                if (appendAble.get()) {
                    // to write a buffer then append to obs
                    LOG.debug("[Append] open stream and already write size "
                        + "equal to buffer size {}, append buffer to obs.", blockSize);
                    flushCurrentBlock();
                } else {
                    // block output stream logic, multi-part upload
                    uploadCurrentBlock();
                }
            }
        }
    }

    /**
     * Start an asynchronous upload of the current block.
     *
     * @throws IOException Problems opening the destination for upload or
     *                     initializing the upload.
     */
    private synchronized void uploadCurrentBlock() throws IOException {
        Preconditions.checkState(hasActiveBlock(), "No active block");
        LOG.debug("Writing block # {}", blockCount);
        try {
            if (multiPartUpload == null) {
                LOG.debug("Initiating Multipart upload");
                multiPartUpload = new MultiPartUpload();
            }
            multiPartUpload.uploadBlockAsync(getActiveBlock());
        } catch (IOException e) {
            hasException.set(true);
            LOG.error("Upload current block on ({}/{}) failed.", fs.getBucket(), key, e);
            throw e;
        } finally {
            clearActiveBlock();
        }
    }

    /**
     * Close the stream.
     *
     * <p>This will not return until the upload is complete or the attempt to
     * perform the upload has failed. Exceptions raised in this method are
     * indicative that the write has failed and data is at risk of being lost.
     *
     * @throws IOException on any failure.
     */
    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            // already closed
            LOG.debug("Ignoring close() as stream is already closed");
            return;
        }

        if (hasException.get()) {
            String closeWarning = String.format("closed has error. bs : pre write obs[%s] has error.", key);
            LOG.warn(closeWarning);
            fs.removeFileBeingWritten(key);
            IOException ioe = new IOException(closeWarning);
            OBSCommonUtils.setMetricsAbnormalInfo(fs, OBSOperateAction.write, ioe);
            throw ioe;
        }

        fs.checkOpen();

        // do upload
        completeCurrentBlock();

        // clear
        clearHFlushOrSync();

        // All end of write operations, including deleting fake parent
        // directories
        writeOperationHelper.writeSuccessful(key);
        fs.removeFileBeingWritten(key);
        closed = true;
    }

    /**
     * If flush has take place, need to append file, else to put object.
     *
     * @throws IOException any problem in append or put object
     */
    private synchronized void putObjectIfNeedAppend() throws IOException {
        if(appendRemoveHead){
            if (appendAble.get()) {
                appendFsFile();
            } else {
                putObject();
            }
            return;
        }
        FileStatus fileStatus = null;
        boolean exists = true;
        if (appendAble.get()) {
            try {
                fileStatus = fs.getFileStatus(OBSCommonUtils.keyToPath(key));
                exists = fileStatus != null;
            } catch (FileNotFoundException e) {
                exists = false;
            }
        }

        if (appendAble.get() && exists) {
            appendFsFile(fileStatus);
        } else {
            putObject();
        }
    }

    /**
     * Append posix file.
     *
     * @throws IOException any problem
     */
    private synchronized void appendFsFile(FileStatus fileStatus) throws IOException {
        LOG.debug("bucket is posix, to append file. key is {}", key);
        final OBSDataBlocks.DataBlock block = getActiveBlock();
        if (block == null) {
            throw new IOException("block is null");
        }
        WriteFileRequest writeFileReq;
        if (block instanceof OBSDataBlocks.DiskBlock) {
            writeFileReq = OBSCommonUtils.newAppendFileRequest(fs, key, objectLen, (File) block.startUpload(), fileStatus);
        } else {
            writeFileReq = OBSCommonUtils.newAppendFileRequest(fs, key, objectLen, (InputStream) block.startUpload(), fileStatus);
        }
        try {
            OBSCommonUtils.appendFile(fs, writeFileReq);
            objectLen += block.dataSize();
        }finally {
            if (writeFileReq.getInput() != null) {
                writeFileReq.getInput().close();
            }
        }
    }

    /**
     * Append posix file without heading.
     *
     * @throws IOException any problem
     */
    private synchronized void appendFsFile() throws IOException {
        LOG.debug("bucket is posix, to append file. key is {}", key);
        final OBSDataBlocks.DataBlock block = getActiveBlock();
        if (block == null) {
            throw new IOException("block is null");
        }
        WriteFileRequest writeFileReq;
        if (block instanceof OBSDataBlocks.DiskBlock) {
            writeFileReq = OBSCommonUtils.newAppendFileRequest(fs, key, objectLen, (File) block.startUpload());
        } else {
            writeFileReq = OBSCommonUtils.newAppendFileRequest(fs, key, objectLen, (InputStream) block.startUpload());
        }
        try {
            OBSCommonUtils.appendFile(fs, writeFileReq);
            objectLen += block.dataSize();
        }finally {
            if (writeFileReq.getInput() != null) {
                writeFileReq.getInput().close();
            }
        }
    }

    /**
     * Upload the current block as a single PUT request; if the buffer is empty
     * a 0-byte PUT will be invoked, as it is needed to create an entry at the
     * far end.
     *
     * @throws IOException any problem.
     */
    private synchronized void putObject() throws IOException {
        LOG.debug("Executing regular upload for {}", writeOperationHelper.toString(key));

        final OBSDataBlocks.DataBlock block = getActiveBlock();
        clearActiveBlock();
        if (block == null) {
            throw new IOException("block is null");
        }
        final int size = block.dataSize();
        final PutObjectRequest putObjectRequest;
        if (block instanceof OBSDataBlocks.DiskBlock) {
            putObjectRequest = writeOperationHelper.newPutRequest(key, (File) block.startUpload(),
                block.getChecksumType(), block.getChecksum()); // getChecksum should be called after startUpload
        } else {
            putObjectRequest = writeOperationHelper.newPutRequest(key, (InputStream) block.startUpload(), size,
                block.getChecksumType(), block.getChecksum()); // getChecksum should be called after startUpload
        }
        putObjectRequest.setAcl(fs.getCannedACL());
        fs.getSchemeStatistics().incrementWriteOps(1);
        try {
            // the putObject call automatically closes the input
            // stream afterwards.
            writeOperationHelper.putObject(putObjectRequest);
            objectLen += size;
        } finally {
            OBSCommonUtils.closeAll(putObjectRequest.getInput(), block);
        }
    }

    @Override
    public synchronized String toString() {
        final StringBuilder sb = new StringBuilder("OBSBlockOutputStream{");
        sb.append(writeOperationHelper.toString());
        sb.append(", blockSize=").append(blockSize);
        OBSDataBlocks.DataBlock block = activeBlock;
        if (block != null) {
            sb.append(", activeBlock=").append(block);
        }
        sb.append('}');
        return sb.toString();
    }

    public synchronized void sync() throws IOException {
        fs.checkOpen();
        checkStreamOpen();
        // need to do
    }

    @Override
    public synchronized void hflush() throws IOException {
        fs.checkOpen();
        checkStreamOpen();
        switch (this.hflushPolicy) {
            case OBSConstants.OUTPUT_STREAM_HFLUSH_POLICY_SYNC:
                // hflush hsyn same
                flushOrSync();
                break;
            case OBSConstants.OUTPUT_STREAM_HFLUSH_POLICY_FLUSH:
                flush();
                break;

            case OBSConstants.OUTPUT_STREAM_HFLUSH_POLICY_EMPTY:
                // do nothing
                break;

            default:
                throw new IOException(String.format("unsupported downgrade policy '%s'", this.hflushPolicy));
        }
    }

    /**
     * Flush local file or multipart to obs. focus: not posix bucket is not
     * support
     *
     * @throws IOException io exception
     */
    private synchronized void flushOrSync() throws IOException {

        checkStreamOpen();
        if (hasException.get()) {
            String flushWarning = String.format("flushOrSync has error. bs : pre write obs[%s] has error.", key);
            LOG.warn(flushWarning);
            throw new IOException(flushWarning);
        }
        if (fs.isFsBucket()) {
            // upload
            flushCurrentBlock();

            // clear
            clearHFlushOrSync();
        } else {
            LOG.warn("not posix bucket, not support hflush or hsync.");
            flush();
        }
    }

    /**
     * Clear for hflush or hsync.
     */
    private synchronized void clearHFlushOrSync() {
        appendAble.set(true);
        multiPartUpload = null;
    }

    /**
     * Upload block to obs.
     *
     * @param block    block
     * @param hasBlock jungle if has block
     * @throws IOException io exception
     */
    private synchronized void uploadWriteBlocks(final OBSDataBlocks.DataBlock block, final boolean hasBlock)
        throws IOException {
        if (multiPartUpload == null) {
            if (hasBlock) {
                // no uploads of data have taken place, put the single block
                // up. This must happen even if there is no data, so that 0 byte
                // files are created.
                putObjectIfNeedAppend();
            }
        } else {
            // there has already been at least one block scheduled for upload;
            // put up the current then wait
            if (hasBlock) {
                if (block == null) {
                    throw new IOException("block is null");
                }

                if (block.hasData()) {
                    // send last part
                    uploadCurrentBlock();
                }
            }
            // wait for the partial uploads to finish
            final List<Pair<PartEtag, Integer>> partETags = multiPartUpload.waitForAllPartUploads();
            List<PartEtag> listPartETags = new ArrayList<>();
            int countSize = 0;
            for (Pair<PartEtag, Integer> pair : partETags) {
                listPartETags.add(pair.getKey());
                countSize += pair.getValue();
            }
            // then complete the operation
            multiPartUpload.complete(listPartETags);
            //update object len
            objectLen = countSize;
        }
        LOG.debug("Upload complete for {}", writeOperationHelper.toString(key));
    }

    private synchronized void completeCurrentBlock() throws IOException {
        OBSDataBlocks.DataBlock block = getActiveBlock();
        boolean hasBlock = hasActiveBlock();
        LOG.debug("{}: complete block #{}: current block= {}", this, blockCount, hasBlock ? block : "(none)");
        try {
            uploadWriteBlocks(block, hasBlock);
        } catch (IOException ioe) {
            LOG.error("Upload data to obs error. io exception : {}", ioe.getMessage());
            throw ioe;
        } catch (Exception e) {
            LOG.error("Upload data to obs error. other exception : {}", e.getMessage());
            throw e;
        } finally {
            OBSCommonUtils.closeAll(block);
            clearActiveBlock();
            fs.removeFileBeingWritten(key);
        }
    }

    private synchronized void flushCurrentBlock() throws IOException {
        OBSDataBlocks.DataBlock block = getActiveBlock();
        boolean hasBlock = hasActiveBlock();
        LOG.debug("{}: complete block #{}: current block= {}", this, blockCount, hasBlock ? block : "(none)");
        try {
            uploadWriteBlocks(block, hasBlock);
        } catch (IOException ioe) {
            LOG.error("hflush data to obs error. io exception : {}", ioe.getMessage());
            hasException.set(true);
            throw ioe;
        } catch (Exception e) {
            LOG.error("hflush data to obs error. other exception : {}", e.getMessage());
            hasException.set(true);
            throw e;
        } finally {
            OBSCommonUtils.closeAll(block);
            clearActiveBlock();
        }
    }

    @Override
    public synchronized void hsync() throws IOException {
        fs.checkOpen();
        checkStreamOpen();
        switch (this.hflushPolicy) {
            case OBSConstants.OUTPUT_STREAM_HFLUSH_POLICY_SYNC:
                // hflush hsyn same
                flushOrSync();
                break;
            case OBSConstants.OUTPUT_STREAM_HFLUSH_POLICY_FLUSH:
                sync();
                break;

            case OBSConstants.OUTPUT_STREAM_HFLUSH_POLICY_EMPTY:
                // do nothing
                break;

            default:
                throw new IOException(String.format("unsupported downgrade policy '%s'", this.hflushPolicy));
        }
    }

    /**
     * Multiple partition upload.
     */
    private class MultiPartUpload {
        /**
         * Upload id for multipart upload.
         */
        private final String uploadId;

        /**
         * List for async part upload future.
         */
        private final List<ListenableFuture<Pair<PartEtag, Integer>>> partETagsFutures;

        MultiPartUpload() throws IOException {
            this.uploadId = writeOperationHelper.initiateMultiPartUpload(key);
            this.partETagsFutures = new ArrayList<>(2);
            LOG.debug("Initiated multi-part upload for {} with , the key is {}" + "id '{}'", writeOperationHelper,
                uploadId, key);
        }

        /**
         * Upload a block of data asynchronously.
         *
         * @param block block to upload
         * @throws IOException upload failure
         */
        private void uploadBlockAsync(@NotNull final OBSDataBlocks.DataBlock block) throws IOException {
            LOG.debug("Queueing upload of {}", block);
            if (block == null) {
                throw new IOException("block is null");
            }
            final int size = block.dataSize();
            final int currentPartNumber = partETagsFutures.size() + 1;
            final UploadPartRequest request;
            if (block instanceof OBSDataBlocks.DiskBlock) {
                request = writeOperationHelper.newUploadPartRequest(key, uploadId, currentPartNumber, size,
                    (File) block.startUpload(), block.getChecksumType(), block.getChecksum()); // getChecksum should be called after startUpload
            } else {
                request = writeOperationHelper.newUploadPartRequest(key, uploadId, currentPartNumber, size,
                    (InputStream) block.startUpload(), block.getChecksumType(), block.getChecksum()); // getChecksum should be called after startUpload
            }
            ListenableFuture<Pair<PartEtag, Integer>> partETagFuture = executorService.submit(() -> {
                // this is the queued upload operation
                LOG.debug("Uploading part {} for id '{}'", currentPartNumber, uploadId);
                // do the upload
                PartEtag partETag = null;
                try {
                    if (mockUploadPartError) {
                        throw new ObsException("mock upload part error");
                    }
                    UploadPartResult uploadPartResult = OBSCommonUtils.uploadPart(fs, request);
                    partETag = new PartEtag(uploadPartResult.getEtag(), uploadPartResult.getPartNumber());
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Completed upload of {} to part {}", block, partETag);
                    }
                } catch (IOException e) {
                    hasException.set(true);
                    throw e;
                } finally {
                    OBSCommonUtils.closeAll(request.getInput(), block);
                }
                return new Pair<PartEtag, Integer>(partETag, size);
            });
            partETagsFutures.add(partETagFuture);
        }

        private List<Pair<PartEtag, Integer>> waitForAllPartUploads() throws IOException {
            LOG.debug("Waiting for {} uploads to complete", partETagsFutures.size());
            try {
                return Futures.allAsList(partETagsFutures).get();
            } catch (InterruptedException ie) {
                LOG.warn("Interrupted partUpload", ie);
                LOG.debug("Cancelling futures");
                for (ListenableFuture<Pair<PartEtag, Integer>> future : partETagsFutures) {
                    future.cancel(true);
                }
                // abort multipartupload
                this.abort();
                throw new IOException("Interrupted multi-part upload with id '" + uploadId + "' to " + key);
            } catch (ExecutionException ee) {
                // there is no way of recovering so abort
                // cancel all partUploads
                LOG.debug("While waiting for upload completion", ee);
                LOG.debug("Cancelling futures");
                for (ListenableFuture<Pair<PartEtag, Integer>> future : partETagsFutures) {
                    future.cancel(true);
                }
                // abort multipartupload
                this.abort();
                throw OBSCommonUtils.extractException("Multi-part upload with id '" + uploadId + "' to " + key, key,
                    ee);
            }
        }

        /**
         * This completes a multipart upload. Sometimes it fails; here retries
         * are handled to avoid losing all data on a transient failure.
         *
         * @param partETags list of partial uploads
         * @return result for completing multipart upload
         * @throws IOException on any problem
         */
        private CompleteMultipartUploadResult complete(final List<PartEtag> partETags) throws IOException {
            String operation = String.format(
                "Completing multi-part upload for key '%s'," + " id '%s' with %s partitions ", key, uploadId,
                partETags.size());
            try {
                LOG.debug(operation);
                return writeOperationHelper.completeMultipartUpload(key, uploadId, partETags);
            } catch (ObsException e) {
                throw OBSCommonUtils.translateException(operation, key, e);
            }
        }

        /**
         * Abort a multi-part upload. Retries are attempted on failures.
         * IOExceptions are caught; this is expected to be run as a cleanup
         * process.
         */
        void abort() {
            String operation = String.format("Aborting multi-part upload for '%s', id '%s", writeOperationHelper,
                uploadId);
            try {
                LOG.debug(operation);
                writeOperationHelper.abortMultipartUpload(key, uploadId);
            } catch (ObsException e) {
                LOG.warn("Unable to abort multipart upload, you may need to purge  " + "uploaded parts", e);
            }
        }
    }
}
