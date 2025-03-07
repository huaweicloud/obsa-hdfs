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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.DirectBufferPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Set of classes to support output streaming into blocks which are then
 * uploaded as to OBS as a single PUT, or as part of a multipart request.
 */
final class OBSDataBlocks {

    /**
     * Class logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(OBSDataBlocks.class);

    private OBSDataBlocks() {
    }

    static void validateWriteArgs(final byte[] b, final int off, final int len) {
        Preconditions.checkNotNull(b);
        boolean offsetInvalid = off < 0 || off > b.length;
        boolean lenInvalid = len < 0;
        boolean endposInvalid = off + len > b.length || off + len < 0;
        boolean invalid = offsetInvalid || lenInvalid || endposInvalid;
        if (invalid) {
            throw new IndexOutOfBoundsException(String.format(Locale.ROOT, "write (b[%d], %d, %d)", b.length, off, len));
        }
    }

    /**
     * Create a factory.
     *
     * @param owner factory owner
     * @param name  factory name -the option from {@link OBSConstants}.
     * @return the factory, ready to be initialized.
     * @throws IllegalArgumentException if the name is unknown.
     */
    static BlockFactory createFactory(final OBSFileSystem owner, final String name) {
        switch (name) {
            case OBSConstants.FAST_UPLOAD_BUFFER_ARRAY:
                return new ByteArrayBlockFactory(owner);
            case OBSConstants.FAST_UPLOAD_BUFFER_DISK:
                return new DiskBlockFactory(owner);
            case OBSConstants.FAST_UPLOAD_BYTEBUFFER:
                return new ByteBufferBlockFactory(owner);
            default:
                throw new IllegalArgumentException("Unsupported block buffer" + " \"" + name + '"');
        }
    }

    enum ChecksumType {
        NONE("NONE") {
            @Override
            public String encode(byte[] bytes) {
                throw new UnsupportedOperationException("Not supported when checksum type is none");
            }
        },
        MD5("MD5") {
            @Override
            public String encode(byte[] bytes) {
                return Base64.getEncoder().encodeToString(bytes);
            }
        },
        SHA256("SHA-256") {
            @Override
            public String encode(byte[] bytes) {
                return OBSCommonUtils.toHex(bytes);
            }
        };

        private String algorithm;

        ChecksumType(String algorithm) {
            this.algorithm = algorithm;
        }

        public String getAlgorithm() {
            return algorithm;
        }

        public abstract String encode(byte[] bytes);
    }

    /**
     * Base class for block factories.
     */
    abstract static class BlockFactory {
        /**
         * OBS file system type.
         */
        private final OBSFileSystem owner;

        protected BlockFactory(final OBSFileSystem obsFileSystem) {
            this.owner = obsFileSystem;
        }

        /**
         * Create a block.
         *
         * @param index index of block
         * @param limit limit of the block.
         * @return a new block.
         * @throws IOException on any failure to create block
         */
        abstract DataBlock create(long index, int limit) throws IOException;

        protected OBSFileSystem getOwner() {
            return owner;
        }

        protected boolean calcMd5() {
            return this.getOwner()
                .getConf()
                .getBoolean(OBSConstants.OUTPUT_STREAM_ATTACH_MD5, OBSConstants.DEFAULT_OUTPUT_STREAM_ATTACH_MD5);
        }

        protected ChecksumType determineChecksumType() throws IOException {
            String type = this.getOwner()
                .getConf()
                .get(OBSConstants.FAST_UPLOAD_CHECKSUM_TYPE, OBSConstants.FAST_UPLOAD_CHECKSUM_TYPE_NONE);
            if (OBSConstants.FAST_UPLOAD_CHECKSUM_TYPE_SHA256.equals(type)) {
                return ChecksumType.SHA256;
            } else if (OBSConstants.FAST_UPLOAD_CHECKSUM_TYPE_MD5.equals(type) || calcMd5()) {
                return ChecksumType.MD5;
            } else if (OBSConstants.FAST_UPLOAD_CHECKSUM_TYPE_NONE.equals(type)) {
                return ChecksumType.NONE;
            } else {
                throw new IOException(String.format("Unsupported fast upload checksum type '%s'", type));
            }
        }

    }

    abstract static class DataBlock implements Closeable {

        /**
         * Data block index.
         */
        private final long index;

        private ChecksumType checksumType;

        private MessageDigest digest;

        private String checksum;

        /**
         * Dest state can be : writing/upload/closed.
         */
        private volatile DestState state = DestState.Writing;

        protected DataBlock(final long dataIndex, final ChecksumType checksumType) {
            this.index = dataIndex;
            if (checksumType == null) {
                this.checksumType = ChecksumType.NONE;
            } else {
                this.checksumType = checksumType;
            }

            if (checksumType != ChecksumType.NONE) {
                LOG.debug("init data block digest state to calculate checksum. checksumType: {}", checksumType);
                initDigestState();
            }
        }

        private void initDigestState() {
            try {
                digest = MessageDigest.getInstance(checksumType.getAlgorithm());
            } catch (NoSuchAlgorithmException e) {
                LOG.warn("load digest algorithm failed", e);
                this.checksumType = ChecksumType.NONE;
            }
        }

        /**
         * Atomically enter a state, verifying current state.
         *
         * @param current current state. null means "no check"
         * @param next    next state
         * @throws IllegalStateException if the current state is not as
         *                               expected
         */
        protected final synchronized void enterState(final DestState current, final DestState next)
            throws IllegalStateException {
            verifyState(current);
            LOG.debug("{}: entering state {}", this, next);
            state = next;
        }

        protected final void verifyState(final DestState expected) throws IllegalStateException {
            if (expected != null && state != expected) {
                throw new IllegalStateException(
                    "Expected stream state " + expected + " -but actual state is " + state + " in " + this);
            }
        }

        protected final DestState getState() {
            return state;
        }

        protected long getIndex() {
            return index;
        }

        abstract int dataSize();

        abstract boolean hasCapacity(long bytes);

        boolean hasData() {
            return dataSize() > 0;
        }

        abstract int remainingCapacity();

        int write(final byte[] buffer, final int offset, final int length) throws IOException {
            verifyState(DestState.Writing);
            Preconditions.checkArgument(buffer != null, "Null buffer");
            Preconditions.checkArgument(length >= 0, "length is negative");
            Preconditions.checkArgument(offset >= 0, "offset is negative");
            Preconditions.checkArgument(!(buffer.length - offset < length),
                "buffer shorter than amount of data to write");
            if (checksumType != ChecksumType.NONE) {
                digest.update(buffer, offset, length);
            }
            return 0;
        }

        void flush() throws IOException {
            verifyState(DestState.Writing);
        }

        Object startUpload() throws IOException {
            LOG.debug("Start datablock[{}] upload", index);
            enterState(DestState.Writing, DestState.Upload);
            if (checksumType != ChecksumType.NONE) {
                finalDigest();
                this.digest = null;
            }
            return null;
        }

        private void finalDigest() {
            if (digest == null) {
                LOG.warn("digest is null");
            }
            if (!getState().equals(DestState.Upload)) {
                throw new IllegalStateException("finalDigest() should in Upload state");
            }
            byte[] bytes = digest.digest();
            checksum = checksumType.encode(bytes);
        }

        public ChecksumType getChecksumType() {
            return checksumType;
        }
        
        public String getChecksum() {
            return checksum;
        }

        @Override
        public void close() throws IOException {
            if (enterClosedState()) {
                LOG.debug("Closed {}", this);
                innerClose();
            }
        }

        protected abstract void innerClose() throws IOException;

        protected synchronized boolean enterClosedState() {
            if (!state.equals(DestState.Closed)) {
                enterState(null, DestState.Closed);
                return true;
            } else {
                return false;
            }
        }

        enum DestState {
            Writing,
            Upload,
            Closed
        }
    }

    static class ByteArrayBlockFactory extends BlockFactory {
        ByteArrayBlockFactory(final OBSFileSystem owner) {
            super(owner);
        }

        @Override
        DataBlock create(final long index, final int limit) throws IOException {
            int firstBlockSize = super.owner.getConf()
                .getInt(OBSConstants.FAST_UPLOAD_BUFFER_ARRAY_FIRST_BLOCK_SIZE,
                    OBSConstants.FAST_UPLOAD_BUFFER_ARRAY_FIRST_BLOCK_SIZE_DEFAULT);
            return new ByteArrayBlock(0, limit, firstBlockSize, determineChecksumType());
        }
    }

    static class OBSByteArrayOutputStream extends ByteArrayOutputStream {
        OBSByteArrayOutputStream(final int size) {
            super(size);
        }

        ByteArrayInputStream getInputStream() {
            ByteArrayInputStream bin = new ByteArrayInputStream(this.buf, 0, count);
            this.reset();
            this.buf = null;
            return bin;
        }
    }

    static class ByteArrayBlock extends DataBlock {

        private final int limit;

        private OBSByteArrayOutputStream buffer;

        private Integer dataSize;

        private int firstBlockSize;

        private ByteArrayInputStream inputStream = null;

        ByteArrayBlock(final long index, final int limitBlockSize, final int blockSize, final ChecksumType checksumType) {
            super(index, checksumType);
            this.limit = limitBlockSize;
            this.buffer = new OBSByteArrayOutputStream(blockSize);
            this.firstBlockSize = blockSize;
        }

        @Override
        int dataSize() {
            return dataSize != null ? dataSize : buffer.size();
        }

        @Override
        boolean hasCapacity(final long bytes) {
            return dataSize() + bytes <= limit;
        }

        @Override
        int remainingCapacity() {
            return limit - dataSize();
        }

        @VisibleForTesting
        public int firstBlockSize() {
            return this.firstBlockSize;
        }

        @Override
        int write(final byte[] b, final int offset, final int len) throws IOException {
            int written = Math.min(remainingCapacity(), len);
            super.write(b, offset, written);
            buffer.write(b, offset, written);
            return written;
        }

        @Override
        protected void innerClose() throws IOException {
            if (buffer != null) {
                buffer.close();
                buffer = null;
            }

            if (inputStream != null) {
                inputStream.close();
                inputStream = null;
            }
        }

        @Override
        InputStream startUpload() throws IOException {
            super.startUpload();
            dataSize = buffer.size();
            inputStream = buffer.getInputStream();
            return inputStream;
        }

        @Override
        public String toString() {
            return "ByteArrayBlock{" + "index=" + getIndex() + ", state=" + getState() + ", limit=" + limit
                + ", dataSize=" + dataSize + '}';
        }
    }

    static class ByteBufferBlockFactory extends BlockFactory {

        private static final DirectBufferPool BUFFER_POOL = new DirectBufferPool();

        private static final AtomicInteger BUFFERS_OUTSTANDING = new AtomicInteger(0);

        ByteBufferBlockFactory(final OBSFileSystem owner) {
            super(owner);
        }

        @Override
        ByteBufferBlock create(final long index, final int limit) throws IOException {
            return new ByteBufferBlock(index, limit, determineChecksumType());
        }

        public static ByteBuffer requestBuffer(final int limit) {
            LOG.debug("Requesting buffer of size {}", limit);
            BUFFERS_OUTSTANDING.incrementAndGet();
            return BUFFER_POOL.getBuffer(limit);
        }

        public static void releaseBuffer(final ByteBuffer buffer) {
            LOG.debug("Releasing buffer");
            BUFFER_POOL.returnBuffer(buffer);
            BUFFERS_OUTSTANDING.decrementAndGet();
        }

        public int getOutstandingBufferCount() {
            return BUFFERS_OUTSTANDING.get();
        }

        @Override
        public String toString() {
            return String.format(Locale.ROOT, "ByteBufferBlockFactory{ buffersOutstanding = %d }", BUFFERS_OUTSTANDING.get());
        }
    }

    static class ByteBufferBlock extends DataBlock {

        private final int bufferSize;

        private ByteBuffer blockBuffer;

        private Integer dataSize;

        private ByteBufferInputStream inputStream;

        ByteBufferBlock(final long index, final int initBufferSize, final ChecksumType checksumType) {
            super(index, checksumType);
            this.bufferSize = initBufferSize;
            blockBuffer = ByteBufferBlockFactory.requestBuffer(initBufferSize);
        }

        private int capacityUsed() {
            return blockBuffer.capacity() - blockBuffer.remaining();
        }

        @Override
        int dataSize() {
            return dataSize != null ? dataSize : capacityUsed();
        }

        @Override
        protected void innerClose() {
            if (blockBuffer != null) {
                ByteBufferBlockFactory.releaseBuffer(blockBuffer);
                blockBuffer = null;
            }
            if (inputStream != null) {
                inputStream.close();
                inputStream = null;
            }
        }

        @Override
        public int remainingCapacity() {
            return blockBuffer != null ? blockBuffer.remaining() : 0;
        }

        @Override
        public boolean hasCapacity(final long bytes) {
            return bytes <= remainingCapacity();
        }

        @Override
        public String toString() {
            return "ByteBufferBlock{" + "index=" + getIndex() + ", state=" + getState() + ", dataSize=" + dataSize()
                + ", limit=" + bufferSize + ", remainingCapacity=" + remainingCapacity() + '}';
        }

        @Override
        int write(final byte[] b, final int offset, final int len) throws IOException {
            int written = Math.min(remainingCapacity(), len);
            super.write(b, offset, written);
            blockBuffer.put(b, offset, written);
            return written;
        }

        class ByteBufferInputStream extends InputStream {

            private final int size;

            private ByteBuffer byteBuffer;

            ByteBufferInputStream(final int streamSize, final ByteBuffer streamByteBuffer) {
                LOG.debug("Creating ByteBufferInputStream of size {}", streamSize);
                this.size = streamSize;
                this.byteBuffer = streamByteBuffer;
            }

            private void verifyOpen() throws IOException {
                if (byteBuffer == null) {
                    throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
                }
            }

            @Override
            public synchronized long skip(final long offset) throws IOException {
                verifyOpen();
                long pos = position() + offset;
                if (pos < 0) {
                    throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK);
                }
                if (pos > size) {
                    throw new EOFException(FSExceptionMessages.CANNOT_SEEK_PAST_EOF);
                }
                byteBuffer.position((int) pos);
                return pos;
            }

            @Override
            public synchronized void close() {
                LOG.debug("ByteBufferInputStream.close() for {}", ByteBufferBlock.super.toString());
                byteBuffer = null;
            }

            public synchronized int read() {
                if (available() > 0) {
                    return byteBuffer.get() & OBSCommonUtils.BYTE_TO_INT_MASK;
                } else {
                    return -1;
                }
            }

            @Override
            public synchronized int available() {
                Preconditions.checkState(byteBuffer != null, FSExceptionMessages.STREAM_IS_CLOSED);
                return byteBuffer.remaining();
            }

            public synchronized int position() {
                return byteBuffer.position();
            }

            public synchronized boolean hasRemaining() {
                return byteBuffer.hasRemaining();
            }

            @Override
            public synchronized void reset() {
                LOG.debug("reset");
                byteBuffer.reset();
            }

            public synchronized int read(final byte[] b, final int offset, final int length) throws IOException {
                Preconditions.checkArgument(length >= 0, "length is negative");
                Preconditions.checkArgument(b != null, "Null buffer");
                if (b.length - offset < length) {
                    throw new IndexOutOfBoundsException(
                        FSExceptionMessages.TOO_MANY_BYTES_FOR_DEST_BUFFER + ": request length =" + length
                            + ", with offset =" + offset + "; buffer capacity =" + (b.length - offset));
                }
                verifyOpen();
                if (!hasRemaining()) {
                    return -1;
                }

                int toRead = Math.min(length, available());
                byteBuffer.get(b, offset, toRead);
                return toRead;
            }

            @Override
            public synchronized void mark(final int readLimit) {
                LOG.debug("mark at {}", position());
                byteBuffer.mark();
            }

            @Override
            public String toString() {
                final StringBuilder sb = new StringBuilder("ByteBufferInputStream{");
                sb.append("size=").append(size);
                if (this.byteBuffer != null) {
                    sb.append(", available=").append(this.byteBuffer.remaining());
                }
                sb.append(", ").append(ByteBufferBlock.super.toString()).append('}');
                return sb.toString();
            }

            @Override
            public boolean markSupported() {
                return true;
            }
        }

        @Override
        InputStream startUpload() throws IOException {
            super.startUpload();
            dataSize = capacityUsed();
            blockBuffer.limit(blockBuffer.position());
            blockBuffer.position(0);
            inputStream = new ByteBufferInputStream(dataSize, blockBuffer);
            return inputStream;
        }
    }

    static class DiskBlockFactory extends BlockFactory {

        private static volatile LocalDirAllocator directoryAllocator;

        DiskBlockFactory(final OBSFileSystem owner) {
            super(owner);
        }

        protected boolean diskForce() {
            return this.getOwner()
                    .getConf()
                    .getBoolean(OBSConstants.OUTPUT_STREAM_DISK_FORCE_FLUSH , OBSConstants.DEFAULT_OUTPUT_STREAM_DISK_FORCE_FLUSH);
        }

        @Override
        DataBlock create(final long index, final int limit) throws IOException {
            File destFile = createTmpFileForWrite(String.format("obs-block-%04d-", index), limit, getOwner().getConf());
            return new DiskBlock(destFile, limit, index, determineChecksumType(), diskForce());
        }

        static synchronized  File createTmpFileForWrite(final String pathStr, final long size, final Configuration conf)
            throws IOException {
            if (directoryAllocator == null) {
                String bufferDir = conf.get(OBSConstants.BUFFER_DIR) != null
                        ? OBSConstants.BUFFER_DIR
                        : "hadoop.tmp.dir";
                directoryAllocator = new LocalDirAllocator(bufferDir);
            }
            Path path = directoryAllocator.getLocalPathForWrite(pathStr, size, conf);
            File dir = new File(path.getParent().toUri().getPath());
            String prefix = path.getName();

            // 获取当前时间戳
            long timestamp = System.currentTimeMillis();
            String timestampString = String.valueOf(timestamp);
            
            return File.createTempFile(prefix, timestampString, dir);
        }
    }

    static class DiskBlock extends DataBlock {

        private final File bufferFile;

        private final int limit;

        private final AtomicBoolean closed = new AtomicBoolean(false);

        private int bytesWritten;

        private boolean diskForce;

        private BufferedOutputStream outputStream;

        private FileOutputStream fileoutputstream;

        DiskBlock(final File destBufferFile, final int limitSize, final long index, final ChecksumType checksumType,
            final boolean diskForce) throws FileNotFoundException {
            super(index, checksumType);
            this.limit = limitSize;
            this.diskForce = diskForce;
            this.bufferFile = destBufferFile;
            this.fileoutputstream = new FileOutputStream(destBufferFile);
            outputStream = new BufferedOutputStream(fileoutputstream);
        }

        @Override
        int dataSize() {
            return bytesWritten;
        }

        @Override
        int write(final byte[] b, final int offset, final int len) throws IOException {
            int writeLen = Math.min(remainingCapacity(), len);
            super.write(b, offset, writeLen);
            outputStream.write(b, offset, writeLen);
            bytesWritten += writeLen;
            return writeLen;
        }

        @Override
        int remainingCapacity() {
            return limit - bytesWritten;
        }

        void closeBlock() {
            LOG.debug("block[{}]: closeBlock()", getIndex());
            if (!closed.getAndSet(true)) {
                if (!bufferFile.delete() && bufferFile.exists()) {
                    LOG.warn("delete({}) returned false", bufferFile.getAbsoluteFile());
                }
            } else {
                LOG.debug("block[{}]: skipping re-entrant closeBlock()", getIndex());
            }
        }

        @Override
        protected void innerClose() {
            final DestState state = getState();
            LOG.debug("Closing {}", this);
            switch (state) {
                case Closed:
                    closeBlock();
                    break;

                case Upload:
                    LOG.debug("Block[{}]: Buffer file {} exists —close upload stream", getIndex(), bufferFile);
                    break;

                case Writing:
                    if (bufferFile.exists()) {
                        LOG.debug("Block[{}]: Deleting buffer file as upload did not start", getIndex());
                        closeBlock();
                    }
                    break;

                default:

            }
        }

        @Override
        File startUpload() throws IOException {
            super.startUpload();
            try {
                outputStream.flush();
                if (diskForce) {
                    fileoutputstream.getChannel().force(true);
                }

            } finally {
                outputStream.close();
                outputStream = null;
            }
            return bufferFile;
        }

        @Override
        public String toString() {
            return "FileBlock{index=" + getIndex() + ", destFile=" + bufferFile + ", state=" + getState()
                + ", dataSize=" + dataSize() + ", limit=" + limit + '}';
        }

        @Override
        void flush() throws IOException {
            super.flush();
            outputStream.flush();
        }

        @Override
        boolean hasCapacity(final long bytes) {
            return dataSize() + bytes <= limit;
        }
    }
}
