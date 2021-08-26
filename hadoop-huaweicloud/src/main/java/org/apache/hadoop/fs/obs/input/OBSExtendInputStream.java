package org.apache.hadoop.fs.obs.input;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.MoreExecutors;
import com.obs.services.ObsClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.CanSetReadahead;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.obs.OBSConstants;
import org.apache.hadoop.fs.obs.OBSFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ExecutorService;

/**
 * The input stream for OSS blob system.
 * The class uses multi-part downloading to read data from the object content
 * stream.
 */
public class OBSExtendInputStream extends FSInputStream implements CanSetReadahead, ByteBufferReadable {
    public static final Logger LOG = LoggerFactory.getLogger(OBSExtendInputStream.class);

    private OBSFileSystem fs;

    private final ObsClient client;

    private Statistics statistics;

    private final String bucketName;

    private final String key;

    private long contentLength;

    private boolean closed;

    private int maxReadAhead;

    private long readaheadSize;

    private long pos;

    private long nextPos;

    private long lastBufferStart;

    private byte[] buffer;

    private long bufferRemaining;

    private ExecutorService readAheadExecutorService;

    private Queue<ReadAheadBuffer> readAheadBufferQueue = new ArrayDeque<>();

    public OBSExtendInputStream(final OBSFileSystem obsFileSystem, Configuration conf,
        ExecutorService readAheadExecutorService, String bucketName, String key, Long contentLength,
        Statistics statistics) {
        LOG.info("use OBSExtendInputStream");
        this.fs = obsFileSystem;
        this.client = fs.getObsClient();
        this.statistics = statistics;

        this.bucketName = bucketName;
        this.key = key;
        this.contentLength = contentLength;

        readaheadSize = conf.getLong(OBSConstants.READAHEAD_RANGE, OBSConstants.DEFAULT_READAHEAD_RANGE);
        this.maxReadAhead = conf.getInt(OBSConstants.READAHEAD_MAX_NUM, OBSConstants.DEFAULT_READAHEAD_MAX_NUM);
        this.readAheadExecutorService = MoreExecutors.listeningDecorator(readAheadExecutorService);

        this.nextPos = 0;
        this.lastBufferStart = -1;

        pos = 0;
        bufferRemaining = 0;
        closed = false;
    }

    private void validateAndResetReopen(long pos) throws EOFException {
        if (pos < 0) {
            throw new EOFException("Cannot seek at negative position:" + pos);
        } else if (pos > contentLength) {
            throw new EOFException("Cannot seek after EOF, contentLength:" + contentLength + " position:" + pos);
        }
        if (this.buffer != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Aborting old stream to open at pos " + pos);
            }
            this.buffer = null;
        }
    }

    private boolean isRandom(long position) {

        boolean isRandom = true;

        if (position == this.nextPos) {
            isRandom = false;
        } else {
            //new seek, remove cache buffers if its byteStart is not equal to pos
            while (readAheadBufferQueue.size() != 0) {
                if (readAheadBufferQueue.element().getByteStart() != position) {
                    readAheadBufferQueue.poll();
                } else {
                    break;
                }
            }
        }
        return isRandom;
    }

    private void getFromBuffer() throws IOException {

        ReadAheadBuffer readBuffer = readAheadBufferQueue.poll();
        readBuffer.lock();
        try {
            readBuffer.await(ReadAheadBuffer.STATUS.INIT);
            if (readBuffer.getStatus() == ReadAheadBuffer.STATUS.ERROR) {
                this.buffer = null;
            } else {
                this.buffer = readBuffer.getBuffer();
            }
        } catch (InterruptedException e) {
            LOG.warn("interrupted when wait a read buffer");
        } finally {
            readBuffer.unlock();
        }

        if (this.buffer == null) {
            throw new IOException("Null IO stream");
        }
    }

    /**
     * Reopen the wrapped stream at give position, by seeking for
     * data of a part length from object content stream.
     *
     * @param position position from start of a file
     * @throws IOException if failed to reopen
     */
    private synchronized void reopen(long position) throws IOException {
        validateAndResetReopen(position);

        long partSize = position + readaheadSize > contentLength ? contentLength - position : readaheadSize;
        boolean isRandom = isRandom(position);
        this.nextPos = position + partSize;
        int currentSize = readAheadBufferQueue.size();
        if (currentSize == 0) {
            lastBufferStart = position - partSize;
        } else {
            ReadAheadBuffer[] readBuffers = readAheadBufferQueue.toArray(new ReadAheadBuffer[currentSize]);
            lastBufferStart = readBuffers[currentSize - 1].getByteStart();
        }

        int maxLen = this.maxReadAhead - currentSize;
        for (int i = 0; i < maxLen && i < (currentSize + 1) * 2; i++) {
            if (lastBufferStart + partSize * (i + 1) >= contentLength) {
                break;
            }

            long byteStart = lastBufferStart + partSize * (i + 1);
            long byteEnd = byteStart + partSize - 1;
            if (byteEnd >= contentLength) {
                byteEnd = contentLength - 1;
            }

            ReadAheadBuffer readBuffer = new ReadAheadBuffer(byteStart, byteEnd);
            if (readBuffer.getBuffer().length == 0) {
                readBuffer.setStatus(ReadAheadBuffer.STATUS.SUCCESS);
            } else {
                this.readAheadExecutorService.execute(new ReadAheadTask(bucketName, key, client, readBuffer));
            }
            readAheadBufferQueue.add(readBuffer);
            if (isRandom) {
                break;
            }
        }
        getFromBuffer();
        pos = position;
        bufferRemaining = partSize;
    }

    @Override
    public synchronized int read() throws IOException {
        checkNotClosed();
        if (bufferRemaining <= 0 && pos < contentLength) {
            reopen(pos);
        }

        int byteRead = -1;
        if (bufferRemaining != 0) {
            byteRead = this.buffer[this.buffer.length - (int) bufferRemaining] & 0xFF;
        }
        if (byteRead >= 0) {
            pos++;
            bufferRemaining--;
        }

        incrementBytesRead(byteRead);

        return byteRead;
    }

    private void checkNotClosed() throws IOException {
        if (closed) {
            throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
        }
    }

    private void validateReadArgs(byte[] buf, int off, int len) {
        if (buf == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > buf.length - off) {
            throw new IndexOutOfBoundsException();
        }
    }

    private void incrementBytesRead(long bytesRead) {
        if (statistics != null && bytesRead > 0) {
            statistics.incrementBytesRead(bytesRead);
        }
    }

    @Override
    public synchronized int read(byte[] buf, int off, int len) throws IOException {
        checkNotClosed();
        validateReadArgs(buf, off, len);
        if (len == 0) {
            return 0;
        }
        int byteRead = 0;
        while (pos < contentLength && byteRead < len) {
            if (bufferRemaining == 0) {
                reopen(pos);
            }

            int bytes = 0;
            for (int i = this.buffer.length - (int) bufferRemaining; i < this.buffer.length; i++) {
                buf[off + byteRead] = this.buffer[i];
                bytes++;
                byteRead++;
                if (off + byteRead >= len) {
                    break;
                }
            }

            if (bytes > 0) {
                pos += bytes;
                bufferRemaining -= bytes;
            } else if (bufferRemaining != 0) {
                throw new IOException("Failed to read from stream. Remaining:" + bufferRemaining);
            }
        }

        incrementBytesRead(byteRead);

        if (byteRead == 0 && len > 0) {
            return -1;
        } else {
            return byteRead;
        }
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        this.buffer = null;
    }

    @Override
    public synchronized int available() throws IOException {
        checkNotClosed();

        long remain = contentLength - pos;
        if (remain > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return (int) remain;
    }

    @Override
    public synchronized void seek(long position) throws IOException {
        checkNotClosed();
        if (position < 0) {
            throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK + " " + position);
        }

        if (this.contentLength <= 0) {
            return;
        }
        if (pos == position) {
            return;
        } else if (position > pos && position < pos + bufferRemaining) {
            long len = position - pos;
            pos = position;
            bufferRemaining -= len;
        } else {
            pos = position;
            bufferRemaining = 0;
        }
    }

    @Override
    public synchronized long getPos() throws IOException {
        checkNotClosed();
        return pos;
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        checkNotClosed();
        return false;
    }

    @Override
    public int read(ByteBuffer byteBuffer) throws IOException {
        int len = byteBuffer.remaining();
        if (len == 0) {
            return 0;
        }

        byte[] buf = new byte[len];
        int size = read(buf, 0, len);
        if (size != -1) {
            byteBuffer.put(buf, 0, size);
        }

        return size;
    }

    @Override
    public synchronized void setReadahead(Long readahead) throws IOException {
        checkNotClosed();
        if (readahead == null) {
            this.readaheadSize = OBSConstants.DEFAULT_READAHEAD_RANGE;
        } else {
            Preconditions.checkArgument(readahead >= 0, "Negative readahead value");
            this.readaheadSize = readahead;
        }
    }

}
