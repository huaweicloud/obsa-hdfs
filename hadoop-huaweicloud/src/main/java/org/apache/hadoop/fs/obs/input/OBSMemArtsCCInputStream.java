package org.apache.hadoop.fs.obs.input;

import com.google.common.base.Preconditions;

import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.CanSetReadahead;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.obs.OBSCommonUtils;
import org.apache.hadoop.fs.obs.OBSConstants;
import org.apache.hadoop.fs.obs.OBSFileStatus;
import org.apache.hadoop.fs.obs.OBSFileSystem;
import org.apache.hadoop.fs.obs.TrafficStatistics;
import org.apache.hadoop.fs.obs.memartscc.MemArtsCCClient;
import org.apache.hadoop.fs.obs.memartscc.MemArtsCCInputStream;
import org.apache.hadoop.fs.obs.memartscc.MemArtsCCInputStreamBase;
import org.apache.hadoop.fs.obs.memartscc.MemArtsCCInputStreamWithPool;
import org.apache.hadoop.fs.obs.memartscc.OBSInputStreamSimulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 功能描述
 *
 * @since 2021-05-19
 */
public class OBSMemArtsCCInputStream extends FSInputStream implements CanSetReadahead, ByteBufferReadable {
    /**
     * Class logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(OBSMemArtsCCInputStream.class);

    /**
     * The State of this inputStream:
     * New: newly created
     * ORead: read from OBS directly
     * MRead: read from MemArtsCC
     * Close: closed
     */
    State state;

    /**
     * The statistics for OBS file system.
     */
    private final FileSystem.Statistics statistics;

    /**
     * MemArtsCC client.
     */
    private final MemArtsCCClient memArtsCCClient;

    /**
     * Bucket name.
     */
    private final String bucket;

    /**
     * Object key.
     */
    private final String key;

    /**
     * Content length.
     */
    private final long contentLength;

    /**
     * Object uri.
     */
    private final String uri;

    /**
     * Obs file system instance.
     */
    private OBSFileSystem fs;

    /**
     * Closed bit. Volatile so reads are non-blocking. Updates must be in a
     * synchronized block to guarantee an atomic check and set
     */
    private volatile boolean closed = false;

    /**
     * This is the actual position within the object, used by lazy seek to
     * decide whether to seek on the next read or not.
     */
    private long nextReadPos = 0;

    /**
     * InputStream used for read from MemArtsCC
     */
    private MemArtsCCInputStreamBase ccStream;

    /**
     * MemArtsCC ccRead fail, or return CacheMiss, use this input stream to escape
     */
    OBSMemArtsPartnerInputStream partnerInputStream;

    private int bufSize;

    private byte[] tailBuf;

    private Counter oCounter;

    private Counter mCounter;

    private TrafficStatistics trafficStatistics;

    private final OBSInputStreamSimulator oisSimulator;

    public OBSMemArtsCCInputStream(final String bucketName, final String objectKey, final long fileStatusLength,
        final FileSystem.Statistics stats, final long readAheadRangeValue, final long memartsccReadAheadRangeValue,
        final OBSFileSystem obsFileSystem, OBSFileStatus fileStatus) {
        Preconditions.checkArgument(OBSCommonUtils.isStringNotEmpty(bucketName), "No Bucket");
        Preconditions.checkArgument(OBSCommonUtils.isStringNotEmpty(objectKey), "No Key");
        Preconditions.checkArgument(fileStatusLength >= 0, "Negative content length");
        this.bucket = bucketName;
        this.key = objectKey;
        this.contentLength = fileStatusLength;
        this.memArtsCCClient = obsFileSystem.getMemArtsCCClient();
        this.statistics = stats;
        this.uri = "obs://" + this.bucket + "/" + this.key;
        this.fs = obsFileSystem;
        this.partnerInputStream = new OBSMemArtsPartnerInputStream(bucketName, objectKey, fileStatusLength,
            obsFileSystem.getObsClient(), stats, readAheadRangeValue, obsFileSystem, this);

        // the initial state of the stream
        this.state = State.MREAD;

        this.bufSize = obsFileSystem.getConf()
            .getInt(OBSConstants.MEMARTSCC_BUFFER_SIZE, OBSConstants.DEFAULT_MEMARTSCC_BUFFER_SIZE);
        int directBufferSize = obsFileSystem.getConf()
            .getInt(OBSConstants.MEMARTSCC_DIRECTBUFFER_SIZE, OBSConstants.DEFAULT_MEMARTSCC_DIRECTBUFFER_SIZE);
        String inputSteamType = obsFileSystem.getConf()
            .get(OBSConstants.MEMARTSCC_INPUTSTREAM_BUFFER_TYPE, OBSConstants.MEMARTSCC_INPUTSTREAM_BUFFER_TYPE_POOL);
        if (inputSteamType.equals(OBSConstants.MEMARTSCC_INPUTSTREAM_BUFFER_TYPE_BIND)) {
            this.ccStream = new MemArtsCCInputStream(
                memArtsCCClient, objectKey, fileStatus, memartsccReadAheadRangeValue, stats, bufSize, directBufferSize);
        } else if (inputSteamType.equals(OBSConstants.MEMARTSCC_INPUTSTREAM_BUFFER_TYPE_POOL)) {
            int borrowTimeout = obsFileSystem.getConf()
                .getInt(OBSConstants.MEMARTSCC_INPUTSTREAM_BUFFER_BORROW_TIMEOUT,
                    OBSConstants.MEMARTSCC_INPUTSTREAM_BUFFER_BORROW_DEFAULT_TIMEOUT);
            this.ccStream = new MemArtsCCInputStreamWithPool(
                memArtsCCClient, objectKey, fileStatus, memartsccReadAheadRangeValue, stats, borrowTimeout);
        } else {
            throw new IllegalArgumentException("invalid input stream type:" + inputSteamType);
        }
        this.oCounter = new Counter();
        this.mCounter = new Counter();

        oisSimulator = new OBSInputStreamSimulator(fileStatusLength, readAheadRangeValue);
        initTrafficReport(obsFileSystem);

        LOG.debug("create OBSMemArtsCCInputStream[{}] for file {}", this.hashCode(), objectKey);
    }

    private void initTrafficReport(OBSFileSystem obsFileSystem) {
        // traffic statistics report
        trafficStatistics = obsFileSystem.getTrafficStatistics();
        partnerInputStream.setTrafficStaticsClass(trafficStatistics);
        ccStream.setTrafficStaticsClass(trafficStatistics);
    }

    public synchronized int available() throws IOException {
        fs.checkOpen();
        checkStreamOpen();

        long remaining = this.contentLength - this.nextReadPos;
        if (remaining > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }

        return (int) remaining;
    }

    /**
     * {@inheritDoc}
     *
     * <p>This updates the statistics on read operations started and whether or
     * not the read operation "completed", that is: returned the exact number of
     * bytes requested.
     *
     * @throws IOException if there are other problems
     */
    @Override
    public synchronized int read(final byte[] buf, final int off, final int len) throws IOException {
        LOG.debug("read(buf,off,len), offset[{}], len[{}] ", off, len);
        fs.checkOpen();
        checkStreamOpen();

        long startTime = System.currentTimeMillis();
        long threadId = Thread.currentThread().getId();
        long endTime;

        validatePositionedReadArgs(nextReadPos, buf, off, len);
        if (len == 0) {
            return 0;
        }

        boolean isTrue = this.contentLength == 0 || nextReadPos >= contentLength;
        if (isTrue) {
            return -1;
        }

        // when the position of read is in the tail of the file
        int bytesRead = 0;
        int posInTail = posInTailBuf(nextReadPos);
        if (posInTail != -1 && tailBuf != null) {
            int copyLen = (int)(contentLength - nextReadPos);
            copyLen = Math.min(copyLen, Math.min(len, buf.length - off));
            System.arraycopy(tailBuf, posInTail, buf, off, copyLen);
            nextReadPos += copyLen;
            this.partnerInputStream.seek(nextReadPos);
            this.ccStream.seek(nextReadPos);
            oisSimulator.seek(nextReadPos);
            incrementBytesRead(copyLen);
            return copyLen;
        }

        bytesRead = readInState(buf, off, len, false);

        if (bytesRead > 0) {
            nextReadPos += bytesRead;
        }

        endTime = System.currentTimeMillis();
        long position = bytesRead >= 0 ? nextReadPos - 1 : nextReadPos;
        LOG.debug("Read-3args uri:{}, contentLength:{}, destLen:{}, readLen:{}, "
                + "position:{}, thread:{}, timeUsedMilliSec:{}", uri, contentLength, len, bytesRead, position, threadId,
            endTime - startTime);
        return bytesRead;
    }

    private int readInputStream(final InputStream is, final byte[] buf, final int off,
        final int len, boolean oneByteRead, final Counter counter, final State state)
        throws IOException {

        int ret;
        int readLen;
        long start;
        long end;
        long pos = this.nextReadPos;

        start = System.nanoTime();
        if (oneByteRead) {
            ret = is.read();
            readLen = 1;
        } else {
            ret = is.read(buf, off, len);
            readLen = ret;
        }
        end = System.nanoTime();
        counter.increase(end - start, readLen);
        LOG.debug("{} {}({},{},{})", this.hashCode(), state, pos, readLen, end - start);
        return ret;
    }

    private int readInState(final byte[] buf, final int off, final int len, boolean oneByteRead) throws IOException {
        switch (this.state) {
            case NEW:
                this.partnerInputStream.reopen("open first connection", this.nextReadPos, len);

                // simulate reopen without passing through MemArts
                long readBytes = oisSimulator.reopen(this.nextReadPos, len);
                increaseSimTraffic(readBytes);

                stateTransitionToORead();
                // continue to oread
            case OREAD:
                try {
                    // lazy seek
                    this.partnerInputStream.seek(this.nextReadPos);
                    oisSimulator.seek(nextReadPos);

                    // read
                    int ret = readInputStream(this.partnerInputStream, buf, off, len, oneByteRead, oCounter, State.OREAD);

                    // simulate the read without passing through MemArts
                    long obsReadBytes = oisSimulator.read(oneByteRead ? 1 : len);
                    increaseSimTraffic(obsReadBytes);

                    return ret;
                } catch (OBSMemArtsPartnerInputStream.OReadToMReadTransitionException e) {
                    if (this.state != State.OREAD) {
                        throw new IllegalStateException("state must be oread");
                    }
                    stateTransitionToMRead();

                    /**
                     * based on the implementation of OBSInputStream,
                     * when OBSInputStream trigger reopen(),
                     * it will not read any data from stream,
                     * thus, we should not update the {nextReadPos}
                     */
                    // continue to mread
                }
            case MREAD:
                try {
                    int ret = tryToReadFromCCStream(buf, off, len, oneByteRead);

                    // simulate the read passing through MemArts
                    long obsReadBytes = oisSimulator.read(oneByteRead ? 1 : len);
                    increaseSimTraffic(obsReadBytes);

                    return ret;
                } catch (EOFException e) {
                    return -1;
                } catch (IOException e) {
                    LOG.error("tryToReadFromCCStream offset[{}] len[{}] of [{}] failed, due to exception[{}]",
                        off, len, uri, e);
                    throw e;
                }

            default:
                throw new IllegalStateException("unreachable code");
        }
    }

    @Override
    public synchronized int read() throws IOException {
        fs.checkOpen();
        checkStreamOpen();
        long startTime = System.currentTimeMillis();
        long threadId = Thread.currentThread().getId();
        long endTime;
        boolean isTrue = this.contentLength == 0 || nextReadPos >= contentLength;

        if (isTrue) {
            return -1;
        }

        int byteRead = 0;
        // tail case
        int posInTail = posInTailBuf(nextReadPos);
        if (posInTail != -1 && tailBuf != null) {
            byteRead = tailBuf[posInTail] & 0xFF;
            nextReadPos++;
            this.partnerInputStream.seek(nextReadPos);
            oisSimulator.seek(nextReadPos);
            this.ccStream.seek(nextReadPos);
            incrementBytesRead(1);
            return byteRead;
        }

        byteRead = readInState(null, 0, 0, true);

        if (byteRead >= 0) {
            nextReadPos++;
        }

        endTime = System.currentTimeMillis();
        long position = byteRead >= 0 ? nextReadPos - 1 : nextReadPos;
        LOG.debug("read-0arg uri:{}, contentLength:{}, position:{}, readValue:{}, " + "thread:{}, timeUsedMilliSec:{}",
            uri, contentLength, position, byteRead, threadId, endTime - startTime);

        return byteRead;
    }

    @Override
    public synchronized int read(ByteBuffer byteBuffer) throws IOException {
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

    private int posInTailBuf(long pos) {
        if (pos < 0) {
            return -1;
        }
        if (pos < contentLength - bufSize) {
            return -1;
        }
        long tailBufHead = contentLength - bufSize;
        if (tailBufHead < 0) tailBufHead = 0;
        long idx = pos - tailBufHead;
        if (idx < 0 || idx >= bufSize) {
            LOG.warn("nextReadPos is in invalid state, pos = {}, contentLength  = {}, bufSize = {}", pos, contentLength, bufSize);
            return -1;
        }
        return (int)idx;
    }

    @Override
    public synchronized void seek(final long targetPos) throws IOException {
        LOG.debug("seek(targetPos), targetPos [{}] ", targetPos);

        fs.checkOpen();
        checkStreamOpen();

        // Do not allow negative seek
        if (targetPos < 0) throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK + " " + targetPos);
        if (targetPos > contentLength) throw new EOFException("Cannot seek after EOF");

        if (this.contentLength <= 0) {
            return;
        }

        // tail case
        if (posInTailBuf(targetPos) != -1 && tailBuf == null) {
            byte[] tmpBuf = new byte[bufSize];
            long tailBufHead = contentLength - bufSize;
            if (tailBufHead < 0) tailBufHead = 0;
            // seek to tailBufHead
            nextReadPos = tailBufHead;
            this.partnerInputStream.seek(tailBufHead);
            oisSimulator.seek(tailBufHead);
            this.ccStream.seek(tailBufHead);
            int off = 0;
            int bytesRead = 0;
            do {
                bytesRead = read(tmpBuf, off, bufSize - off);
                if (bytesRead == -1) {
                    break;
                }
                off += bytesRead;
            } while (off < bufSize);
            tailBuf = tmpBuf;
        }

        // Lazy seek
        nextReadPos = targetPos;
        this.partnerInputStream.seek(targetPos);
        oisSimulator.seek(targetPos);
        this.ccStream.seek(targetPos);
    }

    @Override
    public synchronized long getPos() throws IOException {
        fs.checkOpen();
        checkStreamOpen();
        return nextReadPos < 0 ? 0 : nextReadPos;
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        return false;
    }

    @Override
    public synchronized void setReadahead(Long readahead) throws IOException {
        fs.checkOpen();
        checkStreamOpen();
        this.ccStream.setReadahead(readahead);
    }

    /**
     * Close the stream. This triggers publishing of the stream statistics back
     * to the filesystem statistics. This operation is synchronized, so that
     * only one thread can attempt to close the connection; all later/blocked
     * calls are no-ops.
     *
     * @throws IOException on any problem
     */
    @Override
    public synchronized void close() throws IOException {
        if (!closed) {
            fs.checkOpen();
            // this is actually a no-op
            super.close();
            if (partnerInputStream != null) {
                partnerInputStream.close();
                partnerInputStream = null;
            }
            if (ccStream != null) {
                ccStream.close();
                ccStream = null;
            }
            closed = true;
            oisSimulator.close();
        }
        LOG.debug("{} SUMMARY: OREAD{}, MREAD{}", this.hashCode(), oCounter, mCounter);
    }

    /**
     * Verify that the input stream is open. Non blocking; this gives the last
     * state of the volatile {@link #closed} field.
     *
     * @throws IOException if the connection is closed.
     */
    private void checkStreamOpen() throws IOException {
        if (closed) {
            throw new IOException(uri + ": " + FSExceptionMessages.STREAM_IS_CLOSED);
        }
    }

    /**
     * Increment the bytes read counter if there is a stats instance and the
     * number of bytes read is more than zero.
     *
     * @param bytesRead number of bytes read
     */
    private void incrementBytesRead(final long bytesRead) {
        if (statistics != null && bytesRead > 0) {
            statistics.incrementBytesRead(bytesRead);
        }
    }

    private int tryToReadFromCCStream(final byte[] buf, final int off, final int len, boolean oneByteRead) throws IOException {
        int bytesRead;
        try {
            bytesRead = readInputStream(this.ccStream, buf, off, len, oneByteRead, mCounter, State.MREAD);
            return bytesRead;
        } catch (IOException e) {
            // escape IOException and CacheMiss
            LOG.debug("{} mread escape, caused by {}", this.hashCode(), e.getMessage());
        }

        /**
         * escape:
         * 1. partnerInputStream lazy seek to {nextReadPos}
         * 2. do one read() from partnerInputStream (ORead),
         * 3. transfer to ORead state
         */
        if (this.getState() != State.MREAD) {
            throw new IllegalStateException("cachemiss reopen must in state mread");
        }

        // 1. lazy seek to the right position
        this.partnerInputStream.seek(this.nextReadPos);
        oisSimulator.seek(nextReadPos);

        // 2. do once OBSInputStream.read
        // in MRead state, if this read() trigger reopen(),
        // it will handled by partnerInputStream quietly.
        try {
            bytesRead = readInputStream(this.partnerInputStream, buf, off, len, oneByteRead, oCounter, State.OREAD);
        } catch (OBSMemArtsPartnerInputStream.OReadToMReadTransitionException e) {
            // we should not cache transfer signal in this place,
            LOG.error("catch unexpected reopen signal, {}", e.getMessage());
            throw new IllegalStateException("catch unexpected reopen signal", e);
        }

        // 3. into ORead state, cache signal
        this.stateTransitionToORead();
        return bytesRead;
    }

    public State getState() {
        return this.state;
    }

    private void stateTransitionToMRead() throws IOException {
        if (this.state != State.OREAD) {
            throw new IllegalStateException("cannot transit state from " + this.state.toString() + " to mread");
        }
        this.state = State.MREAD;
        this.ccStream.seek(nextReadPos);
        oisSimulator.seek(nextReadPos);
    }

    private void stateTransitionToORead() {
        if (this.state != State.MREAD && this.state != State.NEW) {
            throw new IllegalStateException("cannot transit state from " + this.state.toString() + " to oread");
        }
        this.state = State.OREAD;
    }

    public enum State {
        /**
         * new created input stream
         */
        NEW("NEW"),

        /**
         * using partnerInputStream(OBSInputStream) to do actual read()
         */
        OREAD("OREAD"),

        /**
         * using memarts to do actual read()
         */
        MREAD("MREAD"),

        /**
         * closed state
         */
        CLOSED("CLOSED");

        private String state;

        State(String s) {
            this.state = s;
        }

        public String toString() {
            return this.state;
        }
    }

    private static class Counter { // because read is synchronized this count need not lock

        private volatile AtomicLong num = new AtomicLong();

        private volatile AtomicLong totalTime = new AtomicLong();

        private volatile AtomicLong totalLen = new AtomicLong();

        public void increase(long time, long len) {
            totalTime.getAndAdd(time);
            totalLen.getAndAdd(len);
            num.getAndIncrement();
        }

        @Override
        public String toString() {
            return String.format("[count=%s,time=%s,size=%s]", num, totalTime, totalLen);
        }
    }

    private void increaseSimTraffic(long value) {
        if (trafficStatistics != null) {
            trafficStatistics.increase(value, TrafficStatistics.TrafficType.Q);
        }
    }
}
