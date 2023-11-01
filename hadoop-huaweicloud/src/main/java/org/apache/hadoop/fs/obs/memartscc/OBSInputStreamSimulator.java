package org.apache.hadoop.fs.obs.memartscc;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.util.Locale;

import static java.lang.Math.min;

public class OBSInputStreamSimulator {
    private final long contentLength;

    private final long readAheadRange;

    private long nextReadPos;

    private InputStreamSimulator mockInputStream;

    private long streamCurrentPos;

    private long contentRangeFinish;

    private long actualReadFromObs;

    private static final Logger LOG = LoggerFactory.getLogger(OBSInputStreamSimulator.class);

    public OBSInputStreamSimulator(final long fileStatusLength, final long readAheadRangeValue) {
        Preconditions.checkArgument(fileStatusLength >= 0, "Negative content length");
        this.contentLength = fileStatusLength;
        this.readAheadRange = readAheadRangeValue;
    }

    private class InputStreamSimulator {

        private final int size;

        private int pos;

        public InputStreamSimulator(int bytes) {
            size = bytes;
            pos = 0;
        }

        public int available() {
            return size - pos;
        }

        public long skip(long n) {
            if (n <= 0) {
                return 0;
            }

            if (pos + n >= size) {
                long skipped = size - pos;
                pos = size;
                return skipped;
            } else {
                pos += n;
                return n;
            }
        }
    }

    /**
     * 模拟读取OBS的流量
     * @param len 需要读多少字节的数据
     * @return 实际上读取了多少OBS的数据
     */
    public synchronized long read(final int len) throws IOException {
        LOG.debug("Simulator.read len: {}", len);

        actualReadFromObs = 0;

        if (len == 0) {
            return 0;
        }
        boolean isTrue = this.contentLength == 0 || nextReadPos >= contentLength;
        if (isTrue) {
            LOG.debug("Simulator.read: no need to read");
            return -1;
        }

        lazySeek(nextReadPos, len);

        // 这里模拟读取操作，假设读取len长度的数据全部成功，那么直接+len
        if (mockInputStream == null) {
            throw new IOException("mockInputStream closed, cannot read.");
        }
        int ioRead = Math.min(len, mockInputStream.available());
        streamCurrentPos += ioRead;
        nextReadPos += ioRead;

        return actualReadFromObs;
    }

    private void lazySeek(final long targetPos, final long len) throws IOException {
        seekInStream(targetPos);

        if (isStreamClosed()) {
            reopen(targetPos, len);
        }
    }

    private boolean isStreamClosed() {
        return mockInputStream == null;
    }

    private void seekInStream(final long targetPos) {
        LOG.debug("Simulator.seekInStream targetPos: {}", targetPos);

        if (isStreamClosed()) {
            LOG.debug("Simulator.seekInStream: the stream is not opened, seekInStream not operated");
            return;
        }
        long diff = targetPos - streamCurrentPos;

        if (diff == 0 && this.contentRangeFinish - this.streamCurrentPos > 0) {
            LOG.debug("Simulator.seekInStream: the seek position does not require seek");
            return;
        }

        if (diff > 0) {
            int available = mockInputStream.available();
            long forwardSeekRange = Math.max(readAheadRange, available);
            long remainingInCurrentRequest = this.contentRangeFinish - this.streamCurrentPos;
            long forwardSeekLimit = min(remainingInCurrentRequest, forwardSeekRange);
            boolean skipForward = remainingInCurrentRequest > 0 && diff <= forwardSeekLimit;
            if (skipForward) {
                long skippedOnce = mockInputStream.skip(diff);
                while (diff > 0 && skippedOnce > 0) {
                    streamCurrentPos += skippedOnce;
                    diff -= skippedOnce;
                    skippedOnce = mockInputStream.skip(diff);
                }

                if (streamCurrentPos == targetPos) {
                    return;
                }
            }
        }

        closeStream();
        streamCurrentPos = targetPos;
    }

    /**
     * Simulate reopen operation in OBSInputStream
     * @param targetPos the position which the stream should open from
     * @param length the length of the stream
     * @return the actual traffic when reading from OBS, unit: bytes
     * @throws IOException
     */
    public long reopen(final long targetPos, final long length) {
        if (!isStreamClosed()) {
            closeStream();
        }

        contentRangeFinish = calculateRequestLimit(targetPos, length, contentLength, readAheadRange);
        long actualLength = contentRangeFinish - targetPos;
        mockInputStream = new InputStreamSimulator((int)actualLength);
        LOG.debug("Opened a simulated stream, param length: {}, length {}.", length, actualLength);

        actualReadFromObs += actualLength;
        this.streamCurrentPos = targetPos;
        return actualLength;
    }

    /**
     * 注意seek和seekInStream
     * @param targetPos seek目标位置
     * @throws IOException
     */
    public void seek(final long targetPos) throws IOException {
        LOG.debug("Simulator.seek targetPos {} ", targetPos);

        if (targetPos > contentLength) {
            LOG.error("The position of seek is beyond content length, targetPos:{}, contentLength:{}",
                    targetPos, contentLength);
            throw new EOFException(String.format(Locale.ENGLISH, "%s %d", FSExceptionMessages.CANNOT_SEEK_PAST_EOF, targetPos));
        }

        if (targetPos < 0) {
            LOG.error("The position of seek negative, targetPos:{}", targetPos);
            throw new EOFException(String.format(Locale.ENGLISH, "%s %d", FSExceptionMessages.NEGATIVE_SEEK, targetPos));
        }

        if (this.contentLength <= 0) {
            return;
        }
        nextReadPos = targetPos;
    }

    protected void closeStream() {
        LOG.debug("Closed a simulator stream");
        mockInputStream = null;
    }

    public void close() throws IOException {
        closeStream();
    }

    protected long calculateRequestLimit(final long targetPos, final long length, final long contentLength,
                                      final long readAhead) {
        return min(contentLength, length < 0 ? contentLength : targetPos + Math.max(readAhead, length));
    }
}
