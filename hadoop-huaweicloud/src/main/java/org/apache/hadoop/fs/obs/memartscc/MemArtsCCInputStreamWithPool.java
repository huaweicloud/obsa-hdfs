package org.apache.hadoop.fs.obs.memartscc;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.obs.OBSFileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * 为了规避CBG spark组件inputstream 泄露，引入规避措施：bufferPool, 每次进行ccRead之前申请buffer，从ccRead读取数据后再还回pool
 * 代价：性能变差
 *  1. 频繁借还buffer
 *  2. 原先inputStream中的buffer具有缓冲数据的作用，当前因为要read完后立即还回去，所以缓冲失效。
 */
public class MemArtsCCInputStreamWithPool extends MemArtsCCInputStreamBase {
    private static final Logger LOG = LoggerFactory.getLogger(MemArtsCCInputStreamWithPool.class);

    private final FileSystem.Statistics statistics;

    private boolean closed;

    private MemArtsCCClient ccClient;

    private final String objKey;

    private long prefetchRange;

    // private final int bufSize;

    private long nextReadPos;

    private final long contentLength;

    private final String etag;

    private final long mtime;

    private final int borrowTimeout;

    public MemArtsCCInputStreamWithPool(MemArtsCCClient ccClient, String objKey, OBSFileStatus fileStatus,
        long prefetchRange, final FileSystem.Statistics stats, int borrowTimeout) {
        this.ccClient = ccClient;
        this.objKey = objKey;
        this.prefetchRange = prefetchRange;
        this.statistics = stats;
        this.contentLength = fileStatus.getLen();
        this.etag = fileStatus.getEtag();
        this.mtime = fileStatus.getModificationTime();
        this.closed = false;
        this.borrowTimeout = borrowTimeout;
    }

    private long getPrefetchEnd(long pos, int len) {
        long prefetchEnd = pos + Math.max(prefetchRange, len);
        return Math.min(prefetchEnd, contentLength);
    }

    private int readInBuffer(byte[] buf, int off, int len, boolean oneByteRead) throws IOException {
        ByteBuffer buffer = null;
        int readLen = oneByteRead ? 1 : Math.min(len, buf.length - off);
        try {
            buffer = MemArtsCCClient.bufferPool.borrowBuffer(borrowTimeout);
            int bytesRead = fillData(buffer, readLen);
            if (bytesRead < 0) {
                return -1;
            }
            if (oneByteRead) {
                return buffer.get() & 0xFF;
            }
            readLen = Math.min(len, buffer.remaining());
            buffer.get(buf, off, readLen);
            return readLen;
        } catch (InterruptedException e) {
            throw new IOException("borrow buffer interrupted", e);
        } finally {
            MemArtsCCClient.bufferPool.returnBuffer(buffer);
        }
    }

    private int fillData(ByteBuffer buffer, int len) throws IOException {
        buffer.position(0);
        buffer.limit(0);
        int readLen = Math.min(len, buffer.capacity());
        readLen = nextReadPos + readLen > contentLength ? (int) (contentLength - nextReadPos) : readLen;
        int bytesRead = ccClient.read(
            true,
            nextReadPos,
            getPrefetchEnd(nextReadPos, len),
            buffer,
            nextReadPos,
            readLen,
            objKey,
            mtime,
            etag,
            true
        );
        if (bytesRead == MemArtsCCClient.CCREAD_RETCODE_CACHEMISS) {
            throw new IOException("cache miss");
        }
        if (bytesRead > 0) {
            buffer.position(0);
            buffer.limit(bytesRead);
        }
        increaseHitTrafficTraffic(readLen);
        incrementBytesRead(readLen);
        return bytesRead;
    }

    @Override
    public int read() throws IOException {
        checkClosed();
        if (nextReadPos >= contentLength) {
            return -1;
        }
        int ret = readInBuffer(null, 0, 0, true);
        if (ret >= 0) {
            nextReadPos++;
        }
        return ret;
    }

    @Override
    public int read(byte[] buf, int off, int len) throws IOException {
        checkClosed();
        if (nextReadPos >= contentLength) {
            return -1;
        }
        if (nextReadPos + len > contentLength) {
            len = (int) (contentLength - nextReadPos);
        }
        int bytesRead = readInBuffer(buf, off, len, false);
        if (bytesRead > 0) {
            nextReadPos += bytesRead;
        }
        return bytesRead;
    }

    @Override
    public void seek(long l) throws IOException {
        checkClosed();
        if (l < 0) {
            throw new EOFException("Cannot seek to negative offset");
        }
        if (l > contentLength) {
            throw new EOFException("seek pos " + l + " is larger than contentLength" + contentLength);
        }
        this.nextReadPos = l;
    }

    @Override
    public long skip(long n) throws IOException {
        checkClosed();
        if (nextReadPos + n > contentLength) {
            n = contentLength - nextReadPos;
        }
        nextReadPos += n;
        return n;
    }

    @Override
    public long getPos() throws IOException {
        checkClosed();
        return nextReadPos;
    }

    @Override
    public boolean seekToNewSource(long l) throws IOException {
        checkClosed();
        return false;
    }

    private void checkClosed() throws IOException {
        if (closed) {
            throw new IOException("MemArtsCCInputStream already closed");
        }
    }

    @Override
    public void close() throws IOException {
        closed = true;
        ccClient = null;
        nextReadPos = 0;
    }

    @Override
    public void setReadahead(Long aLong) throws IOException, UnsupportedOperationException {
        if (aLong <= 0) {
            return;
        }
        this.prefetchRange = aLong;
    }

    private void incrementBytesRead(final long bytesRead) {
        if (statistics != null && bytesRead > 0) {
            statistics.incrementBytesRead(bytesRead);
        }
    }
}
