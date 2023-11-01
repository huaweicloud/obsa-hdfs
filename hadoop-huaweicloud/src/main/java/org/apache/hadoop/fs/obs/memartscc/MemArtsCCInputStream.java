package org.apache.hadoop.fs.obs.memartscc;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.obs.OBSFileStatus;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import sun.nio.ch.DirectBuffer;

public class MemArtsCCInputStream extends MemArtsCCInputStreamBase {
    // private static final Logger LOG = LoggerFactory.getLogger(MemArtsCCInputStream.class);

    private final FileSystem.Statistics statistics;

    private boolean closed;

    private MemArtsCCClient ccClient;

    private final String objKey;

    private long prefetchRange;

    private final int bufSize;

    private long nextReadPos;

    private final long contentLength;

    private final String etag;

    private final long mtime;

    private ByteBuffer buffer;

    private int directBufferSize;

    private long bufferStartPos;

    public MemArtsCCInputStream(MemArtsCCClient ccClient, String objKey, OBSFileStatus fileStatus,
        long prefetchRange, final FileSystem.Statistics stats, int bufSize, int directBufferSize) {
        this.directBufferSize = directBufferSize;
        this.buffer = ByteBuffer.allocateDirect(directBufferSize);
        this.ccClient = ccClient;
        this.objKey = objKey;
        this.prefetchRange = prefetchRange;
        this.statistics = stats;
        this.bufSize = bufSize;
        this.contentLength = fileStatus.getLen();
        this.etag = fileStatus.getEtag();
        this.mtime = fileStatus.getModificationTime();
        this.closed = false;
        buffer.position(0);
        buffer.limit(0);
    }

    private long getPrefetchEnd(long pos, int len) {
        long prefetchEnd = pos + Math.max(prefetchRange, len);
        return Math.min(prefetchEnd, contentLength);
    }

    private int readInBuffer(byte[] buf, int off, int len, boolean oneByteRead) throws IOException {
        if (ensureData(oneByteRead ? 1 : len) < 0) {
            return -1;
        }
        // buffer must have data
        if (oneByteRead) {
            return buffer.get() & 0xFF;
        }
        int readLen = Math.min(len, buffer.remaining());
        buffer.get(buf, off, readLen);
        return readLen;
    }

    private void ensurePos() {
        int bufPos = (int)(nextReadPos - bufferStartPos);
        if (buffer.remaining() == 0 || bufPos < 0 || bufPos >= buffer.limit()) {
            buffer.position(0);
            buffer.limit(0);
        } else {
            buffer.position(bufPos);
        }
    }

    private int ensureData(int len) throws IOException {
        ensurePos();
        int remaining = buffer.remaining();
        if (remaining > 0) {
            return remaining;
        }
        // try to fill
        int readLen = len < bufSize ? bufSize : Math.min(len, directBufferSize);
        readLen = nextReadPos + readLen > contentLength ? (int) (contentLength - nextReadPos) : readLen;
        buffer.position(0);
        buffer.limit(0);
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
            bufferStartPos = nextReadPos;
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
        ensurePos();
        if (nextReadPos + n > contentLength) {
            n = contentLength - nextReadPos;
        }
        if (n > buffer.remaining()) {
            // clear the buffer
            buffer.limit(0);
            buffer.position(0);
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
        ((DirectBuffer)buffer).cleaner().clean();
        buffer = null;
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
