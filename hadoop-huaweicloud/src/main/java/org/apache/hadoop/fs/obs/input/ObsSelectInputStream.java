package org.apache.hadoop.fs.obs.input;

import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.obs.services.model.select.SelectEventVisitor;
import com.obs.services.model.select.SelectInputStream;
import com.obs.services.model.select.SelectObjectResult;

import org.apache.hadoop.fs.CanSetReadahead;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.obs.OBSCommonUtils;
import org.apache.hadoop.fs.obs.OBSConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObsSelectInputStream
        extends FSInputStream implements CanSetReadahead {
    private static final Logger LOG = LoggerFactory.getLogger(ObsSelectInputStream.class);

    private AtomicLong pos = new AtomicLong(0);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean completedSuccessfully = new AtomicBoolean(false);

    private final SelectInputStream recordsInputStream;

    private final String bucket;
    private final String key;
    private final String uri;

    private long readahead;

    public ObsSelectInputStream(
            final String bucket,
            final String key,
            final SelectObjectResult selectResult) {
        this.recordsInputStream = selectResult.getInputStream(
                new SelectEventVisitor() {
                    @Override
                    public void visitEndEvent() {
                        completedSuccessfully.set(true);
                    }
                });

        this.bucket = bucket;
        this.key = key;
        this.uri = String.format("obs://%s/%s", this.bucket, this.key);
    }

    @Override
    public void close()
            throws IOException {
        if (!closed.getAndSet(true)) {
            try {
                // set up for aborts.
                // if we know the available amount > readahead. Abort.
                //
                boolean shouldAbort = recordsInputStream.available() > readahead;
                if (!shouldAbort) {
                    // read our readahead range worth of data
                    long skipped = recordsInputStream.skip(readahead);
                    shouldAbort = recordsInputStream.read() >= 0;
                }
                // now, either there is data left or not.
                if (shouldAbort) {
                    recordsInputStream.abort();
                }
            } catch (IOException e) {
                LOG.debug("While closing stream", e);
            } finally {
                OBSCommonUtils.closeAll(recordsInputStream);
                super.close();
            }
        }
    }

    /**
     * Verify that the input stream is open. Non blocking; this gives
     * the last state of the atomic {@link #closed} field.
     * 
     * @throws PathIOException if the connection is closed.
     */
    private void checkNotClosed()
            throws IOException {
        if (closed.get()) {
            throw new PathIOException(uri, FSExceptionMessages.STREAM_IS_CLOSED);
        }
    }

    @Override
    public int available()
            throws IOException {
        checkNotClosed();
        return recordsInputStream.available();
    }

    @Override
    public long skip(long n)
            throws IOException {
        checkNotClosed();
        long skipped = recordsInputStream.skip(n);
        pos.addAndGet(skipped);
        return skipped;
    }

    @Override
    public long getPos() {
        return pos.get();
    }

    /**
     * from a possibly null Long value, return a valid
     * readahead.
     * 
     * @param readahead new readahead
     * @return a natural number.
     * @throws IllegalArgumentException if the range is invalid.
     */
    public static long validateReadahead(@Nullable Long readahead) {
        if (readahead == null) {
            return OBSConstants.DEFAULT_READAHEAD_RANGE;
        } else {
            Preconditions.checkArgument(readahead >= 0, "Negative readahead value" /* E_NEGATIVE_READAHEAD_VALUE */);
            return readahead;
        }
    }

    @Override
    public void setReadahead(Long readahead) {
        this.readahead = validateReadahead(readahead);
    }

    public long getReadahead() {
        return readahead;
    }

    @Override
    public int read()
            throws IOException {
        checkNotClosed();
        int byteRead;
        try {
            byteRead = recordsInputStream.read();
        } catch (EOFException e) {
            // this could be one of: end of file, some IO failure
            if (completedSuccessfully.get()) {
                // read was successful
                return -1;
            } else {
                // the stream closed prematurely
                LOG.info("Reading of OBS Select data from {} failed before all results "
                        + " were generated.", uri);
                throw new PathIOException(uri,
                        "Read of OBS Select data did not complete");
            }
        }

        if (byteRead >= 0) {
            incrementBytesRead(1);
        }
        return byteRead;
    }

    @Override
    public int read(byte[] buf, int off, int len)
            throws IOException {
        checkNotClosed();
        validatePositionedReadArgs(pos.get(), buf, off, len);
        if (len == 0) {
            return 0;
        }

        int bytesRead;
        try {
            bytesRead = recordsInputStream.read(buf, off, len);
        } catch (EOFException e) {
            // the base implementation swallows EOFs.
            return -1;
        }

        incrementBytesRead(bytesRead);
        return bytesRead;
    }

    @Override
    public void seek(long newPos)
            throws IOException {
        long current = getPos();
        long distance = newPos - current;
        if (distance < 0) {
            throw unsupported("seek() backwards from " + current + " to " + newPos);
        }
        if (distance == 0) {
            LOG.debug("ignoring seek to current position.");
        } else {
            // the complicated one: Forward seeking. Useful for split files.
            LOG.debug("Forward seek by reading {} bytes", distance);
            long bytesSkipped = 0L;
            // read byte-by-byte, hoping that buffering will compensate for this.
            // doing it this way ensures that the seek stops at exactly the right
            // place. skip(len) can return a smaller value, at which point
            // it's not clear what to do.
            while (distance > 0) {
                int r = read();
                if (r == -1) {
                    // reached an EOF too early
                    throw new EOFException("Seek to " + newPos
                            + " reached End of File at offset " + getPos());
                }
                distance--;
                bytesSkipped++;
            }
        }
    }

    /**
     * Build an exception to raise when an operation is not supported here.
     * 
     * @param action action which is unsupported.
     * @return an exception to throw.
     */
    protected PathIOException unsupported(final String action) {
        return new PathIOException(this.uri, action + " not supported");
    }

    @Override
    public boolean seekToNewSource(long targetPos)
            throws IOException {
        return false;
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public synchronized void mark(int readlimit) {
    }

    @Override
    public synchronized void reset()
            throws IOException {
        throw unsupported("Mark");
    }

    public void abort() {
        if (!closed.get()) {
            LOG.debug("Aborting");
            recordsInputStream.abort();
        }
    }

    /**
     * Read at a specific position.
     * Reads at a position earlier than the current {@link #getPos()} position
     * will fail with a {@link PathIOException}. See {@link #seek(long)}.
     * Unlike the base implementation <i>And the requirements of the filesystem
     * specification, this updates the stream position as returned in
     * {@link #getPos()}.</i>
     * 
     * @param position offset in the stream.
     * @param buffer   buffer to read in to.
     * @param offset   offset within the buffer
     * @param length   amount of data to read.
     * @return the result.
     * @throws PathIOException Backwards seek attempted.
     * @throws EOFException    attempt to seek past the end of the stream.
     * @throws IOException     IO failure while seeking in the stream or reading
     *                         data.
     */
    @Override
    public int read(long position, byte[] buffer, int offset, int length)
            throws IOException {
        // maybe seek forwards to the position.
        seek(position);
        return read(buffer, offset, length);
    }

    /**
     * Increment the bytes read counter if there is a stats instance
     * and the number of bytes read is more than zero.
     * This also updates the {@link #pos} marker by the same value.
     * 
     * @param bytesRead number of bytes read
     */
    private void incrementBytesRead(long bytesRead) {
        if (bytesRead > 0) {
            pos.addAndGet(bytesRead);
        }
    }
}
