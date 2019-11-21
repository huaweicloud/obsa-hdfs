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

import com.google.common.base.Preconditions;
import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;
import com.obs.services.model.GetObjectRequest;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.CanSetReadahead;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import static org.apache.hadoop.fs.obs.OBSUtils.translateException;

/**
 * The input stream for an OBS object.
 *
 * <p>As this stream seeks withing an object, it may close then re-open the stream. When this
 * happens, any updated stream data may be retrieved, and, given the consistency model of Huawei
 * OBS, outdated data may in fact be picked up.
 *
 * <p>As a result, the outcome of reading from a stream of an object which is actively manipulated
 * during the read process is "undefined".
 *
 * <p>The class is marked as private as code should not be creating instances themselves. Any extra
 * feature (e.g instrumentation) should be considered unstable.
 *
 * <p>Because it prints some of the state of the instrumentation, the output of {@link #toString()}
 * must also be considered unstable.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class OBSInputStream extends FSInputStream implements CanSetReadahead {
  public static final Logger LOG = OBSFileSystem.LOG;
  private static final int READ_RETRY_TIME = 3;
  private static final int SEEK_RETRY_TIME = 9;
  private static final int DELAY_TIME = 10;
  private final FileSystem.Statistics statistics;
  private final ObsClient client;
  private final String bucket;
  private final String key;
  private final long contentLength;
  private final String uri;
  private final OBSInputPolicy inputPolicy;
  private OBSFileSystem fs;
  /**
   * This is the public position; the one set in {@link #seek(long)} and returned in {@link
   * #getPos()}.
   */
  private long pos;
  /**
   * Closed bit. Volatile so reads are non-blocking. Updates must be in a synchronized block to
   * guarantee an atomic check and set
   */
  private volatile boolean closed;

  private InputStream wrappedStream;
  private long readahead = Constants.DEFAULT_READAHEAD_RANGE;

  /**
   * This is the actual position within the object, used by lazy seek to decide whether to seek on
   * the next read or not.
   */
  private long nextReadPos;
  /**
   * The end of the content range of the last request. This is an absolute value of the range, not a
   * length field.
   */
  private long contentRangeFinish;

  /** The start of the content range of the last request. */
  private long contentRangeStart;

  public OBSInputStream(
          String bucket,
          String key,
          long contentLength,
          ObsClient client,
          FileSystem.Statistics stats,
          long readahead,
          OBSInputPolicy inputPolicy,
          OBSFileSystem fs) {
    Preconditions.checkArgument(StringUtils.isNotEmpty(bucket), "No Bucket");
    Preconditions.checkArgument(StringUtils.isNotEmpty(key), "No Key");
    Preconditions.checkArgument(contentLength >= 0, "Negative content length");
    this.bucket = bucket;
    this.key = key;
    this.contentLength = contentLength;
    this.client = client;
    this.statistics = stats;
    this.uri = "obs://" + this.bucket + "/" + this.key;
    this.inputPolicy = inputPolicy;
    this.fs = fs;
    setReadahead(readahead);
  }

  /**
   * Calculate the limit for a get request, based on input policy and state of object.
   *
   * @param inputPolicy input policy
   * @param targetPos position of the read
   * @param length length of bytes requested; if less than zero "unknown"
   * @param contentLength total length of file
   * @param readahead current readahead value
   * @return the absolute value of the limit of the request.
   */
  static long calculateRequestLimit(
          OBSInputPolicy inputPolicy, long targetPos, long length, long contentLength, long readahead) {
    // cannot read past the end of the object
    return Math.min(contentLength, (length < 0) ? contentLength
            : targetPos + Math.max(readahead, length));
  }

  /**
   * Opens up the stream at specified target position and for given length.
   *
   * @param reason reason for reopen
   * @param targetPos target position
   * @param length length requested
   * @throws IOException on any failure to open the object
   */
  private synchronized void reopen(String reason, long targetPos, long length) throws IOException {

    if (wrappedStream != null) {
      closeStream("reopen(" + reason + ")", contentRangeFinish);
    }

    contentRangeFinish =
            calculateRequestLimit(inputPolicy, targetPos, length, contentLength, readahead);
    LOG.debug(
            "reopen({}) for {} range[{}-{}], length={}," + " streamPosition={}, nextReadPosition={}",
            uri,
            reason,
            targetPos,
            contentRangeFinish,
            length,
            pos,
            nextReadPos);

    try {
      GetObjectRequest request = new GetObjectRequest(bucket, key);
      request.setRangeStart(targetPos);
      request.setRangeEnd(contentRangeFinish);
      if (fs.getSse().isSseCEnable()) {
        request.setSseCHeader(fs.getSse().getSseCHeader());
      }
      wrappedStream = client.getObject(request).getObjectContent();
      contentRangeStart = targetPos;
      if (wrappedStream == null) {
        throw new IOException("Null IO stream from reopen of (" + reason + ") " + uri);
      }
    } catch (ObsException e) {
      throw translateException("Reopen at position " + targetPos, uri, e);
    }

    this.pos = targetPos;
  }

  @Override
  public synchronized long getPos() throws IOException {
    return (nextReadPos < 0) ? 0 : nextReadPos;
  }

  @Override
  public synchronized void seek(long targetPos) throws IOException {
    checkNotClosed();

    // Do not allow negative seek
    if (targetPos < 0) {
      throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK + " " + targetPos);
    }

    if (this.contentLength <= 0) {
      return;
    }

    // Lazy seek
    nextReadPos = targetPos;
  }

  /**
   * Seek without raising any exception. This is for use in {@code finally} clauses
   *
   * @param positiveTargetPos a target position which must be positive.
   */
  private void seekQuietly(long positiveTargetPos) {
    try {
      seek(positiveTargetPos);
    } catch (IOException ioe) {
      LOG.debug("Ignoring IOE on seek of {} to {}", uri, positiveTargetPos, ioe);
    }
  }

  /**
   * Adjust the stream to a specific position.
   *
   * @param targetPos target seek position
   * @param length length of content that needs to be read from targetPos
   * @throws IOException
   */
  private void seekInStream(long targetPos, long length) throws IOException {
    checkNotClosed();
    if (wrappedStream == null) {
      return;
    }
    // compute how much more to skip
    long diff = targetPos - pos;
    if (diff > 0) {
      // forward seek -this is where data can be skipped

      int available = wrappedStream.available();
      if (available < diff) {
        // log a warning; continue to attempt to re-open
        LOG.info("Available size {} little than target. can not to seek on {} to {}. Current position {}. " +
                "must close the current stream.", available, uri, targetPos, pos);
      } else {
        // always seek at least as far as what is available
        long forwardSeekRange = Math.max(readahead, available);
        // work out how much is actually left in the stream
        // then choose whichever comes first: the range or the EOF
        long remainingInCurrentRequest = remainingInCurrentRequest();

        long forwardSeekLimit = Math.min(remainingInCurrentRequest, forwardSeekRange);
        boolean skipForward = remainingInCurrentRequest > 0 && diff <= forwardSeekLimit;
        if (skipForward) {
          // the forward seek range is within the limits
          LOG.debug("Forward seek on {}, of {} bytes", uri, diff);
          long skipped = wrappedStream.skip(diff);
          if (skipped > 0) {
            pos += skipped;
            // as these bytes have been read, they are included in the counter
            incrementBytesRead(skipped);
          }

          if (pos == targetPos) {
            // all is well
            return;
          } else {
            // log a warning; continue to attempt to re-open
            LOG.info("Failed to seek on {} to {}. Current position {}", uri, targetPos, pos);
          }
        }
      }
    } else if (diff < 0) {
      // backwards seek
    } else {
      // targetPos == pos
      if (remainingInCurrentRequest() > 0) {
        // if there is data left in the stream, keep going
        return;
      }
    }

    // if the code reaches here, the stream needs to be reopened.
    // close the stream; if read the object will be opened at the new pos
    closeStream("seekInStream()", this.contentRangeFinish);
    pos = targetPos;
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }

  /**
   * Perform lazy seek and adjust stream to correct position for reading.
   *
   * @param targetPos position from where data should be read
   * @param len length of the content that needs to be read
   */
  private void lazySeek(long targetPos, long len) throws IOException {
    for (int i = 0; i < SEEK_RETRY_TIME; i++) {
      try {
        // For lazy seek
        seekInStream(targetPos, len);

        // re-open at specific location if needed
        if (wrappedStream == null) {
          reopen("read from new offset", targetPos, len);
        }

        break;
      } catch (OBSIOException e) {
        LOG.warn("OBSIOException occurred in lazySeek, retry: {}", i, e);
        if (i == SEEK_RETRY_TIME - 1) {
          throw e;
        }
        try {
          Thread.sleep(DELAY_TIME);
        } catch (InterruptedException ie) {
          throw e;
        }
      }
    }
  }

  /**
   * Increment the bytes read counter if there is a stats instance and the number of bytes read is
   * more than zero.
   *
   * @param bytesRead number of bytes read
   */
  private void incrementBytesRead(long bytesRead) {
    if (statistics != null && bytesRead > 0) {
      statistics.incrementBytesRead(bytesRead);
    }
  }

  @Override
  public synchronized int read() throws IOException {
    checkNotClosed();
    if (this.contentLength == 0 || (nextReadPos >= contentLength)) {
      return -1;
    }

    int byteRead = -1;
    try {
      lazySeek(nextReadPos, 1);
    } catch (EOFException e) {
      onReadFailure(e, 1);
      return -1;
    }

    IOException exception = null;
    for (int retryTime = 1; retryTime <= READ_RETRY_TIME; retryTime++) {
      try {
        byteRead = wrappedStream.read();
        exception = null;
        break;
      } catch (EOFException e) {
        onReadFailure(e, 1);
        return -1;
      } catch (IOException e) {
        exception = e;
        onReadFailure(e, 1);
        LOG.warn("read of [{}] failed, retry time[{}], due to exception[{}]", uri, retryTime, exception);
        if (retryTime < READ_RETRY_TIME) {
          try {
            Thread.sleep(DELAY_TIME);
          } catch (InterruptedException ie) {
            LOG.error("read of [{}] failed, retry time[{}], due to exception[{}]", uri, retryTime, exception);
            throw exception;
          }
        }
      }
    }

    if (exception != null) {
      LOG.error("read of [{}] failed, retry time[{}], due to exception[{}]", uri, READ_RETRY_TIME, exception);
      throw exception;
    }

    if (byteRead >= 0) {
      pos++;
      nextReadPos++;
    }

    if (byteRead >= 0) {
      incrementBytesRead(1);
    }
    return byteRead;
  }

  /**
   * Handle an IOE on a read by attempting to re-open the stream. The filesystem's readException
   * count will be incremented.
   *
   * @param ioe exception caught.
   * @param length length of data being attempted to read
   * @throws IOException any exception thrown on the re-open attempt.
   */
  private void onReadFailure(IOException ioe, int length) throws IOException {
    LOG.info(
            "Got exception while trying to read from stream {}" + " trying to recover: " + ioe, uri);
    LOG.debug("While trying to read from stream {}", uri, ioe);
    reopen("failure recovery", pos, length);
  }

  /**
   * {@inheritDoc}
   *
   * <p>This updates the statistics on read operations started and whether or not the read operation
   * "completed", that is: returned the exact number of bytes requested.
   *
   * @throws IOException if there are other problems
   */
  @Override
  public synchronized int read(byte[] buf, int off, int len) throws IOException {
    checkNotClosed();
    validatePositionedReadArgs(nextReadPos, buf, off, len);
    if (len == 0) {
      return 0;
    }

    if (this.contentLength == 0 || (nextReadPos >= contentLength)) {
      return -1;
    }

    try {
      lazySeek(nextReadPos, len);
    } catch (EOFException e) {
      onReadFailure(e, len);
      // the end of the file has moved
      return -1;
    }

    int bytesRead = -1;
    IOException exception = null;
    for (int retryTime = 1; retryTime <= READ_RETRY_TIME; retryTime++) {
      try {
        bytesRead = wrappedStream.read(buf, off, len);
        exception = null;
        break;
      } catch (EOFException e) {
        onReadFailure(e, len);
        // the base implementation swallows EOFs.
        return -1;
      } catch (IOException e) {
        exception = e;
        onReadFailure(e, len);
        LOG.warn("read offset[{}] len[{}] of [{}] failed, retry time[{}], due to exception[{}]",
                  off,len, uri, retryTime, exception);
        if (retryTime < READ_RETRY_TIME) {
          try {
            Thread.sleep(DELAY_TIME);
          } catch (InterruptedException ie) {
            LOG.error("read offset[{}] len[{}] of [{}] failed, retry time[{}], due to exception[{}]",
                      off,len, uri, retryTime, exception);
            throw exception;
          }
        }
      }
    }

    if (exception != null) {
      LOG.error("read offset[{}] len[{}] of [{}] failed, retry time[{}], due to exception[{}]",
                off,len, uri, READ_RETRY_TIME, exception);
      throw exception;
    }

    if (bytesRead > 0) {
      pos += bytesRead;
      nextReadPos += bytesRead;
    }
    incrementBytesRead(bytesRead);
    return bytesRead;
  }

  /**
   * Verify that the input stream is open. Non blocking; this gives the last state of the volatile
   * {@link #closed} field.
   *
   * @throws IOException if the connection is closed.
   */
  private void checkNotClosed() throws IOException {
    if (closed) {
      throw new IOException(uri + ": " + FSExceptionMessages.STREAM_IS_CLOSED);
    }
  }

  /**
   * Close the stream. This triggers publishing of the stream statistics back to the filesystem
   * statistics. This operation is synchronized, so that only one thread can attempt to close the
   * connection; all later/blocked calls are no-ops.
   *
   * @throws IOException on any problem
   */
  @Override
  public synchronized void close() throws IOException {
    if (!closed) {
      closed = true;
      // close or abort the stream
      closeStream("close() operation", this.contentRangeFinish);
      // this is actually a no-op
      super.close();
    }
  }

  /**
   * Close a stream: decide whether to abort or close, based on the length of the stream and the
   * current position. If a close() is attempted and fails, the operation escalates to an abort.
   *
   * <p>This does not set the {@link #closed} flag.
   *
   * @param reason reason for stream being closed; used in messages
   * @param length length of the stream.
   */
  private void closeStream(String reason, long length) {
    if (wrappedStream != null) {
      try {
        wrappedStream.close();
      } catch (IOException e) {
        // exception escalates to an abort
        LOG.debug("When closing {} stream for {}", uri, reason, e);
      }

      LOG.debug(
              "Stream {} : {}; streamPos={}, nextReadPos={}," + " request range {}-{} length={}",
              uri,
              reason,
              pos,
              nextReadPos,
              contentRangeStart,
              contentRangeFinish,
              length);
      wrappedStream = null;
    }
  }

  /**
   * Forcibly reset the stream, by aborting the connection. The next {@code read()} operation will
   * trigger the opening of a new HTTPS connection.
   *
   * <p>This is potentially very inefficient, and should only be invoked in extreme circumstances.
   * It logs at info for this reason.
   *
   * @return true if the connection was actually reset.
   * @throws IOException if invoked on a closed stream.
   */
  @InterfaceStability.Unstable
  public synchronized boolean resetConnection() throws IOException {
    checkNotClosed();
    boolean connectionOpen = wrappedStream != null;
    if (connectionOpen) {
      LOG.info("Forced reset of connection to {}", uri);
      closeStream("reset()", contentRangeFinish);
    }
    return connectionOpen;
  }

  @Override
  public synchronized int available() throws IOException {
    checkNotClosed();

    long remaining = remainingInFile();
    if (remaining > Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    }
    return (int) remaining;
  }

  /**
   * Bytes left in stream.
   *
   * @return how many bytes are left to read
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public synchronized long remainingInFile() {
    return this.contentLength - this.pos;
  }

  /**
   * Bytes left in the current request. Only valid if there is an active request.
   *
   * @return how many bytes are left to read in the current GET.
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public synchronized long remainingInCurrentRequest() {
    return this.contentRangeFinish - this.pos;
  }

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public synchronized long getContentRangeFinish() {
    return contentRangeFinish;
  }

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public synchronized long getContentRangeStart() {
    return contentRangeStart;
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  /**
   * String value includes statistics as well as stream state. <b>Important: there are no guarantees
   * as to the stability of this value.</b>
   *
   * @return a string value for printing in logs/diagnostics
   */
  @Override
  @InterfaceStability.Unstable
  public String toString() {
    synchronized (this) {
      final StringBuilder sb = new StringBuilder("OBSInputStream{");
      sb.append(uri);
      sb.append(" wrappedStream=").append(wrappedStream != null ? "open" : "closed");
      sb.append(" read policy=").append(inputPolicy);
      sb.append(" pos=").append(pos);
      sb.append(" nextReadPos=").append(nextReadPos);
      sb.append(" contentLength=").append(contentLength);
      sb.append(" contentRangeStart=").append(contentRangeStart);
      sb.append(" contentRangeFinish=").append(contentRangeFinish);
      sb.append(" remainingInCurrentRequest=").append(remainingInCurrentRequest());
      sb.append('\n');
      sb.append('}');
      return sb.toString();
    }
  }

  /**
   * Subclass {@code readFully()} operation which only seeks at the start of the series of
   * operations; seeking back at the end.
   *
   * <p>This is significantly higher performance if multiple read attempts are needed to fetch the
   * data, as it does not break the HTTP connection.
   *
   * <p>To maintain thread safety requirements, this operation is synchronized for the duration of
   * the sequence. {@inheritDoc}
   */
  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    checkNotClosed();
    validatePositionedReadArgs(position, buffer, offset, length);
    if (length == 0) {
      return;
    }
    int nread = 0;
    synchronized (this) {
      long oldPos = getPos();
      try {
        seek(position);
        while (nread < length) {
          int nbytes = read(buffer, offset + nread, length - nread);
          if (nbytes < 0) {
            throw new EOFException(FSExceptionMessages.EOF_IN_READ_FULLY);
          }
          nread += nbytes;
        }
      } finally {
        seekQuietly(oldPos);
      }
    }
  }

  /**
   * Random read
   *
   * @throws IOException
   */
  @Override
  public int read(long position, byte[] buffer, int offset, int length) throws IOException {
    checkNotClosed();
    validatePositionedReadArgs(position, buffer, offset, length);

    int len = 0;
    int nbytes = -1;
    InputStream inputStream = null;
    IOException exception = null;
    GetObjectRequest request = new GetObjectRequest(bucket, key);
    request.setRangeStart(position);
    request.setRangeEnd(position + length);
    if (fs.getSse().isSseCEnable()) {
      request.setSseCHeader(fs.getSse().getSseCHeader());
    }

    for (int retryTime = 1; retryTime <= READ_RETRY_TIME; retryTime++) {
      try {
        inputStream = client.getObject(request).getObjectContent();
        if (inputStream == null) {
          break;
        }
        while (len < length) {
          nbytes = inputStream.read(buffer, offset + len, length - len);
          if (nbytes < 0) {
            throw new EOFException(FSExceptionMessages.EOF_IN_READ_FULLY);
          }

          len += nbytes;
        }
        exception = null;
        break;
      } catch (ObsException | IOException e) {
        if (e instanceof ObsException) {
          exception = translateException("Read at position " + position, uri, (ObsException) e);
        }
        LOG.warn("read position[{}] offset[{}] len[{}] of [{}] failed, retry time[{}], due to exception[{}]",
                  position, offset,len, uri, retryTime, exception);
        if (retryTime < READ_RETRY_TIME) {
          try {
            Thread.sleep(DELAY_TIME);
          } catch (InterruptedException ie) {
            LOG.error("read position[{}] offset[{}] len[{}] of [{}] failed, retry time[{}], due to exception[{}]",
                    position, offset,len, uri, retryTime, exception);
            throw exception;
          }
        }
      } finally {
        if (inputStream != null) {
          inputStream.close();
        }
        len = 0;
      }
    }

    if (inputStream == null || exception != null) {
      LOG.error("read position[{}] offset[{}] len[{}] failed, retry time[{}], due to exception[{}]",
              position, offset,len, READ_RETRY_TIME, exception);
      throw new IOException("read failed of " + uri + ", inputStream is "
                  + inputStream == null ? "null" : "not null", exception);
    }
    LOG.debug("Read uri:{}, position:{}, offset:{}, length:{}", uri, position, offset, len);

    return len;
  }

  /**
   * Get the current readahead value.
   *
   * @return a non-negative readahead value
   */
  public synchronized long getReadahead() {
    return readahead;
  }

  @Override
  public synchronized void setReadahead(Long readahead) {
    if (readahead == null) {
      this.readahead = Constants.DEFAULT_READAHEAD_RANGE;
    } else {
      Preconditions.checkArgument(readahead >= 0, "Negative readahead value");
      this.readahead = readahead;
    }
  }

  @Override
  protected void finalize() throws Throwable {
    closeStream("GC call. to close stream. closed is [" + closed + "]", this.contentRangeFinish);
    super.finalize();
  }
}