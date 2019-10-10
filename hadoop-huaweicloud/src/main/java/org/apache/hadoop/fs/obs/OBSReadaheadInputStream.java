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
import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;
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
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

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
public class OBSReadaheadInputStream extends FSInputStream implements CanSetReadahead {

  public static final Logger LOG = OBSFileSystem.LOG;
  private final FileSystem.Statistics statistics;
  private final ObsClient client;
  private final String bucket;
  private final String key;
  private final long contentLength;
  private final String uri;
  private final OBSInputPolicy inputPolicy;
  private final long MAX_RANGE;
  /**
   * Closed bit. Volatile so reads are non-blocking. Updates must be in a synchronized block to
   * guarantee an atomic check and set
   */
  private volatile boolean closed;

  private long readahead = Constants.DEFAULT_READAHEAD_RANGE;
  private ThreadPoolExecutor readThreadPool;
  private int bufferPartSize;
  private Deque<ReadBuffer> buffers = new LinkedList<>();
  private int readPartRemain;
  private byte[] buffer;
  private long bufferStart;
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

  private OBSFileSystem fs;

  public OBSReadaheadInputStream(
      String bucket,
      String key,
      long contentLength,
      ObsClient client,
      FileSystem.Statistics stats,
      long readahead,
      OBSInputPolicy inputPolicy,
      ThreadPoolExecutor readThreadPool,
      int bufferPartSize,
      long maxRange,
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
    setReadahead(readahead);
    // Thread pool for read
    this.readThreadPool = readThreadPool;
    // Buffer size for each part
    this.bufferPartSize = bufferPartSize;
    this.MAX_RANGE = maxRange;
    nextReadPos = 0;
    bufferStart = -1;
    this.fs = fs;
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
    long rangeLimit;

    rangeLimit = contentLength;

    // cannot read past the end of the object
    rangeLimit = Math.min(contentLength, rangeLimit);
    return rangeLimit;
  }

  /**
   * @param targetPos target position to reopen
   * @param length object total length , or partial length from object start
   * @param append if append is true, use buffers.offer() , else buffers.offerFirst()
   */
  private synchronized void applyBuffersWithinRange(long targetPos, long length, boolean append) {

    if (targetPos >= contentRangeFinish) {
      // Protect
      return;
    }
    if (length > contentRangeFinish) {
      // range protect
      length = contentRangeFinish;
    }
    boolean randomReadBuffered = false;
    Deque<ReadBuffer> tmpBuffers = new LinkedList<>();
    while (length - targetPos > bufferPartSize) {
      final long tmpEndPos = targetPos + bufferPartSize - 1;
      final long tmpTargetPos = targetPos;
      ReadBuffer readBuffer = new ReadBuffer(tmpTargetPos, tmpEndPos);

      Future task = readThreadPool.submit(new MultiReadTask(fs, bucket, key, client, readBuffer));
      readBuffer.setTask(task);
      tmpBuffers.offer(readBuffer);
      targetPos += bufferPartSize;
    }

    // last incomplete buffer
    final long tmpEndPos = length - 1;
    final long tmpTargetPos = targetPos;
    if (!randomReadBuffered) {
      // Random read only once
      ReadBuffer readBuffer = new ReadBuffer(tmpTargetPos, tmpEndPos);
      if (readBuffer.getBuffer().length == 0) {
        // EOF
        readBuffer.setState(ReadBuffer.STATE.FINISH);
      } else {
        Future task = readThreadPool.submit(new MultiReadTask(fs, bucket, key, client, readBuffer));
        readBuffer.setTask(task);
      }
      tmpBuffers.offer(readBuffer);
    }

    // Depends on append
    if (append) {
      while (!tmpBuffers.isEmpty()) {
        buffers.offer(tmpBuffers.poll());
      }
    } else {
      while (!tmpBuffers.isEmpty()) {
        // use tmp buffer as stack
        buffers.offerFirst(tmpBuffers.pollLast());
      }
    }
  }

  private synchronized void closeAndClearBuffers() {
    for (ReadBuffer buffer : buffers) {
      // Interrupt all tasks
      if (buffer.getTask() != null) {
        buffer.getTask().cancel(true);
      }
    }
    // clean buffers
    buffers.clear();
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

    if (targetPos < 0) {
      throw new IOException("io exception");
    }

    contentRangeFinish =
        calculateRequestLimit(inputPolicy, targetPos, length, contentLength, readahead);
    LOG.debug(
        "reopen({}) for {} range[{}-{}], length={}," + " , nextReadPosition={}",
        uri,
        reason,
        targetPos,
        contentRangeFinish,
        length,
        nextReadPos);
    contentRangeStart = targetPos;

    boolean bufferExist = false;
    // Reopen when already have buffers
    // Buffers is considered continuous
    while (buffers.size() != 0) {
      // Peek first buffer
      ReadBuffer buffer = buffers.peek();
      ReadBuffer lastbuffer = buffers.peekLast();
      if (buffer.getStart() <= targetPos && targetPos <= buffer.getEnd()) {
        bufferExist = true;
        // Good,  no need to clear buffers
        break;
      }
      if (targetPos < buffer.getStart()) {
        // Target at left of first buffer
        Iterator<ReadBuffer> iterator = buffers.descendingIterator();
        while (iterator.hasNext()) {
          ReadBuffer buf = iterator.next();
          if (buf.getEnd() - targetPos > MAX_RANGE) {
            // DO remove
            if (buf.getTask() != null) {
              buf.getTask().cancel(true);
            }
            iterator.remove();
          } else {
            // End immediately
            break;
          }
        }
        // Peek after delete
        buffer = buffers.peek();
        lastbuffer = buffers.peekLast();

        if (buffer == null) {
          // applyBuffersWithinRange(targetPos,buffer.getStart(),false);
          applyBuffersWithinRange(targetPos, targetPos + MAX_RANGE, false);
        } else {
          if (lastbuffer.getEnd() - targetPos != MAX_RANGE) {
            applyBuffersWithinRange(lastbuffer.getEnd() + 1, targetPos + MAX_RANGE, true);
          }
          applyBuffersWithinRange(targetPos, buffer.getStart(), false);
        }
        bufferExist = true;
        break;
      } else if (targetPos > lastbuffer.getEnd()) {
        // close all tasks and clear buffers
        closeAndClearBuffers();
      } else {
        // Exceed current buffer, but within all buffers
        // buffers.poll();
        Iterator<ReadBuffer> iterator = buffers.iterator();
        while (iterator.hasNext()) {
          ReadBuffer buf = iterator.next();
          if (buf.getEnd() < targetPos) {
            // DO remove
            if (buf.getTask() != null) {
              buf.getTask().cancel(true);
            }
            iterator.remove();
          } else {
            // End immediately
            break;
          }
        }

        // Peek after delete
        buffer = buffers.peek();
        lastbuffer = buffers.peekLast();

        if (lastbuffer.getEnd() < buffer.getStart() + MAX_RANGE) {
          // Still room for new buffer, append
          applyBuffersWithinRange(lastbuffer.getEnd() + 1, buffer.getStart() + MAX_RANGE, true);
        }
        bufferExist = true;
        break;

        // continue
      }
    }

    try {
      if (!bufferExist) {
        applyBuffersWithinRange(targetPos, targetPos + MAX_RANGE, false);
      }

      // Consume the first buffer
      ReadBuffer readBuffer = buffers.peek();

      if (readBuffer == null) {
        this.buffer = null;
        this.bufferStart = -1;
        this.readPartRemain = 0;
        throw new IOException("exception null buffer");
      }

      try {
        // Wait until state change turn different from START
        readBuffer.getTask().get();
        // readBuffer.await(ReadBuffer.STATE.START);
        if (ReadBuffer.STATE.ERROR.equals(readBuffer.getState())) {
          // ERROR occurred while requesting
          this.buffer = null;
          this.readPartRemain = 0;
          this.bufferStart = -1;
        }
        this.buffer = readBuffer.getBuffer();
        this.readPartRemain = (int) (readBuffer.getEnd() - targetPos + 1);
        this.bufferStart = readBuffer.getStart();
      } catch (InterruptedException e) {
        LOG.warn("Interrupted waiting for reading data");
      } catch (ExecutionException e) {
        // ERROR occurred while requesting
        this.buffer = null;
        this.readPartRemain = 0;
        this.bufferStart = -1;
        LOG.warn("Execute get buffer task fail cause: ", e.getCause());
      }
    } catch (ObsException e) {
      throw translateException("Reopen at position " + targetPos, uri, e);
    }
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
    // LOG.warn("Seek key: "+key+" targetPos: "+targetPos +" current nextReadPos: "+nextReadPos +"
    // current partRemain: "+readPartRemain);
    if (bufferStart != -1) {
      // Must have buffer
      long bufferEnd = bufferStart + buffer.length - 1;
      if (targetPos >= bufferStart && targetPos <= bufferEnd) {
        // Within buffer
        // move right
        // LOG.warn("Seek right inside buffer targetPos: "+targetPos +" current nextReadPos:
        // "+nextReadPos +" current partRemain: "+readPartRemain);
        readPartRemain = (int) (bufferEnd - targetPos + 1);
      } else {
        readPartRemain = 0;
      }
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

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }

  @Override
  public synchronized int read() throws IOException {
    checkNotClosed();
    if (this.contentLength == 0 || (nextReadPos >= contentLength)) {
      return -1;
    }

    if (readPartRemain <= 0 && nextReadPos < contentLength) {
      reopen("open", nextReadPos, contentLength);
    }

    int byteRead = -1;
    if (readPartRemain != 0) {
      // Get first byte from buffer
      byteRead = this.buffer[this.buffer.length - readPartRemain] & 0xFF;
    }

    if (byteRead >= 0) {
      // remove byte
      nextReadPos++;
      readPartRemain--;
    }
    if (statistics != null && byteRead >= 0) {
      statistics.incrementBytesRead(byteRead);
    }

    return byteRead;
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

    // LOG.warn("---read--- key: "+key+" buflength: "+buf.length+"off: "+off+"len: "+len);
    checkNotClosed();
    validatePositionedReadArgs(nextReadPos, buf, off, len);
    if (len == 0) {
      return 0;
    }

    if (this.contentLength == 0 || (nextReadPos >= contentLength)) {
      // LOG.warn("Read exceed!nextPos: "+nextReadPos+" contentLength: "+contentLength);
      return -1;
    }

    long bytescount = 0;
    while (nextReadPos < contentLength && bytescount < len) {
      if (readPartRemain == 0) {
        // LOG.warn("reopen , nextReadPos: "+nextReadPos+"length: "+(len-bytescount));
        reopen("continue buffer read", nextReadPos, len - bytescount);
      }

      int bytes = 0;
      for (int i = this.buffer.length - readPartRemain; i < this.buffer.length; i++) {
        /*try {*/
        buf[(int) (off + bytescount)] = this.buffer[i];
        /* } catch (ArrayIndexOutOfBoundsException e){
          throw new ArrayIndexOutOfBoundsException("buffer size is "+this.buffer.length+" remain part: "+readPartRemain+" index:"+i);
        }*/
        bytes++;
        bytescount++;
        if (bytescount >= len) {
          // LOG.warn("bufcopy break off: "+off+" bytescount: "+bytescount +" len: "+len+"
          // bytes:"+bytes);
          break;
        }
      }
      if (bytes > 0) {
        nextReadPos += bytes;
        readPartRemain -= bytes;
      } else if (readPartRemain != 0) {
        // not fully read from stream
        throw new IOException("Sfailed to read , remain :" + readPartRemain);
      }
    }
    if (statistics != null && bytescount > 0) {
      statistics.incrementBytesRead(bytescount);
    }
    if (bytescount == 0 && len > 0) {
      // LOG.warn("Read normally finished! key"+key+" nextPos: "+nextReadPos+" contentLength:
      // "+contentLength);
      return -1;
    } else {
      // LOG.warn("Read normally finished key"+key+" with bytescount: "+bytescount+"! nextPos:
      // "+nextReadPos+" contentLength: "+contentLength);
      return (int) (bytescount);
    }
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
    // LOG.warn("Closed ! key"+key);
    if (!closed) {

      closed = true;
      closeAndClearBuffers();

      // close or abort the stream
      // closeStream("close() operation", this.contentRangeFinish, false);
      // this is actually a no-op
      super.close();
    }
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
    return this.contentLength - this.nextReadPos;
  }

  /**
   * Bytes left in the current request. Only valid if there is an active request.
   *
   * @return how many bytes are left to read in the current GET.
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public synchronized long remainingInCurrentRequest() {
    return this.contentRangeFinish - this.nextReadPos;
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
      final StringBuilder sb = new StringBuilder("OBSReadaheadInputStream{");
      sb.append(uri);
      sb.append(" read policy=").append(inputPolicy);
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

  @VisibleForTesting
  public Deque<ReadBuffer> getBuffers() {
    return buffers;
  }
}
