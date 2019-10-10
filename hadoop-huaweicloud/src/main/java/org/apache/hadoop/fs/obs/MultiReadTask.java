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

import com.google.common.io.ByteStreams;
import com.obs.services.ObsClient;
import com.obs.services.internal.io.UnrecoverableIOException;
import com.obs.services.model.GetObjectRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.util.concurrent.Callable;

public class MultiReadTask implements Callable<Void> {

  private static final Logger LOG = LoggerFactory.getLogger(MultiReadTask.class);
  private final int RETRY_TIME = 3;
  private String bucket;
  private String key;
  private ObsClient client;
  private ReadBuffer readBuffer;
  private OBSFileSystem fs;

  public MultiReadTask(OBSFileSystem fs, String bucket, String key, ObsClient client, ReadBuffer readBuffer) {
    this.fs = fs;
    this.bucket = bucket;
    this.key = key;
    this.client = client;
    this.readBuffer = readBuffer;
  }

  @Override
  public Void call() throws Exception {
    GetObjectRequest request = new GetObjectRequest(bucket, key);
    request.setRangeStart(readBuffer.getStart());
    request.setRangeEnd(readBuffer.getEnd());
    if (fs.getSse().isSseCEnable()) {
      request.setSseCHeader(fs.getSse().getSseCHeader());
    }
    InputStream stream = null;
    readBuffer.setState(ReadBuffer.STATE.ERROR);

    boolean interrupted = false;

    for (int i = 0; i < RETRY_TIME; i++) {
      try {
        if (Thread.interrupted()) {
          throw new InterruptedException("Interrupted read task");
        }
        stream = client.getObject(request).getObjectContent();
        ByteStreams.readFully(
            stream,
            readBuffer.getBuffer(),
            0,
            (int) (readBuffer.getEnd() - readBuffer.getStart() + 1));
        readBuffer.setState(ReadBuffer.STATE.FINISH);

        return null;
      } catch (IOException e) {
        if (e instanceof InterruptedIOException) {
          // LOG.info("Buffer closed, task abort");
          interrupted = true;
          throw e;
        }
        LOG.warn("IOException occurred in Read task", e);
        readBuffer.setState(ReadBuffer.STATE.ERROR);
        if (i == RETRY_TIME - 1) {
          throw e;
        }
      } catch (Exception e) {
        readBuffer.setState(ReadBuffer.STATE.ERROR);
        if (e instanceof UnrecoverableIOException) {
          // LOG.info("Buffer closed, task abort");
          interrupted = true;
          throw e;
        } else {
          LOG.warn("Exception occurred in Read task", e);
          if (i == RETRY_TIME - 1) {
            throw e;
          }
        }
      } finally {
        // Return to connection pool
        if (stream != null) {
          stream.close();
        }

        // SLEEP
        if (!interrupted && readBuffer.getState() != ReadBuffer.STATE.FINISH) {
          try {
            Thread.sleep(3000);
          } catch (InterruptedException e) {
            // TODO
            interrupted = true;
            throw e;
          }
        }
      }
    }
    return null;
  }
}
