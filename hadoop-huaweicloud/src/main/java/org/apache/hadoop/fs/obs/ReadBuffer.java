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

import java.util.concurrent.Future;

/** Buffer with state */
public class ReadBuffer {

  private Future task;
  private STATE state;
  private long start;
  private long end;
  private byte[] buffer;

  public ReadBuffer(long start, long end) {
    this.start = start;
    this.end = end;
    this.state = STATE.START;
    this.buffer = new byte[(int) (end - start + 1)];
    this.task = null;
  }

  public STATE getState() {
    return state;
  }

  public void setState(STATE state) {
    this.state = state;
  }

  public long getStart() {
    return start;
  }

  public void setStart(long start) {
    this.start = start;
  }

  public long getEnd() {
    return end;
  }

  public void setEnd(long end) {
    this.end = end;
  }

  public byte[] getBuffer() {
    return buffer;
  }

  public void setBuffer(byte[] buffer) {
    this.buffer = buffer;
  }

  public Future getTask() {
    return task;
  }

  public void setTask(Future task) {
    this.task = task;
  }

  enum STATE {
    START,
    ERROR,
    FINISH
  }
}
