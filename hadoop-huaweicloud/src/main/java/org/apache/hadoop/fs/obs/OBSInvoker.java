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

import com.obs.services.exception.ObsException;

import org.apache.hadoop.io.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;

public class OBSInvoker {
    private static final Logger LOG = LoggerFactory.getLogger(OBSInvoker.class);

    public static final RetryCallback LOG_EVENT = new RetryCallback() {
        @Override
        public void onFailure(String text,
            IOException exception,
            int retries,
            boolean idempotent) {
            LOG.debug("retry #{}, {}", retries, exception);
        }
    };

    private final RetryPolicyWithMaxTime retryPolicy;
    private final RetryCallback retryCallback;
    private final OBSFileSystem fs;

    public OBSInvoker(OBSFileSystem fs, RetryPolicyWithMaxTime retryPolicy, RetryCallback callback) {
        this.retryPolicy = retryPolicy;
        this.retryCallback = callback;
        this.fs = fs;
    }

    public <T> T retryByMaxTime(OBSOperateAction action, String path, OBSCallable<T> operation, boolean idempotent)
        throws IOException {
        return retryByMaxTime(action, path, operation, idempotent, retryCallback);
    }


    public <T> T retryByMaxTime(OBSOperateAction action, String path, OBSCallable<T> operation, boolean idempotent, RetryCallback retrying)
        throws IOException {
        int retryCount = 0;
        long startTime = System.currentTimeMillis();
        IOException translated = null;
        RetryPolicy.RetryAction retryAction;
        boolean shouldRetry;
        do {
            try {
                return operation.call();
            } catch (IOException e) {
                translated = e;
            } catch (ObsException e) {
                translated = OBSCommonUtils.translateException(action.toString(), path, e);
            }

            OBSCommonUtils.putQosMetric(fs, action, translated);

            String text = action + " on " + path;

            try {
                retryAction = retryPolicy.shouldRetryByMaxTime(startTime, translated, retryCount, 0,
                    idempotent);
                shouldRetry = retryAction.action.equals(
                    RetryPolicy.RetryAction.RETRY.action);
                if (shouldRetry) {
                    retryCount++;
                    retrying.onFailure(text, translated, retryCount, idempotent);
                    Thread.sleep(retryAction.delayMillis);
                }
            } catch (InterruptedException e) {
                translated = new InterruptedIOException(text + ",interrupted in retry process");
                translated.initCause(e);
                shouldRetry = false;
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                translated = new IOException(text + ",unexpect exception in retry process", e);
                shouldRetry = false;
            }
        } while (shouldRetry);

        if (retryCount != 0) {
            LOG.error("retry {} times fail: {}", retryCount, translated.toString());
        }
        throw translated;
    }


    @FunctionalInterface
    public interface RetryCallback {
        void onFailure(
            String text,
            IOException exception,
            int retries,
            boolean idempotent);
    }
}
