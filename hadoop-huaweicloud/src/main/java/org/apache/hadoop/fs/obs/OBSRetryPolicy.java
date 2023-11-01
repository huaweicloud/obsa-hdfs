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

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.NoRouteToHostException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.RetryPolicy;


public class OBSRetryPolicy implements RetryPolicyWithMaxTime {

  private RetryPolicyWithMaxTime defaultPolicy;
  private Map<Class<? extends Exception>, RetryPolicyWithMaxTime> exceptionToPolicyMap;

  private final RetryPolicyWithMaxTime throttleRetryPolicy;

  private final RetryPolicyWithMaxTime idempotencyIoRetryPolicy;

  private final RetryPolicyWithMaxTime failRetryPolicy;

  public OBSRetryPolicy(Configuration conf) {
    failRetryPolicy = new NoRetryPolicy();

    ExponentialBackoffRetryPolicy baseRetryPolicy = new ExponentialBackoffRetryPolicy(
        conf.getInt(OBSConstants.RETRY_LIMIT, OBSConstants.DEFAULT_RETRY_LIMIT),
        conf.getLong(OBSConstants.RETRY_MAXTIME, OBSConstants.DEFAULT_RETRY_MAXTIME),
        conf.getLong(OBSConstants.RETRY_SLEEP_BASETIME, OBSConstants.DEFAULT_RETRY_SLEEP_BASETIME),
        conf.getLong(OBSConstants.RETRY_SLEEP_MAXTIME, OBSConstants.DEFAULT_RETRY_SLEEP_MAXTIME));
    idempotencyIoRetryPolicy = new IdempotencyIoRetryPolicy(baseRetryPolicy);

    throttleRetryPolicy = new ExponentialBackoffRetryPolicy(
        conf.getInt(OBSConstants.RETRY_QOS_LIMIT, OBSConstants.DEFAULT_RETRY_QOS_LIMIT),
        conf.getLong(OBSConstants.RETRY_QOS_MAXTIME, OBSConstants.DEFAULT_RETRY_QOS_MAXTIME),
        conf.getLong(OBSConstants.RETRY_QOS_SLEEP_BASETIME, OBSConstants.DEFAULT_RETRY_QOS_SLEEP_BASETIME),
        conf.getLong(OBSConstants.RETRY_QOS_SLEEP_MAXTIME, OBSConstants.DEFAULT_RETRY_QOS_SLEEP_MAXTIME));

    exceptionToPolicyMap = createExceptionMap();
    defaultPolicy = idempotencyIoRetryPolicy;
  }

  private Map<Class<? extends Exception>, RetryPolicyWithMaxTime> createExceptionMap() {
    Map<Class<? extends Exception>, RetryPolicyWithMaxTime> policyMap = new HashMap<>();

    policyMap.put(UnknownHostException.class, failRetryPolicy);
    policyMap.put(NoRouteToHostException.class, failRetryPolicy);
    policyMap.put(InterruptedIOException.class, failRetryPolicy);
    policyMap.put(InterruptedException.class, failRetryPolicy);

    policyMap.put(AccessControlException.class, failRetryPolicy);
    policyMap.put(FileNotFoundException.class, failRetryPolicy);
    policyMap.put(OBSIllegalArgumentException.class, failRetryPolicy);
    policyMap.put(OBSMethodNotAllowedException.class, failRetryPolicy);
    policyMap.put(OBSFileConflictException.class, failRetryPolicy);
    policyMap.put(EOFException.class, failRetryPolicy);

    policyMap.put(OBSQosException.class, throttleRetryPolicy);
    policyMap.put(OBSIOException.class, idempotencyIoRetryPolicy);
    return policyMap;
  }

  @Override
  public RetryAction shouldRetry(Exception exception, int retries, int failovers, boolean idempotent)
      throws Exception {
    RetryPolicy policy = exceptionToPolicyMap.get(exception.getClass());
    if (policy == null) {
      policy = defaultPolicy;
    }
    return policy.shouldRetry(exception, retries, failovers, idempotent);
  }

  @Override
  public RetryAction shouldRetryByMaxTime(long startTime, Exception exception, int retries, int failovers,
      boolean idempotent) throws Exception {
    RetryPolicyWithMaxTime policy = exceptionToPolicyMap.get(exception.getClass());
    if (policy == null) {
      policy = defaultPolicy;
    }
    return policy.shouldRetryByMaxTime(startTime, exception, retries, failovers, idempotent);
  }

  @Override
  public RetryAction shouldRetryByMix(long startTime, Exception e, int retries, int failovers, boolean idempotent)
      throws Exception {
    RetryPolicyWithMaxTime policy = exceptionToPolicyMap.get(e.getClass());
    if (policy == null) {
      policy = defaultPolicy;
    }
    return policy.shouldRetryByMaxTime(startTime, e, retries, failovers, idempotent);
  }

  public static class IdempotencyIoRetryPolicy implements RetryPolicyWithMaxTime {

    private RetryPolicyWithMaxTime next;

    public IdempotencyIoRetryPolicy(RetryPolicyWithMaxTime retryPolicy) {
      this.next = retryPolicy;
    }

    @Override
    public RetryAction shouldRetry(Exception e, int retries, int failovers, boolean idempotent) throws Exception {
        return next.shouldRetry(e, retries, failovers, idempotent);
    }

    @Override
    public RetryAction shouldRetryByMaxTime(long startTime, Exception e, int retries, int failovers, boolean idempotent)
        throws Exception {
      if (!(e instanceof IOException) || !idempotent) {
        return RetryAction.FAIL;
      }
      return next.shouldRetryByMaxTime(startTime, e, retries, failovers, idempotent);
    }

    @Override
    public RetryAction shouldRetryByMix(long startTime, Exception e, int retries, int failovers, boolean idempotent)
        throws Exception {
      return next.shouldRetryByMix(startTime, e, retries, failovers, idempotent);
    }
  }

  public static class ExponentialBackoffRetryPolicy implements RetryPolicyWithMaxTime {
    private final int maxRetries;
    private final long maxTime;
    private final long sleepTime;
    private final long sleepMaxTime;

    public ExponentialBackoffRetryPolicy(int maxRetries, long maxTime, long sleepTime,
        long sleepMaxTime ) {
      this.maxRetries = maxRetries;
      this.maxTime = maxTime;
      this.sleepTime = sleepTime;
      this.sleepMaxTime = sleepMaxTime;
    }

    private long calculateExponentialTime(int retries) {
      long baseTime = Math.min(sleepTime * ((int) Math.pow(2.0D, (double) retries)), sleepMaxTime);
      return (long)((double)baseTime * (ThreadLocalRandom.current().nextDouble() + 0.5D));
    }

    @Override
    public RetryAction shouldRetry(Exception e, int retries, int failovers, boolean idempotent) {
      return retries >= maxRetries ?
          new RetryAction(RetryAction.RetryDecision.FAIL, 0 , "") :
          new RetryAction(RetryAction.RetryDecision.RETRY,
          TimeUnit.MILLISECONDS.toMillis(this.calculateExponentialTime(retries)), "");
    }

    @Override
    public RetryAction shouldRetryByMaxTime(long startTime, Exception e, int retries, int failovers,
        boolean idempotent) {
      return System.currentTimeMillis() - startTime > maxTime ?
          new RetryAction(RetryAction.RetryDecision.FAIL, 0L, "") :
          new RetryAction(RetryAction.RetryDecision.RETRY,
          TimeUnit.MILLISECONDS.toMillis(this.calculateExponentialTime(retries)), "");
    }

    @Override
    public RetryAction shouldRetryByMix(long startTime, Exception e, int retries, int failovers,
        boolean idempotent) {
      return retries >= maxRetries || (System.currentTimeMillis() - startTime > maxTime) ?
          new RetryAction(RetryAction.RetryDecision.FAIL, 0 , "") :
          new RetryAction(RetryAction.RetryDecision.RETRY,
              TimeUnit.MILLISECONDS.toMillis(this.calculateExponentialTime(retries)), "");
    }
  }

  public static class NoRetryPolicy implements RetryPolicyWithMaxTime {
    @Override
    public RetryAction shouldRetry(Exception e, int retries, int failovers, boolean idempotent) {
      return new RetryAction(RetryAction.RetryDecision.FAIL, 0L, "try once and fail.");
    }
    @Override
    public RetryAction shouldRetryByMaxTime(long startTime, Exception e, int retries, int failovers,
        boolean idempotent) {
      return new RetryAction(RetryAction.RetryDecision.FAIL, 0L, "try once and fail.");
    }

    @Override
    public RetryAction shouldRetryByMix(long startTime, Exception e, int retries, int failovers,
        boolean idempotent) {
      return new RetryAction(RetryAction.RetryDecision.FAIL, 0L, "try once and fail.");
    }
  }
}
