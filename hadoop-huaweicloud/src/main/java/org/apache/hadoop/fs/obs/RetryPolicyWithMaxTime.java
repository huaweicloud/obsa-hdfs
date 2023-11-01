package org.apache.hadoop.fs.obs;

import org.apache.hadoop.io.retry.RetryPolicy;

/**
 * description
 *
 * @since 2022-05-10
 */
public interface RetryPolicyWithMaxTime extends RetryPolicy {
    RetryPolicy.RetryAction shouldRetryByMaxTime(long startTime, Exception e, int retries, int failovers,
        boolean idempotent)
        throws Exception;
    RetryPolicy.RetryAction shouldRetryByMix(long startTime, Exception e, int retries, int failovers,
        boolean idempotent)
        throws Exception;
}
