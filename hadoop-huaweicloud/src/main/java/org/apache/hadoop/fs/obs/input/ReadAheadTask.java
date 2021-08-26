package org.apache.hadoop.fs.obs.input;

import com.obs.services.ObsClient;
import com.obs.services.model.GetObjectRequest;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Used by {@link OBSExtendInputStream} as an task that submitted
 * to the thread pool.
 * Each FileReaderTask reads one part of the file so that
 * we can accelerate the sequential read.
 */
public class ReadAheadTask implements Runnable {
    public final Logger log = LoggerFactory.getLogger(ReadAheadTask.class);

    private String bucketName;

    private String key;

    private ObsClient client;

    private ReadAheadBuffer buffer;

    private static final int MAX_RETRIES = 3;

    private RetryPolicy retryPolicy;

    public ReadAheadTask(String bucketName, String key, ObsClient client, ReadAheadBuffer buffer) {
        this.bucketName = bucketName;
        this.key = key;
        this.client = client;
        this.buffer = buffer;
        RetryPolicy defaultPolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(MAX_RETRIES, 3, TimeUnit.SECONDS);
        Map<Class<? extends Exception>, RetryPolicy> policies = new HashMap<>();
        policies.put(IOException.class, defaultPolicy);
        policies.put(IndexOutOfBoundsException.class, RetryPolicies.TRY_ONCE_THEN_FAIL);
        policies.put(NullPointerException.class, RetryPolicies.TRY_ONCE_THEN_FAIL);

        this.retryPolicy = RetryPolicies.retryByException(defaultPolicy, policies);
    }

    private boolean shouldRetry(Exception e, int retries) {
        boolean shouldRetry = true;
        try {
            RetryPolicy.RetryAction retry = retryPolicy.shouldRetry(e, retries, 0, true);
            if (retry.action == RetryPolicy.RetryAction.RetryDecision.RETRY) {
                Thread.sleep(retry.delayMillis);
            } else {
                //should not retry
                shouldRetry = false;
            }
        } catch (Exception ex) {
            //FAIL
            log.warn("Exception thrown when call shouldRetry, exception " + ex);
            shouldRetry = false;
        }
        return shouldRetry;
    }

    @Override
    public void run() {
        int retries = 0;
        buffer.lock();
        try {
            GetObjectRequest request = new GetObjectRequest(bucketName, key);
            request.setRangeStart(buffer.getByteStart());
            request.setRangeEnd(buffer.getByteEnd());
            while (true) {
                try (InputStream in = client.getObject(request).getObjectContent()) {
                    IOUtils.readFully(in, buffer.getBuffer(), 0, buffer.getBuffer().length);
                    buffer.setStatus(ReadAheadBuffer.STATUS.SUCCESS);
                    break;
                } catch (Exception e) {
                    log.warn("Exception thrown when retrieve key: " + this.key + ", exception: " + e);
                    retries++;
                    if (!shouldRetry(e, retries)) {
                        break;
                    }
                }
            }

            if (buffer.getStatus() != ReadAheadBuffer.STATUS.SUCCESS) {
                buffer.setStatus(ReadAheadBuffer.STATUS.ERROR);
            }

            //notify main thread which wait for this buffer
            buffer.signalAll();
        } finally {
            buffer.unlock();
        }
    }
}