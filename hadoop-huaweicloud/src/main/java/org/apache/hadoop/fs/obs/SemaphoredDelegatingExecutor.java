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

import com.google.common.util.concurrent.ForwardingListeningExecutorService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.sun.istack.NotNull;

import org.apache.hadoop.classification.InterfaceAudience;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * This ExecutorService blocks the submission of new tasks when its queue is
 * already full by using a semaphore. Task submissions require permits, task
 * completions release permits.
 *
 * <p>This is a refactoring of {@link BlockingThreadPoolExecutorService}; that
 * code contains the thread pool logic, whereas this isolates the semaphore and
 * submit logic for use with other thread pools and delegation models. In
 * particular, it
 * <i>permits multiple per stream executors to share a single per-FS-instance
 * executor; the latter to throttle overall load from the the FS, the others to
 * limit the amount of load which a single output stream can generate.</i>
 *
 * <p>This is inspired by s4 thread pool (see https://github
 * .com/apache/incubator-s4/blob/master/subprojects/s4-comm/src/main/java/org
 * /apache/s4/comm/staging/BlockingThreadPoolExecutorService.java)
 */
@InterfaceAudience.Private
public class SemaphoredDelegatingExecutor extends ForwardingListeningExecutorService {
    /**
     * Number of permits queued.
     */
    private final Semaphore queueingPermits;

    /**
     * Executor instance.
     */
    private final ListeningExecutorService executorDelegatee;

    /**
     * Number of permits.
     */
    private final int permitCount;

    /**
     * Instantiate.
     *
     * @param listExecutorDelegatee executor to delegate to
     * @param permitSize            number of permits into the queue permitted
     * @param fair                  should the semaphore be "fair"
     */
    public SemaphoredDelegatingExecutor(final ListeningExecutorService listExecutorDelegatee, final int permitSize,
        final boolean fair) {
        this.permitCount = permitSize;
        queueingPermits = new Semaphore(permitSize, fair);
        this.executorDelegatee = listExecutorDelegatee;
    }

    @Override
    protected ListeningExecutorService delegate() {
        return executorDelegatee;
    }

    @NotNull
    @Override
    public <T> ListenableFuture<T> submit(@NotNull final Callable<T> task) {
        try {
            queueingPermits.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Futures.immediateFailedCheckedFuture(e);
        }
        return super.submit(new CallableWithPermitRelease<>(task));
    }

    @NotNull
    @Override
    public <T> List<Future<T>> invokeAll(@NotNull final Collection<? extends Callable<T>> tasks) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @NotNull
    @Override
    public <T> List<Future<T>> invokeAll(@NotNull final Collection<? extends Callable<T>> tasks, final long timeout,
        @NotNull final TimeUnit unit) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @NotNull
    @Override
    public <T> T invokeAny(@NotNull final Collection<? extends Callable<T>> tasks) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public <T> T invokeAny(@NotNull final Collection<? extends Callable<T>> tasks, final long timeout,
        @NotNull final TimeUnit unit) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @NotNull
    @Override
    public <T> ListenableFuture<T> submit(@NotNull final Runnable task, @NotNull final T result) {
        try {
            queueingPermits.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Futures.immediateFailedCheckedFuture(e);
        }
        return super.submit(new RunnableWithPermitRelease(task), result);
    }

    public int getAvailablePermits() {
        return queueingPermits.availablePermits();
    }

    @Override
    public void execute(@NotNull final Runnable command) {
        try {
            queueingPermits.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        super.execute(new RunnableWithPermitRelease(command));
    }

    public int getWaitingCount() {
        return queueingPermits.getQueueLength();
    }

    public int getPermitCount() {
        return permitCount;
    }

    @NotNull
    @Override
    public ListenableFuture<?> submit(@NotNull final Runnable task) {
        try {
            queueingPermits.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Futures.immediateFailedCheckedFuture(e);
        }
        return super.submit(new RunnableWithPermitRelease(task));
    }

    @Override
    public String toString() {
        return "SemaphoredDelegatingExecutor{" + "permitCount=" + getPermitCount() + ", available="
            + getAvailablePermits() + ", waiting=" + getWaitingCount() + '}';
    }

    /**
     * Releases a permit after the task is executed.
     */
    class RunnableWithPermitRelease implements Runnable {

        /**
         * Delegatee : Executor to delegate to.
         */
        private Runnable delegatee;

        RunnableWithPermitRelease(final Runnable exeDelegatee) {
            this.delegatee = exeDelegatee;
        }

        @Override
        public void run() {
            try {
                delegatee.run();
            } finally {
                queueingPermits.release();
            }
        }
    }

    /**
     * Releases a permit after the task is completed.
     *
     * @param <T> the result type of method {@code call}
     */
    class CallableWithPermitRelease<T> implements Callable<T> {

        /**
         * Delegatee : Executor to delegate to.
         */
        private Callable<T> delegatee;

        CallableWithPermitRelease(final Callable<T> delegateeTask) {
            this.delegatee = delegateeTask;
        }

        @Override
        public T call() throws Exception {
            try {
                return delegatee.call();
            } finally {
                queueingPermits.release();
            }
        }
    }
}
