/**
 * Copyright (c) 2026 Original Author(s), PhonePe India Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.phonepe.ignis.scheduler;

import com.phonepe.ignis.utils.Constants;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Runs user message handlers off the worker thread, so one that never returns cannot hold a worker
 * thread for ever. It can stop waiting and free that thread; it cannot stop the handler, since
 * {@code cancel(true)} only interrupts - so a handler that times out repeatedly leaks threads.
 * <p>
 * A handler occupies a thread here only while its worker is blocked on it, so this adds no
 * concurrency: one extra thread per executing handler, reclaimed after 60s idle. At saturation the
 * handler runs inline, without a timeout, rather than being rejected.
 */
@Slf4j
public final class HandlerExecutor {

    private final ThreadPoolExecutor executor;
    private final AtomicInteger inlineExecutions = new AtomicInteger();

    public HandlerExecutor(final int maxThreads) {
        this.executor = new ThreadPoolExecutor(0, Math.max(1, maxThreads),
                60L, TimeUnit.SECONDS, new SynchronousQueue<>(), threadFactory());
    }

    /**
     * @throws TimeoutException when the handler did not finish in time. It may still be running;
     *                          the caller has simply stopped waiting.
     * @throws Exception        whatever the handler threw, unwrapped, so existing error handling
     *                          behaves as it did when the handler ran inline.
     */
    public <T> T call(final Callable<T> task, final long timeoutMillis) throws Exception {
        final Future<T> future;
        try {
            future = executor.submit(task);
        } catch (RejectedExecutionException e) {
            inlineExecutions.incrementAndGet();
            log.warn("Handler pool saturated; running this batch inline and without a timeout. "
                    + "Inline executions so far: {}", inlineExecutions.get());
            return task.call();
        }
        try {
            return future.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            // Best effort. An uninterruptible handler ignores this and keeps its thread.
            future.cancel(true);
            throw e;
        } catch (ExecutionException e) {
            throw unwrap(e);
        }
    }

    /** Batches that had to run inline, without a timeout, because the pool was full. */
    public int inlineExecutions() {
        return inlineExecutions.get();
    }

    public int activeHandlers() {
        return executor.getActiveCount();
    }

    /**
     * @return false when a handler is ignoring interruption, which this cannot fix.
     */
    public boolean stop() {
        executor.shutdownNow();
        try {
            final boolean stopped = executor.awaitTermination(
                    Constants.SCHEDULER_SHUTDOWN_GRACE_IN_MS, TimeUnit.MILLISECONDS);
            if (!stopped) {
                log.warn("{} handler(s) did not stop within {}ms; they are ignoring interruption",
                        executor.getActiveCount(), Constants.SCHEDULER_SHUTDOWN_GRACE_IN_MS);
            }
            return stopped;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    public boolean isStopped() {
        return executor.isShutdown();
    }

    private static Exception unwrap(final ExecutionException e) {
        final Throwable cause = e.getCause();
        if (cause instanceof Exception exception) {
            return exception;
        }
        if (cause instanceof Error error) {
            throw error;
        }
        return e;
    }

    private static ThreadFactory threadFactory() {
        final AtomicInteger counter = new AtomicInteger();
        return runnable -> {
            final Thread thread = Executors.defaultThreadFactory().newThread(runnable);
            thread.setName("ignismq-handler-" + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }
}
