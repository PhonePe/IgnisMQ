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

import com.phonepe.ignis.metric.IgnisMetrics;
import com.phonepe.ignis.utils.Constants;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tags;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Runs user message handlers off the worker thread, so one that never returns cannot hold a worker
 * thread for ever. It can stop waiting and free that thread; it cannot stop the handler, since
 * {@code cancel(true)} only interrupts.
 * <p>
 * A handler is never run without a timeout: the bound is clamped to half the sweep duration so a
 * handler cannot still be running when the sweeper deletes the record it is working on. Saturation
 * therefore refuses the batch rather than running it unbounded.
 */
@Slf4j
public final class HandlerExecutor {

    private final ThreadPoolExecutor executor;
    private final AtomicInteger saturationRefusals = new AtomicInteger();
    private final AtomicReference<Counter> pooled = new AtomicReference<>();
    private final AtomicReference<Counter> refused = new AtomicReference<>();

    public HandlerExecutor(final int maxThreads) {
        this.executor = new ThreadPoolExecutor(0, Math.max(1, maxThreads),
                60L, TimeUnit.SECONDS, new SynchronousQueue<>(), threadFactory(),
                HandlerExecutor::waitForAThread);
    }

    public void bindTo(final IgnisMetrics metrics) {
        metrics.getRegistry().gauge(IgnisMetrics.HANDLER_THREADS_ACTIVE, executor,
                ThreadPoolExecutor::getActiveCount);
        this.pooled.set(metrics.counter(IgnisMetrics.HANDLER_EXECUTIONS,
                Tags.of(IgnisMetrics.TAG_MODE, IgnisMetrics.POOLED)));
        this.refused.set(metrics.counter(IgnisMetrics.HANDLER_EXECUTIONS,
                Tags.of(IgnisMetrics.TAG_MODE, IgnisMetrics.REFUSED)));
    }

    /**
     * @throws TimeoutException          when the handler did not finish in time. It may still be
     *                                   running; the caller has simply stopped waiting.
     * @throws HandlerSaturatedException when no handler thread became available. The handler was
     *                                   <em>not</em> run, so the batch is untouched.
     * @throws Exception                 whatever the handler threw, unwrapped, so existing error
     *                                   handling behaves as it did when the handler ran inline.
     */
    // S112: the handler is arbitrary caller code. Its exception is rethrown unchanged so that error
    // handling written against the inline handler keeps working; wrapping it would break that.
    @SuppressWarnings("java:S112")
    public <T> T call(final Callable<T> task, final long timeoutMillis) throws Exception {
        final Future<T> future;
        try {
            future = executor.submit(task);
        } catch (RejectedExecutionException e) {
            saturationRefusals.incrementAndGet();
            increment(refused);
            throw new HandlerSaturatedException(executor.getMaximumPoolSize(),
                    Constants.HANDLER_SATURATION_GRACE_IN_MS, saturationRefusals.get(), e);
        }
        increment(pooled);
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

    public int saturationRefusals() {
        return saturationRefusals.get();
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

    /**
     * Waits a bounded time for a handler thread, then refuses. Handler demand cannot exceed worker
     * demand, so exhausting the grace period means threads have leaked rather than that the pool is
     * merely busy.
     */
    private static void waitForAThread(final Runnable task, final ThreadPoolExecutor executor) {
        try {
            final BlockingQueue<Runnable> queue = executor.getQueue();
            if (executor.isShutdown()
                    || !queue.offer(task, Constants.HANDLER_SATURATION_GRACE_IN_MS, TimeUnit.MILLISECONDS)) {
                throw new RejectedExecutionException("no handler thread became available");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RejectedExecutionException("interrupted waiting for a handler thread", e);
        }
    }

    private static void increment(final AtomicReference<Counter> ref) {
        final Counter counter = ref.get();
        if (Objects.nonNull(counter)) {
            counter.increment();
        }
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
