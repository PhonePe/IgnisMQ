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
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Delayed;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A named, bounded pool of scheduled tasks, with daemon threads.
 * <p>
 * ignisMQ runs two of these rather than one - see {@link IgnisSchedulers} for why the split matters.
 */
@Slf4j
public final class IgnisSchedulerCommands {

    private static final String RETRIED = "retried";
    private static final String FATAL = "fatal";

    private final ScheduledThreadPoolExecutor executor;
    private final int maxThreads;
    private final AtomicInteger recurringTasks = new AtomicInteger();
    /** Membership rather than a count, so a double cancel or a cancelled one-shot is a no-op. */
    private final Set<ScheduledFuture<?>> recurring = ConcurrentHashMap.newKeySet();
    private volatile String poolTag;
    /** Null until {@link #bindTo}; unit tests and the standalone TaskInitializer stay unbound. */
    private volatile Timer taskWait;
    private volatile IgnisMetrics metrics;

    public IgnisSchedulerCommands() {
        this("ignismq-scheduler", Constants.SCHEDULER_BASE_THREADS, Constants.DEFAULT_WORKER_THREADS);
    }

    /**
     * @param maxThreads ceiling on growth. A pool that must never be crowded out passes the same
     *                   value for both, making it fixed.
     */
    public IgnisSchedulerCommands(final String namePrefix, final int coreThreads, final int maxThreads) {
        this.maxThreads = Math.max(1, maxThreads);
        this.executor = new ScheduledThreadPoolExecutor(
                Math.max(1, Math.min(coreThreads, this.maxThreads)), threadFactory(namePrefix));
        // Otherwise a cancelled task sits in the delay queue until its next scheduled run.
        this.executor.setRemoveOnCancelPolicy(true);
    }

    /**
     * {@code tasks.due} counts overdue tasks, not queued ones: every idle periodic task sits in the
     * queue awaiting its next due time, so a raw queue size cannot tell idle from saturated.
     */
    public void bindTo(final IgnisMetrics metrics, final String pool) {
        this.poolTag = pool;
        this.metrics = metrics;
        final Tags tags = Tags.of(IgnisMetrics.TAG_POOL, pool);
        metrics.getRegistry().gauge(IgnisMetrics.POOL_THREADS, tags, executor,
                ScheduledThreadPoolExecutor::getPoolSize);
        metrics.getRegistry().gauge(IgnisMetrics.POOL_THREADS_MAX, tags, this, commands -> commands.maxThreads);
        metrics.getRegistry().gauge(IgnisMetrics.POOL_TASKS_ACTIVE, tags, executor,
                ScheduledThreadPoolExecutor::getActiveCount);
        metrics.getRegistry().gauge(IgnisMetrics.POOL_TASKS_DUE, tags, executor,
                IgnisSchedulerCommands::overdueTasks);
        this.taskWait = metrics.timer(IgnisMetrics.POOL_TASK_WAIT, tags);
    }

    /**
     * Fixed <em>delay</em>, not fixed rate: a consumer slower than its period must not have runs
     * queued up behind it. Grows the pool towards this scheduler's ceiling.
     */
    public ScheduledFuture<?> scheduleRepeating(final Runnable task, final long initialDelayMillis,
                                                final long delayMillis) {
        growFor(recurringTasks.incrementAndGet());
        final ScheduledFuture<?> future = executor.scheduleWithFixedDelay(
                guard(timed(task, initialDelayMillis, delayMillis)),
                initialDelayMillis, delayMillis, TimeUnit.MILLISECONDS);
        recurring.add(future);
        return future;
    }

    /**
     * Use this rather than {@code future.cancel(...)}: cancelling the future alone leaves the pool
     * sized for the task, so scaling a queue down and up would ratchet the thread count up.
     */
    public void cancelRepeating(final ScheduledFuture<?> task) {
        // Interrupts a consumer sitting on its poll interval; the task treats that as a stop.
        task.cancel(true);
        if (recurring.remove(task)) {
            shrinkTo(recurringTasks.decrementAndGet());
        }
    }

    /** Schedules a one-shot task. Does not grow the pool: one-shots are transient by definition. */
    public ScheduledFuture<?> scheduleOnce(final Runnable task, final long delayMillis) {
        return executor.schedule(guard(task), delayMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * Stops accepting work and waits for running tasks to finish.
     * <p>
     * Idempotent, and safe to call without a preceding schedule. Interrupts rather than waits out a
     * consumer sitting on its batching deadline.
     *
     * @return true when every thread stopped within the grace period.
     */
    public boolean stop() {
        recurring.clear();
        executor.shutdownNow();
        try {
            final boolean stopped = executor.awaitTermination(
                    Constants.SCHEDULER_SHUTDOWN_GRACE_IN_MS, TimeUnit.MILLISECONDS);
            if (!stopped) {
                log.warn("Scheduler did not stop within {}ms; {} task(s) still running",
                        Constants.SCHEDULER_SHUTDOWN_GRACE_IN_MS, executor.getActiveCount());
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

    /** Visible for tests that assert the pool is sized and torn down as intended. */
    public int poolSize() {
        return executor.getPoolSize();
    }

    public int corePoolSize() {
        return executor.getCorePoolSize();
    }

    /** Escape hatch for callers that genuinely need the executor, such as tests. */
    ScheduledExecutorService executor() {
        return executor;
    }

    private static double overdueTasks(final ScheduledThreadPoolExecutor executor) {
        return executor.getQueue().stream()
                .filter(Delayed.class::isInstance)
                .filter(task -> ((Delayed) task).getDelay(TimeUnit.NANOSECONDS) <= 0L)
                .count();
    }

    /**
     * Time spent waiting for a thread. Under fixed delay the next run is due exactly
     * {@code delayMillis} after the previous finished, so anything past that is time queued.
     */
    private Runnable timed(final Runnable task, final long initialDelayMillis, final long delayMillis) {
        final AtomicLong dueAt = new AtomicLong(
                System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(initialDelayMillis));
        return () -> {
            final Timer waited = taskWait;
            if (Objects.nonNull(waited)) {
                waited.record(Math.max(0L, System.nanoTime() - dueAt.get()), TimeUnit.NANOSECONDS);
            }
            try {
                task.run();
            } finally {
                dueAt.set(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(delayMillis));
            }
        };
    }

    private void growFor(final int tasks) {
        final int target = Math.min(tasks, maxThreads);
        if (target > executor.getCorePoolSize()) {
            executor.setCorePoolSize(target);
        }
    }

    private void shrinkTo(final int tasks) {
        final int target = Math.max(Constants.SCHEDULER_BASE_THREADS, Math.min(tasks, maxThreads));
        if (target < executor.getCorePoolSize()) {
            executor.setCorePoolSize(target);
        }
    }

    /**
     * {@code ScheduledExecutorService} cancels a recurring task the first time it throws, silently,
     * so every task is wrapped. {@code Error} is counted and then rethrown: rescheduling through an
     * {@code OutOfMemoryError} loops on a condition that will not clear, so {@code outcome=fatal}
     * is the signal that a schedule is gone until restart.
     */
    private Runnable guard(final Runnable task) {
        return () -> {
            try {
                task.run();
            } catch (Exception e) {
                count(RETRIED);
                log.error("Scheduled task failed; it remains scheduled and will run again", e);
            } catch (Error e) {
                count(FATAL);
                log.error("Scheduled task died with an Error; its schedule is lost until this process "
                        + "restarts. This is not retried: rescheduling through an Error loops on a "
                        + "condition that will not clear", e);
                throw e;
            }
        };
    }

    private void count(final String outcome) {
        final IgnisMetrics bound = metrics;
        if (Objects.nonNull(bound)) {
            bound.counter(IgnisMetrics.POOL_TASK_FAILURES,
                    Tags.of(IgnisMetrics.TAG_POOL, poolTag, IgnisMetrics.TAG_OUTCOME, outcome)).increment();
        }
    }

    private static ThreadFactory threadFactory(final String namePrefix) {
        final AtomicInteger counter = new AtomicInteger();
        return runnable -> {
            final Thread thread = Executors.defaultThreadFactory().newThread(runnable);
            thread.setName(namePrefix + "-" + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }
}
