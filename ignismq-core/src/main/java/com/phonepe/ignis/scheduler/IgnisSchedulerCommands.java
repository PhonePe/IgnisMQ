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

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A named, bounded pool of scheduled tasks.
 * <p>
 * Replaces the six independent {@link java.util.Timer}s that used to sit behind consumers, shovels,
 * shovel retries, the queue watcher and the sweeper. {@code Timer} was the wrong primitive for three
 * reasons, in ascending order of severity:
 * <ol>
 *   <li><strong>A thread per timer.</strong> Scheduling cost scaled with configured concurrency
 *       rather than with actual work, and the thread existed whether or not the task had anything
 *       to do.</li>
 *   <li><strong>One uncaught exception killed the schedule.</strong> {@code Timer} terminates its
 *       thread on any escaping {@code RuntimeException} and every later run is silently lost - the
 *       failure that looks like "the queue just stopped draining". A pool completes that one
 *       execution exceptionally and, for a repeating task, is the same trap unless the task is
 *       wrapped. It is wrapped here.</li>
 *   <li><strong>No introspection.</strong> No queue depth, no run duration, no rejections - which
 *       is observable, unlike a Timer.</li>
 * </ol>
 * Threads are daemons, so a JVM shutting down without calling {@link #stop()} is not held open.
 * <p>
 * ignisMQ runs two of these rather than one - see {@link IgnisSchedulers} for why the split matters.
 */
@Slf4j
public final class IgnisSchedulerCommands {

    private final ScheduledThreadPoolExecutor executor;
    private final int maxThreads;
    /** Live recurring tasks. Decremented on cancellation, so the pool tracks demand in both directions. */
    private final AtomicInteger recurringTasks = new AtomicInteger();
    /**
     * The futures this scheduler grew the pool for.
     * <p>
     * Membership, not a count, because callers legitimately hold a mixed bag: a shovel is recurring
     * or one-shot depending on {@code autoDelete}, and the caller cancelling it no longer knows
     * which. Deciding from the set makes a double cancel, or a cancel of a one-shot, a no-op
     * instead of an off-by-one that would shrink the pool below real demand.
     */
    private final Set<ScheduledFuture<?>> recurring = ConcurrentHashMap.newKeySet();

    public IgnisSchedulerCommands() {
        this("ignismq-scheduler", Constants.SCHEDULER_BASE_THREADS, Constants.DEFAULT_WORKER_THREADS);
    }

    /**
     * @param namePrefix thread name prefix, so a stack dump says which pool is saturated.
     * @param coreThreads threads before any recurring task is registered.
     * @param maxThreads ceiling on growth. A pool that must never be crowded out passes the same
     *                   value for both, making it fixed.
     */
    public IgnisSchedulerCommands(final String namePrefix, final int coreThreads, final int maxThreads) {
        this.maxThreads = Math.max(1, maxThreads);
        this.executor = new ScheduledThreadPoolExecutor(
                Math.max(1, Math.min(coreThreads, this.maxThreads)), threadFactory(namePrefix));
        // Without this a cancelled consumer's task sits in the delay queue until its next scheduled
        // run, so repeatedly scaling a queue down and up would accumulate dead entries.
        this.executor.setRemoveOnCancelPolicy(true);
    }

    /**
     * Schedules a task to repeat with a fixed gap between the end of one run and the start of the
     * next.
     * <p>
     * Fixed <em>delay</em>, not fixed rate: a consumer that takes longer than its period must not
     * have runs queued up behind it. The pool is grown to keep pace with the number of recurring
     * tasks, up to this scheduler's ceiling, so that a queue configured for sixteen consumers
     * actually gets sixteen able to run at once.
     */
    public ScheduledFuture<?> scheduleRepeating(final Runnable task, final long initialDelayMillis,
                                                final long delayMillis) {
        growFor(recurringTasks.incrementAndGet());
        final ScheduledFuture<?> future = executor.scheduleWithFixedDelay(guard(task),
                initialDelayMillis, delayMillis, TimeUnit.MILLISECONDS);
        recurring.add(future);
        return future;
    }

    /**
     * Cancels a task registered through {@link #scheduleRepeating}, releasing its share of the pool.
     * <p>
     * Use this rather than {@code future.cancel(...)} directly. Cancelling the future alone stops
     * the task but leaves the pool sized for it, so scaling a queue down and up repeatedly would
     * ratchet the thread count up without bound - the pool would grow for demand that no longer
     * exists and never give it back.
     *
     * @param task the future returned by {@link #scheduleRepeating}.
     */
    public void cancelRepeating(final ScheduledFuture<?> task) {
        // Interrupts a consumer sitting on its poll interval rather than waiting it out; the task
        // treats interruption as a reason to stop, not an error.
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
     * Keeps a repeating task repeating.
     * <p>
     * {@code ScheduledExecutorService} cancels a recurring task the first time it throws, silently -
     * the same defect as {@code Timer}, and the reason swapping the primitive alone would not have
     * fixed anything. Every task is therefore wrapped so nothing escapes.
     */
    private static Runnable guard(final Runnable task) {
        return () -> {
            try {
                task.run();
            } catch (Exception e) {
                log.error("Scheduled task failed; it remains scheduled and will run again", e);
            }
        };
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

    /** Escape hatch for callers that genuinely need the executor, such as tests. */
    ScheduledExecutorService executor() {
        return executor;
    }
}
