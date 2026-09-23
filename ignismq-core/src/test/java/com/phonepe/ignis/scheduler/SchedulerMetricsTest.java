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
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pool saturation, which shipped with a knob to tune it and no way of reading it.
 * <p>
 * These are the meters that make {@code workerThreads} tunable: without them the only symptom of an
 * undersized pool is latency with nothing attached to it, and the handler-execution timeout is
 * invisible whether it fires or not.
 */
class SchedulerMetricsTest {

    private IgnisSchedulers schedulers;
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final IgnisMetrics metrics = new IgnisMetrics(registry);

    @AfterEach
    void tearDown() {
        if (schedulers != null) {
            schedulers.stop();
        }
    }

    @Test
    @DisplayName("both pools and the handler pool publish their size and ceiling")
    void poolsPublishTheirShape() {
        schedulers = new IgnisSchedulers(7);
        schedulers.bindTo(metrics);

        assertEquals(7.0, gauge(IgnisMetrics.POOL_THREADS_MAX, "worker"),
                "the worker ceiling must be readable, or the knob cannot be compared against demand");
        assertTrue(gauge(IgnisMetrics.POOL_THREADS_MAX, "control") > 0);
        assertEquals(0.0, gauge(IgnisMetrics.POOL_TASKS_ACTIVE, "worker"));
        assertEquals(0.0, gauge(IgnisMetrics.POOL_TASKS_DUE, "worker"));
    }

    /**
     * The headline. A pool with one thread and more due work than it can start is the saturation an
     * operator has to be able to see, and it must be visible while it is happening rather than
     * inferred afterwards from throughput.
     */
    @Test
    @DisplayName("work that is due and cannot be started shows up as queue depth")
    void saturationShowsUpAsDueTasks() throws Exception {
        schedulers = new IgnisSchedulers(1);
        schedulers.bindTo(metrics);
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);

        schedulers.getWorker().scheduleRepeating(() -> {
            started.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, 0, 10);
        assertTrue(started.await(10, TimeUnit.SECONDS));
        // Three more tasks, all due immediately, against a pool of one that is already blocked.
        for (int i = 0; i < 3; i++) {
            schedulers.getWorker().scheduleRepeating(() -> {
            }, 0, 10_000);
        }

        try {
            await().untilAsserted(() -> assertTrue(gauge(IgnisMetrics.POOL_TASKS_DUE, "worker") >= 3.0,
                    "three tasks are due and the single thread is blocked, so depth must show it"));
            assertEquals(1.0, gauge(IgnisMetrics.POOL_TASKS_ACTIVE, "worker"));
        } finally {
            release.countDown();
        }
    }

    /**
     * A raw queue size would count every idle periodic task and read in the hundreds on a completely
     * healthy process, making it indistinguishable from a saturated one. Depth therefore counts only
     * tasks that are already due.
     */
    @Test
    @DisplayName("tasks merely waiting for their next turn are not queue depth")
    void idlePeriodicTasksAreNotDepth() {
        schedulers = new IgnisSchedulers(4);
        schedulers.bindTo(metrics);
        for (int i = 0; i < 20; i++) {
            schedulers.getWorker().scheduleRepeating(() -> {
            }, 60_000, 60_000);
        }

        assertEquals(0.0, gauge(IgnisMetrics.POOL_TASKS_DUE, "worker"),
                "twenty tasks are queued but none is due; a healthy idle process must read zero");
    }

    @Test
    @DisplayName("a run that had to wait for a thread records how long it waited")
    void taskWaitIsRecorded() throws Exception {
        schedulers = new IgnisSchedulers(1);
        schedulers.bindTo(metrics);
        final CountDownLatch runs = new CountDownLatch(2);

        schedulers.getWorker().scheduleRepeating(runs::countDown, 0, 10);
        assertTrue(runs.await(10, TimeUnit.SECONDS));

        await().untilAsserted(() -> assertTrue(registry.find(IgnisMetrics.POOL_TASK_WAIT)
                        .tags(Tags.of(IgnisMetrics.TAG_POOL, "worker")).timer().count() >= 2L,
                "every run records its lateness, so the timer must have samples"));
    }

    /**
     * The residual left by the pool-isolation work, settled. An {@code Error} still stops the
     * schedule - rescheduling through an OutOfMemoryError would loop on a condition that is not
     * going to clear - but the defect being removed was the silence, not the stopping.
     */
    @Test
    @DisplayName("a task killed by an Error is counted as fatal, and one that merely throws is not")
    void aFatalTaskIsCounted() throws Exception {
        schedulers = new IgnisSchedulers(2);
        schedulers.bindTo(metrics);
        final CountDownLatch thrown = new CountDownLatch(1);
        final AtomicInteger runs = new AtomicInteger();

        schedulers.getWorker().scheduleRepeating(() -> {
            runs.incrementAndGet();
            thrown.countDown();
            throw new StackOverflowError("boom");
        }, 0, 20);
        assertTrue(thrown.await(10, TimeUnit.SECONDS));

        await().untilAsserted(() -> assertEquals(1.0, failures("worker", "fatal"),
                "an Error must be counted before the schedule is allowed to die"));
        // And it really is dead, which is the deliberate half of the decision. The task repeats
        // every 20ms, so staying at one run across many periods is the assertion.
        Awaitility.await().during(Duration.ofMillis(300)).atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> assertEquals(1, runs.get(),
                        "an Error stops the schedule; it is not retried"));
    }

    @Test
    @DisplayName("an ordinary exception is counted as retried and the task stays scheduled")
    void aThrowingTaskIsCountedAsRetried() throws Exception {
        schedulers = new IgnisSchedulers(2);
        schedulers.bindTo(metrics);
        final CountDownLatch runs = new CountDownLatch(3);

        schedulers.getWorker().scheduleRepeating(() -> {
            runs.countDown();
            throw new IllegalStateException("boom");
        }, 0, 20);

        assertTrue(runs.await(10, TimeUnit.SECONDS), "a throwing task must stay scheduled");
        assertTrue(failures("worker", "retried") >= 3.0);
        assertEquals(0.0, failures("worker", "fatal"));
    }

    /**
     * A refused batch is the alertable condition: it means handler threads have leaked to handlers
     * that ignored their interrupt.
     */
    @Test
    @DisplayName("handler executions are split by whether a thread was available")
    void handlerSaturationIsPublished() throws Exception {
        final HandlerExecutor executor = new HandlerExecutor(1);
        executor.bindTo(metrics);
        final CountDownLatch hold = new CountDownLatch(1);
        final Thread blocker = new Thread(() -> {
            try {
                executor.call(() -> {
                    hold.await();
                    return true;
                }, 30_000);
            } catch (Exception e) {
                Thread.currentThread().interrupt();
            }
        });
        blocker.start();

        try {
            await().untilAsserted(() -> assertEquals(1.0, handlerExecutions(IgnisMetrics.POOLED)));
            // The only thread is occupied, so this batch cannot be given one.
            try {
                executor.call(() -> true, 1_000);
            } catch (HandlerSaturatedException expected) {
                // the meter is what this test is about
            }

            assertEquals(1.0, handlerExecutions(IgnisMetrics.REFUSED),
                    "a refused batch must be visible, not just logged");
        } finally {
            hold.countDown();
            blocker.join(5_000);
            executor.stop();
        }
    }

    private static org.awaitility.core.ConditionFactory await() {
        return Awaitility.await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(20));
    }

    private double gauge(final String name, final String pool) {
        return registry.get(name).tags(IgnisMetrics.TAG_POOL, pool).gauge().value();
    }

    private double counter(final String name, final Tags tags) {
        final io.micrometer.core.instrument.Counter counter = registry.find(name).tags(tags).counter();
        return counter == null ? 0.0 : counter.count();
    }

    private double failures(final String pool, final String outcome) {
        return counter(IgnisMetrics.POOL_TASK_FAILURES,
                Tags.of(IgnisMetrics.TAG_POOL, pool, IgnisMetrics.TAG_OUTCOME, outcome));
    }

    private double handlerExecutions(final String mode) {
        return counter(IgnisMetrics.HANDLER_EXECUTIONS, Tags.of(IgnisMetrics.TAG_MODE, mode));
    }
}
