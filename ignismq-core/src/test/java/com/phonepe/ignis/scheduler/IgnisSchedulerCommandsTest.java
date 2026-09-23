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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class IgnisSchedulerCommandsTest {

    private IgnisSchedulerCommands scheduler;

    @AfterEach
    public void tearDown() {
        if (scheduler != null) {
            scheduler.stop();
        }
    }

    /**
     * The defect that made {@code Timer} unusable, and which a plain executor reproduces: a
     * repeating task that throws is cancelled silently and every later run is lost. That is the
     * "queue just stopped draining" failure, so swapping the primitive without wrapping the task
     * would have fixed nothing.
     */
    @Test
    public void testAThrowingTaskKeepsRunning() throws Exception {
        scheduler = new IgnisSchedulerCommands();
        final CountDownLatch runs = new CountDownLatch(3);

        scheduler.scheduleRepeating(() -> {
            runs.countDown();
            throw new IllegalStateException("boom");
        }, 0, 20);

        assertTrue(runs.await(10, TimeUnit.SECONDS), "a throwing task must stay scheduled");
    }

    /**
     * A cancelled task stops running and does not linger in the delay queue.
     */
    @Test
    public void testACancelledTaskStopsRunning() throws Exception {
        scheduler = new IgnisSchedulerCommands();
        final AtomicInteger runs = new AtomicInteger();
        final ScheduledFuture<?> task = scheduler.scheduleRepeating(runs::incrementAndGet, 0, 20);

        Thread.sleep(200);
        task.cancel(true);
        final int atCancellation = runs.get();
        Thread.sleep(200);

        assertEquals(atCancellation, runs.get(), "no runs may happen after cancellation");
    }

    /**
     * The pool grows with the number of recurring tasks so a queue configured for N consumers
     * actually gets N able to run at once, rather than time-sharing a fixed handful.
     */
    @Test
    public void testThePoolGrowsWithRecurringTasks() {
        scheduler = new IgnisSchedulerCommands();
        final int base = scheduler.corePoolSize();

        for (int i = 0; i < base + 5; i++) {
            scheduler.scheduleRepeating(() -> {
            }, 60_000, 60_000);
        }

        assertTrue(scheduler.corePoolSize() > base, "pool should have grown past its base");
    }

    /**
     * Growth stops at the ceiling; above it, tasks time-share rather than allocating a thread each.
     */
    @Test
    public void testThePoolIsCapped() {
        scheduler = new IgnisSchedulerCommands();

        for (int i = 0; i < Constants.DEFAULT_WORKER_THREADS + 10; i++) {
            scheduler.scheduleRepeating(() -> {
            }, 60_000, 60_000);
        }

        assertEquals(Constants.DEFAULT_WORKER_THREADS, scheduler.corePoolSize());
    }

    /**
     * A one-shot must not grow the pool: it is transient by definition.
     */
    @Test
    public void testAOneShotDoesNotGrowThePool() {
        scheduler = new IgnisSchedulerCommands();
        final int base = scheduler.corePoolSize();

        scheduler.scheduleOnce(() -> {
        }, 60_000);

        assertEquals(base, scheduler.corePoolSize());
    }

    @Test
    public void testStopLeavesNoThreadsRunning() throws Exception {
        scheduler = new IgnisSchedulerCommands();
        final CountDownLatch started = new CountDownLatch(1);
        scheduler.scheduleRepeating(() -> {
            started.countDown();
            try {
                Thread.sleep(60_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, 0, 10);
        assertTrue(started.await(5, TimeUnit.SECONDS));

        assertTrue(scheduler.stop(), "stop must complete within its grace period");
        assertTrue(scheduler.isStopped());

        final long deadline = System.currentTimeMillis() + 10_000L;
        while (liveSchedulerThreads() > 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertEquals(0L, liveSchedulerThreads(), "scheduler threads must not survive stop()");
    }

    /**
     * A framework lifecycle stops things it never started, and may stop them twice.
     */
    @Test
    public void testStopIsIdempotentAndSafeWithoutWork() {
        scheduler = new IgnisSchedulerCommands();

        assertFalse(scheduler.isStopped());
        assertTrue(scheduler.stop());
        assertTrue(scheduler.stop());
        assertTrue(scheduler.isStopped());
    }

    /**
     * Cancelling through the scheduler gives the pool back.
     * <p>
     * The pool grew because a task existed; when it stops existing the pool must shrink, or scaling
     * a queue down and up repeatedly ratchets the thread count up for demand that has gone. The
     * count only ever grew before, so this failed against the previous implementation.
     */
    @Test
    public void testCancellingRecurringTasksGivesThreadsBack() {
        scheduler = new IgnisSchedulerCommands();
        final int base = scheduler.corePoolSize();
        final List<ScheduledFuture<?>> tasks = new ArrayList<>();
        for (int i = 0; i < base + 10; i++) {
            tasks.add(scheduler.scheduleRepeating(() -> {
            }, 60_000, 60_000));
        }
        final int grown = scheduler.corePoolSize();
        assertTrue(grown > base);

        tasks.forEach(scheduler::cancelRepeating);

        assertTrue(scheduler.corePoolSize() < grown, "the pool must shrink once the tasks it grew for are gone");
    }

    /**
     * A one-shot never grew the pool, so cancelling one must not shrink it. Shovels are recurring or
     * one-shot depending on autoDelete and the caller cancelling them no longer knows which, so this
     * has to be decided by the scheduler rather than trusted from the call site.
     */
    @Test
    public void testCancellingAOneShotDoesNotShrinkThePool() {
        scheduler = new IgnisSchedulerCommands();
        for (int i = 0; i < 10; i++) {
            scheduler.scheduleRepeating(() -> {
            }, 60_000, 60_000);
        }
        final int grown = scheduler.corePoolSize();

        final ScheduledFuture<?> oneShot = scheduler.scheduleOnce(() -> {
        }, 60_000);
        scheduler.cancelRepeating(oneShot);
        scheduler.cancelRepeating(oneShot);

        assertEquals(grown, scheduler.corePoolSize(), "a one-shot must not affect pool sizing");
    }

    private static long liveSchedulerThreads() {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(Thread::isAlive)
                .filter(thread -> thread.getName().startsWith("ignismq-scheduler-"))
                .count();
    }
}
