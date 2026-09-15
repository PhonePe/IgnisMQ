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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class IgnisSchedulersTest {

    private IgnisSchedulers schedulers;

    @AfterEach
    public void tearDown() {
        if (schedulers != null) {
            schedulers.stop();
        }
    }

    /**
     * The reason the pools are split at all.
     * <p>
     * Saturate the worker pool the way a backlogged queue does - every thread occupied, none of them
     * returning - and the queue watcher and the sweeper must still run. On a single shared pool they
     * would not: the control tasks would sit in the delay queue behind work that never completes,
     * and a process would silently stop refreshing queues and stop recovering orphaned messages
     * while looking perfectly healthy.
     */
    @Test
    public void testSaturatedWorkersCannotStarveTheControlPool() throws Exception {
        schedulers = new IgnisSchedulers();

        // One task per worker thread, each of which never returns - the shape of a consumer
        // draining a permanent backlog, which is precisely what the run budget now prevents but
        // which the isolation must survive regardless.
        final int workers = Constants.DEFAULT_WORKER_THREADS;
        final CountDownLatch occupied = new CountDownLatch(workers);
        final CountDownLatch release = new CountDownLatch(1);
        for (int i = 0; i < workers; i++) {
            schedulers.getWorker().scheduleRepeating(() -> {
                occupied.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, 0, 10);
        }
        assertTrue(occupied.await(30, TimeUnit.SECONDS), "worker pool should have filled");

        final CountDownLatch controlRuns = new CountDownLatch(3);
        schedulers.getControl().scheduleRepeating(controlRuns::countDown, 0, 20);

        assertTrue(controlRuns.await(10, TimeUnit.SECONDS), "control tasks must run while every worker thread is occupied");
        release.countDown();
    }

    /**
     * The control pool is fixed: it must not grow, so it can never become the thing that is starved.
     */
    @Test
    public void testTheControlPoolDoesNotGrow() {
        schedulers = new IgnisSchedulers();

        for (int i = 0; i < 20; i++) {
            schedulers.getControl().scheduleRepeating(() -> {
            }, 60_000, 60_000);
        }

        assertEquals(Constants.SCHEDULER_CONTROL_THREADS, schedulers.getControl().corePoolSize());
    }

    /**
     * The worker pool grows, and only up to its own ceiling.
     */
    @Test
    public void testTheWorkerPoolGrowsToItsCeiling() {
        schedulers = new IgnisSchedulers();

        for (int i = 0; i < Constants.DEFAULT_WORKER_THREADS + 10; i++) {
            schedulers.getWorker().scheduleRepeating(() -> {
            }, 60_000, 60_000);
        }

        assertEquals(Constants.DEFAULT_WORKER_THREADS, schedulers.getWorker().corePoolSize());
    }

    /**
     * The configured ceiling binds, and the handler pool is sized from the same number: a handler
     * thread is only occupied while its worker is blocked on it.
     */
    @Test
    public void testAConfiguredCeilingBindsInsteadOfTheDefault() {
        schedulers = new IgnisSchedulers(3);

        for (int i = 0; i < 10; i++) {
            schedulers.getWorker().scheduleRepeating(() -> {
            }, 60_000, 60_000);
        }

        assertEquals(3, schedulers.getWorker().corePoolSize());
    }

    /**
     * Threads are named per pool, so a saturated pool is identifiable from a stack dump alone.
     */
    @Test
    public void testPoolsAreNamedDistinctly() throws Exception {
        schedulers = new IgnisSchedulers();
        final CountDownLatch ran = new CountDownLatch(2);
        schedulers.getControl().scheduleRepeating(ran::countDown, 0, 20);
        schedulers.getWorker().scheduleRepeating(ran::countDown, 0, 20);
        assertTrue(ran.await(10, TimeUnit.SECONDS));

        assertTrue(liveThreads("ignismq-control-") > 0, "control threads must be identifiable");
        assertTrue(liveThreads("ignismq-worker-") > 0, "worker threads must be identifiable");
    }

    /**
     * Both pools stop, and nothing survives. A framework lifecycle may also stop them twice.
     */
    @Test
    public void testStopHaltsBothPoolsAndIsIdempotent() throws Exception {
        schedulers = new IgnisSchedulers();
        final CountDownLatch ran = new CountDownLatch(2);
        schedulers.getControl().scheduleRepeating(ran::countDown, 0, 20);
        schedulers.getWorker().scheduleRepeating(ran::countDown, 0, 20);
        assertTrue(ran.await(10, TimeUnit.SECONDS));

        assertFalse(schedulers.isStopped());
        assertTrue(schedulers.stop());
        assertTrue(schedulers.stop());
        assertTrue(schedulers.isStopped());

        final long deadline = System.currentTimeMillis() + 10_000L;
        while ((liveThreads("ignismq-control-") + liveThreads("ignismq-worker-")) > 0
                && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertEquals(0L, liveThreads("ignismq-control-") + liveThreads("ignismq-worker-"), "no pool thread may survive stop()");
    }

    private static long liveThreads(final String prefix) {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(Thread::isAlive)
                .filter(thread -> thread.getName().startsWith(prefix))
                .count();
    }
}
