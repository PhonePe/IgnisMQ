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

package com.phonepe.ignis.metric;

import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The metrics switch, and the one meter whose value costs a storage read.
 */
class IgnisMetricsTest {

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();

    @Test
    @DisplayName("metrics are on unless asked otherwise")
    void enabledByDefault() {
        final IgnisMetrics metrics = new IgnisMetrics(registry);
        assertTrue(metrics.isEnabled());

        metrics.counter(IgnisMetrics.SIDELINE, Tags.of(IgnisMetrics.TAG_QUEUE, "Q")).increment();

        assertEquals(1.0, registry.get(IgnisMetrics.SIDELINE).tags(IgnisMetrics.TAG_QUEUE, "Q")
                .counter().count());
    }

    @Test
    @DisplayName("disabling keeps every call working but publishes nothing to the caller's registry")
    void disabledPublishesNothing() throws Exception {
        final IgnisMetrics metrics = new IgnisMetrics(registry, false);
        assertFalse(metrics.isEnabled());

        // Everything still has to be callable: disabling instrumentation must never become a second
        // code path with its own null checks for the rest of the codebase to get wrong.
        metrics.counter(IgnisMetrics.SIDELINE, Tags.of(IgnisMetrics.TAG_QUEUE, "Q")).increment();
        metrics.timer(IgnisMetrics.CONSUME, Tags.empty()).record(java.time.Duration.ofMillis(1));
        metrics.summary(IgnisMetrics.HANDLER_BATCH_SIZE, Tags.empty()).record(5);
        metrics.gauge(IgnisMetrics.QUEUE_DEPTH, Tags.empty(), this, self -> 1.0);
        assertEquals("ok", metrics.record(IgnisMetrics.QUEUE_CREATE, Tags.empty(), () -> "ok"));

        assertTrue(registry.getMeters().isEmpty(),
                "a disabled registry must leave the application's own registry untouched");
    }

    @Test
    @DisplayName("a disabled timing helper still propagates the exception it did not record")
    void disabledStillPropagates() {
        final IgnisMetrics metrics = new IgnisMetrics(registry, false);

        assertThrowsIllegalState(() -> metrics.record(IgnisMetrics.QUEUE_CREATE, Tags.empty(),
                () -> {
                    throw new IllegalStateException("boom");
                }));
    }

    /**
     * The property that keeps queue depth affordable. Gauges are pulled, so a naive implementation
     * would issue a storage read per gauge per queue per scrape - hundreds of reads a minute purely
     * to produce metrics.
     */
    @Test
    @DisplayName("every depth gauge on every queue is served by one shared, cached read")
    void depthGaugesShareOneRefresh() {
        final IgnisMetrics metrics = new IgnisMetrics(registry);
        final AtomicInteger loads = new AtomicInteger();
        final QueueDepthMetrics depth = new QueueDepthMetrics(metrics, () -> {
            loads.incrementAndGet();
            return List.of(
                    QueueStat.builder().name("Q1").published(10).consumed(4).unConsumed(6).build(),
                    QueueStat.builder().name("Q2").published(7).consumed(7).unConsumed(0).build());
        }, 60_000L);

        depth.register("Q1");
        depth.register("Q2");

        // Two queues x five gauges, read twice over: twenty pulls.
        for (int round = 0; round < 2; round++) {
            assertEquals(6.0, gauge(IgnisMetrics.QUEUE_DEPTH, "Q1"));
            assertEquals(10.0, gauge(IgnisMetrics.QUEUE_PUBLISHED, "Q1"));
            assertEquals(4.0, gauge(IgnisMetrics.QUEUE_CONSUMED, "Q1"));
            assertEquals(0.0, gauge(IgnisMetrics.QUEUE_SIDELINED, "Q1"));
            assertEquals(0.0, gauge(IgnisMetrics.QUEUE_SHOVELLED, "Q1"));
            assertEquals(0.0, gauge(IgnisMetrics.QUEUE_DEPTH, "Q2"));
        }

        assertEquals(1, loads.get(),
                "twenty gauge pulls across two queues must cost exactly one storage refresh");
    }

    @Test
    @DisplayName("registering a queue twice does not double its gauges")
    void registrationIsIdempotent() {
        final IgnisMetrics metrics = new IgnisMetrics(registry);
        final QueueDepthMetrics depth = new QueueDepthMetrics(metrics,
                () -> List.of(QueueStat.builder().name("Q1").unConsumed(3).build()), 60_000L);

        depth.register("Q1");
        depth.register("Q1");

        assertEquals(1, registry.find(IgnisMetrics.QUEUE_DEPTH).gauges().size(),
                "the watcher re-registers on every refresh cycle, so this must be idempotent");
    }

    /**
     * A deactivated queue whose gauges survive reads as a permanently backlogged queue that no
     * longer exists - worse than not reporting at all, because it will hold an alert open forever.
     */
    @Test
    @DisplayName("deactivating a queue stops it reporting rather than freezing its last value")
    void deregistrationRemovesTheGauges() {
        final IgnisMetrics metrics = new IgnisMetrics(registry);
        final QueueDepthMetrics depth = new QueueDepthMetrics(metrics,
                () -> List.of(QueueStat.builder().name("Q1").unConsumed(99).build()), 60_000L);
        depth.register("Q1");
        assertEquals(99.0, gauge(IgnisMetrics.QUEUE_DEPTH, "Q1"));

        depth.deregister("Q1");

        assertNull(registry.find(IgnisMetrics.QUEUE_DEPTH).tags(IgnisMetrics.TAG_QUEUE, "Q1").gauge());
    }

    @Test
    @DisplayName("a storage failure serves the previous snapshot instead of propagating")
    void aFailedRefreshKeepsServing() {
        final IgnisMetrics metrics = new IgnisMetrics(registry);
        final AtomicInteger calls = new AtomicInteger();
        final QueueDepthMetrics depth = new QueueDepthMetrics(metrics, () -> {
            if (calls.incrementAndGet() > 1) {
                throw new IllegalStateException("aerospike is down");
            }
            return List.of(QueueStat.builder().name("Q1").unConsumed(5).build());
        }, 0L);

        depth.register("Q1");
        assertEquals(5.0, gauge(IgnisMetrics.QUEUE_DEPTH, "Q1"));

        // A monitoring scrape must never be the thing that surfaces a storage outage as an error.
        assertEquals(5.0, gauge(IgnisMetrics.QUEUE_DEPTH, "Q1"),
                "a failed refresh serves the last known value rather than throwing");
        assertTrue(calls.get() > 1);
    }

    @Test
    @DisplayName("depth gauges are not registered at all when metrics are off")
    void disabledRegistersNoDepthGauges() {
        final IgnisMetrics metrics = new IgnisMetrics(registry, false);
        final AtomicInteger loads = new AtomicInteger();
        final QueueDepthMetrics depth = new QueueDepthMetrics(metrics, () -> {
            loads.incrementAndGet();
            return List.of();
        }, 0L);

        depth.register("Q1");

        assertTrue(registry.getMeters().isEmpty());
        assertEquals(0, loads.get(), "a disabled gauge must not reach storage even once");
    }

    private double gauge(final String name, final String queue) {
        final io.micrometer.core.instrument.Gauge gauge =
                registry.find(name).tags(IgnisMetrics.TAG_QUEUE, queue).gauge();
        assertNotNull(gauge, name + " should be registered for " + queue);
        return gauge.value();
    }

    private static void assertThrowsIllegalState(final Executable executable) {
        try {
            executable.run();
        } catch (IllegalStateException e) {
            return;
        } catch (Exception e) {
            throw new AssertionError("expected IllegalStateException, got " + e);
        }
        throw new AssertionError("expected IllegalStateException");
    }

    @FunctionalInterface
    private interface Executable {
        void run() throws Exception;
    }
}
