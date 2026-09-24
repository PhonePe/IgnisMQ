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

import com.codahale.metrics.MetricRegistry;
import com.phonepe.magazine.metrics.MagazineMetrics;
import com.phonepe.magazine.metrics.StorageOperation;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class DropwizardMagazineMetricsTest {

    private final MetricRegistry metrics = new MetricRegistry();
    private final MeterRegistry bridge = DropwizardMagazineMetrics.bridgedTo(metrics);

    @Test
    void publishesMagazineMetricsToDropwizard() {
        new MagazineMetrics(bridge).aerospikeCall("queue", StorageOperation.BATCH_READ_METADATA);

        assertEquals(1, metrics.meter(
                "magazine.aerospike.calls.magazine.queue.operation.batch_read_metadata").getCount());
    }

    /**
     * ignisMQ's own per-queue timers carry no tags. Their names must survive the bridge byte for
     * byte, otherwise every existing dashboard and alert built on {@code commands.*} breaks
     * silently on upgrade.
     */
    @Test
    void untaggedMeterNamesArePreservedVerbatim() {
        bridge.timer("commands.order-events_publish.all").record(5, TimeUnit.MILLISECONDS);

        final com.codahale.metrics.Timer timer =
                metrics.getTimers().get("commands.order-events_publish.all");
        assertNotNull(timer, "untagged timer name must not be rewritten");
        assertEquals(1, timer.getCount());
    }

    /**
     * Tag flattening must be deterministic, since the flattened string is what operators alert on.
     */
    @Test
    void tagsAreFlattenedIntoTheHierarchicalName() {
        bridge.counter("magazine.fire.outcomes", "magazine", "orders", "outcome", "delivered")
                .increment();

        assertEquals(1, metrics.meter(
                "magazine.fire.outcomes.magazine.orders.outcome.delivered").getCount());
    }

    /**
     * Magazine registers a fixed set of series per magazine up front (a queue name crossed with
     * closed outcome enums). The invariant that matters for a never-evicting Dropwizard registry
     * is that recording volume does not create new series — only new queue names do.
     */
    @Test
    void seriesCountIsIndependentOfRecordingVolume() {
        final MagazineMetrics magazineMetrics = new MagazineMetrics(bridge);
        magazineMetrics.aerospikeCall("orders", StorageOperation.BATCH_READ_METADATA);
        final int afterFirstCall = metrics.getMetrics().size();

        for (int i = 0; i < 500; i++) {
            magazineMetrics.aerospikeCall("orders", StorageOperation.BATCH_READ_METADATA);
        }

        assertEquals(afterFirstCall, metrics.getMetrics().size(), "recording must reuse existing series");

        // A second queue adds its own bounded block, and no more.
        magazineMetrics.aerospikeCall("payments", StorageOperation.BATCH_READ_METADATA);
        assertEquals(2 * afterFirstCall, metrics.getMetrics().size(), "each queue must contribute the same fixed number of series");
    }

    @Test
    void writesIntoTheSuppliedRegistryRatherThanTheGlobalOne() {
        bridge.counter("ignis.local.only").increment();

        assertTrue(metrics.getMeters().containsKey("ignis.local.only"));
        assertTrue(io.micrometer.core.instrument.Metrics.globalRegistry.getMeters().isEmpty(), "must not leak into Micrometer's static global registry");
    }
}
