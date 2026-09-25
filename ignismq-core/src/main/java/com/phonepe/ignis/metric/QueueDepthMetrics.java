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
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;

/**
 * Per-queue backlog gauges, all served from one shared snapshot refreshed at most once per
 * interval. Gauges are pulled, so reading storage per gauge per scrape would cost hundreds of
 * reads a minute purely to produce metrics.
 */
@Slf4j
public final class QueueDepthMetrics {

    private final IgnisMetrics metrics;
    private final Supplier<List<QueueStat>> stats;
    private final long refreshIntervalMillis;
    private final Set<String> registered = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean refreshing = new AtomicBoolean();

    private final AtomicReference<Map<String, QueueStat>> snapshot = new AtomicReference<>(Map.of());
    private volatile long refreshedAt;

    public QueueDepthMetrics(final IgnisMetrics metrics,
                             final Supplier<List<QueueStat>> stats,
                             final long refreshIntervalMillis) {
        this.metrics = metrics;
        this.stats = stats;
        this.refreshIntervalMillis = refreshIntervalMillis;
    }

    /**
     * Idempotent: the queue watcher calls this on every refresh cycle.
     */
    public void register(final String queueName) {
        if (!metrics.isEnabled() || !registered.add(queueName)) {
            return;
        }
        final Tags tags = Tags.of(IgnisMetrics.TAG_QUEUE, queueName);
        gauge(IgnisMetrics.QUEUE_DEPTH, tags, queueName, QueueStat::getUnConsumed);
        gauge(IgnisMetrics.QUEUE_PUBLISHED, tags, queueName, QueueStat::getPublished);
        gauge(IgnisMetrics.QUEUE_CONSUMED, tags, queueName, QueueStat::getConsumed);
        gauge(IgnisMetrics.QUEUE_SIDELINED, tags, queueName, QueueStat::getSidelined);
        gauge(IgnisMetrics.QUEUE_SHOVELLED, tags, queueName, QueueStat::getShovelled);
    }

    /**
     * Without this a deactivated queue's gauges freeze at their last value and hold alerts open.
     * <p>
     * The consumer count is included even though {@link #register} does not create it: that gauge
     * needs the live queue object, so the queue registers it, but deactivation is the only place
     * that knows the queue has gone.
     */
    public void deregister(final String queueName) {
        if (!registered.remove(queueName)) {
            return;
        }
        final Tags tags = Tags.of(IgnisMetrics.TAG_QUEUE, queueName);
        for (String name : List.of(IgnisMetrics.QUEUE_DEPTH, IgnisMetrics.QUEUE_PUBLISHED,
                IgnisMetrics.QUEUE_CONSUMED, IgnisMetrics.QUEUE_SIDELINED,
                IgnisMetrics.QUEUE_SHOVELLED, IgnisMetrics.QUEUE_CONSUMERS)) {
            metrics.remove(name, tags);
        }
    }

    private void gauge(final String name, final Tags tags, final String queueName,
                       final ToDoubleFunction<QueueStat> value) {
        metrics.gauge(name, tags, this, self -> {
            final QueueStat stat = self.current().get(queueName);
            return Objects.isNull(stat) ? 0.0 : value.applyAsDouble(stat);
        });
    }

    private Map<String, QueueStat> current() {
        if (System.currentTimeMillis() - refreshedAt < refreshIntervalMillis) {
            return snapshot.get();
        }
        // One refresher at a time; everyone else keeps the previous snapshot.
        if (!refreshing.compareAndSet(false, true)) {
            return snapshot.get();
        }
        try {
            final Map<String, QueueStat> refreshed = new HashMap<>();
            stats.get().forEach(stat -> refreshed.put(stat.getName(), stat));
            snapshot.set(refreshed);
        } catch (Exception e) {
            log.warn("Could not refresh queue depth metrics; serving the previous snapshot", e);
        } finally {
            // Stamped even on failure, so a failing backend is retried on the normal interval
            // rather than on every scrape.
            refreshedAt = System.currentTimeMillis();
            refreshing.set(false);
        }
        return snapshot.get();
    }
}
