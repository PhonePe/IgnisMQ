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

package com.phonepe.ignis.service;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.phonepe.ignis.IQueue;
import com.phonepe.ignis.IgnisMQManager;
import com.phonepe.ignis.IgnisMQSettings;
import com.phonepe.ignis.common.QueueMetaData;
import com.phonepe.ignis.common.ShardDepth;
import com.phonepe.ignis.response.ActionResult;
import com.phonepe.ignis.response.InstanceMetrics;
import com.phonepe.ignis.response.MeterSample;
import com.phonepe.ignis.response.InstanceQueue;
import com.phonepe.ignis.response.InstanceView;
import com.phonepe.ignis.response.QueueDepth;
import com.phonepe.ignis.response.QueueDetail;
import com.phonepe.ignis.response.QueueSummary;
import com.phonepe.ignis.entity.QueueEntity;
import com.phonepe.ignis.refresh.RefreshableQueue;
import com.phonepe.ignis.request.ShovelConfig;
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.ClientErrorException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The two views, kept apart on purpose.
 * <p>
 * Cluster state comes from storage and is true wherever it is read. Instance state comes from this
 * process's own registry and is true of nothing else. Every method below belongs to exactly one of
 * the two, and the response types say which.
 */
public final class IgnisMQService {

    private static final String SCOPE_CLUSTER = "cluster";
    private static final String SCOPE_INSTANCE = "instance";
    private static final Set<String> METER_PREFIXES = Set.of("ignismq.", "ignis.", "magazine.");

    private final IgnisMQManager manager;
    private final String clientId;
    private final String farmId;
    private final IgnisMQSettings settings;
    private final MeterRegistry meterRegistry;
    private final LoadingCache<Boolean, Map<String, QueueEntity>> inventoryCache;
    private final LoadingCache<String, QueueDepth> depthCache;

    public IgnisMQService(final IgnisMQManager manager,
                          final String clientId,
                          final String farmId,
                          final IgnisMQSettings settings,
                          final MeterRegistry meterRegistry,
                          final int cacheSeconds) {
        this.manager = manager;
        this.clientId = clientId;
        this.farmId = farmId;
        this.settings = settings;
        this.meterRegistry = meterRegistry;
        if (cacheSeconds > 0) {
            final Duration window = Duration.ofSeconds(cacheSeconds);
            this.inventoryCache = Caffeine.newBuilder().expireAfterWrite(window).build(manager::getStoredQueues);
            this.depthCache = Caffeine.newBuilder().expireAfterWrite(window).build(this::readDepth);
        } else {
            this.inventoryCache = null;
            this.depthCache = null;
        }
    }

    public List<QueueSummary> queues() {
        final Map<String, IQueue<?>> held = manager.getAllQueues();
        final List<QueueSummary> summaries = new ArrayList<>();
        storedQueues(true).forEach((name, entity) -> summaries.add(toSummary(name, entity, held.containsKey(name))));
        storedQueues(false).forEach((name, entity) -> summaries.add(toSummary(name, entity, held.containsKey(name))));
        summaries.sort(Comparator.comparing(QueueSummary::name));
        return summaries;
    }

    public QueueDetail queue(final String name) {
        final QueueEntity entity = manager.getStoredQueue(name)
                .orElseThrow(() -> new NotFoundException("No such queue: " + name));
        final IQueue<?> queue = manager.getAllQueues().get(name);
        final QueueSummary summary = toSummary(name, entity, Objects.nonNull(queue));
        if (Objects.isNull(queue)) {
            return new QueueDetail(summary, null, null, null);
        }
        return new QueueDetail(summary, depth(name), queue.getShardDepths(), instanceQueue(name, queue));
    }

    public List<ShardDepth> shards(final String name) {
        return localQueue(name).getShardDepths();
    }

    public InstanceView instance() {
        final List<InstanceQueue> queues = new ArrayList<>();
        manager.getAllQueues().forEach((name, queue) -> queues.add(instanceQueue(name, queue)));
        queues.sort(Comparator.comparing(InstanceQueue::name));
        return new InstanceView(clientId, farmId, settings.getWorkerThreads(), settings.isMetricsEnabled(), queues);
    }

    public InstanceMetrics metrics() {
        final List<MeterSample> samples = meterRegistry.getMeters().stream()
                .filter(meter -> METER_PREFIXES.stream().anyMatch(meter.getId().getName()::startsWith))
                .map(IgnisMQService::toSample)
                .sorted(Comparator.comparing(MeterSample::name).thenComparing(sample -> sample.tags().toString()))
                .toList();
        return new InstanceMetrics(clientId, farmId, settings.isMetricsEnabled(), samples.size(), samples);
    }

    public ActionResult shovel(final String name, final int concurrency) {
        if (concurrency < 1) {
            throw new BadRequestException("concurrency must be at least 1");
        }
        localQueue(name).shovel(concurrency);
        return new ActionResult(name, "shovel", SCOPE_INSTANCE,
                "Started a one-shot shovel with " + concurrency + " consumers on this instance.");
    }

    public ActionResult scheduleShovel(final String name, final int concurrency, final int intervalSeconds) {
        if (concurrency < 1) {
            throw new BadRequestException("concurrency must be at least 1");
        }
        requireStored(name);
        manager.scheduleShoveling(name, ShovelConfig.builder()
                .concurrency(concurrency)
                .timeIntervalInSecs(intervalSeconds)
                .build());
        return new ActionResult(name, "schedule-shovel", SCOPE_CLUSTER,
                "Stored a repeating shovel every " + intervalSeconds + "s; other instances adopt it on refresh.");
    }

    public ActionResult sweep(final String name) {
        requireStored(name);
        manager.sweepQueue(name);
        return new ActionResult(name, "sweep", SCOPE_CLUSTER, "Swept the queue once, from this instance.");
    }

    public ActionResult consumers(final String name, final int delta) {
        if (delta == 0) {
            throw new BadRequestException("delta must not be zero");
        }
        localQueue(name);
        if (delta > 0) {
            manager.increaseConsumers(name, delta);
        } else {
            manager.decreaseConsumers(name, -delta);
        }
        return new ActionResult(name, "consumers", SCOPE_INSTANCE,
                "This instance now runs " + consumerCount(name) + " consumers.");
    }

    public ActionResult deactivate(final String name, final String confirmation) {
        requireStored(name);
        if (!name.equals(confirmation)) {
            throw new BadRequestException("Deactivating '" + name + "' requires confirm=" + name);
        }
        manager.deactivateQueue(name);
        invalidate(name);
        return new ActionResult(name, "deactivate", SCOPE_CLUSTER,
                "Deactivated the queue. There is no supported way to activate it again.");
    }

    private QueueEntity requireStored(final String name) {
        return manager.getStoredQueue(name)
                .orElseThrow(() -> new NotFoundException("No such queue: " + name));
    }

    private IQueue<?> localQueue(final String name) {
        final IQueue<?> queue = manager.getAllQueues().get(name);
        if (Objects.isNull(queue)) {
            requireStored(name);
            throw new ClientErrorException("Queue '" + name + "' exists but is not served by this instance",
                    Response.Status.CONFLICT);
        }
        return queue;
    }

    private Map<String, QueueEntity> storedQueues(final boolean active) {
        return Objects.isNull(inventoryCache)
                ? manager.getStoredQueues(active)
                : inventoryCache.get(active);
    }

    private QueueDepth depth(final String name) {
        return Objects.isNull(depthCache) ? readDepth(name) : depthCache.get(name);
    }

    private QueueDepth readDepth(final String name) {
        final QueueMetaData metaData = localQueue(name).getMetaData();
        return new QueueDepth(metaData.getPublished(), metaData.getConsumed(),
                Math.max(metaData.getPublished() - metaData.getConsumed(), 0L),
                metaData.getSidelined(), metaData.getShovelled());
    }

    private void invalidate(final String name) {
        if (Objects.nonNull(depthCache)) {
            depthCache.invalidate(name);
        }
        if (Objects.nonNull(inventoryCache)) {
            inventoryCache.invalidateAll();
        }
    }

    private int consumerCount(final String name) {
        final IQueue<?> queue = manager.getAllQueues().get(name);
        return queue instanceof RefreshableQueue refreshable ? refreshable.getNoOfConsumers() : 0;
    }

    private static InstanceQueue instanceQueue(final String name, final IQueue<?> queue) {
        if (!(queue instanceof RefreshableQueue refreshable)) {
            return new InstanceQueue(name, 0, 0, null, null);
        }
        final ShovelConfig shovelConfig = refreshable.getShovelConfig();
        return new InstanceQueue(name, refreshable.getNoOfConsumers(), refreshable.getNoOfShovelConsumers(),
                Objects.isNull(shovelConfig) ? null : shovelConfig.getConcurrency(),
                Objects.isNull(shovelConfig) ? null : shovelConfig.getTimeIntervalInSecs());
    }

    private static QueueSummary toSummary(final String name, final QueueEntity entity, final boolean heldHere) {
        return new QueueSummary(name, entity.isActive(), heldHere, entity.getShards(), entity.getConcurrency(),
                entity.getCreatedAt(), entity.getMessageExpiry(), entity.getQueueExpiry(),
                entity.getSweepDuration(), entity.getHandlerTimeout(),
                // -1 is how "no shovel configured" is stored, and reporting it as a concurrency of
                // minus one would be read as a value rather than an absence.
                positiveOrNull(entity.getShovelConcurrency()),
                positiveOrNull(entity.getShovelTimeIntervalInSecs()),
                Objects.isNull(entity.getBatchingConfig()) ? null : entity.getBatchingConfig().getMaxBatchSize(),
                Objects.isNull(entity.getBatchingConfig()) ? null : entity.getBatchingConfig().getMaxWaitTimeInSecs(),
                entity.getMessageHandlerType());
    }

    private static Integer positiveOrNull(final int value) {
        return value > 0 ? value : null;
    }

    private static MeterSample toSample(final Meter meter) {
        final Map<String, String> tags = new LinkedHashMap<>();
        meter.getId().getTags().forEach(tag -> tags.put(tag.getKey(), tag.getValue()));
        final Map<String, Double> measurements = new LinkedHashMap<>();
        for (final Measurement measurement : meter.measure()) {
            measurements.put(measurement.getStatistic().name(), measurement.getValue());
        }
        return new MeterSample(meter.getId().getName(), tags, meter.getId().getType().name(), measurements);
    }
}
