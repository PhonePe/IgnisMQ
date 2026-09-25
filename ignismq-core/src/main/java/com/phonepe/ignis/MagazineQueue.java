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

package com.phonepe.ignis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.phonepe.ignis.client.StorageClient;
import com.phonepe.ignis.common.MessageHandler;
import com.phonepe.ignis.common.QueueMetaData;
import com.phonepe.ignis.common.ShardDepth;
import com.phonepe.ignis.config.BatchingConfig;
import com.phonepe.ignis.consumer.MagazineConsumerTask;
import com.phonepe.ignis.exception.ErrorCode;
import com.phonepe.ignis.exception.IgnisMQException;
import com.phonepe.ignis.metric.IgnisMetrics;
import com.phonepe.ignis.metric.QueueMeters;
import com.phonepe.ignis.refresh.RefreshableQueue;
import com.phonepe.ignis.request.ShovelConfig;
import com.phonepe.ignis.scheduler.HandlerExecutor;
import com.phonepe.ignis.scheduler.IgnisSchedulerCommands;
import com.phonepe.ignis.scheduler.ScheduledTask;
import com.phonepe.ignis.shovel.ShovelTask;
import com.phonepe.ignis.storage.BaseStorage;
import com.phonepe.ignis.storage.MagazineStorageVisitor;
import com.phonepe.ignis.utils.Constants;
import com.phonepe.ignis.utils.Utils;
import com.phonepe.magazine.Magazine;
import com.phonepe.magazine.core.BaseMagazineStorage;
import com.phonepe.magazine.entity.MetaData;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

/**
 * @author shantanu.tiwari
 */
@Slf4j
public final class MagazineQueue<M> implements IQueue<M>, RefreshableQueue {
    private final Magazine<String> magazine;
    private final Magazine<String> sidelineMagazine;
    private final MessageHandler<M> messageHandler;
    private final List<ScheduledTask> consumers = new ArrayList<>();
    private final List<ScheduledTask> sidelineConsumers = new ArrayList<>();
    private final IgnisSchedulerCommands scheduler;
    private final HandlerExecutor handlerExecutor;
    @Getter(AccessLevel.PACKAGE)
    private final long handlerTimeoutMillis;
    private final ObjectMapper mapper;
    private final Class<M> clazz;
    @Getter
    private final ShovelConfig shovelConfig;
    private final BatchingConfig batchingConfig;
    private final QueueMeters meters;

    MagazineQueue(
            final String clientId,
            final String farmId,
            final String queueName,
            final int queueShards,
            final int recordTtlInSeconds,
            final int metaDataTtlInSeconds,
            final StorageClient client,
            final BaseStorage storage,
            final int concurrency,
            final MessageHandler<M> messageHandler,
            final ShovelConfig shovelConfig,
            final ObjectMapper mapper,
            final Class<M> clazz,
            final BatchingConfig batchingConfig,
            final long sweepDurationInMillis,
            final long handlerTimeoutInMillis,
            final IgnisSchedulerCommands scheduler,
            final HandlerExecutor handlerExecutor,
            final MeterRegistry meterRegistry,
            final IgnisMetrics metrics) {
        this.meters = new QueueMeters(metrics, queueName);
        this.messageHandler = messageHandler;
        this.mapper = mapper;
        this.clazz = clazz;
        this.batchingConfig = batchingConfig;
        this.scheduler = scheduler;
        this.handlerExecutor = handlerExecutor;
        this.handlerTimeoutMillis = Utils.handlerTimeoutMillis(handlerTimeoutInMillis, sweepDurationInMillis);
        // One storage, two magazines. Every cache Magazine keeps - key layout, active shards,
        // checkpoint claims - is keyed by MagazineContext, so a storage is explicitly designed to
        // serve several magazines. Building one per magazine doubled the object graph and the
        // Caffeine caches held for the lifetime of the queue, and bought nothing.
        final BaseMagazineStorage<String> magazineStorage = storage.accept(new MagazineStorageVisitor(
                clientId, recordTtlInSeconds, metaDataTtlInSeconds, client, queueShards, farmId,
                meterRegistry, sweepDurationInMillis));
        this.magazine = Magazine.<String>builder()
                .baseMagazineStorage(magazineStorage)
                .magazineIdentifier(queueName)
                .build();
        this.sidelineMagazine = Magazine.<String>builder()
                .baseMagazineStorage(magazineStorage)
                .magazineIdentifier(Utils.getSidelineQueueName(queueName))
                .build();
        meters.gaugeConsumers(this, queue -> queue.consumers.size());
        createConsumers(concurrency);
        this.shovelConfig = shovelConfig;
        if (Objects.nonNull(shovelConfig)) {
            scheduleShoveling(shovelConfig.getConcurrency(), shovelConfig.getTimeIntervalInSecs());
        }
    }

    @Override
    public boolean publish(final M message) throws JsonProcessingException {
        log.debug("Publishing '{}' message in queue '{}'", message, magazine.getMagazineIdentifier());
        // Timed by hand rather than through Timer.recordCallable, which would force the checked
        // JsonProcessingException through a wrapper and change what callers catch.
        final long startNanos = System.nanoTime();
        boolean failed = false;
        try {
            return magazine.load(mapper.writeValueAsString(message));
        } catch (RuntimeException | JsonProcessingException e) {
            failed = true;
            throw e;
        } finally {
            meters.recordPublish(System.nanoTime() - startNanos, failed);
        }
    }

    @Override
    public long getUnconsumedCount() {
        val allShardsMetaData = magazine.getMetaData().values();
        long unConsumedCount =
                Utils.getMagazineCount(allShardsMetaData, MetaData::getLoadPointer) -
                        Utils.getMagazineCount(allShardsMetaData, MetaData::getFirePointer);
        return unConsumedCount >= 0 ? unConsumedCount : 0L;
    }

    @Override
    public QueueMetaData getMetaData() {
        val allShardsMetaData = magazine.getMetaData().values();
        val allSidelineShardsMetaData = sidelineMagazine.getMetaData().values();

        final long sidelineLoadPointer = Utils.getMagazineCount(allSidelineShardsMetaData, MetaData::getLoadPointer);
        final long sidelineFirePointer = Utils.getMagazineCount(allSidelineShardsMetaData, MetaData::getFirePointer);
        return QueueMetaData.builder()
                .published(Utils.getMagazineCount(allShardsMetaData, MetaData::getLoadPointer))
                .consumed(Utils.getMagazineCount(allShardsMetaData, MetaData::getFirePointer))
                .sidelined(Math.max(sidelineLoadPointer - sidelineFirePointer, 0L))
                .shovelled(sidelineFirePointer)
                .build();
    }

    @Override
    public List<ShardDepth> getShardDepths() {
        return ShardDepth.from(magazine.getMetaData());
    }

    @Override
    public void createConsumers(final int count) {
        if (consumers.size() + count > Constants.MAX_CONSUMERS_ALLOWED) {
            throw IgnisMQException.builder()
                    .errorCode(ErrorCode.MAX_ALLOWED_CONSUMERS_EXCEEDED)
                    .build();
        }

        IntStream.range(0, count).boxed()
                .forEach(i -> consumers.add(scheduler.scheduleRepeating(
                        new MagazineConsumerTask<>(magazine, sidelineMagazine, messageHandler,
                                mapper, clazz, batchingConfig, Constants.CONSUMER_RUN_BUDGET_IN_MS,
                                handlerExecutor, handlerTimeoutMillis, meters),
                        Constants.INITIAL_DELAY_IN_MS,
                        Constants.DELAY_PERIOD_IN_MS)));
        log.info("Created {} new consumers of queue '{}', Total consumers = {}",
                count, magazine.getMagazineIdentifier(), consumers.size());
    }

    @Override
    public int getNoOfConsumers() {
        return consumers.size();
    }

    @Override
    public void stopConsumers(final int count) {
        final int consumerToStopCount = Math.min(count, consumers.size());
        IntStream.range(0, consumerToStopCount).boxed()
                // Through the scheduler, not the future: cancelling the future alone would stop the
                // task but leave the pool sized for it, so scaling a queue down and up repeatedly
                // would ratchet the thread count up for demand that no longer exists.
                .forEach(i -> scheduler.cancelRepeating(consumers.remove(consumers.size() - 1)));
        log.info("Stopped {} consumers of queue '{}', Total consumers = {}",
                consumerToStopCount, magazine.getMagazineIdentifier(), consumers.size());
    }

    @Override
    public void shovel(final int concurrency) {
        createShovel(concurrency, true, 0);
    }

    @Override
    public void scheduleShoveling(int concurrency, int timeIntervalInSecs) {
        createShovel(concurrency, false, timeIntervalInSecs);
    }

    @Override
    public void stopShovelConsumers(final int count) {
        final int consumerToStopCount = Math.min(count, sidelineConsumers.size());
        IntStream.range(0, consumerToStopCount).boxed()
                .forEach(i -> scheduler.cancelRepeating(
                        sidelineConsumers.remove(sidelineConsumers.size() - 1)));
        log.info("Stopped {} consumers of sideline queue '{}', Total consumers = {}",
                consumerToStopCount, magazine.getMagazineIdentifier(), sidelineConsumers.size());
    }

    @Override
    public int getNoOfShovelConsumers() {
        releaseFinishedShovels();
        return sidelineConsumers.size();
    }

    /**
     * A one-shot shovel finishes and is never cancelled, so nothing would otherwise remove its
     * future. Left in place it holds a slot against the shovel cap for the life of the queue, and a
     * process that drains its sideline on demand would eventually be unable to shovel at all.
     * Repeating shovels are only done once cancelled, so this cannot drop a live one.
     */
    private void releaseFinishedShovels() {
        sidelineConsumers.removeIf(ScheduledTask::isDone);
    }

    /**
     * The live magazines behind this queue.
     * <p>
     * Package-private on purpose: the sweeper needs them, users of {@link IQueue} must not have
     * them. {@code IgnisMQManager} reaches these from the same package and exposes them no further
     * than the internal {@code MagazineRegistry} lambda.
     */
    Magazine<String> mainMagazine() {
        return magazine;
    }

    Magazine<String> sidelineMagazine() {
        return sidelineMagazine;
    }

    private void createShovel(int concurrency, boolean autoDelete, int timeIntervalInSecs) {
        if (timeIntervalInSecs > Constants.MAX_ALLOWED_SHOVEL_TIME_INTERVAL_IN_SECONDS || timeIntervalInSecs < 0) {
            throw IgnisMQException.builder()
                    .errorCode(ErrorCode.INVALID_SHOVEL_TIME_INTERVAL)
                    .build();
        }
        releaseFinishedShovels();
        if (sidelineConsumers.size() + concurrency > Constants.MAX_CONSUMERS_ALLOWED) {
            throw IgnisMQException.builder()
                    .errorCode(ErrorCode.MAX_ALLOWED_CONSUMERS_EXCEEDED)
                    .build();
        }
        log.info("Creating {} shoveling task for queue '{}'", concurrency, magazine.getMagazineIdentifier());
        IntStream.range(0, concurrency).boxed()
                .forEach(i -> {
                    final ShovelTask shovelTask =
                            new ShovelTask(magazine, sidelineMagazine, autoDelete, scheduler, meters);
                    sidelineConsumers.add(scheduleShovel(shovelTask, autoDelete, timeIntervalInSecs));
                });
        log.info("All shovels tasks scheduled.");
    }

    /**
     * An auto-deleting shovel drains once and stops; a configured one repeats on its interval, or
     * on the default poll period when no interval was given.
     */
    private ScheduledTask scheduleShovel(final ShovelTask shovelTask, final boolean autoDelete,
                                         final int timeIntervalInSecs) {
        if (autoDelete) {
            return scheduler.scheduleOnce(shovelTask, Constants.INITIAL_DELAY_IN_MS);
        }
        final long periodMillis = timeIntervalInSecs == 0
                ? Constants.DELAY_PERIOD_IN_MS
                : timeIntervalInSecs * 1000L;
        return scheduler.scheduleRepeating(shovelTask, Constants.INITIAL_DELAY_IN_MS, periodMillis);
    }

}
