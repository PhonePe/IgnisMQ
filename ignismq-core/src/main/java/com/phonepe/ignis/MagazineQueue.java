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

import com.aerospike.client.IAerospikeClient;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.phonepe.ignis.client.StorageClient;
import com.phonepe.ignis.common.MessageHandler;
import com.phonepe.ignis.common.QueueMetaData;
import com.phonepe.ignis.config.BatchingConfig;
import com.phonepe.ignis.consumer.MagazineConsumerTask;
import com.phonepe.ignis.exception.ErrorCode;
import com.phonepe.ignis.exception.IgnisMQException;
import com.phonepe.ignis.request.ShovelConfig;
import com.phonepe.ignis.service.QueueService;
import com.phonepe.ignis.shovel.ShovelTask;
import com.phonepe.ignis.storage.AerospikeStorage;
import com.phonepe.ignis.storage.BaseStorage;
import com.phonepe.ignis.storage.StorageVisitor;
import com.phonepe.ignis.utils.Constants;
import com.phonepe.ignis.utils.Utils;
import com.phonepe.magazine.Magazine;
import com.phonepe.magazine.common.MetaData;
import com.phonepe.magazine.core.BaseMagazineStorage;
import com.phonepe.magazine.impl.aerospike.AerospikeStorageConfig;
import com.phonepe.magazine.scope.MagazineScope;
import io.appform.functionmetrics.MonitoredFunction;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Timer;
import java.util.stream.IntStream;

/**
 * @author shantanu.tiwari
 */
@Slf4j
public final class MagazineQueue<M> implements IQueue<M> {
    private final Magazine<String> magazine;
    private final Magazine<String> sidelineMagazine;
    private final MessageHandler<M> messageHandler;
    private final List<Timer> consumers = new ArrayList<>();
    private final List<Timer> sidelineConsumers = new ArrayList<>();
    private final ObjectMapper mapper;
    private final Class<M> clazz;
    @Getter
    private final ShovelConfig shovelConfig;
    private final QueueService queueService;
    private final com.codahale.metrics.Timer publishMetricTimer;
    private final com.codahale.metrics.Timer consumeMetricTimer;
    private final BatchingConfig batchingConfig;

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
            final QueueService queueService,
            final BatchingConfig batchingConfig,
            final com.codahale.metrics.Timer publishMetricTimer,
            final com.codahale.metrics.Timer consumeMetricTimer) throws Exception {
        this.messageHandler = messageHandler;
        this.mapper = mapper;
        this.clazz = clazz;
        this.publishMetricTimer = publishMetricTimer;
        this.consumeMetricTimer = consumeMetricTimer;
        this.batchingConfig = batchingConfig;
        this.magazine = Magazine.<String>builder()
                .baseMagazineStorage(storage.accept(new MagazineStorageVisitor(
                        clientId, recordTtlInSeconds, metaDataTtlInSeconds, client, queueShards, farmId)))
                .magazineIdentifier(queueName)
                .build();
        this.sidelineMagazine = Magazine.<String>builder()
                .baseMagazineStorage(storage.accept(new MagazineStorageVisitor(
                        clientId, recordTtlInSeconds, metaDataTtlInSeconds, client, queueShards, farmId)))
                .magazineIdentifier(Utils.getSidelineQueueName(queueName))
                .build();
        this.queueService = queueService;
        createConsumers(concurrency);
        this.shovelConfig = shovelConfig;
        if (Objects.nonNull(shovelConfig)) {
            scheduleShoveling(shovelConfig.getConcurrency(), shovelConfig.getTimeIntervalInSecs());
        }
    }

    @Override
    @MonitoredFunction
    public boolean publish(final M message) throws JsonProcessingException {
        log.debug("Publishing '{}' message in queue '{}'", message, magazine.getMagazineIdentifier());
        final com.codahale.metrics.Timer.Context timerContext = publishMetricTimer.time();
        try {
            return magazine.load(mapper.writeValueAsString(message));
        } finally {
            timerContext.stop();
        }
    }

    @Override
    @MonitoredFunction
    public long getUnconsumedCount() {
        val allShardsMetaData = magazine.getMetaData().values();
        long unConsumedCount =
                Utils.getMagazineCount(allShardsMetaData, MetaData::getLoadPointer) -
                        Utils.getMagazineCount(allShardsMetaData, MetaData::getFirePointer);
        return unConsumedCount >= 0 ? unConsumedCount : 0L;
    }

    @Override
    @MonitoredFunction
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

    @MonitoredFunction
    void createConsumers(final int count) {
        if (consumers.size() + count >= Constants.MAX_CONSUMERS_ALLOWED) {
            throw IgnisMQException.builder()
                    .errorCode(ErrorCode.MAX_ALLOWED_CONSUMERS_EXCEEDED)
                    .build();
        }

        IntStream.range(0, count).boxed()
                .forEach(i -> {
                    Timer consumer = new Timer();
                    consumer.schedule(
                            new MagazineConsumerTask<>(magazine, sidelineMagazine, messageHandler,
                                    mapper, clazz, consumeMetricTimer, queueService, batchingConfig),
                            Constants.INITIAL_DELAY_IN_MS,
                            Constants.DELAY_PERIOD_IN_MS
                    );
                    consumers.add(consumer);
                });
        log.info("Created {} new consumers of queue '{}', Total consumers = {}",
                count, magazine.getMagazineIdentifier(), consumers.size());
    }

    @MonitoredFunction
    int getNoOfConsumers() {
        return consumers.size();
    }

    @MonitoredFunction
    int getNoOfShovelConsumers() {
        return sidelineConsumers.size();
    }

    @MonitoredFunction
    void stopConsumers(final int count) {
        final int consumerToStopCount = Math.min(count, consumers.size());
        IntStream.range(0, consumerToStopCount).boxed()
                .forEach(i -> {
                    Timer consumer = consumers.get(consumers.size() - 1);
                    consumer.cancel();
                    consumer.purge();
                    consumers.remove(consumer);
                });
        log.info("Stopped {} consumers of queue '{}', Total consumers = {}",
                consumerToStopCount, magazine.getMagazineIdentifier(), consumers.size());
    }

    @Override
    @MonitoredFunction
    public void shovel(final int concurrency) {
        createShovel(concurrency, true, 0);
    }

    @MonitoredFunction
    void scheduleShoveling(int concurrency, int timeIntervalInSecs) {
        createShovel(concurrency, false, timeIntervalInSecs);
    }

    @MonitoredFunction
    private void createShovel(int concurrency, boolean autoDelete, int timeIntervalInSecs) {
        if (timeIntervalInSecs > Constants.MAX_ALLOWED_SHOVEL_TIME_INTERVAL_IN_SECONDS || timeIntervalInSecs < 0) {
            throw IgnisMQException.builder()
                    .errorCode(ErrorCode.INVALID_SHOVEL_TIME_INTERVAL)
                    .build();
        }
        if (sidelineConsumers.size() + concurrency >= Constants.MAX_CONSUMERS_ALLOWED) {
            throw IgnisMQException.builder()
                    .errorCode(ErrorCode.MAX_ALLOWED_CONSUMERS_EXCEEDED)
                    .build();
        }
        log.info("Creating {} shoveling task for queue '{}'", concurrency, magazine.getMagazineIdentifier());
        IntStream.range(0, concurrency).boxed()
                .forEach(i -> {
                    final Timer shovel = new Timer();
                    final ShovelTask shovelTask = new ShovelTask(magazine, sidelineMagazine, queueService, autoDelete);
                    if (autoDelete) {
                        shovel.schedule(
                                shovelTask,
                                Constants.INITIAL_DELAY_IN_MS
                        );
                    } else {
                        shovel.schedule(
                                shovelTask,
                                Constants.INITIAL_DELAY_IN_MS,
                                timeIntervalInSecs == 0 ? Constants.DELAY_PERIOD_IN_MS : timeIntervalInSecs * 1000L
                        );
                    }
                    sidelineConsumers.add(shovel);
                });
        log.info("All shovels tasks scheduled.");
    }

    @MonitoredFunction
    void stopShovelConsumers(final int count) {
        final int consumerToStopCount = Math.min(count, sidelineConsumers.size());
        IntStream.range(0, consumerToStopCount).boxed()
                .forEach(i -> {
                    Timer consumer = sidelineConsumers.get(sidelineConsumers.size() - 1);
                    consumer.cancel();
                    consumer.purge();
                    sidelineConsumers.remove(consumer);
                });
        log.info("Stopped {} consumers of sideline queue '{}', Total consumers = {}",
                consumerToStopCount, magazine.getMagazineIdentifier(), sidelineConsumers.size());
    }

    @AllArgsConstructor
    public static class MagazineStorageVisitor implements StorageVisitor<BaseMagazineStorage<String>> {
        private final String clientId;
        private final int recordTtlInSeconds;
        private final int metaDataTtlInSeconds;
        private final StorageClient client;
        private final int shards;
        private final String farmId;

        @Override
        public BaseMagazineStorage<String> visit(final AerospikeStorage storage) {
            return com.phonepe.magazine.impl.aerospike.AerospikeStorage.<String>builder()
                    .clazz(String.class)
                    .storageConfig(AerospikeStorageConfig.builder()
                            .dataSetName(Utils.getMagazineSet(clientId, Constants.AEROSPIKE_DATA_SET))
                            .metaSetName(Utils.getMagazineSet(clientId, Constants.AEROSPIKE_META_SET))
                            .namespace(storage.getNamespace())
                            .shards(shards)
                            .recordTtl(recordTtlInSeconds)
                            .metaDataTtl(metaDataTtlInSeconds)
                            .build())
                    .aerospikeClient((IAerospikeClient) client.getClient())
                    .enableDeDupe(false)
                    .farmId(farmId)
                    .scope(MagazineScope.LOCAL)
                    .clientId(clientId)
                    .build();
        }
    }
}
