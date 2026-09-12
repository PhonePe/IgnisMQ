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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.phonepe.ignis.client.StorageClient;
import com.phonepe.ignis.client.impl.AerospikeStoreClient;
import com.phonepe.ignis.common.MagazineRegistry;
import com.phonepe.ignis.common.MessageHandler;
import com.phonepe.ignis.config.BatchingConfig;
import com.phonepe.ignis.entity.QueueEntity;
import com.phonepe.ignis.exception.ErrorCode;
import com.phonepe.ignis.exception.IgnisMQException;
import com.phonepe.ignis.leadership.TaskInitializer;
import com.phonepe.ignis.guage.QueueStatGuage;
import com.phonepe.ignis.metric.QueueStat;
import com.phonepe.ignis.request.CreateQueueRequest;
import com.phonepe.ignis.request.ShovelConfig;
import com.phonepe.ignis.scheduler.IgnisSchedulers;
import com.phonepe.ignis.service.AerospikeQueueService;
import com.phonepe.ignis.service.QueueService;
import com.phonepe.ignis.storage.AerospikeStorage;
import com.phonepe.ignis.storage.BaseStorage;
import com.phonepe.ignis.storage.StorageVisitor;
import com.phonepe.ignis.sweep.QueueSweeper;
import com.phonepe.ignis.utils.Constants;
import com.phonepe.ignis.utils.ErrorMessage;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

import javax.validation.Valid;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author shantanu.tiwari
 */
@Slf4j
public final class IgnisMQManager {
    private static final String METRIC_TIMER_FORMAT = "commands.%s_%s.all";
    private final MeterRegistry magazineMeterRegistry;
    private final Map<String, IQueue<?>> ignisMQMap = new ConcurrentHashMap<>();
    private final String clientId;
    private final BaseStorage storage;
    private final ObjectMapper mapper;
    @Getter
    private final TaskInitializer taskInitializer;
    private final String farmId;
    private Map<String, Map.Entry<Class, MessageHandler>> messageHandlers = new HashMap<>();
    private QueueService queueService;
    private StorageClient storageClient;
    private boolean ownsStorageClient;
    private final QueueStatGuage queueStatGuage;
    private final QueueSweeper queueSweeper;

    private final IgnisSchedulers schedulers = new IgnisSchedulers();

    public IgnisMQManager(final String clientId, final BaseStorage storage, final ObjectMapper mapper,
                          final MeterRegistry meterRegistry, final CuratorFramework curatorFramework,
                          final String farmId) throws Exception {
        this.clientId = clientId;
        this.storage = storage;
        this.mapper = mapper;
        this.magazineMeterRegistry = Objects.requireNonNull(meterRegistry, "Meter registry is required.");
        this.farmId = farmId;
        this.start();

        this.taskInitializer = new TaskInitializer(curatorFramework, queueService, clientId,
                storage, storageClient, farmId, magazineMeterRegistry, schedulers.getControl(),
                this::lookupMagazines);
        this.queueStatGuage = new QueueStatGuage(queueService, this::getAllQueues);
        this.queueSweeper = new QueueSweeper(queueService, clientId, storage, storageClient, farmId,
                magazineMeterRegistry, this::lookupMagazines);
        scheduleWatcher();
    }

    public IgnisMQManager(final String clientId, final BaseStorage storage, final ObjectMapper mapper,
                          final MeterRegistry meterRegistry, final StorageClient storageClient,
                          final CuratorFramework curatorFramework, final String farmId) throws Exception {
        this.clientId = clientId;
        this.farmId = farmId;
        this.storage = storage;
        this.mapper = mapper;
        this.magazineMeterRegistry = Objects.requireNonNull(meterRegistry, "Meter registry is required.");
        this.storageClient = storageClient;
        this.ownsStorageClient = false;
        this.queueService = buildQueueCommands(storage, storageClient);
        this.taskInitializer = new TaskInitializer(curatorFramework, queueService, clientId,
                storage, storageClient, farmId, magazineMeterRegistry, schedulers.getControl(),
                this::lookupMagazines);
        this.queueStatGuage = new QueueStatGuage(queueService, this::getAllQueues);
        this.queueSweeper = new QueueSweeper(queueService, clientId, storage, storageClient, farmId,
                magazineMeterRegistry, this::lookupMagazines);
        scheduleWatcher();
    }

    public void initialiseMessageHandlers(final Map<String, Map.Entry<Class, MessageHandler>> messageHandlers) {
        Preconditions.checkNotNull(messageHandlers, "Message handler map cannot be null");
        Preconditions.checkArgument(this.messageHandlers.isEmpty(), "Message handler map is already initialised.");
        this.messageHandlers = messageHandlers;
        refreshQueues();
    }

    /**
     * Point-in-time statistics for every active queue. Callers are expected to cache this: it
     * issues metadata reads per queue and is not meant for a hot path.
     */
    public List<QueueStat> getQueueStats() {
        return queueStatGuage.get();
    }

    /**
     * @param name: Name of the queue
     * @return IQueue -> Instance of Magazine queue which can be used to publish the message.
     * @throws IgnisMQException with ErrorCode QUEUE_NOT_FOUND if queue doesn't exist in the ignisMQMap.
     */
    public <M> IQueue<M> getQueue(final String name) {
        final IQueue<M> queue = (IQueue<M>) ignisMQMap.get(name);
        if (Objects.isNull(queue)) {
            throw IgnisMQException.builder()
                    .errorCode(ErrorCode.QUEUE_NOT_FOUND)
                    .build();
        }
        return queue;
    }

    private MagazineRegistry.QueueMagazines lookupMagazines(final String queueName) {
        final IQueue<?> queue = ignisMQMap.get(queueName);
        if (!(queue instanceof MagazineQueue<?> magazineQueue)) {
            return null;
        }
        return new MagazineRegistry.QueueMagazines(magazineQueue.mainMagazine(),
                magazineQueue.sidelineMagazine());
    }

    /**
     * To get all the active queues in particular instance
     *
     * @return ignisMQMap
     */
    public Map<String, IQueue<?>> getAllQueues() {
        return ignisMQMap;
    }

    /**
     * To get all the active queue names across all instances
     *
     * @return ignisMQMap
     */
    public Set<String> getAllQueuesFromDB() {
        return new HashSet<>(queueService.getQueues(true).keySet());
    }

    /**
     * This method will create the magazine queue
     *
     * @param queueRequest -> The request for creating the queue
     * @throws IgnisMQException with ErrorCode INVALID_REQUEST if queueRequest is invalid
     * @throws IgnisMQException with ErrorCode QUEUE_ALREADY_EXISTS if queue already exists in the ignisMQMap.
     * @throws Exception        if magazine creation has failed.
     */
    public void createQueue(final CreateQueueRequest queueRequest) throws Exception {
        validateRequest(queueRequest);
        final long sweepDuration = queueRequest.getSweepDurationInMins() * 60 * 1000L;
        IQueue<?> queue = createMagazine(
                queueRequest.getName(), queueRequest.getShards(), queueRequest.getMessageExpiry().toSeconds(),
                queueRequest.getQueueExpiry().toSeconds() * Constants.TTL_FACTOR_FOR_QUEUE_EXPIRY,
                queueRequest.getConcurrency(), queueRequest.getMessageHandlerType(), queueRequest.getShovelConfig(),
                queueRequest.getBatchingConfig(), sweepDuration
        );

        //Storing the queue details
        queueService.store(
                queueRequest.getName(),
                QueueEntity.builder()
                        .shards(queueRequest.getShards())
                        .queueExpiry(queueRequest.getQueueExpiry().toSeconds())
                        .messageExpiry(queueRequest.getMessageExpiry().toSeconds())
                        .concurrency(queueRequest.getConcurrency())
                        .shovelConcurrency(Objects.nonNull(queueRequest.getShovelConfig())
                                ? queueRequest.getShovelConfig().getConcurrency() : -1)
                        .shovelTimeIntervalInSecs(Objects.nonNull(queueRequest.getShovelConfig())
                                ? queueRequest.getShovelConfig().getTimeIntervalInSecs() : -1)
                        .messageHandlerType(queueRequest.getMessageHandlerType())
                        .active(true)
                        .sweepDuration(sweepDuration)
                        .createdAt(System.currentTimeMillis())
                        .batchingConfig(queueRequest.getBatchingConfig())
                        .build(),
                // Queue needs to be persisted for (queueExpiry + messageExpiry) to avoid magazine identifier clashes.
                // And (queueExpiry > messageExpiry) ==> (queueExpiry * 2) >= (queueExpiry + messageExpiry). So factor = 2 is chosen
                queueRequest.getQueueExpiry().toSeconds() * Constants.TTL_FACTOR_FOR_QUEUE_EXPIRY
        );
        ignisMQMap.put(queueRequest.getName(), queue);
        log.info("Queue '{}' successfully created", queueRequest.getName());
        refreshQueues();
    }

    /**
     * This function is to stop the consumers and shovel tasks and remove queue from active queues.
     * <p>
     * Caution: Use this function carefully, once queue is deactivated, there is no option to activate again.
     * And queue will be completely deactivated once refresh job is completed.
     *
     * @param queueName: Name of the queue to be deleted
     */
    public void deactivateQueue(final String queueName) {
        log.info("Deactivating queue '{}'", queueName);
        if (ignisMQMap.containsKey(queueName)) {
            final MagazineQueue queue = (MagazineQueue) getQueue(queueName);
            queue.stopConsumers(Constants.MAX_CONSUMERS_ALLOWED);
            queue.stopShovelConsumers(Constants.MAX_CONSUMERS_ALLOWED);
            ignisMQMap.remove(queueName);
        }
        queueService.get(queueName).ifPresent(queueEntity -> {
            if (queueEntity.isActive()) {
                queueService.updateState(queueName, false);
            }
        });
        log.info("Queue '{}' successfully deactivated", queueName);
    }

    /**
     * To create new consumers of a queue.
     *
     * @param queueName -> Name of the queue
     * @param count     -> The number of consumers to be created
     */
    public void increaseConsumers(final String queueName, final int count) {
        final MagazineQueue queue = (MagazineQueue) getQueue(queueName);
        queue.createConsumers(count);
        queueService.updateConcurrency(queueName, queue.getNoOfConsumers());
    }

    /**
     * To stop consumers of main queue.
     *
     * @param queueName -> Name of the queue
     * @param count     -> The number of consumers to be stopped
     */
    public void decreaseConsumers(final String queueName, final int count) {
        final MagazineQueue queue = (MagazineQueue) getQueue(queueName);
        queue.stopConsumers(count);
        queueService.updateConcurrency(queueName, queue.getNoOfConsumers());
    }

    /**
     * To shovel the messages from sideline magazine to main magazine every x seconds.
     * If any queue shoveling config is missed during queue creation then shoveling can be scheduled using this method.
     *
     * @param queueName    -> Name of the queue
     * @param shovelConfig -> Shovel config
     */
    public void scheduleShoveling(final String queueName, @Valid final ShovelConfig shovelConfig) {
        final MagazineQueue queue = (MagazineQueue) getQueue(queueName);
        queue.scheduleShoveling(shovelConfig.getConcurrency(), shovelConfig.getTimeIntervalInSecs());
        queueService.updateShovelConfig(queueName, queue.getNoOfShovelConsumers(), shovelConfig.getTimeIntervalInSecs());
    }

    public void sweepQueue(final String queueName) {
        final QueueEntity queueEntity = queueService.get(queueName).orElse(null);
        if (Objects.isNull(queueEntity)) {
            log.info("Queue {} doesn't exist", queueName);
            return;
        }
        queueSweeper.sweepQueue(queueName, queueEntity);
    }

    public void start() throws Exception {
        if (Objects.isNull(storageClient)) {
            storageClient = storage.accept(new StorageVisitor<>() {
                @Override
                public StorageClient visit(final AerospikeStorage aerospikeStorage) {
                    return new AerospikeStoreClient(aerospikeStorage.getConfiguration());
                }
            });
            this.ownsStorageClient = true;
            this.queueService = buildQueueCommands(storage, storageClient);
        }
    }

    /**
     * Releases everything this manager owns: every scheduled task - consumers, shovels, the queue
     * watcher - and, when the manager created the storage client itself, the client's connection
     * pool. Safe to call more than once. A caller that supplied its own {@link StorageClient} keeps
     * ownership of it.
     */
    public void stop() {
        schedulers.stop();
        if (ownsStorageClient && Objects.nonNull(storageClient)) {
            try {
                storageClient.stop();
            } catch (Exception e) {
                log.error("Error while closing the storage client", e);
            }
        }
    }

    public void refreshQueues() {
        if (messageHandlers.isEmpty()) {
            log.info("No message handlers registered, so queue refreshing is not possible, gracefully ignoring.");
            return;
        }

        final Map<String, QueueEntity> activeQueuesInDB = queueService.getQueues(true);
        final Map<String, QueueEntity> inactiveQueuesInDB = queueService.getQueues(false);

        final Map<String, QueueEntity> allQueuesInDB = new HashMap<>();
        allQueuesInDB.putAll(activeQueuesInDB);
        allQueuesInDB.putAll(inactiveQueuesInDB);

        log.info("Removing expired queues...");
        allQueuesInDB.entrySet().stream()
                .filter(entry -> entry.getValue().isActive())
                .filter(entry -> (entry.getValue().getCreatedAt() + (entry.getValue().getQueueExpiry() * 1000L))
                        <= System.currentTimeMillis())
                .forEach(entry -> {
                    deactivateQueue(entry.getKey());
                    activeQueuesInDB.remove(entry.getKey());
                });

        final List<String> cachedActiveQueues = new ArrayList<>(ignisMQMap.keySet());

        log.info("Refreshing... Checking for new queues");
        activeQueuesInDB.entrySet().stream()
                .filter(entry -> !cachedActiveQueues.contains(entry.getKey()))
                .forEach(entry -> {
                    try {
                        boolean isNullShovelConfig = entry.getValue().getShovelConcurrency() > 0
                                && entry.getValue().getShovelTimeIntervalInSecs() > 0;
                        ShovelConfig shovelConfig = isNullShovelConfig
                                ? ShovelConfig.builder()
                                .concurrency(entry.getValue().getShovelConcurrency())
                                .timeIntervalInSecs(entry.getValue().getShovelTimeIntervalInSecs())
                                .build()
                                : null;
                        IQueue<?> queue = createMagazine(
                                entry.getKey(), entry.getValue().getShards(), entry.getValue().getMessageExpiry(),
                                entry.getValue().getQueueExpiry() * Constants.TTL_FACTOR_FOR_QUEUE_EXPIRY,
                                entry.getValue().getConcurrency(), entry.getValue().getMessageHandlerType(),
                                shovelConfig, entry.getValue().getBatchingConfig(),
                                entry.getValue().getSweepDuration());
                        ignisMQMap.put(entry.getKey(), queue);
                        log.info("Queue '{}' successfully created", entry.getKey());
                    } catch (Exception e) {
                        log.error("Error creating queues in watcher", e);
                    }
                });

        log.info("Refreshing... Deactivating inactive queues");
        inactiveQueuesInDB.keySet().stream()
                .filter(cachedActiveQueues::contains)
                .forEach(this::deactivateQueue);

        log.info("Refreshing... Checking if any queue concurrency or shovel config has updated");
        cachedActiveQueues.stream()
                .filter(ignisMQMap::containsKey)
                .filter(activeQueuesInDB::containsKey)
                .forEach(queueName -> {
                    MagazineQueue queue = (MagazineQueue) ignisMQMap.get(queueName);
                    QueueEntity queueEntity = activeQueuesInDB.get(queueName);

                    if (queue.getNoOfConsumers() < queueEntity.getConcurrency()) {
                        queue.createConsumers(queueEntity.getConcurrency() - queue.getNoOfConsumers());
                    } else if (queue.getNoOfConsumers() > queueEntity.getConcurrency()) {
                        queue.stopConsumers(queue.getNoOfConsumers() - queueEntity.getConcurrency());
                    }

                    if (Objects.nonNull(queue.getShovelConfig())) {
                        if (queue.getShovelConfig().getConcurrency() != queueEntity.getShovelConcurrency()
                                || queue.getShovelConfig().getTimeIntervalInSecs() != queueEntity.getShovelTimeIntervalInSecs()) {
                            queue.stopShovelConsumers(Constants.MAX_CONSUMERS_ALLOWED);
                            queue.scheduleShoveling(queueEntity.getShovelConcurrency(), queueEntity.getShovelTimeIntervalInSecs());
                        }
                    } else if (queueEntity.getShovelConcurrency() > 0 && queueEntity.getShovelTimeIntervalInSecs() > 0) {
                        queue.stopShovelConsumers(Constants.MAX_CONSUMERS_ALLOWED);
                        queue.scheduleShoveling(queueEntity.getShovelConcurrency(), queueEntity.getShovelTimeIntervalInSecs());
                    }
                });
    }

    /*
        Watcher to activate and deactivate queues.
        Queue can be created only once, and this watcher will then have a role to create the active queue and deactivate inactive ones.
     */
    private void scheduleWatcher() {
        schedulers.getControl().scheduleRepeating(this::refreshQueues,
                Constants.WATCHER_INITIAL_DELAY_IN_MS, Constants.REFRESH_INTERVAL_IN_MS);
    }

    private <M> IQueue<M> createMagazine(final String queueName,
                                         final int queueShards,
                                         final int recordTtlInSeconds,
                                         final int metaDataTtlInSeconds,
                                         final int concurrency,
                                         final String messageHandlerType,
                                         final ShovelConfig shovelConfig,
                                         final BatchingConfig batchingConfig,
                                         final long sweepDurationInMillis) throws Exception {
        if (!messageHandlers.containsKey(messageHandlerType)) {
            throw IgnisMQException.builder()
                    .errorCode(ErrorCode.INVALID_MESSAGE_HANDLER)
                    .message(ErrorMessage.MESSAGE_HANDLER_NOT_REGISTERED)
                    .build();
        }

        final Timer publishMetricTimer =
                magazineMeterRegistry.timer(String.format(METRIC_TIMER_FORMAT, queueName, "publish"));
        final Timer consumeMetricTimer =
                magazineMeterRegistry.timer(String.format(METRIC_TIMER_FORMAT, queueName, "consume"));
        return new MagazineQueue<M>(
                clientId, farmId, queueName, queueShards, recordTtlInSeconds, metaDataTtlInSeconds,
                storageClient, storage, concurrency, messageHandlers.get(messageHandlerType).getValue(),
                shovelConfig, mapper, messageHandlers.get(messageHandlerType).getKey(),
                batchingConfig, sweepDurationInMillis, schedulers.getWorker(), publishMetricTimer,
                consumeMetricTimer, magazineMeterRegistry
        );
    }

    private QueueService buildQueueCommands(final BaseStorage storage, final StorageClient storageClient) throws Exception {
        return storage.accept(new StorageVisitor<>() {
            @Override
            public QueueService visit(final AerospikeStorage aerospikeStorage) {
                return new AerospikeQueueService(
                        (IAerospikeClient) storageClient.getClient(),
                        aerospikeStorage.getConfiguration(),
                        aerospikeStorage.getNamespace(),
                        clientId,
                        farmId
                );
            }
        });
    }

    private void validateRequest(final CreateQueueRequest queueRequest) {
        if (!queueRequest.isValid()) {
            throw IgnisMQException.builder()
                    .errorCode(ErrorCode.INVALID_REQUEST)
                    .message(ErrorMessage.QUEUE_EXPIRY_VALIDATION_MESSAGE)
                    .build();
        }

        if (ignisMQMap.containsKey(queueRequest.getName()) || queueService.exists(queueRequest.getName())) {
            throw IgnisMQException.builder()
                    .errorCode(ErrorCode.QUEUE_ALREADY_EXISTS)
                    .build();
        }
    }
}
