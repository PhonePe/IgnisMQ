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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.phonepe.ignis.client.StorageClient;
import com.phonepe.ignis.common.MessageHandler;
import com.phonepe.ignis.common.TimeToLive;
import com.phonepe.ignis.config.BatchingConfig;
import com.phonepe.ignis.common.TimeUnit;
import com.phonepe.ignis.entity.QueueEntity;
import com.phonepe.ignis.exception.ErrorCode;
import com.phonepe.ignis.exception.IgnisMQException;
import com.phonepe.ignis.request.CreateQueueRequest;
import com.phonepe.ignis.request.ShovelConfig;
import com.phonepe.ignis.scheduler.IgnisSchedulers;
import com.phonepe.ignis.service.AerospikeQueueService;
import com.phonepe.ignis.util.AerospikeTestBase;
import com.phonepe.ignis.common.ShardDepth;
import com.phonepe.ignis.util.RequestFactory;
import com.phonepe.ignis.util.TestMessageHandler;
import com.phonepe.ignis.utils.Constants;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.curator.framework.CuratorFramework;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.never;

public class IgnisMQManagerTest extends AerospikeTestBase {
    private static final String MESSAGE_HANDLER_TYPE = "messageHandler";

    private static void assertIgnisError(ErrorCode expected, Executable executable) {
        final IgnisMQException exception = Assertions.assertThrows(IgnisMQException.class, executable);
        Assertions.assertEquals(expected, exception.getErrorCode());
    }

    private StorageClient<com.aerospike.client.IAerospikeClient> storageClient;
    private IgnisMQManager ignisMQManager;
    private AerospikeQueueService aerospikeQueueService;
    private SimpleMeterRegistry metricRegistry;

    @BeforeEach
    public void setUp() throws Exception {
        storageClient = Mockito.mock(StorageClient.class);
        Mockito.when(storageClient.getClient()).thenReturn(aerospikeClient);

        metricRegistry = new SimpleMeterRegistry();
        ignisMQManager = new IgnisMQManager(
                CLIENT_ID, createBaseStorage(), new ObjectMapper(), metricRegistry,
                storageClient, Mockito.mock(CuratorFramework.class), FARM_ID, null);
        aerospikeQueueService = Mockito.spy(createQueueService());

        Field f = IgnisMQManager.class.getDeclaredField("queueService");
        f.setAccessible(true);
        f.set(ignisMQManager, aerospikeQueueService);
        Mockito.doReturn(Collections.emptyMap()).when(aerospikeQueueService).getQueues(Mockito.anyBoolean());
        Assertions.assertEquals(Collections.emptySet(), ignisMQManager.getAllQueuesFromDB());

        // Initialise Message handler
        Map<String, Map.Entry<Class, MessageHandler>> messageHandlerMap = new HashMap<>();
        messageHandlerMap.put(MESSAGE_HANDLER_TYPE,
                new AbstractMap.SimpleEntry<>(String.class, new TestMessageHandler()));
        ignisMQManager.initialiseMessageHandlers(messageHandlerMap);
    }

    @AfterEach
    public void stopManager() {
        if (ignisMQManager != null) {
            ignisMQManager.stop();
            ignisMQManager = null;
        }
    }

    @Test
    public void createQueueAndPublishSuccessfully() throws Exception {
        ignisMQManager.createQueue(RequestFactory.createQueueRequest("QUEUE_1", MESSAGE_HANDLER_TYPE));
        boolean success = ignisMQManager.getQueue("QUEUE_1").publish("true");
        Assertions.assertTrue(success);
        Assertions.assertEquals(1, metricRegistry.find("magazine.load.outcomes")
                .tag("magazine", "QUEUE_1").tag("outcome", "loaded").counter().count(), 0);
    }

    /**
     * The per-shard view against a real magazine rather than a mock of one. It is the only view that
     * shows where a backlog actually sits, and its totals have to agree with the whole-queue numbers
     * or it is worse than not having it.
     */
    @Test
    public void testPerShardDepthsAgreeWithTheQueueTotals() throws Exception {
        ignisMQManager.createQueue(RequestFactory.createQueueRequest("QUEUE_1", MESSAGE_HANDLER_TYPE));
        final IQueue<String> queue = ignisMQManager.getQueue("QUEUE_1");
        for (int message = 0; message < 5; message++) {
            queue.publish("true");
        }

        final List<ShardDepth> depths = queue.getShardDepths();

        Assertions.assertFalse(depths.isEmpty(), "a created queue must report at least one shard");
        Assertions.assertEquals(queue.getMetaData().getPublished(),
                depths.stream().mapToLong(ShardDepth::getPublished).sum(),
                "the shards must account for every published message");
    }

    /**
     * The cluster inventory the console reads. Unlike getAllQueues it does not depend on this
     * process having adopted the queue, and unlike getAllQueuesFromDB it keeps the configuration
     * rather than throwing it away.
     */
    @Test
    public void testStoredQueuesCarryTheirConfigurationAndSurviveDeactivation() throws Exception {
        // setUp stubs the inventory query out for every other test in this class; this one is about
        // that query, so it goes back to the real one.
        Mockito.doCallRealMethod().when(aerospikeQueueService).getQueues(Mockito.anyBoolean());
        ignisMQManager.createQueue(RequestFactory.createQueueRequest("QUEUE_1", MESSAGE_HANDLER_TYPE));

        Assertions.assertEquals(4, ignisMQManager.getStoredQueues(true).get("QUEUE_1").getConcurrency());
        Assertions.assertTrue(ignisMQManager.getStoredQueue("QUEUE_1").isPresent());

        ignisMQManager.deactivateQueue("QUEUE_1");

        Assertions.assertFalse(ignisMQManager.getStoredQueues(true).containsKey("QUEUE_1"));
        Assertions.assertTrue(ignisMQManager.getStoredQueues(false).containsKey("QUEUE_1"),
                "a deactivated queue is still a queue the console has to be able to show");
        Assertions.assertTrue(ignisMQManager.getStoredQueue("QUEUE_1").isPresent());
    }

    @Test
    public void getAllQueuesTest() throws Exception {
        ignisMQManager.createQueue(RequestFactory.createQueueRequest("QUEUE_1", MESSAGE_HANDLER_TYPE));
        ignisMQManager.createQueue(RequestFactory.createQueueRequest("QUEUE_2", MESSAGE_HANDLER_TYPE));

        Set<String> expected = ImmutableSet.of("QUEUE_1", "QUEUE_2");
        Assertions.assertEquals(expected, ignisMQManager.getAllQueues().keySet());
    }

    @Test
    public void deactivateQueueTest() throws Exception {
        ignisMQManager.createQueue(RequestFactory.createQueueRequest("QUEUE_1", MESSAGE_HANDLER_TYPE));
        Assertions.assertTrue(aerospikeQueueService.get("QUEUE_1").get().isActive());

        ignisMQManager.deactivateQueue("QUEUE_1");
        ignisMQManager.refreshQueues();

        assertIgnisError(ErrorCode.QUEUE_NOT_FOUND, () -> ignisMQManager.getQueue("QUEUE_1"));
    }

    @Test
    public void deactivateNonExistentQueue() {
        // Deactivating a queue not in the map - should not throw
        ignisMQManager.deactivateQueue("NON_EXISTENT");
    }

    @Test
    public void queueNotFoundInGetQueueTest() {
        assertIgnisError(ErrorCode.QUEUE_NOT_FOUND, () -> ignisMQManager.getQueue("QUEUE_1"));
    }

    @Test
    public void increaseQueueConsumers() throws Exception {
        CreateQueueRequest queueRequest = RequestFactory.createQueueRequest("QUEUE_1", MESSAGE_HANDLER_TYPE);
        ignisMQManager.createQueue(queueRequest);

        int count = 2;
        ignisMQManager.increaseConsumers("QUEUE_1", count);
        MagazineQueue magazineQueue = (MagazineQueue) ignisMQManager.getQueue("QUEUE_1");
        Assertions.assertEquals(count + queueRequest.getConcurrency(), magazineQueue.getNoOfConsumers());
    }

    /**
     * A queue created with no concurrency accepts publishes and runs nothing, and consumption can be
     * turned on later without recreating it. This is the publish-now-consume-later shape, and it is
     * pinned here because it is documented as supported rather than as an accident of validation.
     */
    @Test
    public void aQueueWithNoConcurrencyPublishesWithoutConsumingUntilConsumersAreAdded() throws Exception {
        final CreateQueueRequest request = CreateQueueRequest.builder()
                .concurrency(0)
                .name("QUEUE_1")
                .messageHandlerType(MESSAGE_HANDLER_TYPE)
                .messageExpiry(TimeToLive.builder().duration(5).timeUnit(TimeUnit.MINUTE).build())
                .queueExpiry(TimeToLive.builder().duration(5).timeUnit(TimeUnit.MINUTE).build())
                .batchingConfig(BatchingConfig.builder().build())
                .build();
        ignisMQManager.createQueue(request);

        final MagazineQueue<String> queue = (MagazineQueue<String>) ignisMQManager.<String>getQueue("QUEUE_1");
        Assertions.assertEquals(0, queue.getNoOfConsumers(), "nothing should be consuming yet");
        Assertions.assertTrue(queue.publish("held for later"), "the queue still accepts publishes");

        ignisMQManager.increaseConsumers("QUEUE_1", 2);

        Assertions.assertEquals(2, queue.getNoOfConsumers());
        Assertions.assertEquals(2, ignisMQManager.getStoredQueue("QUEUE_1").orElseThrow().getConcurrency(),
                "the new count is persisted, so every other instance adopts it on its next refresh");
    }

    @Test
    public void decreaseQueueConsumers() throws Exception {
        CreateQueueRequest queueRequest = RequestFactory.createQueueRequest("QUEUE_1", MESSAGE_HANDLER_TYPE);
        ignisMQManager.createQueue(queueRequest);

        int count = 1;
        ignisMQManager.decreaseConsumers("QUEUE_1", count);
        MagazineQueue magazineQueue = (MagazineQueue) ignisMQManager.getQueue("QUEUE_1");
        Assertions.assertEquals(queueRequest.getConcurrency() - count, magazineQueue.getNoOfConsumers());
    }

    @Test
    public void increaseShovelConsumers() throws Exception {
        ignisMQManager.createQueue(RequestFactory.createQueueRequest("QUEUE_1", MESSAGE_HANDLER_TYPE));
        ShovelConfig shovelConfig = ShovelConfig.builder().concurrency(5).build();
        ignisMQManager.scheduleShoveling("QUEUE_1", shovelConfig);
        MagazineQueue magazineQueue = (MagazineQueue) ignisMQManager.getQueue("QUEUE_1");
        Assertions.assertEquals(9, magazineQueue.getNoOfShovelConsumers());

        ignisMQManager.createQueue(
                CreateQueueRequest.builder().name("QUEUE_2")
                        .concurrency(5)
                        .messageHandlerType(MESSAGE_HANDLER_TYPE)
                        .build());
        ignisMQManager.scheduleShoveling("QUEUE_2", shovelConfig);
        magazineQueue = (MagazineQueue) ignisMQManager.getQueue("QUEUE_2");
        Assertions.assertEquals(5, magazineQueue.getNoOfShovelConsumers());
    }

    @Test
    public void metaDataTest() throws Exception {
        ignisMQManager.createQueue(RequestFactory.createQueueRequest("QUEUE_1", MESSAGE_HANDLER_TYPE));
        MagazineQueue magazineQueue = (MagazineQueue) ignisMQManager.getQueue("QUEUE_1");

        Assertions.assertEquals(0, magazineQueue.getUnconsumedCount());
        Assertions.assertEquals(0, magazineQueue.getMetaData().getPublished());
        Assertions.assertEquals(0, magazineQueue.getMetaData().getConsumed());

        ignisMQManager.getQueue("QUEUE_1").publish("true");
        ignisMQManager.getQueue("QUEUE_1").publish("true");
        ignisMQManager.getQueue("QUEUE_1").publish("false");
        ignisMQManager.getQueue("QUEUE_1").publish("true");
        ignisMQManager.getQueue("QUEUE_1").publish("false");

        Assertions.assertEquals(5, magazineQueue.getMetaData().getPublished());
    }

    @Test
    public void maxAllowedConsumerExceededExceptionInMainMagazineTest() throws Exception {
        ignisMQManager.createQueue(RequestFactory.createQueueRequest("QUEUE_1", MESSAGE_HANDLER_TYPE));
        MagazineQueue magazineQueue = (MagazineQueue) ignisMQManager.getQueue("QUEUE_1");

        assertIgnisError(ErrorCode.MAX_ALLOWED_CONSUMERS_EXCEEDED, () -> magazineQueue.createConsumers(100));
    }

    /**
     * The cap and the request validation disagreed: {@code @Max(100)} accepted a concurrency of 100
     * that the queue then refused at creation, because the guard rejected on reaching the limit
     * rather than on passing it.
     */
    @Test
    public void testTheConsumerCapIsReachableRatherThanOneShortOfIt() throws Exception {
        ignisMQManager.createQueue(RequestFactory.createQueueRequest("QUEUE_1", MESSAGE_HANDLER_TYPE));
        MagazineQueue magazineQueue = (MagazineQueue) ignisMQManager.getQueue("QUEUE_1");
        final int room = Constants.MAX_CONSUMERS_ALLOWED - magazineQueue.getNoOfConsumers();

        try {
            magazineQueue.createConsumers(room);

            Assertions.assertEquals(Constants.MAX_CONSUMERS_ALLOWED, magazineQueue.getNoOfConsumers());
            assertIgnisError(ErrorCode.MAX_ALLOWED_CONSUMERS_EXCEEDED, () -> magazineQueue.createConsumers(1));
        } finally {
            magazineQueue.stopConsumers(Constants.MAX_CONSUMERS_ALLOWED);
        }
    }

    /**
     * A manual shovel is a one-shot. Its future was kept in the queue's shovel list for ever, so
     * every on-demand drain permanently consumed one of the queue's shovel slots and a long-lived
     * process would eventually be unable to shovel at all.
     */
    @Test
    public void testAFinishedOneShotShovelReleasesItsSlot() throws Exception {
        ignisMQManager.createQueue(RequestFactory.createQueueRequest("QUEUE_1", MESSAGE_HANDLER_TYPE));
        MagazineQueue magazineQueue = (MagazineQueue) ignisMQManager.getQueue("QUEUE_1");
        final int scheduled = magazineQueue.getNoOfShovelConsumers();

        magazineQueue.shovel(1);

        final long deadline = System.currentTimeMillis() + 30_000L;
        while (magazineQueue.getNoOfShovelConsumers() > scheduled && System.currentTimeMillis() < deadline) {
            Thread.sleep(200L);
        }
        Assertions.assertEquals(scheduled, magazineQueue.getNoOfShovelConsumers(),
                "a completed one-shot shovel must not keep holding a slot");
    }

    @Test
    public void invalidShovelTimeInternalExceptionTest() throws Exception {
        ignisMQManager.createQueue(RequestFactory.createQueueRequest("QUEUE_1", MESSAGE_HANDLER_TYPE));
        MagazineQueue magazineQueue = (MagazineQueue) ignisMQManager.getQueue("QUEUE_1");

        assertIgnisError(ErrorCode.INVALID_SHOVEL_TIME_INTERVAL, () -> magazineQueue.scheduleShoveling(0, 100000));
    }

    @Test
    public void negativeShovelTimeInternalExceptionTest() throws Exception {
        ignisMQManager.createQueue(RequestFactory.createQueueRequest("QUEUE_1", MESSAGE_HANDLER_TYPE));
        MagazineQueue magazineQueue = (MagazineQueue) ignisMQManager.getQueue("QUEUE_1");

        assertIgnisError(ErrorCode.INVALID_SHOVEL_TIME_INTERVAL, () -> magazineQueue.scheduleShoveling(0, -1));
    }

    @Test
    public void maxAllowedConsumerExceededExceptionInSidelineMagazineTest() throws Exception {
        ignisMQManager.createQueue(RequestFactory.createQueueRequest("QUEUE_1", MESSAGE_HANDLER_TYPE));
        MagazineQueue magazineQueue = (MagazineQueue) ignisMQManager.getQueue("QUEUE_1");

        assertIgnisError(ErrorCode.MAX_ALLOWED_CONSUMERS_EXCEEDED, () -> magazineQueue.shovel(100));
    }

    @Test
    public void invalidQueueExpiryExceptionTest() throws Exception {
        assertIgnisError(ErrorCode.INVALID_REQUEST, () -> ignisMQManager.createQueue(
                CreateQueueRequest.builder().name("QUEUE_1")
                        .concurrency(5)
                        .messageHandlerType(MESSAGE_HANDLER_TYPE)
                        .queueExpiry(TimeToLive.builder()
                                .timeUnit(TimeUnit.DAY)
                                .duration(1000)
                                .build())
                        .build()));
    }

    @Test
    public void invalidMessageExpiryExceptionTest() throws Exception {
        assertIgnisError(ErrorCode.INVALID_REQUEST, () -> ignisMQManager.createQueue(
                CreateQueueRequest.builder().name("QUEUE_1")
                        .concurrency(5)
                        .messageHandlerType(MESSAGE_HANDLER_TYPE)
                        .messageExpiry(TimeToLive.builder()
                                .timeUnit(TimeUnit.DAY)
                                .duration(1000)
                                .build())
                        .build()));
    }

    @Test
    public void messageExpiryMoreThanQueueExpiryExceptionTest() throws Exception {
        assertIgnisError(ErrorCode.INVALID_REQUEST, () -> ignisMQManager.createQueue(
                CreateQueueRequest.builder().name("QUEUE_1")
                        .concurrency(5)
                        .messageHandlerType(MESSAGE_HANDLER_TYPE)
                        .messageExpiry(TimeToLive.builder()
                                .timeUnit(TimeUnit.DAY)
                                .duration(2)
                                .build())
                        .queueExpiry(TimeToLive.builder()
                                .timeUnit(TimeUnit.DAY)
                                .duration(1)
                                .build())
                        .build()));
    }

    @Test
    public void createExistingQueueExceptionTest() throws Exception {
        ignisMQManager.createQueue(RequestFactory.createQueueRequest("QUEUE_1", MESSAGE_HANDLER_TYPE));
        assertIgnisError(ErrorCode.QUEUE_ALREADY_EXISTS,
                () -> ignisMQManager.createQueue(RequestFactory.createQueueRequest("QUEUE_1", MESSAGE_HANDLER_TYPE)));
    }

    @Test
    public void invalidMessageHandlerExceptionTest() throws Exception {
        assertIgnisError(ErrorCode.INVALID_MESSAGE_HANDLER,
                () -> ignisMQManager.createQueue(RequestFactory.createQueueRequest("QUEUE_1", "INVALID")));
    }

    @Test
    public void testSweepQueueNonExistentQueue() {
        ignisMQManager.sweepQueue("NON_EXISTENT");
        // Should log "Queue doesn't exist" and return
    }

    @Test
    public void testSweepQueueExistingQueue() throws Exception {
        ignisMQManager.createQueue(RequestFactory.createQueueRequest("QUEUE_1", MESSAGE_HANDLER_TYPE));
        // sweepQueue calls Utils.sweepQueue - it won't throw even if there's nothing to sweep
        ignisMQManager.sweepQueue("QUEUE_1");
    }

    @Test
    public void testRefreshQueuesWithNoHandlers() throws Exception {
        // Create a fresh manager without handlers
        StorageClient<com.aerospike.client.IAerospikeClient> sc = Mockito.mock(StorageClient.class);
        Mockito.when(sc.getClient()).thenReturn(aerospikeClient);

        IgnisMQManager manager = new IgnisMQManager(
                CLIENT_ID, createBaseStorage(), new ObjectMapper(), new SimpleMeterRegistry(),
                sc, Mockito.mock(CuratorFramework.class), FARM_ID, null);
        try {
            // Don't initialize message handlers
            manager.refreshQueues(); // Should log "No message handlers registered" and return
        } finally {
            manager.stop();
        }
    }

    @Test
    public void testRefreshQueuesCreatesNewActiveQueues() throws Exception {
        // Create a queue in the real aerospike but NOT in ignisMQManager's map
        // Then refreshQueues should pick it up
        QueueEntity entity = QueueEntity.builder()
                .active(true).shards(1).queueExpiry(300).messageExpiry(60)
                .concurrency(1).messageHandlerType(MESSAGE_HANDLER_TYPE)
                .shovelConcurrency(0).shovelTimeIntervalInSecs(0)
                .createdAt(System.currentTimeMillis())
                .sweepDuration(20 * 60 * 1000L)
                .build();

        // Reset the spy to return actual data
        Mockito.reset(aerospikeQueueService);
        AerospikeQueueService realService = createQueueService();
        realService.store("REFRESH_QUEUE", entity, 1200);

        // Re-spy
        aerospikeQueueService = Mockito.spy(realService);
        Field f = IgnisMQManager.class.getDeclaredField("queueService");
        f.setAccessible(true);
        f.set(ignisMQManager, aerospikeQueueService);

        ignisMQManager.refreshQueues();

        // The queue should now be in the map
        Assertions.assertNotNull(ignisMQManager.getAllQueues().get("REFRESH_QUEUE"));
    }

    /**
     * The upgrade path for every queue that exists today. A queue created before the handler
     * timeout was configurable has no such bin, which reads back as zero - and zero is not merely
     * an unhelpful default, it clamps to a 1 ms timeout, which would time out and sideline every
     * batch on the queue. The fallback is what stands between an upgrade and total data diversion.
     */
    @Test
    public void testAQueueCreatedBeforeTheHandlerTimeoutBinFallsBackToTheDefault() throws Exception {
        final long sweepDuration = 60 * 60 * 1000L;
        QueueEntity entity = QueueEntity.builder()
                .active(true).shards(1).queueExpiry(300).messageExpiry(60)
                .concurrency(1).messageHandlerType(MESSAGE_HANDLER_TYPE)
                .shovelConcurrency(0).shovelTimeIntervalInSecs(0)
                .createdAt(System.currentTimeMillis())
                .sweepDuration(sweepDuration)
                .build();

        Mockito.reset(aerospikeQueueService);
        AerospikeQueueService realService = createQueueService();
        realService.store("LEGACY_TIMEOUT_QUEUE", entity, 1200);
        // Written, then stripped, so the record is shaped exactly as one from a version that had
        // never heard of the bin rather than one that wrote a zero into it.
        aerospikeClient.put(null,
                new Key(AEROSPIKE_NAMESPACE,
                        String.format("%s_%s_ignis_queues", FARM_ID, CLIENT_ID), "LEGACY_TIMEOUT_QUEUE"),
                Bin.asNull("handlerTimeout"));
        Assertions.assertEquals(0L,
                realService.get("LEGACY_TIMEOUT_QUEUE").orElseThrow().getHandlerTimeout(),
                "precondition: the bin is absent and reads back as the sentinel");

        aerospikeQueueService = Mockito.spy(realService);
        Field f = IgnisMQManager.class.getDeclaredField("queueService");
        f.setAccessible(true);
        f.set(ignisMQManager, aerospikeQueueService);

        ignisMQManager.refreshQueues();

        MagazineQueue queue = (MagazineQueue) ignisMQManager.getAllQueues().get("LEGACY_TIMEOUT_QUEUE");
        Assertions.assertNotNull(queue);
        // A one-hour sweep duration puts the clamp ceiling at 30 minutes, well clear of the
        // 10-minute default, so the assertion distinguishes the fallback from the clamp rather
        // than passing on a coincidence of the two.
        Assertions.assertEquals(Constants.DEFAULT_HANDLER_TIMEOUT_IN_MINS * 60 * 1000L,
                queue.getHandlerTimeoutMillis(),
                "a queue predating the bin must fall back to the default, not to a 1 ms timeout");
    }

    @Test
    public void testRefreshQueuesDeactivatesInactiveQueues() throws Exception {
        // First create a queue
        ignisMQManager.createQueue(RequestFactory.createQueueRequest("QUEUE_1", MESSAGE_HANDLER_TYPE));
        Assertions.assertNotNull(ignisMQManager.getAllQueues().get("QUEUE_1"));

        // Now deactivate in DB
        Mockito.reset(aerospikeQueueService);
        AerospikeQueueService realService = createQueueService();
        // The queue already exists in aerospike from createQueue. Mark it inactive.
        realService.updateState("QUEUE_1", false);

        aerospikeQueueService = Mockito.spy(realService);
        Field f = IgnisMQManager.class.getDeclaredField("queueService");
        f.setAccessible(true);
        f.set(ignisMQManager, aerospikeQueueService);

        ignisMQManager.refreshQueues();

        // Queue should be removed from map
        Assertions.assertFalse(ignisMQManager.getAllQueues().containsKey("QUEUE_1"));
    }

    @Test
    public void testRefreshQueuesUpdatesConcurrency() throws Exception {
        ignisMQManager.createQueue(RequestFactory.createQueueRequest("QUEUE_1", MESSAGE_HANDLER_TYPE));
        MagazineQueue q = (MagazineQueue) ignisMQManager.getQueue("QUEUE_1");
        int originalConsumers = q.getNoOfConsumers();

        // Reset spy and update concurrency in DB
        Mockito.reset(aerospikeQueueService);
        AerospikeQueueService realService = createQueueService();
        realService.updateConcurrency("QUEUE_1", originalConsumers + 2);

        aerospikeQueueService = Mockito.spy(realService);
        Field f = IgnisMQManager.class.getDeclaredField("queueService");
        f.setAccessible(true);
        f.set(ignisMQManager, aerospikeQueueService);

        ignisMQManager.refreshQueues();

        // Consumers should have increased
        Assertions.assertEquals(originalConsumers + 2, q.getNoOfConsumers());
    }

    @Test
    public void testRefreshQueuesDecreaseConcurrency() throws Exception {
        ignisMQManager.createQueue(RequestFactory.createQueueRequest("QUEUE_1", MESSAGE_HANDLER_TYPE));
        MagazineQueue q = (MagazineQueue) ignisMQManager.getQueue("QUEUE_1");
        int originalConsumers = q.getNoOfConsumers();
        Assertions.assertTrue(originalConsumers > 1);

        // Reset spy and update concurrency in DB to lower value
        Mockito.reset(aerospikeQueueService);
        AerospikeQueueService realService = createQueueService();
        realService.updateConcurrency("QUEUE_1", 1);

        aerospikeQueueService = Mockito.spy(realService);
        Field f = IgnisMQManager.class.getDeclaredField("queueService");
        f.setAccessible(true);
        f.set(ignisMQManager, aerospikeQueueService);

        ignisMQManager.refreshQueues();

        Assertions.assertEquals(1, q.getNoOfConsumers());
    }

    @Test
    public void testRefreshQueuesUpdatesShovelConfig() throws Exception {
        ignisMQManager.createQueue(RequestFactory.createQueueRequest("QUEUE_1", MESSAGE_HANDLER_TYPE));

        // Queue was created with shovel config. Update the shovel config in DB
        Mockito.reset(aerospikeQueueService);
        AerospikeQueueService realService = createQueueService();
        realService.updateShovelConfig("QUEUE_1", 3, 10);

        aerospikeQueueService = Mockito.spy(realService);
        Field f = IgnisMQManager.class.getDeclaredField("queueService");
        f.setAccessible(true);
        f.set(ignisMQManager, aerospikeQueueService);

        ignisMQManager.refreshQueues();
    }

    @Test
    public void testRefreshQueuesAddsShovelConfigWhenNone() throws Exception {
        // Create queue without shovel config
        ignisMQManager.createQueue(
                CreateQueueRequest.builder().name("QUEUE_NS")
                        .concurrency(2)
                        .messageHandlerType(MESSAGE_HANDLER_TYPE)
                        .build());

        MagazineQueue q = (MagazineQueue) ignisMQManager.getQueue("QUEUE_NS");
        Assertions.assertNull(q.getShovelConfig());

        // Now set shovel config in DB
        Mockito.reset(aerospikeQueueService);
        AerospikeQueueService realService = createQueueService();
        realService.updateShovelConfig("QUEUE_NS", 2, 5);

        aerospikeQueueService = Mockito.spy(realService);
        Field f = IgnisMQManager.class.getDeclaredField("queueService");
        f.setAccessible(true);
        f.set(ignisMQManager, aerospikeQueueService);

        ignisMQManager.refreshQueues();
    }

    @Test
    public void testStopMethod() {
        ignisMQManager.stop();
    }

    /**
     * The manager did not build this storage client, so it must not close it: the caller that
     * supplied it may still be using it.
     */
    @Test
    public void testStopDoesNotCloseAnExternallyOwnedStorageClient() {
        ignisMQManager.stop();
        Mockito.verify(storageClient, never()).stop();
    }

    @Test
    public void testStopIsIdempotent() {
        ignisMQManager.stop();
        ignisMQManager.stop();
        Mockito.verify(storageClient, never()).stop();
    }

    @Test
    public void testStopLeavesNoSchedulerThreadsBehind() throws Exception {
        final Field schedulersField = IgnisMQManager.class.getDeclaredField("schedulers");
        schedulersField.setAccessible(true);
        final IgnisSchedulers schedulers = (IgnisSchedulers) schedulersField.get(ignisMQManager);
        assertNotNull(schedulers, "the manager must own its schedulers");

        ignisMQManager.stop();

        assertTrue(schedulers.isStopped(), "both pools must be shut down");
        final long deadline = System.currentTimeMillis() + 15_000L;
        while (schedulers.getWorker().poolSize() + schedulers.getControl().poolSize() > 0
                && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertEquals(0, schedulers.getWorker().poolSize(), "no worker thread may survive stop()");
        assertEquals(0, schedulers.getControl().poolSize(), "no control thread may survive stop()");
    }

    @Test
    public void testMeterRegistryIsRequired() {
        Assertions.assertThrows(NullPointerException.class,
                () -> new IgnisMQManager(CLIENT_ID, createBaseStorage(), new ObjectMapper(), null,
                        storageClient, Mockito.mock(CuratorFramework.class), FARM_ID, null));
    }

    @Test
    public void testGetTaskInitializer() {
        Assertions.assertNotNull(ignisMQManager.getTaskInitializer());
    }
}
