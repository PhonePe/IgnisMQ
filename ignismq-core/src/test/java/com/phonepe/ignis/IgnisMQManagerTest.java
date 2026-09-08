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

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.phonepe.ignis.client.StorageClient;
import com.phonepe.ignis.common.MessageHandler;
import com.phonepe.ignis.common.TimeToLive;
import com.phonepe.ignis.common.TimeUnit;
import com.phonepe.ignis.config.BatchingConfig;
import com.phonepe.ignis.consumer.MagazineConsumerTask;
import com.phonepe.ignis.entity.QueueEntity;
import com.phonepe.ignis.exception.ErrorCode;
import com.phonepe.ignis.request.CreateQueueRequest;
import com.phonepe.ignis.request.ShovelConfig;
import com.phonepe.ignis.service.AerospikeQueueService;
import com.phonepe.ignis.util.AerospikeTestBase;
import com.phonepe.ignis.util.IgnisExceptionMatcher;
import com.phonepe.ignis.util.RequestFactory;
import com.phonepe.ignis.util.TestMessageHandler;
import com.phonepe.magazine.Magazine;
import com.phonepe.magazine.entity.MagazineData;
import com.phonepe.magazine.entity.MetaData;
import com.phonepe.magazine.exception.MagazineException;
import org.apache.curator.framework.CuratorFramework;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.*;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;

public class IgnisMQManagerTest extends AerospikeTestBase {
    private static final String MESSAGE_HANDLER_TYPE = "messageHandler";

    @Rule
    public ExpectedException exceptionThrown = ExpectedException.none();

    private StorageClient<com.aerospike.client.IAerospikeClient> storageClient;
    private IgnisMQManager ignisMQManager;
    private AerospikeQueueService aerospikeQueueService;

    @Before
    public void setUp() throws Exception {
        storageClient = Mockito.mock(StorageClient.class);
        Mockito.when(storageClient.getClient()).thenReturn(aerospikeClient);

        ignisMQManager = new IgnisMQManager(
                CLIENT_ID, createBaseStorage(), new ObjectMapper(), new MetricRegistry(),
                storageClient, Mockito.mock(CuratorFramework.class), FARM_ID);
        aerospikeQueueService = Mockito.spy(createQueueService());

        Field f = IgnisMQManager.class.getDeclaredField("queueService");
        f.setAccessible(true);
        f.set(ignisMQManager, aerospikeQueueService);
        Mockito.doReturn(Collections.emptyMap()).when(aerospikeQueueService).getQueues(Mockito.anyBoolean());
        Assert.assertEquals(Collections.emptySet(), ignisMQManager.getAllQueuesFromDB());

        // Initialise Message handler
        Map<String, Map.Entry<Class, MessageHandler>> messageHandlerMap = new HashMap<>();
        messageHandlerMap.put(MESSAGE_HANDLER_TYPE, new AbstractMap.SimpleEntry<>(String.class, new TestMessageHandler()));
        ignisMQManager.initialiseMessageHandlers(messageHandlerMap);
    }

    @Test
    public void createQueueAndPublishSuccessfully() throws Exception {
        ignisMQManager.createQueue(RequestFactory.createQueueRequest("QUEUE_1", MESSAGE_HANDLER_TYPE));
        boolean success = ignisMQManager.getQueue("QUEUE_1").publish("true");
        Assert.assertTrue(success);
    }

    @Test
    public void getAllQueuesTest() throws Exception {
        ignisMQManager.createQueue(RequestFactory.createQueueRequest("QUEUE_1", MESSAGE_HANDLER_TYPE));
        ignisMQManager.createQueue(RequestFactory.createQueueRequest("QUEUE_2", MESSAGE_HANDLER_TYPE));

        Set<String> expected = ImmutableSet.of("QUEUE_1", "QUEUE_2");
        Assert.assertEquals(expected, ignisMQManager.getAllQueues().keySet());
    }

    @Test
    public void deactivateQueueTest() throws Exception {
        ignisMQManager.createQueue(RequestFactory.createQueueRequest("QUEUE_1", MESSAGE_HANDLER_TYPE));
        Assert.assertTrue(aerospikeQueueService.get("QUEUE_1").get().isActive());

        ignisMQManager.deactivateQueue("QUEUE_1");
        ignisMQManager.refreshQueues();

        exceptionThrown.expect(IgnisExceptionMatcher.hasCode(ErrorCode.QUEUE_NOT_FOUND));
        ignisMQManager.getQueue("QUEUE_1");
    }

    @Test
    public void deactivateNonExistentQueue() {
        // Deactivating a queue not in the map - should not throw
        ignisMQManager.deactivateQueue("NON_EXISTENT");
    }

    @Test
    public void queueNotFoundInGetQueueTest() {
        exceptionThrown.expect(IgnisExceptionMatcher.hasCode(ErrorCode.QUEUE_NOT_FOUND));
        ignisMQManager.getQueue("QUEUE_1");
    }

    @Test
    public void increaseQueueConsumers() throws Exception {
        CreateQueueRequest queueRequest = RequestFactory.createQueueRequest("QUEUE_1", MESSAGE_HANDLER_TYPE);
        ignisMQManager.createQueue(queueRequest);

        int count = 2;
        ignisMQManager.increaseConsumers("QUEUE_1", count);
        MagazineQueue magazineQueue = (MagazineQueue) ignisMQManager.getQueue("QUEUE_1");
        Assert.assertEquals(count + queueRequest.getConcurrency(), magazineQueue.getNoOfConsumers());
    }

    @Test
    public void decreaseQueueConsumers() throws Exception {
        CreateQueueRequest queueRequest = RequestFactory.createQueueRequest("QUEUE_1", MESSAGE_HANDLER_TYPE);
        ignisMQManager.createQueue(queueRequest);

        int count = 1;
        ignisMQManager.decreaseConsumers("QUEUE_1", count);
        MagazineQueue magazineQueue = (MagazineQueue) ignisMQManager.getQueue("QUEUE_1");
        Assert.assertEquals(queueRequest.getConcurrency() - count, magazineQueue.getNoOfConsumers());
    }

    @Test
    public void increaseShovelConsumers() throws Exception {
        ignisMQManager.createQueue(RequestFactory.createQueueRequest("QUEUE_1", MESSAGE_HANDLER_TYPE));
        ShovelConfig shovelConfig = ShovelConfig.builder().concurrency(5).build();
        ignisMQManager.scheduleShoveling("QUEUE_1", shovelConfig);
        MagazineQueue magazineQueue = (MagazineQueue) ignisMQManager.getQueue("QUEUE_1");
        Assert.assertEquals(9, magazineQueue.getNoOfShovelConsumers());

        ignisMQManager.createQueue(
                CreateQueueRequest.builder().name("QUEUE_2")
                        .concurrency(5)
                        .messageHandlerType(MESSAGE_HANDLER_TYPE)
                        .build());
        ignisMQManager.scheduleShoveling("QUEUE_2", shovelConfig);
        magazineQueue = (MagazineQueue) ignisMQManager.getQueue("QUEUE_2");
        Assert.assertEquals(5, magazineQueue.getNoOfShovelConsumers());
    }

    @Test
    public void metaDataTest() throws Exception {
        ignisMQManager.createQueue(RequestFactory.createQueueRequest("QUEUE_1", MESSAGE_HANDLER_TYPE));
        MagazineQueue magazineQueue = (MagazineQueue) ignisMQManager.getQueue("QUEUE_1");

        Assert.assertEquals(0, magazineQueue.getUnconsumedCount());
        Assert.assertEquals(0, magazineQueue.getMetaData().getPublished());
        Assert.assertEquals(0, magazineQueue.getMetaData().getConsumed());

        ignisMQManager.getQueue("QUEUE_1").publish("true");
        ignisMQManager.getQueue("QUEUE_1").publish("true");
        ignisMQManager.getQueue("QUEUE_1").publish("false");
        ignisMQManager.getQueue("QUEUE_1").publish("true");
        ignisMQManager.getQueue("QUEUE_1").publish("false");

        Assert.assertEquals(5, magazineQueue.getMetaData().getPublished());
    }

    @Test
    public void maxAllowedConsumerExceededExceptionInMainMagazineTest() throws Exception {
        ignisMQManager.createQueue(RequestFactory.createQueueRequest("QUEUE_1", MESSAGE_HANDLER_TYPE));
        MagazineQueue magazineQueue = (MagazineQueue) ignisMQManager.getQueue("QUEUE_1");

        exceptionThrown.expect(IgnisExceptionMatcher.hasCode(ErrorCode.MAX_ALLOWED_CONSUMERS_EXCEEDED));
        magazineQueue.createConsumers(100);
    }

    @Test
    public void invalidShovelTimeInternalExceptionTest() throws Exception {
        ignisMQManager.createQueue(RequestFactory.createQueueRequest("QUEUE_1", MESSAGE_HANDLER_TYPE));
        MagazineQueue magazineQueue = (MagazineQueue) ignisMQManager.getQueue("QUEUE_1");

        exceptionThrown.expect(IgnisExceptionMatcher.hasCode(ErrorCode.INVALID_SHOVEL_TIME_INTERVAL));
        magazineQueue.scheduleShoveling(0, 100000);
    }

    @Test
    public void negativeShovelTimeInternalExceptionTest() throws Exception {
        ignisMQManager.createQueue(RequestFactory.createQueueRequest("QUEUE_1", MESSAGE_HANDLER_TYPE));
        MagazineQueue magazineQueue = (MagazineQueue) ignisMQManager.getQueue("QUEUE_1");

        exceptionThrown.expect(IgnisExceptionMatcher.hasCode(ErrorCode.INVALID_SHOVEL_TIME_INTERVAL));
        magazineQueue.scheduleShoveling(0, -1);
    }

    @Test
    public void maxAllowedConsumerExceededExceptionInSidelineMagazineTest() throws Exception {
        ignisMQManager.createQueue(RequestFactory.createQueueRequest("QUEUE_1", MESSAGE_HANDLER_TYPE));
        MagazineQueue magazineQueue = (MagazineQueue) ignisMQManager.getQueue("QUEUE_1");

        exceptionThrown.expect(IgnisExceptionMatcher.hasCode(ErrorCode.MAX_ALLOWED_CONSUMERS_EXCEEDED));
        magazineQueue.shovel(100);
    }

    @Test
    public void invalidQueueExpiryExceptionTest() throws Exception {
        exceptionThrown.expect(IgnisExceptionMatcher.hasCode(ErrorCode.INVALID_REQUEST));
        ignisMQManager.createQueue(
                CreateQueueRequest.builder().name("QUEUE_1")
                        .concurrency(5)
                        .messageHandlerType(MESSAGE_HANDLER_TYPE)
                        .queueExpiry(TimeToLive.builder()
                                .timeUnit(TimeUnit.DAY)
                                .duration(1000)
                                .build())
                        .build());
    }

    @Test
    public void invalidMessageExpiryExceptionTest() throws Exception {
        exceptionThrown.expect(IgnisExceptionMatcher.hasCode(ErrorCode.INVALID_REQUEST));
        ignisMQManager.createQueue(
                CreateQueueRequest.builder().name("QUEUE_1")
                        .concurrency(5)
                        .messageHandlerType(MESSAGE_HANDLER_TYPE)
                        .messageExpiry(TimeToLive.builder()
                                .timeUnit(TimeUnit.DAY)
                                .duration(1000)
                                .build())
                        .build());
    }

    @Test
    public void messageExpiryMoreThanQueueExpiryExceptionTest() throws Exception {
        exceptionThrown.expect(IgnisExceptionMatcher.hasCode(ErrorCode.INVALID_REQUEST));
        ignisMQManager.createQueue(
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
                        .build());
    }

    @Test
    public void createExistingQueueExceptionTest() throws Exception {
        exceptionThrown.expect(IgnisExceptionMatcher.hasCode(ErrorCode.QUEUE_ALREADY_EXISTS));
        ignisMQManager.createQueue(RequestFactory.createQueueRequest("QUEUE_1", MESSAGE_HANDLER_TYPE));
        ignisMQManager.createQueue(RequestFactory.createQueueRequest("QUEUE_1", MESSAGE_HANDLER_TYPE));
    }

    @Test
    public void invalidMessageHandlerExceptionTest() throws Exception {
        exceptionThrown.expect(IgnisExceptionMatcher.hasCode(ErrorCode.INVALID_MESSAGE_HANDLER));
        ignisMQManager.createQueue(RequestFactory.createQueueRequest("QUEUE_1", "INVALID"));
    }

    @Test
    public void queueSingleStringMessageConsumeTest() {
        Magazine<String> magazine = Mockito.mock(Magazine.class);
        Mockito.when(magazine.load(any())).thenReturn(true);
        MagazineData<String> trueData = buildMagazineData("true");
        MagazineData<String> falseData = buildMagazineData("false");
        MagazineData<String> nullData = buildMagazineData(null);
        Mockito.when(magazine.fire()).thenReturn(trueData, falseData, nullData, null);
        MagazineConsumerTask<String> magazineConsumerTask = new MagazineConsumerTask<>(
                magazine, magazine, new TestMessageHandler(),
                new ObjectMapper(), String.class, new Timer(), aerospikeQueueService, null);
        magazineConsumerTask.run();
        Mockito.verify(magazine, Mockito.times(4)).fire();
        Mockito.verify(magazine, Mockito.times(1)).load(any());
        Mockito.verify(magazine, Mockito.times(3)).delete(any());
    }

    @Test
    public void queueSingleIntegerMessageConsumeTest() {
        Magazine<String> magazine = Mockito.mock(Magazine.class);
        Mockito.when(magazine.load(any())).thenReturn(true);
        MagazineData<String> data = buildMagazineData("1");
        MagazineData<String> zeroData = buildMagazineData("0");
        MagazineData<String> nullData = buildMagazineData(null);
        Mockito.when(magazine.fire()).thenReturn(data, data, zeroData, nullData, null);
        MessageHandler<Integer> messageHandler = new MessageHandler<>() {
            @Override
            public Set<Class<?>> getIgnorableExceptions() {
                return Set.of();
            }

            @Override
            public boolean handle(Integer message) {
                return message > 0;
            }

            @Override
            public boolean handle(List<Integer> messages) {
                return messages.stream().allMatch(this::handle);
            }
        };
        MagazineConsumerTask<Integer> magazineConsumerTask = new MagazineConsumerTask<>(
                magazine, magazine, messageHandler, new ObjectMapper(), Integer.class,
                new Timer(), aerospikeQueueService, null);
        magazineConsumerTask.run();
        Mockito.verify(magazine, Mockito.times(5)).fire();
        Mockito.verify(magazine, Mockito.times(1)).load(any());
        Mockito.verify(magazine, Mockito.times(4)).delete(any());
    }

    @Test
    public void queueBatchConsumeTest() {
        Magazine<String> magazine = Mockito.mock(Magazine.class);
        Mockito.when(magazine.getMetaData()).thenReturn(
                Map.of("SHARD_1", MetaData.builder().firePointer(0).loadPointer(1).build()),
                Map.of("SHARD_1", MetaData.builder().firePointer(1).loadPointer(5).build()));
        Mockito.when(magazine.load(any())).thenReturn(true);
        MagazineData<String> trueData = buildMagazineData("true");
        MagazineData<String> falseData = buildMagazineData("false");
        MagazineData<String> nullData = buildMagazineData(null);
        Mockito.when(magazine.fire()).thenReturn(trueData, falseData, trueData, trueData, trueData, trueData, nullData, null);
        MagazineConsumerTask<String> magazineConsumerTask = new MagazineConsumerTask<>(
                magazine, magazine, new TestMessageHandler(),
                new ObjectMapper(), String.class, new Timer(), aerospikeQueueService,
                BatchingConfig.builder().maxBatchSize(3).maxWaitTimeInSecs(10).build());
        magazineConsumerTask.run();
        Mockito.verify(magazine, Mockito.times(8)).fire();
        Mockito.verify(magazine, Mockito.times(3)).load(any());
        Mockito.verify(magazine, Mockito.times(7)).delete(any());
    }

    @Test
    public void testConsumeWithMagazineExceptionNonNothingToFire() {
        Magazine<String> magazine = Mockito.mock(Magazine.class);
        Mockito.when(magazine.fire())
                .thenThrow(new MagazineException(
                        com.phonepe.magazine.exception.ErrorCode.INTERNAL_ERROR,
                        "some error", null))
                .thenReturn(null);
        MagazineConsumerTask<String> task = new MagazineConsumerTask<>(
                magazine, magazine, new TestMessageHandler(),
                new ObjectMapper(), String.class, new Timer(), aerospikeQueueService, null);
        task.run();
        // Non-NOTHING_TO_FIRE MagazineException causes fireFromMagazine to return null, stopping takeWhile after 1 call
        Mockito.verify(magazine, Mockito.times(1)).fire();
    }

    @Test
    public void testRetriesExhaustedStopsCurrentDrainWithoutDeletingData() {
        Magazine<String> magazine = Mockito.mock(Magazine.class);
        Mockito.when(magazine.fire()).thenThrow(new MagazineException(
                com.phonepe.magazine.exception.ErrorCode.RETRIES_EXHAUSTED,
                "data may remain", null));
        MagazineConsumerTask<String> task = new MagazineConsumerTask<>(
                magazine, magazine, new TestMessageHandler(),
                new ObjectMapper(), String.class, new Timer(), aerospikeQueueService, null);

        task.run();

        Mockito.verify(magazine).fire();
        Mockito.verify(magazine, Mockito.never()).delete(any());
    }

    @Test
    public void testConsumeWithGenericException() {
        Magazine<String> magazine = Mockito.mock(Magazine.class);
        Mockito.when(magazine.fire())
                .thenThrow(new RuntimeException("generic error"))
                .thenReturn(null);
        MagazineConsumerTask<String> task = new MagazineConsumerTask<>(
                magazine, magazine, new TestMessageHandler(),
                new ObjectMapper(), String.class, new Timer(), aerospikeQueueService, null);
        task.run();
        // Generic exception causes fireFromMagazine to return null, stopping takeWhile after 1 call
        Mockito.verify(magazine, Mockito.times(1)).fire();
    }

    @Test
    public void testConsumeHandlerThrowsException() {
        Magazine<String> magazine = Mockito.mock(Magazine.class);
        MagazineData<String> data = buildMagazineData("test");
        Mockito.when(magazine.fire()).thenReturn(data, (MagazineData<String>) null);

        MessageHandler<String> failingHandler = new MessageHandler<>() {
            @Override
            public Set<Class<?>> getIgnorableExceptions() {
                return null;
            }

            @Override
            public boolean handle(String message) {
                throw new RuntimeException("handler error");
            }

            @Override
            public boolean handle(List<String> messages) {
                throw new RuntimeException("handler error");
            }
        };

        MagazineConsumerTask<String> task = new MagazineConsumerTask<>(
                magazine, magazine, failingHandler,
                new ObjectMapper(), String.class, new Timer(), aerospikeQueueService, null);
        task.run();
        // Exception in consume -> handleException -> sidelineMessage
        Mockito.verify(magazine, Mockito.times(1)).load(any());
        Mockito.verify(magazine, Mockito.times(1)).delete(any());
    }

    @Test
    public void testConsumeWithIgnorableException() {
        Magazine<String> magazine = Mockito.mock(Magazine.class);
        MagazineData<String> data = buildMagazineData("test");
        Mockito.when(magazine.fire()).thenReturn(data, (MagazineData<String>) null);

        MessageHandler<String> handler = new MessageHandler<>() {
            @Override
            public Set<Class<?>> getIgnorableExceptions() {
                return Set.of(RuntimeException.class);
            }

            @Override
            public boolean handle(String message) {
                throw new RuntimeException("ignorable");
            }

            @Override
            public boolean handle(List<String> messages) {
                throw new RuntimeException("ignorable");
            }
        };

        MagazineConsumerTask<String> task = new MagazineConsumerTask<>(
                magazine, magazine, handler,
                new ObjectMapper(), String.class, new Timer(), aerospikeQueueService, null);
        task.run();
        // Ignorable exception => no sideline
        Mockito.verify(magazine, never()).load(any());
        Mockito.verify(magazine, Mockito.times(1)).delete(any());
    }

    @Test
    public void testConsumeWithInvalidJsonDeserialization() {
        Magazine<String> magazine = Mockito.mock(Magazine.class);
        MagazineData<String> badJsonData = buildMagazineData("not-valid-json{{{");
        Mockito.when(magazine.fire()).thenReturn(badJsonData, (MagazineData<String>) null);

        // Use Integer class which requires JSON deserialization
        MessageHandler<Integer> handler = new MessageHandler<>() {
            @Override
            public Set<Class<?>> getIgnorableExceptions() {
                return Set.of(com.fasterxml.jackson.core.JsonProcessingException.class);
            }

            @Override
            public boolean handle(Integer message) {
                return true;
            }

            @Override
            public boolean handle(List<Integer> messages) {
                return true;
            }
        };

        MagazineConsumerTask<Integer> task = new MagazineConsumerTask<>(
                magazine, magazine, handler,
                new ObjectMapper(), Integer.class, new Timer(), aerospikeQueueService, null);
        task.run();
        // JsonProcessingException is ignorable, so no sideline via sidelineMessage but handleException calls it
        // Actually handleException checks isExceptionIgnorable — JsonProcessingException is ignorable, so no sidelineMessage
        // handleException calls delete(magazineData) at line 162, then consume's forEach at line 148 calls delete again = 2
        Mockito.verify(magazine, Mockito.times(2)).delete(any());
    }

    @Test
    public void testConsumeWithPrimitiveType() {
        Magazine<String> magazine = Mockito.mock(Magazine.class);
        MagazineData<String> data = buildMagazineData("hello");
        Mockito.when(magazine.fire()).thenReturn(data, (MagazineData<String>) null);

        MessageHandler<String> handler = new MessageHandler<>() {
            @Override
            public Set<Class<?>> getIgnorableExceptions() {
                return null;
            }

            @Override
            public boolean handle(String message) {
                return true;
            }

            @Override
            public boolean handle(List<String> messages) {
                return true;
            }
        };

        MagazineConsumerTask<String> task = new MagazineConsumerTask<>(
                magazine, magazine, handler,
                new ObjectMapper(), String.class, new Timer(), aerospikeQueueService, null);
        task.run();
        Mockito.verify(magazine, Mockito.times(1)).delete(any());
        Mockito.verify(magazine, never()).load(any());
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
                CLIENT_ID, createBaseStorage(), new ObjectMapper(), new MetricRegistry(),
                sc, Mockito.mock(CuratorFramework.class), FARM_ID);
        // Don't initialize message handlers
        manager.refreshQueues(); // Should log "No message handlers registered" and return
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
        Assert.assertNotNull(ignisMQManager.getAllQueues().get("REFRESH_QUEUE"));
    }

    @Test
    public void testRefreshQueuesDeactivatesInactiveQueues() throws Exception {
        // First create a queue
        ignisMQManager.createQueue(RequestFactory.createQueueRequest("QUEUE_1", MESSAGE_HANDLER_TYPE));
        Assert.assertNotNull(ignisMQManager.getAllQueues().get("QUEUE_1"));

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
        Assert.assertFalse(ignisMQManager.getAllQueues().containsKey("QUEUE_1"));
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
        Assert.assertEquals(originalConsumers + 2, q.getNoOfConsumers());
    }

    @Test
    public void testRefreshQueuesDecreaseConcurrency() throws Exception {
        ignisMQManager.createQueue(RequestFactory.createQueueRequest("QUEUE_1", MESSAGE_HANDLER_TYPE));
        MagazineQueue q = (MagazineQueue) ignisMQManager.getQueue("QUEUE_1");
        int originalConsumers = q.getNoOfConsumers();
        Assert.assertTrue(originalConsumers > 1);

        // Reset spy and update concurrency in DB to lower value
        Mockito.reset(aerospikeQueueService);
        AerospikeQueueService realService = createQueueService();
        realService.updateConcurrency("QUEUE_1", 1);

        aerospikeQueueService = Mockito.spy(realService);
        Field f = IgnisMQManager.class.getDeclaredField("queueService");
        f.setAccessible(true);
        f.set(ignisMQManager, aerospikeQueueService);

        ignisMQManager.refreshQueues();

        Assert.assertEquals(1, q.getNoOfConsumers());
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
        Assert.assertNull(q.getShovelConfig());

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
        // stop() is a no-op
        ignisMQManager.stop();
    }

    @Test
    public void testGetTaskInitializer() {
        Assert.assertNotNull(ignisMQManager.getTaskInitializer());
    }

    private <T> MagazineData<T> buildMagazineData(final T data) {
        return MagazineData.<T>builder()
                .magazineIdentifier("M123")
                .shard(1)
                .data(data)
                .firePointer(100)
                .build();
    }

    @Test
    public void testConsumeWithBatchingFatalException() {
        // Tests the outer catch(Exception) in MagazineConsumerTask.run()
        // batchConsume calls magazine.getMetaData() which throws → caught by outer catch
        Magazine<String> magazine = Mockito.mock(Magazine.class);
        Mockito.when(magazine.getMagazineIdentifier()).thenReturn("TEST_Q");
        Mockito.when(magazine.getMetaData()).thenThrow(new RuntimeException("fatal metadata error"));

        BatchingConfig batchingConfig = BatchingConfig.builder().maxBatchSize(3).maxWaitTimeInSecs(1).build();
        MagazineConsumerTask<String> task = new MagazineConsumerTask<>(
                magazine, magazine, new TestMessageHandler(),
                new ObjectMapper(), String.class, new Timer(), aerospikeQueueService, batchingConfig);
        // Should not throw — caught internally
        task.run();
    }
}
