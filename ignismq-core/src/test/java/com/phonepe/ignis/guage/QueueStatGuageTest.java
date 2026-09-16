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

package com.phonepe.ignis.guage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.phonepe.ignis.IgnisMQManager;
import com.phonepe.ignis.client.StorageClient;
import com.phonepe.ignis.common.MessageHandler;
import com.phonepe.ignis.entity.QueueEntity;
import com.phonepe.ignis.service.AerospikeQueueService;
import com.phonepe.ignis.util.AerospikeTestBase;
import com.phonepe.ignis.util.RequestFactory;
import com.phonepe.ignis.util.TestMessageHandler;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.curator.framework.CuratorFramework;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

public class QueueStatGuageTest extends AerospikeTestBase {

    private AerospikeQueueService queueService;
    private IgnisMQManager ignisMQManager;
    private SimpleMeterRegistry meterRegistry;

    @BeforeEach
    public void setUp() throws Exception {
        StorageClient storageClient = Mockito.mock(StorageClient.class);
        Mockito.when(storageClient.getClient()).thenReturn(aerospikeClient);

        meterRegistry = new SimpleMeterRegistry();
        ignisMQManager = new IgnisMQManager(
                CLIENT_ID, createBaseStorage(), new ObjectMapper(), meterRegistry,
                storageClient, Mockito.mock(CuratorFramework.class), FARM_ID, null);
        queueService = Mockito.spy(createQueueService());

        Field f = IgnisMQManager.class.getDeclaredField("queueService");
        f.setAccessible(true);
        f.set(ignisMQManager, queueService);
        doReturn(Collections.emptyMap()).when(queueService).getQueues(Mockito.anyBoolean());
    }

    @AfterEach
    public void stopManager() {
        if (ignisMQManager != null) {
            ignisMQManager.stop();
            ignisMQManager = null;
        }
    }

    @Test
    public void testLoadValueEmpty() {
        Map<String, Map.Entry<Class, MessageHandler>> messageHandlerMap = new HashMap<>();
        messageHandlerMap.put("handler", new AbstractMap.SimpleEntry<>(String.class, new TestMessageHandler()));
        ignisMQManager.initialiseMessageHandlers(messageHandlerMap);

        QueueStatGuage guage = new QueueStatGuage(queueService, ignisMQManager::getAllQueues);
        doReturn(Collections.emptyMap()).when(queueService).getQueues(true);

        var result = guage.get();
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testLoadValueWithQueues() throws Exception {
        Map<String, Map.Entry<Class, MessageHandler>> messageHandlerMap = new HashMap<>();
        messageHandlerMap.put("handler", new AbstractMap.SimpleEntry<>(String.class, new TestMessageHandler()));
        ignisMQManager.initialiseMessageHandlers(messageHandlerMap);

        ignisMQManager.createQueue(RequestFactory.createQueueRequest("QUEUE_1", "handler"));

        Map<String, QueueEntity> dbQueues = new HashMap<>();
        dbQueues.put("QUEUE_1", QueueEntity.builder().active(true).build());
        doReturn(dbQueues).when(queueService).getQueues(true);

        QueueStatGuage guage = new QueueStatGuage(queueService, ignisMQManager::getAllQueues);
        var result = guage.get();
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("QUEUE_1", result.get(0).getName());
    }

    @Test
    public void testLoadValueException() {
        QueueStatGuage guage = new QueueStatGuage(queueService, ignisMQManager::getAllQueues);
        doThrow(new RuntimeException("error")).when(queueService).getQueues(true);

        var result = guage.get();
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testLoadValueQueueInDbNotInCache() {
        Map<String, Map.Entry<Class, MessageHandler>> messageHandlerMap = new HashMap<>();
        messageHandlerMap.put("handler", new AbstractMap.SimpleEntry<>(String.class, new TestMessageHandler()));
        ignisMQManager.initialiseMessageHandlers(messageHandlerMap);

        QueueStatGuage guage = new QueueStatGuage(queueService, ignisMQManager::getAllQueues);
        Map<String, QueueEntity> dbQueues = new HashMap<>();
        dbQueues.put("QUEUE_1", QueueEntity.builder().active(true).build());
        doReturn(dbQueues).when(queueService).getQueues(true);

        var result = guage.get();
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testRefreshIssuesTwoMetadataBatchReadsPerQueue() throws Exception {
        Map<String, Map.Entry<Class, MessageHandler>> messageHandlerMap = new HashMap<>();
        messageHandlerMap.put("handler", new AbstractMap.SimpleEntry<>(String.class, new TestMessageHandler()));
        ignisMQManager.initialiseMessageHandlers(messageHandlerMap);

        ignisMQManager.createQueue(RequestFactory.createQueueRequest("QUEUE_READS", "handler"));
        Map<String, QueueEntity> dbQueues = new HashMap<>();
        dbQueues.put("QUEUE_READS", QueueEntity.builder().active(true).build());
        doReturn(dbQueues).when(queueService).getQueues(true);

        QueueStatGuage guage = new QueueStatGuage(queueService, ignisMQManager::getAllQueues);
        guage.get(); // warm active-shard discovery so it does not pollute the measured refresh

        final double before = metadataBatchReads();
        assertEquals(1, guage.get().size());
        assertEquals(2.0, metadataBatchReads() - before, 0.0);
    }

    @Test
    public void testUnconsumedIsDerivedFromTheSameSnapshot() throws Exception {
        Map<String, Map.Entry<Class, MessageHandler>> messageHandlerMap = new HashMap<>();
        messageHandlerMap.put("handler", new AbstractMap.SimpleEntry<>(String.class, new TestMessageHandler()));
        ignisMQManager.initialiseMessageHandlers(messageHandlerMap);

        ignisMQManager.createQueue(RequestFactory.createQueueRequest("QUEUE_UNCONSUMED", "handler"));
        ignisMQManager.<String>getQueue("QUEUE_UNCONSUMED").publish("one");
        ignisMQManager.<String>getQueue("QUEUE_UNCONSUMED").publish("two");

        Map<String, QueueEntity> dbQueues = new HashMap<>();
        dbQueues.put("QUEUE_UNCONSUMED", QueueEntity.builder().active(true).build());
        doReturn(dbQueues).when(queueService).getQueues(true);

        var stat = new QueueStatGuage(queueService, ignisMQManager::getAllQueues).get().get(0);
        assertEquals(2L, stat.getPublished());
        assertEquals(2L, stat.getUnConsumed());
        assertEquals(stat.getPublished() - stat.getConsumed(), stat.getUnConsumed());
    }

    private double metadataBatchReads() {
        return meterRegistry.find("magazine.aerospike.calls")
                .tag("operation", "batch_read_metadata")
                .counters().stream().mapToDouble(io.micrometer.core.instrument.Counter::count).sum();
    }
}
