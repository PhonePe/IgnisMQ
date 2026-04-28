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

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.phonepe.ignis.IgnisMQManager;
import com.phonepe.ignis.client.StorageClient;
import com.phonepe.ignis.common.MessageHandler;
import com.phonepe.ignis.entity.QueueEntity;
import com.phonepe.ignis.service.AerospikeQueueService;
import com.phonepe.ignis.util.AerospikeTestBase;
import com.phonepe.ignis.util.RequestFactory;
import com.phonepe.ignis.util.TestMessageHandler;
import org.apache.curator.framework.CuratorFramework;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class QueueStatGuageTest extends AerospikeTestBase {

    private AerospikeQueueService queueService;
    private IgnisMQManager ignisMQManager;

    @Before
    public void setUp() throws Exception {
        StorageClient storageClient = Mockito.mock(StorageClient.class);
        Mockito.when(storageClient.getClient()).thenReturn(aerospikeClient);

        ignisMQManager = new IgnisMQManager(
                CLIENT_ID, createBaseStorage(), new ObjectMapper(), new MetricRegistry(),
                storageClient, Mockito.mock(CuratorFramework.class), FARM_ID);
        queueService = Mockito.spy(createQueueService());

        Field f = IgnisMQManager.class.getDeclaredField("queueService");
        f.setAccessible(true);
        f.set(ignisMQManager, queueService);
        doReturn(Collections.emptyMap()).when(queueService).getQueues(Mockito.anyBoolean());
    }

    @Test
    public void testLoadValueEmpty() {
        Map<String, Map.Entry<Class, MessageHandler>> messageHandlerMap = new HashMap<>();
        messageHandlerMap.put("handler", new AbstractMap.SimpleEntry<>(String.class, new TestMessageHandler()));
        ignisMQManager.initialiseMessageHandlers(messageHandlerMap);

        QueueStatGuage guage = new QueueStatGuage(1, TimeUnit.SECONDS, queueService, ignisMQManager);
        doReturn(Collections.emptyMap()).when(queueService).getQueues(true);

        var result = guage.getValue();
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

        QueueStatGuage guage = new QueueStatGuage(1, TimeUnit.SECONDS, queueService, ignisMQManager);
        var result = guage.getValue();
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("QUEUE_1", result.get(0).getName());
    }

    @Test
    public void testLoadValueException() {
        QueueStatGuage guage = new QueueStatGuage(1, TimeUnit.SECONDS, queueService, ignisMQManager);
        doThrow(new RuntimeException("error")).when(queueService).getQueues(true);

        var result = guage.getValue();
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testLoadValueQueueInDbNotInCache() {
        Map<String, Map.Entry<Class, MessageHandler>> messageHandlerMap = new HashMap<>();
        messageHandlerMap.put("handler", new AbstractMap.SimpleEntry<>(String.class, new TestMessageHandler()));
        ignisMQManager.initialiseMessageHandlers(messageHandlerMap);

        QueueStatGuage guage = new QueueStatGuage(1, TimeUnit.SECONDS, queueService, ignisMQManager);
        Map<String, QueueEntity> dbQueues = new HashMap<>();
        dbQueues.put("QUEUE_1", QueueEntity.builder().active(true).build());
        doReturn(dbQueues).when(queueService).getQueues(true);

        var result = guage.getValue();
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }
}
