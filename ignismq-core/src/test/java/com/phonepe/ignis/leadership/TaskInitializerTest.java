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

package com.phonepe.ignis.leadership;

import com.phonepe.ignis.client.StorageClient;
import com.phonepe.ignis.service.AerospikeQueueService;
import com.phonepe.ignis.storage.AerospikeStorage;
import com.phonepe.ignis.util.AerospikeTestBase;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.when;

public class TaskInitializerTest extends AerospikeTestBase {

    private CuratorFramework curatorFramework;
    private AerospikeQueueService queueService;
    private AerospikeStorage storage;
    private StorageClient storageClient;

    @Before
    public void setUp() {
        curatorFramework = Mockito.mock(CuratorFramework.class, RETURNS_DEEP_STUBS);
        queueService = Mockito.spy(createQueueService());
        storage = (AerospikeStorage) createBaseStorage();
        storageClient = Mockito.mock(StorageClient.class);
    }

    @Test
    public void testConstants() {
        assertEquals(15 * 60 * 1000, TaskInitializer.DELAY_FOR_SWEEPER_TASK);
    }

    @Test
    public void testConstructor() {
        TaskInitializer taskInitializer = new TaskInitializer(curatorFramework, queueService,
                CLIENT_ID, storage, storageClient, FARM_ID);
        assertNotNull(taskInitializer);
    }

    @Test
    public void testStartAndStop() throws Exception {
        TaskInitializer taskInitializer = new TaskInitializer(curatorFramework, queueService,
                CLIENT_ID, storage, storageClient, FARM_ID);

        // Deep stubs on curatorFramework handle the full create chain automatically
        when(curatorFramework.create().creatingParentContainersIfNeeded()
                .withMode(any(CreateMode.class)).forPath(anyString())).thenReturn("");

        taskInitializer.start();

        // Calling start again should be a no-op ("Already initialised")
        taskInitializer.start();

        // Stop should call leaderElector.stop()
        taskInitializer.stop();
    }

    @Test
    public void testStartSetsLeaderElector() throws Exception {
        TaskInitializer taskInitializer = new TaskInitializer(curatorFramework, queueService,
                CLIENT_ID, storage, storageClient, FARM_ID);

        // Mock create chain
        when(curatorFramework.create().creatingParentContainersIfNeeded()
                .withMode(any(CreateMode.class)).forPath(anyString())).thenReturn("");

        // Verify leaderElector is null before start
        Field leField = TaskInitializer.class.getDeclaredField("leaderElector");
        leField.setAccessible(true);
        assertNull(leField.get(taskInitializer));

        taskInitializer.start();

        assertNotNull(leField.get(taskInitializer));
    }
}
