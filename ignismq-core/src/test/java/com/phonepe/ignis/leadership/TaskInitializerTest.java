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
import com.phonepe.ignis.metric.IgnisMetrics;
import com.phonepe.ignis.scheduler.IgnisSchedulerCommands;
import com.phonepe.ignis.service.AerospikeQueueService;
import com.phonepe.ignis.storage.AerospikeStorage;
import com.phonepe.ignis.util.AerospikeTestBase;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.when;

public class TaskInitializerTest extends AerospikeTestBase {

    private CuratorFramework curatorFramework;
    private AerospikeQueueService queueService;
    private AerospikeStorage storage;
    private StorageClient storageClient;

    @BeforeEach
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
                CLIENT_ID, storage, storageClient, FARM_ID,
                new IgnisMetrics(new SimpleMeterRegistry()), null, null);
        assertNotNull(taskInitializer);
    }

    @Test
    public void testStartAndStop() throws Exception {
        TaskInitializer taskInitializer = new TaskInitializer(curatorFramework, queueService,
                CLIENT_ID, storage, storageClient, FARM_ID,
                new IgnisMetrics(new SimpleMeterRegistry()), null, null);

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
                CLIENT_ID, storage, storageClient, FARM_ID,
                new IgnisMetrics(new SimpleMeterRegistry()), null, null);

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

    /**
     * A framework lifecycle calls stop even when start never ran or failed part way through.
     * This used to NPE on the null leader elector.
     */
    @Test
    public void testStopWithoutStartIsSafe() {
        TaskInitializer taskInitializer = new TaskInitializer(curatorFramework, queueService,
                CLIENT_ID, storage, storageClient, FARM_ID,
                new IgnisMetrics(new SimpleMeterRegistry()), null, null);

        taskInitializer.stop();
    }

    @Test
    public void testStopIsIdempotentAndCancelsTheSweeperTask() throws Exception {
        TaskInitializer taskInitializer = new TaskInitializer(curatorFramework, queueService,
                CLIENT_ID, storage, storageClient, FARM_ID,
                new IgnisMetrics(new SimpleMeterRegistry()), null, null);
        when(curatorFramework.create().creatingParentContainersIfNeeded()
                .withMode(any(CreateMode.class)).forPath(anyString())).thenReturn("");
        taskInitializer.start();

        Field taskField = TaskInitializer.class.getDeclaredField("sweeperTask");
        taskField.setAccessible(true);
        assertNotNull(taskField.get(taskInitializer));

        taskInitializer.stop();
        assertNull(taskField.get(taskInitializer), "sweeper task must be released on stop");

        // Second stop must not throw.
        taskInitializer.stop();
    }

    /**
     * A task initializer given no scheduler builds its own, and must therefore shut it down. One
     * that is handed the manager's must not, or stopping the sweeper would stop every consumer.
     */
    @Test
    public void testASuppliedSchedulerOutlivesTheTaskInitializer() {
        final IgnisSchedulerCommands shared = new IgnisSchedulerCommands();
        final TaskInitializer taskInitializer = new TaskInitializer(curatorFramework, queueService,
                CLIENT_ID, storage, storageClient, FARM_ID,
                new IgnisMetrics(new SimpleMeterRegistry()), shared, null);

        taskInitializer.stop();

        assertFalse(shared.isStopped(), "a scheduler owned by the caller must survive");
        shared.stop();
        assertTrue(shared.isStopped());
    }

    @Test
    public void testMetricsAreRequired() {
        assertThrows(NullPointerException.class,
                () -> new TaskInitializer(curatorFramework, queueService, CLIENT_ID, storage, storageClient,
                        FARM_ID, null, null, null));
    }
}
