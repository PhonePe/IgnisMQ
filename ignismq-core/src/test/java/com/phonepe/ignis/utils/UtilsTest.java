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

package com.phonepe.ignis.utils;

import com.phonepe.ignis.client.StorageClient;
import com.phonepe.ignis.entity.QueueEntity;
import com.phonepe.ignis.exception.IgnisMQException;
import com.phonepe.ignis.service.AerospikeQueueService;
import com.phonepe.ignis.storage.AerospikeStorage;
import com.phonepe.ignis.util.AerospikeTestBase;
import com.phonepe.magazine.entity.MetaData;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

public class UtilsTest extends AerospikeTestBase {

    private AerospikeQueueService queueService;
    private StorageClient storageClient;
    private AerospikeStorage storage;

    @Before
    public void setUp() {
        queueService = Mockito.spy(createQueueService());
        storageClient = Mockito.mock(StorageClient.class);
        when(storageClient.getClient()).thenReturn(aerospikeClient);
        storage = (AerospikeStorage) createBaseStorage();
    }

    @Test
    public void testGetSidelineQueueName() {
        assertEquals("QUEUE_1_SIDELINE", Utils.getSidelineQueueName("QUEUE_1"));
    }

    @Test
    public void testCreateMagazineAerospikeKey() {
        String key = Utils.createMagazineAerospikeKey(5, 2, "QUEUE_1");
        assertEquals("QUEUE_1_SHARD_2_5", key);
    }

    @Test
    public void testCreateMetadataKey() {
        assertEquals("QUEUE_1_SHARD_3_METADATA", Utils.createMetadataKey("QUEUE_1", 3, "METADATA"));
        assertEquals("QUEUE_1_POINTERS", Utils.createMetadataKey("QUEUE_1", null, "POINTERS"));
    }

    @Test
    public void testGetMagazineSet() {
        String set = Utils.getMagazineSet("CLIENT_ID", "data_set");
        assertEquals("CLIENT_ID_data_set", set);
    }

    @Test
    public void testGetShardId() {
        String shardId = Utils.getShardId(5);
        assertTrue(shardId.contains("5"));
    }

    @Test
    public void testGetMagazineCountEmpty() {
        Collection<MetaData> empty = Collections.emptyList();
        assertEquals(0L, Utils.getMagazineCount(empty, MetaData::getLoadPointer));
    }

    @Test
    public void testGetMagazineCountMultiple() {
        List<MetaData> metaDataList = List.of(
                MetaData.builder().loadPointer(10).firePointer(5).build(),
                MetaData.builder().loadPointer(20).firePointer(15).build()
        );
        assertEquals(30L, Utils.getMagazineCount(metaDataList, MetaData::getLoadPointer));
        assertEquals(20L, Utils.getMagazineCount(metaDataList, MetaData::getFirePointer));
    }

    @Test
    public void testWaitForRequestsCompletionSuccess() {
        List<Future<Boolean>> futures = new ArrayList<>();
        futures.add(CompletableFuture.completedFuture(true));
        futures.add(CompletableFuture.completedFuture(true));

        Utils.waitForRequestsCompletion(futures);
        assertTrue(futures.isEmpty());
    }

    @Test(expected = IgnisMQException.class)
    public void testWaitForRequestsCompletionFailure() {
        List<Future<Boolean>> futures = new ArrayList<>();
        CompletableFuture<Boolean> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("test"));
        futures.add(failedFuture);

        Utils.waitForRequestsCompletion(futures);
    }

    @Test
    public void testSweepQueueWithRealAerospike() {
        // Store a queue entity in aerospike
        QueueEntity entity = QueueEntity.builder()
                .active(true).shards(1).queueExpiry(600).messageExpiry(300)
                .concurrency(1).messageHandlerType("handler")
                .shovelConcurrency(0).shovelTimeIntervalInSecs(0)
                .createdAt(System.currentTimeMillis())
                .sweepDuration(20 * 60 * 1000L)
                .build();
        queueService.store("UTIL_SWEEP_Q", entity, 1200);

        // Call sweepQueue - should not throw
        Utils.sweepQueue(queueService, CLIENT_ID, storageClient, storage,
                "UTIL_SWEEP_Q", entity, FARM_ID, new SimpleMeterRegistry());
    }

    @Test
    public void testSweepQueueHandlesExceptionGracefully() {
        // Use a queue entity that will cause sweepQueue to fail gracefully
        QueueEntity entity = QueueEntity.builder()
                .active(true).shards(1).queueExpiry(600).messageExpiry(300)
                .concurrency(1).messageHandlerType("handler")
                .shovelConcurrency(0).shovelTimeIntervalInSecs(0)
                .createdAt(System.currentTimeMillis())
                .sweepDuration(0L)
                .build();

        // Use a broken storage client that will cause an exception
        StorageClient brokenClient = Mockito.mock(StorageClient.class);
        when(brokenClient.getClient()).thenThrow(new RuntimeException("broken"));

        // Should not throw - exception caught internally
        Utils.sweepQueue(queueService, CLIENT_ID, brokenClient, storage,
                "NON_EXISTENT", entity, FARM_ID, new SimpleMeterRegistry());
    }

    @Test
    public void testExecutorServiceNotNull() {
        assertNotNull(Utils.executorService);
    }

    @Test(expected = IgnisMQException.class)
    public void testWaitForRequestsCompletionWithInterruptedException() {
        List<Future<Boolean>> futures = new ArrayList<>();
        Future<Boolean> future = new Future<>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                return false;
            }

            @Override
            public boolean isCancelled() {
                return false;
            }

            @Override
            public boolean isDone() {
                return false;
            }

            @Override
            public Boolean get() throws InterruptedException {
                throw new InterruptedException("interrupted");
            }

            @Override
            public Boolean get(long timeout, TimeUnit unit) throws InterruptedException {
                throw new InterruptedException("interrupted");
            }
        };
        futures.add(future);
        Utils.waitForRequestsCompletion(futures);
    }
}
