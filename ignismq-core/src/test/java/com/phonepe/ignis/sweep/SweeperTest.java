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

package com.phonepe.ignis.sweep;

import com.phonepe.ignis.client.StorageClient;
import com.phonepe.ignis.entity.QueueEntity;
import com.phonepe.ignis.service.AerospikeQueueService;
import com.phonepe.ignis.storage.AerospikeStorage;
import com.phonepe.ignis.util.AerospikeTestBase;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;

import static org.mockito.Mockito.*;

public class SweeperTest extends AerospikeTestBase {

    private AerospikeQueueService queueService;
    private StorageClient storageClient;
    private AerospikeStorage storage;
    private Sweeper sweeper;

    @Before
    public void setUp() {
        queueService = Mockito.spy(createQueueService());
        storageClient = Mockito.mock(StorageClient.class);
        when(storageClient.getClient()).thenReturn(aerospikeClient);
        storage = (AerospikeStorage) createBaseStorage();
        sweeper = new Sweeper(queueService, CLIENT_ID, storage, storageClient, FARM_ID);
    }

    @Test
    public void testRunWhenInactive() {
        sweeper.run();
        verify(queueService, never()).getQueues(anyBoolean());
    }

    @Test
    public void testRunWhenActiveNoQueues() {
        sweeper.activate();
        doReturn(Collections.emptyMap()).when(queueService).getQueues(true);

        sweeper.run();

        verify(queueService, times(1)).getQueues(true);
    }

    @Test
    public void testActivateAndDeactivate() {
        sweeper.activate();
        doReturn(Collections.emptyMap()).when(queueService).getQueues(true);
        sweeper.run();
        verify(queueService, times(1)).getQueues(true);

        sweeper.deactivate();
        sweeper.run();
        verify(queueService, times(1)).getQueues(true);
    }

    @Test
    public void testRunWhenActiveWithException() {
        sweeper.activate();
        doThrow(new RuntimeException("error")).when(queueService).getQueues(true);

        sweeper.run();
    }

    @Test
    public void testRunWithRealQueues() {
        sweeper.activate();

        // Store a queue in aerospike
        QueueEntity entity = QueueEntity.builder()
                .active(true).shards(1).queueExpiry(600).messageExpiry(300)
                .concurrency(1).messageHandlerType("handler")
                .shovelConcurrency(0).shovelTimeIntervalInSecs(0)
                .createdAt(System.currentTimeMillis())
                .sweepDuration(20 * 60 * 1000L)
                .build();
        queueService.store("SWEEP_TEST_Q", entity, 1200);

        // Reset spy for clean verification
        Mockito.reset(queueService);
        // Re-spy the service
        queueService = Mockito.spy(createQueueService());
        sweeper = new Sweeper(queueService, CLIENT_ID, storage, storageClient, FARM_ID);
        sweeper.activate();

        sweeper.run();

        verify(queueService, times(1)).getQueues(true);
    }

    @Test
    public void testActivateIdempotent() {
        sweeper.activate();
        sweeper.activate(); // Should be fine, just sets true again
        doReturn(Collections.emptyMap()).when(queueService).getQueues(true);
        sweeper.run();
        verify(queueService, times(1)).getQueues(true);
    }

    @Test
    public void testDeactivateIdempotent() {
        sweeper.deactivate();
        sweeper.deactivate(); // Should be fine
        sweeper.run();
        verify(queueService, never()).getQueues(anyBoolean());
    }
}
