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
import com.phonepe.ignis.metric.IgnisMetrics;
import com.phonepe.ignis.service.AerospikeQueueService;
import com.phonepe.ignis.storage.AerospikeStorage;
import com.phonepe.ignis.util.AerospikeTestBase;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.mockito.Mockito.*;

class SweeperTest extends AerospikeTestBase {

    private AerospikeQueueService queueService;
    private StorageClient storageClient;
    private AerospikeStorage storage;
    private Sweeper sweeper;
    private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    @BeforeEach
    void setUp() {
        queueService = spy(createQueueService());
        storageClient = mock(StorageClient.class);
        when(storageClient.getClient()).thenReturn(aerospikeClient);
        storage = (AerospikeStorage) createBaseStorage();
        sweeper = new Sweeper(queueService, CLIENT_ID, storage, storageClient, FARM_ID,
                new IgnisMetrics(meterRegistry), null);
    }

    @Test
    void testRunWhenInactive() {
        sweeper.run();
        verify(queueService, never()).getQueues(anyBoolean());
    }

    @Test
    void testRunWhenActiveNoQueues() {
        sweeper.activate();
        doReturn(Collections.emptyMap()).when(queueService).getQueues(true);

        sweeper.run();

        verify(queueService, times(1)).getQueues(true);
    }

    @Test
    void testActivateAndDeactivate() {
        sweeper.activate();
        doReturn(Collections.emptyMap()).when(queueService).getQueues(true);
        sweeper.run();
        verify(queueService, times(1)).getQueues(true);

        sweeper.deactivate();
        sweeper.run();
        verify(queueService, times(1)).getQueues(true);
    }

    @Test
    void testRunWhenActiveWithException() {
        sweeper.activate();
        doThrow(new RuntimeException("error")).when(queueService).getQueues(true);

        sweeper.run();
    }

    @Test
    void testRunWithRealQueues() {
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
        reset(queueService);
        // Re-spy the service
        queueService = spy(createQueueService());
        sweeper = new Sweeper(queueService, CLIENT_ID, storage, storageClient, FARM_ID,
                new IgnisMetrics(meterRegistry), null);
        sweeper.activate();

        sweeper.run();

        verify(queueService, times(1)).getQueues(true);
    }

    @Test
    void testActivateIdempotent() {
        sweeper.activate();
        sweeper.activate(); // Should be fine, just sets true again
        doReturn(Collections.emptyMap()).when(queueService).getQueues(true);
        sweeper.run();
        verify(queueService, times(1)).getQueues(true);
    }

    @Test
    void testDeactivateIdempotent() {
        sweeper.deactivate();
        sweeper.deactivate(); // Should be fine
        sweeper.run();
        verify(queueService, never()).getQueues(anyBoolean());
    }
}
