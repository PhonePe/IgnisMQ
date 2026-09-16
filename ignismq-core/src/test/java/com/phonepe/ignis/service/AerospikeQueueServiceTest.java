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

package com.phonepe.ignis.service;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.phonepe.ignis.config.BatchingConfig;
import com.phonepe.ignis.entity.QueueEntity;
import com.phonepe.ignis.util.AerospikeTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

public class AerospikeQueueServiceTest extends AerospikeTestBase {

    private AerospikeQueueService service;

    @BeforeEach
    public void setUp() {
        service = createQueueService();
    }

    @Test
    public void testStoreAndExists() {
        QueueEntity entity = buildEntity(true);
        service.store("QUEUE_1", entity, 1200);
        assertTrue(service.exists("QUEUE_1"));
        assertFalse(service.exists("QUEUE_2"));
    }

    @Test
    public void testStoreAndGet() {
        QueueEntity entity = buildEntity(true);
        service.store("QUEUE_1", entity, 1200);
        Optional<QueueEntity> result = service.get("QUEUE_1");

        assertTrue(result.isPresent());
        assertEquals(32, result.get().getShards());
        assertEquals(600, result.get().getQueueExpiry());
        assertEquals(300, result.get().getMessageExpiry());
        assertEquals(4, result.get().getConcurrency());
        assertEquals("handler", result.get().getMessageHandlerType());
        assertTrue(result.get().isActive());
    }

    @Test
    public void testGetNonExistent() {
        Optional<QueueEntity> result = service.get("NONEXISTENT");
        assertFalse(result.isPresent());
    }

    @Test
    public void testUpdateState() {
        QueueEntity entity = buildEntity(true);
        service.store("QUEUE_1", entity, 1200);
        service.updateState("QUEUE_1", false);

        Optional<QueueEntity> result = service.get("QUEUE_1");
        assertTrue(result.isPresent());
        assertFalse(result.get().isActive());
    }

    @Test
    public void testUpdateConcurrency() {
        QueueEntity entity = buildEntity(true);
        service.store("QUEUE_1", entity, 1200);
        service.updateConcurrency("QUEUE_1", 10);

        Optional<QueueEntity> result = service.get("QUEUE_1");
        assertTrue(result.isPresent());
        assertEquals(10, result.get().getConcurrency());
    }

    @Test
    public void testUpdateShovelConfig() {
        QueueEntity entity = buildEntity(true);
        service.store("QUEUE_1", entity, 1200);
        service.updateShovelConfig("QUEUE_1", 8, 300);

        Optional<QueueEntity> result = service.get("QUEUE_1");
        assertTrue(result.isPresent());
        assertEquals(8, result.get().getShovelConcurrency());
        assertEquals(300, result.get().getShovelTimeIntervalInSecs());
    }

    @Test
    public void testStoreWithBatchingConfig() {
        BatchingConfig batchingConfig = BatchingConfig.builder()
                .maxBatchSize(50)
                .maxWaitTimeInSecs(10)
                .build();

        QueueEntity entity = QueueEntity.builder()
                .active(true).shards(32).queueExpiry(600).messageExpiry(300)
                .concurrency(4).messageHandlerType("handler")
                .shovelConcurrency(2).shovelTimeIntervalInSecs(600)
                .createdAt(1000L).sweepDuration(20 * 60 * 1000L)
                .batchingConfig(batchingConfig)
                .build();

        service.store("QUEUE_1", entity, 1200);
        Optional<QueueEntity> result = service.get("QUEUE_1");
        assertTrue(result.isPresent());
        assertNotNull(result.get().getBatchingConfig());
        assertEquals(50, result.get().getBatchingConfig().getMaxBatchSize());
        assertEquals(10, result.get().getBatchingConfig().getMaxWaitTimeInSecs());
    }

    @Test
    public void testGetQueuesActive() {
        service.store("QUEUE_1", buildEntity(true), 1200);
        service.store("QUEUE_2", buildEntity(false), 1200);

        var activeQueues = service.getQueues(true);
        assertTrue(activeQueues.containsKey("QUEUE_1"));
        assertFalse(activeQueues.containsKey("QUEUE_2"));
    }

    @Test
    public void testGetQueuesInactive() {
        service.store("QUEUE_1", buildEntity(true), 1200);
        service.store("QUEUE_2", buildEntity(false), 1200);

        var inactiveQueues = service.getQueues(false);
        assertFalse(inactiveQueues.containsKey("QUEUE_1"));
        assertTrue(inactiveQueues.containsKey("QUEUE_2"));
    }

    @Test
    public void testStoreWithNullBatchingConfig() {
        QueueEntity entity = QueueEntity.builder()
                .active(true).shards(32).queueExpiry(600).messageExpiry(300)
                .concurrency(4).messageHandlerType("handler")
                .shovelConcurrency(2).shovelTimeIntervalInSecs(600)
                .createdAt(1000L).sweepDuration(20 * 60 * 1000L)
                .batchingConfig(null)
                .build();

        service.store("QUEUE_NB", entity, 1200);
        Optional<QueueEntity> result = service.get("QUEUE_NB");
        assertTrue(result.isPresent());
        assertNull(result.get().getBatchingConfig());
    }

    @Test
    public void testHandlerTimeoutRoundTripsThroughTheBin() {
        QueueEntity entity = QueueEntity.builder()
                .active(true).shards(32).queueExpiry(600).messageExpiry(300)
                .concurrency(4).messageHandlerType("handler")
                .shovelConcurrency(2).shovelTimeIntervalInSecs(600)
                .createdAt(1000L).sweepDuration(20 * 60 * 1000L)
                .handlerTimeout(7 * 60 * 1000L)
                .build();

        service.store("TIMEOUT_Q", entity, 1200);

        assertEquals(7 * 60 * 1000L, service.get("TIMEOUT_Q").orElseThrow().getHandlerTimeout());
    }

    /**
     * A queue written before the bin existed. The absent bin is the upgrade path for every queue
     * that exists today, and it must read back as zero rather than failing, because zero is the
     * sentinel the caller's fallback keys off.
     */
    @Test
    public void testAQueueWrittenBeforeTheBinExistedReadsBackZero() {
        service.store("LEGACY_Q", buildEntity(true), 1200);
        removeHandlerTimeoutBin("LEGACY_Q");

        QueueEntity stored = service.get("LEGACY_Q").orElseThrow();

        assertEquals(0L, stored.getHandlerTimeout(), "an absent bin must read back as the sentinel");
        assertEquals(20 * 60 * 1000L, stored.getSweepDuration(), "the rest of the record still reads");
    }

    /**
     * Deletes the bin outright rather than writing zero, so the record is shaped exactly as one
     * written by a version that had never heard of it.
     */
    private void removeHandlerTimeoutBin(final String queue) {
        aerospikeClient.put(null,
                new Key(AEROSPIKE_NAMESPACE, String.format("%s_%s_ignis_queues", FARM_ID, CLIENT_ID), queue),
                Bin.asNull("handlerTimeout"));
    }

    private QueueEntity buildEntity(boolean active) {
        return QueueEntity.builder()
                .active(active).shards(32).queueExpiry(600).messageExpiry(300)
                .concurrency(4).messageHandlerType("handler")
                .shovelConcurrency(2).shovelTimeIntervalInSecs(600)
                .createdAt(1000L).sweepDuration(20 * 60 * 1000L)
                .build();
    }

    @Test
    public void testStoreWithBatchingConfigAndRetrieve() {
        QueueEntity entity = QueueEntity.builder()
                .active(true).shards(1).queueExpiry(600).messageExpiry(300)
                .concurrency(4).messageHandlerType("handler")
                .shovelConcurrency(2).shovelTimeIntervalInSecs(600)
                .createdAt(1000L).sweepDuration(20 * 60 * 1000L)
                .batchingConfig(BatchingConfig.builder().maxBatchSize(5).maxWaitTimeInSecs(10).build())
                .build();
        service.store("BATCH_Q", entity, 1200);
        Optional<QueueEntity> result = service.get("BATCH_Q");
        assertTrue(result.isPresent());
        assertNotNull(result.get().getBatchingConfig());
        assertEquals(5, result.get().getBatchingConfig().getMaxBatchSize());
        assertEquals(10, result.get().getBatchingConfig().getMaxWaitTimeInSecs());
    }

    /**
     * Progress is stored as the next slot to examine, not as a delta. A pass that re-runs over the
     * same range therefore converges rather than compounding - which the previous increment-based
     * marker did not, and which is what let a repeated sweep run off the end of a shard.
     */
    @Test
    public void testUpdateSweepProgressStoresAbsolutePointers() {
        service.store("PROGRESS_Q", buildEntity(true), 1200);

        service.updateSweepProgress("PROGRESS_Q", false, Map.of("SHARD_0", 40L, "SHARD_1", 7L), 3L);
        service.updateSweepProgress("PROGRESS_Q", false, Map.of("SHARD_0", 40L), 3L);

        final QueueEntity stored = service.get("PROGRESS_Q").orElseThrow();
        assertEquals(Long.valueOf(40L), stored.getSweepPointers().get("SHARD_0"));
        assertEquals(Long.valueOf(7L), stored.getSweepPointers().get("SHARD_1"));
        assertEquals(3L, stored.getSweptCounter());
    }

    /**
     * The main and sideline sweeps track their own magazines and must never overwrite each other's
     * position; they share a queue record but not a pointer.
     */
    @Test
    public void testUpdateSweepProgressKeepsSidelineProgressSeparate() {
        service.store("PROGRESS_SIDELINE_Q", buildEntity(true), 1200);

        service.updateSweepProgress("PROGRESS_SIDELINE_Q", false, Map.of("SHARD_0", 11L), 1L);
        service.updateSweepProgress("PROGRESS_SIDELINE_Q", true, Map.of("SHARD_0", 99L), 5L);

        final QueueEntity stored = service.get("PROGRESS_SIDELINE_Q").orElseThrow();
        assertEquals(Long.valueOf(11L), stored.getSweepPointers().get("SHARD_0"));
        assertEquals(1L, stored.getSweptCounter());
        assertEquals(Long.valueOf(99L), stored.getSidelineSweepPointers().get("SHARD_0"));
        assertEquals(5L, stored.getSidelineSweptCounter());
    }

    /**
     * A sweep round that turned up no shards still calls in. Writing an empty operate would be a
     * pointless round trip on the queue record.
     */
    @Test
    public void testUpdateSweepProgressWithNoShardsIsANoOp() {
        service.store("PROGRESS_EMPTY_Q", buildEntity(true), 1200);

        service.updateSweepProgress("PROGRESS_EMPTY_Q", false, Map.of(), 0L);

        assertNull(service.get("PROGRESS_EMPTY_Q").orElseThrow().getSweepPointers());
    }

    @Test
    public void testExistsReturnsFalse() {
        assertFalse(service.exists("NON_EXISTENT_Q"));
    }

    @Test
    public void testExistsReturnsTrue() {
        service.store("EXISTS_Q", buildEntity(true), 1200);
        assertTrue(service.exists("EXISTS_Q"));
    }
}
