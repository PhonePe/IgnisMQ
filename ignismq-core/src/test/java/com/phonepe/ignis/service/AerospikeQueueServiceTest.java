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

import com.phonepe.ignis.config.BatchingConfig;
import com.phonepe.ignis.entity.QueueEntity;
import com.phonepe.ignis.exception.IgnisMQException;
import com.phonepe.ignis.util.AerospikeTestBase;
import com.phonepe.ignis.utils.Constants;
import com.phonepe.ignis.utils.Utils;
import com.phonepe.magazine.Magazine;
import com.phonepe.magazine.common.MagazineData;
import com.phonepe.magazine.impl.aerospike.AerospikeStorage;
import com.phonepe.magazine.impl.aerospike.AerospikeStorageConfig;
import com.phonepe.magazine.scope.MagazineScope;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.*;

public class AerospikeQueueServiceTest extends AerospikeTestBase {

    private AerospikeQueueService service;

    @Before
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
    public void testAddFireTimestamp() throws Exception {
        // Create a magazine and load a message to create the data record
        AerospikeStorage<String> magazineStorage = AerospikeStorage.<String>builder()
                .clazz(String.class)
                .storageConfig(AerospikeStorageConfig.builder()
                        .dataSetName(Utils.getMagazineSet(CLIENT_ID, Constants.AEROSPIKE_DATA_SET))
                        .metaSetName(Utils.getMagazineSet(CLIENT_ID, Constants.AEROSPIKE_META_SET))
                        .namespace(AEROSPIKE_NAMESPACE)
                        .shards(1)
                        .recordTtl(300)
                        .metaDataTtl(600)
                        .build())
                .aerospikeClient(aerospikeClient)
                .enableDeDupe(false)
                .farmId(FARM_ID)
                .scope(MagazineScope.LOCAL)
                .clientId(CLIENT_ID)
                .build();

        Magazine<String> magazine = Magazine.<String>builder()
                .baseMagazineStorage(magazineStorage)
                .magazineIdentifier("TEST_Q")
                .build();

        // Load a message to create data in aerospike
        assertTrue(magazine.load("test-message"));

        // Fire to get a MagazineData
        MagazineData<String> fired = magazine.fire();
        assertNotNull(fired);

        // Now add fire timestamp - should not throw
        service.addFireTimestamp(fired, System.currentTimeMillis());
    }

    @Test
    public void testSweepWithNoData() throws Exception {
        // Store queue entity
        QueueEntity entity = QueueEntity.builder()
                .active(true).shards(1).queueExpiry(600).messageExpiry(300)
                .concurrency(4).messageHandlerType("handler")
                .shovelConcurrency(2).shovelTimeIntervalInSecs(600)
                .createdAt(1000L).sweepDuration(20 * 60 * 1000L)
                .build();
        service.store("SWEEP_Q", entity, 1200);

        // Create a sideline magazine
        AerospikeStorage<String> magazineStorage = AerospikeStorage.<String>builder()
                .clazz(String.class)
                .storageConfig(AerospikeStorageConfig.builder()
                        .dataSetName(Utils.getMagazineSet(CLIENT_ID, Constants.AEROSPIKE_DATA_SET))
                        .metaSetName(Utils.getMagazineSet(CLIENT_ID, Constants.AEROSPIKE_META_SET))
                        .namespace(AEROSPIKE_NAMESPACE)
                        .shards(1)
                        .recordTtl(300)
                        .metaDataTtl(600)
                        .build())
                .aerospikeClient(aerospikeClient)
                .enableDeDupe(false)
                .farmId(FARM_ID)
                .scope(MagazineScope.LOCAL)
                .clientId(CLIENT_ID)
                .build();

        Magazine<String> sidelineMagazine = Magazine.<String>builder()
                .baseMagazineStorage(magazineStorage)
                .magazineIdentifier(Utils.getSidelineQueueName("SWEEP_Q"))
                .build();

        // Sweep with no data loaded - should handle gracefully (null meta record)
        service.sweep("SWEEP_Q", 0, System.currentTimeMillis(), sidelineMagazine);
    }

    @Test(expected = IgnisMQException.class)
    public void testSweepQueueNotFound() throws Exception {
        AerospikeStorage<String> magazineStorage = AerospikeStorage.<String>builder()
                .clazz(String.class)
                .storageConfig(AerospikeStorageConfig.builder()
                        .dataSetName(Utils.getMagazineSet(CLIENT_ID, Constants.AEROSPIKE_DATA_SET))
                        .metaSetName(Utils.getMagazineSet(CLIENT_ID, Constants.AEROSPIKE_META_SET))
                        .namespace(AEROSPIKE_NAMESPACE)
                        .shards(1)
                        .recordTtl(300)
                        .metaDataTtl(600)
                        .build())
                .aerospikeClient(aerospikeClient)
                .enableDeDupe(false)
                .farmId(FARM_ID)
                .scope(MagazineScope.LOCAL)
                .clientId(CLIENT_ID)
                .build();

        Magazine<String> sidelineMagazine = Magazine.<String>builder()
                .baseMagazineStorage(magazineStorage)
                .magazineIdentifier("NON_EXISTENT_SIDELINE")
                .build();

        service.sweep("NON_EXISTENT", 0, System.currentTimeMillis(), sidelineMagazine);
    }

    @Test
    public void testSweepWithLoadedAndFiredData() throws Exception {
        // Store queue entity
        QueueEntity entity = QueueEntity.builder()
                .active(true).shards(1).queueExpiry(600).messageExpiry(300)
                .concurrency(4).messageHandlerType("handler")
                .shovelConcurrency(2).shovelTimeIntervalInSecs(5)
                .createdAt(1000L).sweepDuration(0L) // 0 sweep duration => sweep everything
                .build();
        service.store("SWEEP_Q2", entity, 1200);

        // Create magazine
        AerospikeStorage<String> magazineStorage = AerospikeStorage.<String>builder()
                .clazz(String.class)
                .storageConfig(AerospikeStorageConfig.builder()
                        .dataSetName(Utils.getMagazineSet(CLIENT_ID, Constants.AEROSPIKE_DATA_SET))
                        .metaSetName(Utils.getMagazineSet(CLIENT_ID, Constants.AEROSPIKE_META_SET))
                        .namespace(AEROSPIKE_NAMESPACE)
                        .shards(1)
                        .recordTtl(300)
                        .metaDataTtl(600)
                        .build())
                .aerospikeClient(aerospikeClient)
                .enableDeDupe(false)
                .farmId(FARM_ID)
                .scope(MagazineScope.LOCAL)
                .clientId(CLIENT_ID)
                .build();

        Magazine<String> magazine = Magazine.<String>builder()
                .baseMagazineStorage(magazineStorage)
                .magazineIdentifier("SWEEP_Q2")
                .build();

        Magazine<String> sidelineMagazine = Magazine.<String>builder()
                .baseMagazineStorage(magazineStorage)
                .magazineIdentifier(Utils.getSidelineQueueName("SWEEP_Q2"))
                .build();

        // Load and fire messages
        magazine.load("msg1");
        magazine.load("msg2");
        MagazineData<String> d1 = magazine.fire();
        assertNotNull(d1);
        service.addFireTimestamp(d1, System.currentTimeMillis() - 100000); // old timestamp

        MagazineData<String> d2 = magazine.fire();
        assertNotNull(d2);
        service.addFireTimestamp(d2, System.currentTimeMillis() - 100000); // old timestamp

        // Now sweep - should move messages to sideline
        service.sweep("SWEEP_Q2", 0, System.currentTimeMillis(), sidelineMagazine);
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

    private QueueEntity buildEntity(boolean active) {
        return QueueEntity.builder()
                .active(active).shards(32).queueExpiry(600).messageExpiry(300)
                .concurrency(4).messageHandlerType("handler")
                .shovelConcurrency(2).shovelTimeIntervalInSecs(600)
                .createdAt(1000L).sweepDuration(20 * 60 * 1000L)
                .build();
    }

    private AerospikeStorage<String> createMagazineStorage() {
        return AerospikeStorage.<String>builder()
                .clazz(String.class)
                .storageConfig(AerospikeStorageConfig.builder()
                        .dataSetName(Utils.getMagazineSet(CLIENT_ID, Constants.AEROSPIKE_DATA_SET))
                        .metaSetName(Utils.getMagazineSet(CLIENT_ID, Constants.AEROSPIKE_META_SET))
                        .namespace(AEROSPIKE_NAMESPACE)
                        .shards(1)
                        .recordTtl(300)
                        .metaDataTtl(600)
                        .build())
                .aerospikeClient(aerospikeClient)
                .enableDeDupe(false)
                .farmId(FARM_ID)
                .scope(MagazineScope.LOCAL)
                .clientId(CLIENT_ID)
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

    @Test
    public void testSweepWithRecentFireTimestamp() throws Exception {
        // This tests the branch where fireTS >= sweepTillFireTimestamp (sweptTillAllowedFireTS = true)
        QueueEntity entity = QueueEntity.builder()
                .active(true).shards(1).queueExpiry(600).messageExpiry(300)
                .concurrency(4).messageHandlerType("handler")
                .shovelConcurrency(2).shovelTimeIntervalInSecs(600)
                .createdAt(1000L).sweepDuration(20 * 60 * 1000L)
                .build();
        service.store("SWEEP_RECENT", entity, 1200);

        AerospikeStorage<String> magazineStorage = createMagazineStorage();

        Magazine<String> magazine = Magazine.<String>builder()
                .baseMagazineStorage(magazineStorage)
                .magazineIdentifier("SWEEP_RECENT")
                .build();

        Magazine<String> sidelineMagazine = Magazine.<String>builder()
                .baseMagazineStorage(magazineStorage)
                .magazineIdentifier(Utils.getSidelineQueueName("SWEEP_RECENT"))
                .build();

        // Load and fire messages with RECENT timestamps
        magazine.load("msg1");
        MagazineData<String> d1 = magazine.fire();
        assertNotNull(d1);
        long recentTS = System.currentTimeMillis();
        service.addFireTimestamp(d1, recentTS);

        // Sweep with a threshold OLDER than the fire timestamp => should hit sweptTillAllowedFireTS
        service.sweep("SWEEP_RECENT", 0, recentTS - 10000, sidelineMagazine);
    }

    @Test
    public void testSweepWithExistingSweepPointers() throws Exception {
        // This tests getSweepPointer with non-null sweepPointers map
        // First do a sweep to create sweep pointers, then sweep again
        QueueEntity entity = QueueEntity.builder()
                .active(true).shards(1).queueExpiry(600).messageExpiry(300)
                .concurrency(4).messageHandlerType("handler")
                .shovelConcurrency(2).shovelTimeIntervalInSecs(5)
                .createdAt(1000L).sweepDuration(0L)
                .build();
        service.store("SWEEP_PTR", entity, 1200);

        AerospikeStorage<String> magazineStorage = createMagazineStorage();

        Magazine<String> magazine = Magazine.<String>builder()
                .baseMagazineStorage(magazineStorage)
                .magazineIdentifier("SWEEP_PTR")
                .build();

        Magazine<String> sidelineMagazine = Magazine.<String>builder()
                .baseMagazineStorage(magazineStorage)
                .magazineIdentifier(Utils.getSidelineQueueName("SWEEP_PTR"))
                .build();

        // Load, fire, set old timestamp
        magazine.load("msg1");
        MagazineData<String> d1 = magazine.fire();
        assertNotNull(d1);
        service.addFireTimestamp(d1, System.currentTimeMillis() - 200000);

        // First sweep creates sweep pointers
        service.sweep("SWEEP_PTR", 0, System.currentTimeMillis(), sidelineMagazine);

        // Load more, fire, set old timestamp
        magazine.load("msg2");
        MagazineData<String> d2 = magazine.fire();
        assertNotNull(d2);
        service.addFireTimestamp(d2, System.currentTimeMillis() - 200000);

        // Second sweep should use existing sweep pointers (non-null map path)
        service.sweep("SWEEP_PTR", 0, System.currentTimeMillis(), sidelineMagazine);
    }

    @Test
    public void testSweepPointerAlreadyAtFirePointer() throws Exception {
        // Tests sweepPointer >= currentFirePointer early return
        QueueEntity entity = QueueEntity.builder()
                .active(true).shards(1).queueExpiry(600).messageExpiry(300)
                .concurrency(4).messageHandlerType("handler")
                .shovelConcurrency(2).shovelTimeIntervalInSecs(5)
                .createdAt(1000L).sweepDuration(0L)
                .build();
        service.store("SWEEP_EQ", entity, 1200);

        AerospikeStorage<String> magazineStorage = createMagazineStorage();

        Magazine<String> magazine = Magazine.<String>builder()
                .baseMagazineStorage(magazineStorage)
                .magazineIdentifier("SWEEP_EQ")
                .build();

        Magazine<String> sidelineMagazine = Magazine.<String>builder()
                .baseMagazineStorage(magazineStorage)
                .magazineIdentifier(Utils.getSidelineQueueName("SWEEP_EQ"))
                .build();

        // Load and fire one message, sweep it
        magazine.load("msg1");
        MagazineData<String> d1 = magazine.fire();
        assertNotNull(d1);
        service.addFireTimestamp(d1, System.currentTimeMillis() - 200000);
        service.sweep("SWEEP_EQ", 0, System.currentTimeMillis(), sidelineMagazine);

        // Sweep again with no new messages — sweep pointer should already be at fire pointer
        service.sweep("SWEEP_EQ", 0, System.currentTimeMillis(), sidelineMagazine);
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
