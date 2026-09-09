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

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.phonepe.ignis.entity.QueueEntity;
import com.phonepe.ignis.util.AerospikeTestBase;
import com.phonepe.ignis.utils.Constants;
import com.phonepe.ignis.utils.Utils;
import com.phonepe.magazine.Magazine;
import com.phonepe.magazine.entity.MagazineData;
import com.phonepe.magazine.entity.MagazineScope;
import com.phonepe.magazine.exception.MagazineException;
import com.phonepe.magazine.impl.aerospike.AerospikeStorage;
import com.phonepe.magazine.impl.aerospike.AerospikeStorageConfig;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.*;

/**
 * B1 and B2: the sweep must move delivered-and-stale messages to the sideline without ever destroying
 * a message. It reads a fixed window of keys and deletes what it finds, so its stopping conditions are
 * the only thing standing between a stale message and an undelivered one.
 */
public class SweepDataLossTest extends AerospikeTestBase {

    private static final long ONE_DAY_MS = 24 * 60 * 60 * 1000L;

    private AerospikeQueueService service;
    private AerospikeStorage<String> storage;

    @Before
    public void setUp() {
        service = createQueueService();
        storage = AerospikeStorage.<String>builder()
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

    /**
     * B1: the sweep read a fixed 1,000-key window irrespective of the fire pointer. Everything past
     * the fire pointer is loaded but undelivered and has no fire timestamp, which the old code read as
     * 0 and therefore as "fired long ago" - so it sidelined and deleted messages no consumer had seen.
     */
    @Test
    public void testSweepLeavesUndeliveredMessagesAlone() {
        final String queue = "SWEEP_UNDELIVERED";
        store(queue);
        final Magazine<String> magazine = magazine(queue);
        final Magazine<String> sideline = magazine(Utils.getSidelineQueueName(queue));

        magazine.load("delivered");
        magazine.load("undelivered-1");
        magazine.load("undelivered-2");

        final MagazineData<String> delivered = magazine.fire();
        assertEquals("delivered", delivered.getData());
        service.addFireTimestamp(delivered, System.currentTimeMillis() - ONE_DAY_MS);

        service.sweep(queue, 0, System.currentTimeMillis(), sideline);

        // The stale delivered message moved to the sideline...
        assertEquals("delivered", sideline.fire().getData());
        // ...and the two that were never delivered are still there to be consumed.
        assertEquals("undelivered-1", magazine.fire().getData());
        assertEquals("undelivered-2", magazine.fire().getData());
    }

    /**
     * B1: Magazine advances the fire pointer before ignisMQ writes the fire timestamp, so a delivered
     * record can briefly carry no timestamp. Absent must not be read as 0, which would sweep a message
     * the consumer is still working on.
     */
    @Test
    public void testSweepStopsAtAMessageWithNoFireTimestamp() {
        final String queue = "SWEEP_NO_TIMESTAMP";
        store(queue);
        final Magazine<String> magazine = magazine(queue);
        final Magazine<String> sideline = magazine(Utils.getSidelineQueueName(queue));

        magazine.load("in-flight");
        final MagazineData<String> inFlight = magazine.fire();
        assertEquals("in-flight", inFlight.getData());
        // Deliberately no addFireTimestamp: the consumer has claimed it but not recorded when.

        service.sweep(queue, 0, System.currentTimeMillis(), sideline);

        assertNotNull("record with no fire timestamp must survive the sweep", dataRecord(queue, inFlight));
        assertNothingToFire(sideline);
    }

    /**
     * B2: the source record was deleted whether or not the sideline had accepted the message.
     */
    @Test
    public void testSweepKeepsSourceWhenSidelineRefusesTheMessage() {
        final String queue = "SWEEP_SIDELINE_REFUSES";
        store(queue);
        final Magazine<String> magazine = magazine(queue);
        final Magazine<String> sideline = Mockito.spy(magazine(Utils.getSidelineQueueName(queue)));
        Mockito.doReturn(false).when(sideline).load(Mockito.any());

        magazine.load("stale");
        final MagazineData<String> stale = magazine.fire();
        service.addFireTimestamp(stale, System.currentTimeMillis() - ONE_DAY_MS);

        service.sweep(queue, 0, System.currentTimeMillis(), sideline);

        Mockito.verify(sideline).load("stale");
        assertNotNull("message must not be deleted when the sideline would not take it",
                dataRecord(queue, stale));
    }

    /**
     * The sweep must still do its job: a stale delivered message ends up on the sideline and is gone
     * from the main magazine.
     */
    @Test
    public void testSweepMovesStaleDeliveredMessages() {
        final String queue = "SWEEP_STALE";
        store(queue);
        final Magazine<String> magazine = magazine(queue);
        final Magazine<String> sideline = magazine(Utils.getSidelineQueueName(queue));

        magazine.load("stale-1");
        magazine.load("stale-2");
        final MagazineData<String> first = magazine.fire();
        service.addFireTimestamp(first, System.currentTimeMillis() - ONE_DAY_MS);
        final MagazineData<String> second = magazine.fire();
        service.addFireTimestamp(second, System.currentTimeMillis() - ONE_DAY_MS);

        service.sweep(queue, 0, System.currentTimeMillis(), sideline);

        assertNull(dataRecord(queue, first));
        assertNull(dataRecord(queue, second));
        assertEquals("stale-1", sideline.fire().getData());
        assertEquals("stale-2", sideline.fire().getData());
    }

    private void store(final String queue) {
        service.store(queue, QueueEntity.builder()
                .active(true).shards(1).queueExpiry(600).messageExpiry(300)
                .concurrency(1).messageHandlerType("handler")
                .shovelConcurrency(1).shovelTimeIntervalInSecs(5)
                .createdAt(1000L).sweepDuration(0L)
                .build(), 1200);
    }

    private Magazine<String> magazine(final String identifier) {
        return Magazine.<String>builder()
                .baseMagazineStorage(storage)
                .magazineIdentifier(identifier)
                .build();
    }

    private Record dataRecord(final String queue, final MagazineData<String> data) {
        return aerospikeClient.get(aerospikeClient.getReadPolicyDefault(), new Key(
                AEROSPIKE_NAMESPACE,
                Utils.resolveLocalMagazineSet(CLIENT_ID, Constants.AEROSPIKE_DATA_SET, FARM_ID),
                Utils.createMagazineAerospikeKey(data.getFirePointer(), data.getShard(), queue)));
    }

    private static void assertNothingToFire(final Magazine<String> magazine) {
        try {
            final MagazineData<String> unexpected = magazine.fire();
            fail("expected an empty magazine but fired: " + unexpected.getData());
        } catch (MagazineException e) {
            // expected: the magazine is drained
        }
    }
}
