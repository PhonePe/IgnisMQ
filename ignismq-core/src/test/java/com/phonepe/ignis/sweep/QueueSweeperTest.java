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
import com.phonepe.ignis.common.MagazineRegistry;
import com.phonepe.ignis.entity.QueueEntity;
import com.phonepe.ignis.service.AerospikeQueueService;
import com.phonepe.ignis.storage.BaseStorage;
import com.phonepe.ignis.util.AerospikeTestBase;
import com.phonepe.ignis.utils.Constants;
import com.phonepe.ignis.utils.Utils;
import com.phonepe.magazine.Magazine;
import com.phonepe.magazine.entity.MagazineData;
import com.phonepe.magazine.entity.MagazineScope;
import com.phonepe.magazine.exception.ErrorCode;
import com.phonepe.magazine.exception.MagazineException;
import com.phonepe.magazine.impl.aerospike.AerospikeStorage;
import com.phonepe.magazine.impl.aerospike.AerospikeStorageConfig;
import org.junit.Before;
import org.junit.Test;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.mockito.Mockito;

import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The sweeper's whole job is to re-home messages that were delivered and never acknowledged, without
 * ever destroying one. Its stopping conditions are all that stands between a genuinely abandoned
 * message and one a consumer is still working on, so they are what these tests pin down.
 * <p>
 * Two passes appear throughout. That is not incidental: a checkpoint written during a pass is
 * deliberately invisible to that same pass - resolution happens against the pre-write snapshot - so
 * the first pass establishes history and the second is the first that can act on it. A single-pass
 * sweeper would answer questions about the present, which is exactly the thing the watermark exists
 * to avoid.
 */
public class QueueSweeperTest extends AerospikeTestBase {

    private static final long ONE_HOUR_MS = 60 * 60 * 1000L;

    private AerospikeQueueService service;
    private QueueSweeper sweeper;
    private AerospikeStorage<String> storage;
    private StorageClient storageClient;
    private BaseStorage baseStorage;

    @Before
    public void setUp() {
        service = createQueueService();
        storageClient = Mockito.mock(StorageClient.class);
        Mockito.when(storageClient.getClient()).thenReturn(aerospikeClient);
        baseStorage = createBaseStorage();
        sweeper = new QueueSweeper(service, CLIENT_ID, baseStorage, storageClient, FARM_ID,
                new SimpleMeterRegistry());
        storage = AerospikeStorage.<String>builder()
                .clazz(String.class)
                .storageConfig(AerospikeStorageConfig.builder()
                        .dataSetName(Utils.getMagazineSet(CLIENT_ID, Constants.AEROSPIKE_DATA_SET))
                        .metaSetName(Utils.getMagazineSet(CLIENT_ID, Constants.AEROSPIKE_META_SET))
                        .namespace(AEROSPIKE_NAMESPACE)
                        .shards(1)
                        .recordTtl(300)
                        .metaDataTtl(600)
                        .fireHistoryEnabled(true)
                        .fireHistoryWindowSeconds(1)
                        .fireHistoryEntries(Constants.FIRE_HISTORY_ENTRIES)
                        .build())
                .aerospikeClient(aerospikeClient)
                .enableDeDupe(false)
                .farmId(FARM_ID)
                .scope(MagazineScope.LOCAL)
                .clientId(CLIENT_ID)
                .build();
    }

    /**
     * The watermark is a position, not a per-message timestamp, and everything above it is filled but
     * unclaimed. Sweeping into that region would delete a message no consumer has ever seen.
     */
    @Test
    public void testSweepLeavesUndeliveredMessagesAlone() {
        final String queue = "SWEEP_UNDELIVERED";
        final QueueEntity entity = store(queue, 0L);
        final Magazine<String> magazine = magazine(queue);
        final Magazine<String> sideline = magazine(Utils.getSidelineQueueName(queue));

        magazine.load("delivered");
        magazine.load("undelivered-1");
        magazine.load("undelivered-2");

        assertEquals("delivered", magazine.fire().getData());

        sweepTwice(queue, entity, magazine, sideline);

        // The abandoned message moved to the sideline...
        assertEquals("delivered", sideline.fire().getData());
        // ...and the two that were never handed out are still there to be consumed.
        assertEquals("undelivered-1", magazine.fire().getData());
        assertEquals("undelivered-2", magazine.fire().getData());
    }

    /**
     * A message claimed moments ago is in flight, not abandoned. No checkpoint reaches back a whole
     * sweep duration yet, and an absent watermark must mean "sweep nothing", never "sweep everything".
     */
    @Test
    public void testSweepLeavesRecentlyDeliveredMessagesAlone() {
        final String queue = "SWEEP_IN_FLIGHT";
        final QueueEntity entity = store(queue, ONE_HOUR_MS);
        final Magazine<String> magazine = magazine(queue);
        final Magazine<String> sideline = magazine(Utils.getSidelineQueueName(queue));

        magazine.load("in-flight");
        final MagazineData<String> inFlight = magazine.fire();
        assertEquals("in-flight", inFlight.getData());

        sweepTwice(queue, entity, magazine, sideline);

        assertTrue("a message claimed moments ago must survive the sweep", exists(magazine, inFlight));
        assertNothingToFire(sideline);
    }

    /**
     * B2, still the rule: a message may only be deleted from where it is once it demonstrably exists
     * somewhere else.
     */
    @Test
    public void testSweepKeepsSourceWhenSidelineRefusesTheMessage() {
        final String queue = "SWEEP_SIDELINE_REFUSES";
        final QueueEntity entity = store(queue, 0L);
        final Magazine<String> magazine = magazine(queue);
        final Magazine<String> sideline = Mockito.spy(magazine(Utils.getSidelineQueueName(queue)));
        Mockito.doReturn(false).when(sideline).load(Mockito.any());

        magazine.load("stale");
        final MagazineData<String> stale = magazine.fire();

        sweepTwice(queue, entity, magazine, sideline);

        Mockito.verify(sideline, Mockito.atLeastOnce()).load("stale");
        assertTrue("message must not be deleted when the sideline would not take it",
                exists(magazine, stale));
    }

    /**
     * A refused transfer stops that shard where it stands rather than stepping over the message. If
     * the progress marker advanced past it, the retry the sweeper is built around could never happen.
     */
    @Test
    public void testSweepDoesNotAdvancePastAMessageItCouldNotReHome() {
        final String queue = "SWEEP_NO_ADVANCE";
        final QueueEntity entity = store(queue, 0L);
        final Magazine<String> magazine = magazine(queue);
        final Magazine<String> refusing = Mockito.spy(magazine(Utils.getSidelineQueueName(queue)));
        Mockito.doReturn(false).when(refusing).load(Mockito.any());

        magazine.load("stale-1");
        magazine.load("stale-2");
        magazine.fire();
        magazine.fire();

        sweepTwice(queue, entity, magazine, refusing);

        // The sideline now accepts messages again; the same pass must still find both of them.
        final Magazine<String> sideline = magazine(Utils.getSidelineQueueName(queue));
        sweeper.sweep(queue, reload(queue), magazine, sideline);

        assertEquals("stale-1", sideline.fire().getData());
        assertEquals("stale-2", sideline.fire().getData());
    }

    /**
     * The sweeper must still do its job.
     */
    @Test
    public void testSweepMovesAbandonedMessages() {
        final String queue = "SWEEP_STALE";
        final QueueEntity entity = store(queue, 0L);
        final Magazine<String> magazine = magazine(queue);
        final Magazine<String> sideline = magazine(Utils.getSidelineQueueName(queue));

        magazine.load("stale-1");
        magazine.load("stale-2");
        final MagazineData<String> first = magazine.fire();
        final MagazineData<String> second = magazine.fire();

        sweepTwice(queue, entity, magazine, sideline);

        assertFalse(exists(magazine, first));
        assertFalse(exists(magazine, second));
        assertEquals("stale-1", sideline.fire().getData());
        assertEquals("stale-2", sideline.fire().getData());
    }

    /**
     * Progress is recorded per shard as the next slot to examine, so a second pass over an already
     * swept range costs one batch read and re-homes nothing.
     */
    @Test
    public void testSweepIsIdempotentOverAnAlreadySweptRange() {
        final String queue = "SWEEP_IDEMPOTENT";
        final QueueEntity entity = store(queue, 0L);
        final Magazine<String> magazine = magazine(queue);
        final Magazine<String> sideline = magazine(Utils.getSidelineQueueName(queue));

        magazine.load("once");
        magazine.fire();

        sweepTwice(queue, entity, magazine, sideline);
        sweeper.sweep(queue, reload(queue), magazine, sideline);

        assertEquals("once", sideline.fire().getData());
        assertNothingToFire(sideline);
    }

    /**
     * B3: the shard loop is now Magazine's own persisted count rather than ignisMQ's stored one, so
     * the two can no longer disagree and skip whole shards. An unsharded magazine reports one shard
     * and keys its records without a shard fragment; the sweeper must follow it rather than the
     * queue record, which here claims eight.
     */
    @Test
    public void testSweepFollowsTheMagazinesShardCountNotTheQueueRecords() {
        final String queue = "SWEEP_SHARD_COUNT";
        final QueueEntity entity = store(queue, 0L);
        entity.setShards(8);
        final Magazine<String> magazine = magazine(queue);
        final Magazine<String> sideline = magazine(Utils.getSidelineQueueName(queue));
        assertEquals(1, magazine.getShards());

        magazine.load("only-shard");
        magazine.fire();

        sweepTwice(queue, entity, magazine, sideline);

        assertEquals("only-shard", sideline.fire().getData());
        // One shard existed, so exactly one shard's progress was recorded. Iterating the queue
        // record's eight would have written seven more, all of them for shards that do not exist -
        // and under the old key naming, seven redundant passes over the same unsharded key space.
        assertEquals(Set.of("SHARD_0"), reload(queue).getSweepPointers().keySet());
    }

    /**
     * The common case, and the one that went untested: almost every slot below the watermark was
     * acknowledged and deleted long ago, so a sweep usually scans a range and finds nothing. The
     * shard must still advance - a pass that cannot get past an empty range never progresses at all.
     */
    @Test
    public void testSweepAdvancesThroughARangeWithNothingLeftInIt() {
        final String queue = "SWEEP_EMPTY_RANGE";
        final QueueEntity entity = store(queue, 0L);
        final Magazine<String> magazine = magazine(queue);
        final Magazine<String> sideline = magazine(Utils.getSidelineQueueName(queue));

        magazine.load("acked-1");
        magazine.load("acked-2");
        magazine.delete(magazine.fire());
        magazine.delete(magazine.fire());

        sweepTwice(queue, entity, magazine, sideline);

        assertNothingToFire(sideline);
        assertEquals("progress must move past a range that held nothing",
                Long.valueOf(3L), reload(queue).getSweepPointers().get("SHARD_0"));
    }

    /**
     * C7: the manager already holds a live pair for every queue this process serves, so the sweeper
     * uses those rather than constructing a throwaway pair per pass. Building its own also builds a
     * fresh checkpoint-claim map, which re-issues a write per shard the consumers already made.
     */
    @Test
    public void testSweepQueueReusesMagazinesTheProcessAlreadyHolds() {
        final String queue = "SWEEP_REUSE";
        final QueueEntity entity = store(queue, 0L);
        final Magazine<String> main = Mockito.spy(magazine(queue));
        final Magazine<String> sideline = Mockito.spy(magazine(Utils.getSidelineQueueName(queue)));
        final QueueSweeper reusing = new QueueSweeper(service, CLIENT_ID, baseStorage, storageClient,
                FARM_ID, new SimpleMeterRegistry(),
                name -> new MagazineRegistry.QueueMagazines(main, sideline));

        main.load("orphan");
        main.fire();

        awaitWindowRollover();
        reusing.sweepQueue(queue, entity);
        reusing.sweepQueue(queue, reload(queue));

        // The registry's magazines were used, not freshly built ones.
        Mockito.verify(main, Mockito.atLeastOnce()).firePointerBefore(Mockito.any());
        assertEquals("orphan", magazine(Utils.getSidelineQueueName(queue)).fire().getData());
    }

    /**
     * A queue created on another instance is genuinely absent here until the watcher catches up, on
     * a five-minute cycle. Absent must mean "build one", never "skip the queue" - silently not
     * sweeping would be worse than the allocation it saves.
     */
    @Test
    public void testSweepQueueBuildsItsOwnWhenTheRegistryHasNothing() {
        final String queue = "SWEEP_REGISTRY_MISS";
        final QueueEntity entity = store(queue, 0L);
        final QueueSweeper missing = new QueueSweeper(service, CLIENT_ID, baseStorage, storageClient,
                FARM_ID, new SimpleMeterRegistry(), name -> null);

        final Magazine<String> magazine = magazine(queue);
        magazine.load("orphan");
        magazine.fire();

        awaitWindowRollover();
        missing.sweepQueue(queue, entity);
        missing.sweepQueue(queue, reload(queue));

        assertEquals("orphan", magazine(Utils.getSidelineQueueName(queue)).fire().getData());
    }

    /**
     * A magazine that cannot be swept must not cost the other one its turn. The main and sideline
     * sweeps are independent pieces of work sharing only a queue record.
     */
    @Test
    public void testAFailingMainMagazineStillLetsTheSidelineBeSwept() {
        final String queue = "SWEEP_CONTAINED";
        final QueueEntity entity = store(queue, 0L);

        final Magazine<String> failing = Mockito.mock(Magazine.class);
        Mockito.when(failing.getMagazineIdentifier()).thenReturn(queue);
        Mockito.when(failing.firePointerBefore(Mockito.any()))
                .thenThrow(new MagazineException(ErrorCode.INVALID_REQUEST, "history evicted", null));
        final Magazine<String> sideline = Mockito.spy(magazine(Utils.getSidelineQueueName(queue)));

        sweeper.sweep(queue, entity, failing, sideline);

        Mockito.verify(sideline).firePointerBefore(Mockito.any());
    }

    /**
     * The entry point the scheduled sweeper actually calls: it builds the queue's two magazines
     * from stored configuration rather than being handed them.
     */
    @Test
    public void testSweepQueueBuildsItsOwnMagazines() {
        final String queue = "SWEEP_SELF_BUILT";
        final QueueEntity entity = store(queue, 0L);

        final Magazine<String> magazine = magazine(queue);
        magazine.load("orphan");
        magazine.fire();

        awaitWindowRollover();
        sweeper.sweepQueue(queue, entity);
        sweeper.sweepQueue(queue, reload(queue));

        assertEquals("orphan", magazine(Utils.getSidelineQueueName(queue)).fire().getData());
    }

    /**
     * One unsweepable queue must not abort the pass: the sweeper runs over every queue in turn and
     * the next cycle retries this one from the same progress marker.
     */
    @Test
    public void testSweepQueueSwallowsAFailure() {
        final StorageClient broken = Mockito.mock(StorageClient.class);
        Mockito.when(broken.getClient()).thenThrow(new IllegalStateException("broken"));
        final QueueSweeper failing = new QueueSweeper(service, CLIENT_ID, baseStorage, broken, FARM_ID,
                new SimpleMeterRegistry());

        failing.sweepQueue("NEVER_EXISTED", store("NEVER_EXISTED", 0L));
    }

    /**
     * Drives the sweeper to the first pass that can actually act.
     * <p>
     * Two things have to happen before a claim is visible to a watermark, and both are deliberate.
     * The checkpoint window must roll, because a checkpoint records where the pointer stood when the
     * window opened - which for these tests is before anything was fired. And a checkpoint written
     * during a pass is invisible to that same pass, because resolution runs against the pre-write
     * snapshot, so the pass that records is never the pass that acts. Hence: wait out one window,
     * record, then sweep.
     */
    private void sweepTwice(final String queue, final QueueEntity entity,
                            final Magazine<String> magazine, final Magazine<String> sideline) {
        awaitWindowRollover();
        sweeper.sweep(queue, entity, magazine, sideline);
        sweeper.sweep(queue, reload(queue), magazine, sideline);
    }

    /** The test storage is configured with a one-second checkpoint window. */
    private static void awaitWindowRollover() {
        try {
            Thread.sleep(1200L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private QueueEntity reload(final String queue) {
        return service.get(queue).orElseThrow();
    }

    private QueueEntity store(final String queue, final long sweepDuration) {
        final QueueEntity entity = QueueEntity.builder()
                .active(true).shards(1).queueExpiry(600).messageExpiry(300)
                .concurrency(1).messageHandlerType("handler")
                .shovelConcurrency(1).shovelTimeIntervalInSecs(5)
                .createdAt(1000L).sweepDuration(sweepDuration)
                .build();
        service.store(queue, entity, 1200);
        return entity;
    }

    private Magazine<String> magazine(final String identifier) {
        return Magazine.<String>builder()
                .baseMagazineStorage(storage)
                .magazineIdentifier(identifier)
                .build();
    }

    private static boolean exists(final Magazine<String> magazine, final MagazineData<String> data) {
        final int shard = data.getShard() == null ? 0 : data.getShard();
        return !magazine.peek(Map.of(shard, Set.of(data.getFirePointer()))).isEmpty();
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
