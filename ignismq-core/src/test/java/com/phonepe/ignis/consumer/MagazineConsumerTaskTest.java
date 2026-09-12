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

package com.phonepe.ignis.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.phonepe.ignis.common.MessageHandler;
import com.phonepe.ignis.config.BatchingConfig;
import com.phonepe.ignis.utils.Constants;
import com.phonepe.magazine.Magazine;
import com.phonepe.magazine.entity.MagazineData;
import com.phonepe.magazine.exception.ErrorCode;
import com.phonepe.magazine.exception.MagazineException;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Covers the transfer-then-delete contract of the consumer: a message may only leave the main
 * magazine once the handler has accepted it or the sideline magazine has taken responsibility for it.
 */
public class MagazineConsumerTaskTest {

    private Magazine<String> magazine;
    private Magazine<String> sidelineMagazine;
    private Timer consumeTimer;

    @Before
    public void setUp() {
        magazine = Mockito.mock(Magazine.class);
        sidelineMagazine = Mockito.mock(Magazine.class);
        consumeTimer = new SimpleMeterRegistry().timer("test.consume");
        when(magazine.getMagazineIdentifier()).thenReturn("TEST_QUEUE");
    }

    @Test
    public void testSuccessfulHandlingDeletesWithoutSidelining() {
        firesThen("msg1");

        task(handler(true)).run();

        verify(sidelineMagazine, never()).load(any());
        verify(magazine, times(1)).delete(any());
    }

    @Test
    public void testRejectedMessageIsSidelinedThenDeleted() {
        firesThen("msg1");
        when(sidelineMagazine.load("msg1")).thenReturn(true);

        task(handler(false)).run();

        verify(sidelineMagazine, times(1)).load("msg1");
        verify(magazine, times(1)).delete(any());
    }

    /**
     * B2: the handler rejected the message and the sideline would not take it. The record in the main
     * magazine is the only copy left, so it must survive for the sweeper to retry.
     */
    @Test
    public void testRejectedMessageIsNotDeletedWhenSidelineLoadReturnsFalse() {
        firesThen("msg1");
        when(sidelineMagazine.load("msg1")).thenReturn(false);

        task(handler(false)).run();

        verify(sidelineMagazine, times(1)).load("msg1");
        verify(magazine, never()).delete(any());
    }

    /**
     * B2: same contract when the sideline load throws, which is the reachable failure mode against
     * Magazine 2's Aerospike storage.
     */
    @Test
    public void testRejectedMessageIsNotDeletedWhenSidelineLoadThrows() {
        firesThen("msg1");
        when(sidelineMagazine.load("msg1")).thenThrow(new RuntimeException("sideline down"));

        task(handler(false)).run();

        verify(magazine, never()).delete(any());
    }

    /**
     * B2: a handler that throws takes the same route. Previously the delete happened regardless of
     * whether the sideline had accepted the message.
     */
    @Test
    public void testThrowingHandlerDoesNotDeleteWhenSidelineRefuses() {
        firesThen("msg1");
        when(sidelineMagazine.load("msg1")).thenReturn(false);

        task(throwingHandler(Collections.emptySet())).run();

        verify(magazine, never()).delete(any());
    }

    /**
     * An ignorable exception means the message is deliberately dropped, so no sideline write is
     * attempted and the delete must still happen.
     */
    @Test
    public void testIgnorableExceptionDeletesWithoutSidelining() {
        firesThen("msg1");

        task(throwingHandler(Set.of(IllegalStateException.class))).run();

        verify(sidelineMagazine, never()).load(any());
        verify(magazine, times(1)).delete(any());
    }

    /**
     * One undeliverable message must not prevent the rest of the batch from being retired.
     */
    @Test
    public void testOneFailedSidelineDoesNotBlockTheRestOfTheBatch() {
        when(magazine.fire())
                .thenReturn(data("msg1"), data("msg2"))
                .thenThrow(new MagazineException(ErrorCode.NOTHING_TO_FIRE, "nothing", null));
        when(sidelineMagazine.load("msg1")).thenReturn(false);
        when(sidelineMagazine.load("msg2")).thenReturn(true);

        task(handler(false)).run();

        verify(sidelineMagazine, times(1)).load("msg1");
        verify(sidelineMagazine, times(1)).load("msg2");
        verify(magazine, times(1)).delete(any());
    }

    /**
     * B5: the old depth check used {@code <=}, so a queue holding exactly one full batch was judged
     * not ready and the consumer slept for another five seconds - contradicting its own comment.
     * A full batch must go straight to the handler.
     */
    @Test
    public void testAFullBatchIsHandedOverWithoutWaiting() {
        when(magazine.fire())
                .thenReturn(data("msg1"), data("msg2"), data("msg3"))
                .thenThrow(new MagazineException(ErrorCode.NOTHING_TO_FIRE, "nothing", null));
        final CapturingHandler handler = new CapturingHandler(true);

        final long started = System.currentTimeMillis();
        batchTask(handler, 3, 30).run();
        final long elapsed = System.currentTimeMillis() - started;

        assertEquals(List.of(3), handler.batchSizes);
        assertTrue("a full batch must not wait for the batching deadline; waited " + elapsed + "ms",
                elapsed < 5_000);
    }

    /**
     * A batch that never fills is still delivered once the caller's stated wait has elapsed, and
     * the wait is not overshot. The previous fixed five-second sleep could overrun a one-second
     * deadline by almost its whole length.
     */
    @Test
    public void testAPartialBatchIsHandedOverAtTheDeadlineWithoutOvershooting() {
        when(magazine.fire())
                .thenReturn(data("msg1"))
                .thenThrow(new MagazineException(ErrorCode.NOTHING_TO_FIRE, "nothing", null));
        final CapturingHandler handler = new CapturingHandler(true);

        final long started = System.currentTimeMillis();
        batchTask(handler, 10, 1).run();
        final long elapsed = System.currentTimeMillis() - started;

        assertEquals(List.of(1), handler.batchSizes);
        assertTrue("must wait for the deadline, waited only " + elapsed + "ms", elapsed >= 900);
        assertTrue("must not overshoot the deadline, waited " + elapsed + "ms", elapsed < 3_000);
    }

    /**
     * Draining more than one batch's worth emits full batches as they fill rather than accumulating
     * everything and flushing once.
     */
    @Test
    public void testABacklogIsEmittedAsSuccessiveFullBatches() {
        when(magazine.fire())
                .thenReturn(data("m1"), data("m2"), data("m3"), data("m4"), data("m5"))
                .thenThrow(new MagazineException(ErrorCode.NOTHING_TO_FIRE, "nothing", null));
        final CapturingHandler handler = new CapturingHandler(true);

        batchTask(handler, 2, 1).run();

        assertEquals(List.of(2, 2, 1), handler.batchSizes);
    }

    /**
     * An empty magazine must not be interrogated for depth. The old path issued one metadata batch
     * read per consumer per five-second wait even with nothing to do.
     */
    @Test
    public void testAnEmptyMagazineIsNeverAskedForItsDepth() {
        when(magazine.fire())
                .thenThrow(new MagazineException(ErrorCode.NOTHING_TO_FIRE, "nothing", null));

        batchTask(new CapturingHandler(true), 5, 1).run();

        verify(magazine, never()).getMetaData();
    }

    /**
     * The bug that made the shared pool unsafe: a queue with a standing backlog never returned.
     * <p>
     * The batching loop consulted its deadline only once {@code fire()} came back empty, so while
     * messages kept arriving it emitted full batches forever. Under thread-per-consumer
     * {@code Timer}s that was survivable - the consumer owned its thread. On a bounded shared pool
     * it takes a thread out of circulation for as long as the backlog lasts, and enough of them
     * starve every other consumer on the pool.
     * <p>
     * Mutation check: this fails against the previous implementation not by assertion but by
     * hanging - {@code fire()} never returns null here, which is exactly the production case.
     */
    @Test(timeout = 30_000)
    public void testABackloggedBatchingConsumerReturnsAtItsDeadline() {
        when(magazine.fire()).thenAnswer(invocation -> data("endless"));
        final CapturingHandler handler = new CapturingHandler(true);

        final long started = System.currentTimeMillis();
        batchTask(handler, 2, 1, 1_000L).run();
        final long elapsed = System.currentTimeMillis() - started;

        assertTrue("a backlogged consumer must hand its thread back; ran for " + elapsed + "ms",
                elapsed < 15_000);
        assertTrue("it must still have done real work before yielding", !handler.batchSizes.isEmpty());
    }

    /**
     * The same unbounded drain in the non-batching path, which had no deadline at all: it looped
     * while {@code fire()} kept returning.
     */
    @Test(timeout = 30_000)
    public void testABackloggedSingleMessageConsumerReturnsAtItsBudget() {
        when(magazine.fire()).thenAnswer(invocation -> data("endless"));

        final long started = System.currentTimeMillis();
        // A one-second budget rather than the production thirty: the property under test is that
        // the loop terminates on the budget at all, and asserting it against the real value would
        // cost this suite thirty seconds to learn nothing extra.
        new MagazineConsumerTask<>(magazine, sidelineMagazine, handler(true), new ObjectMapper(),
                String.class, consumeTimer, null, 1_000L).run();
        final long elapsed = System.currentTimeMillis() - started;

        assertTrue("a backlogged consumer must hand its thread back; ran for " + elapsed + "ms",
                elapsed < 15_000);
        verify(magazine, atLeastOnce()).delete(any());
    }

    /** The production default is what an unconfigured consumer actually gets. */
    @Test
    public void testTheDefaultRunBudgetIsTheProductionConstant() {
        assertEquals(30_000L, Constants.CONSUMER_RUN_BUDGET_IN_MS);
    }

    /**
     * The budget bounds the turn; it must never cost a delivery. A message is claimed the moment
     * {@code fire()} returns it, so yielding before consuming it would strand it below the fire
     * pointer with no consumer coming - recoverable only by the sweeper, one sweepDuration later.
     */
    @Test(timeout = 30_000)
    public void testEveryClaimedMessageIsConsumedEvenWhenTheBudgetExpires() {
        when(magazine.fire()).thenAnswer(invocation -> data("endless"));
        final CapturingHandler handler = new CapturingHandler(true);

        batchTask(handler, 2, 1, 1_000L).run();

        final int handedOver = handler.batchSizes.stream().mapToInt(Integer::intValue).sum();
        verify(magazine, times(handedOver)).fire();
        verify(magazine, times(handedOver)).delete(any());
    }

    private MagazineConsumerTask<String> batchTask(final MessageHandler<String> handler,
                                                   final int maxBatchSize, final int maxWaitSeconds) {
        return new MagazineConsumerTask<>(magazine, sidelineMagazine, handler, new ObjectMapper(),
                String.class, consumeTimer, BatchingConfig.builder()
                .maxBatchSize(maxBatchSize).maxWaitTimeInSecs(maxWaitSeconds).build());
    }

    private MagazineConsumerTask<String> batchTask(final MessageHandler<String> handler,
                                                   final int maxBatchSize, final int maxWaitSeconds,
                                                   final long runBudgetMillis) {
        return new MagazineConsumerTask<>(magazine, sidelineMagazine, handler, new ObjectMapper(),
                String.class, consumeTimer, BatchingConfig.builder()
                .maxBatchSize(maxBatchSize).maxWaitTimeInSecs(maxWaitSeconds).build(), runBudgetMillis);
    }

    /**
     * The run budget must never cut a linger short. A consumer may be configured to wait up to 300
     * seconds for a batch to fill, which is far beyond the default budget, and the caller's stated
     * latency has to win - so the budget is the greater of the two, not the batching wait alone.
     */
    @Test(timeout = 30_000)
    public void testTheRunBudgetNeverCutsTheBatchingWaitShort() {
        when(magazine.fire())
                .thenReturn(data("msg1"))
                .thenThrow(new MagazineException(ErrorCode.NOTHING_TO_FIRE, "nothing", null));
        final CapturingHandler handler = new CapturingHandler(true);

        final long started = System.currentTimeMillis();
        // Budget far shorter than the batching wait: the partial batch must still linger the full
        // two seconds the caller asked for.
        batchTask(handler, 10, 2, 50L).run();
        final long elapsed = System.currentTimeMillis() - started;

        assertEquals(List.of(1), handler.batchSizes);
        assertTrue("the run budget must not truncate the caller's batching wait; waited only "
                + elapsed + "ms", elapsed >= 1_900);
    }

    /** Records the shape of every batch the consumer hands over. */
    private static final class CapturingHandler implements MessageHandler<String> {
        private final List<Integer> batchSizes = Collections.synchronizedList(new ArrayList<>());
        private final boolean outcome;

        private CapturingHandler(final boolean outcome) {
            this.outcome = outcome;
        }

        @Override
        public Set<Class<?>> getIgnorableExceptions() {
            return Set.of();
        }

        @Override
        public boolean handle(final String message) {
            return outcome;
        }

        @Override
        public boolean handle(final List<String> messages) {
            batchSizes.add(messages.size());
            return outcome;
        }
    }

    private void firesThen(final String message) {
        when(magazine.fire())
                .thenReturn(data(message))
                .thenThrow(new MagazineException(ErrorCode.NOTHING_TO_FIRE, "nothing", null));
    }

    private MagazineConsumerTask<String> task(final MessageHandler<String> handler) {
        return new MagazineConsumerTask<>(magazine, sidelineMagazine, handler, new ObjectMapper(),
                String.class, consumeTimer, null);
    }

    private static MagazineData<String> data(final String message) {
        return MagazineData.<String>builder()
                .magazineIdentifier("TEST_QUEUE")
                .shard(1)
                .data(message)
                .firePointer(1)
                .build();
    }

    private static MessageHandler<String> handler(final boolean outcome) {
        return new MessageHandler<>() {
            @Override
            public Set<Class<?>> getIgnorableExceptions() {
                return Collections.emptySet();
            }

            @Override
            public boolean handle(final String message) {
                return outcome;
            }

            @Override
            public boolean handle(final List<String> messages) {
                return outcome;
            }
        };
    }

    private static MessageHandler<String> throwingHandler(final Set<Class<?>> ignorable) {
        return new MessageHandler<>() {
            @Override
            public Set<Class<?>> getIgnorableExceptions() {
                return ignorable;
            }

            @Override
            public boolean handle(final String message) {
                throw new IllegalStateException("boom");
            }

            @Override
            public boolean handle(final List<String> messages) {
                throw new IllegalStateException("boom");
            }
        };
    }
}
