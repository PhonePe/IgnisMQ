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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.phonepe.ignis.common.MessageHandler;
import com.phonepe.ignis.config.BatchingConfig;
import com.phonepe.ignis.metric.QueueMeters;
import com.phonepe.ignis.scheduler.HandlerExecutor;
import com.phonepe.ignis.util.TestMessageHandler;
import com.phonepe.ignis.utils.Constants;
import com.phonepe.magazine.Magazine;
import com.phonepe.magazine.entity.MagazineData;
import com.phonepe.magazine.exception.ErrorCode;
import com.phonepe.magazine.exception.MagazineException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Covers the transfer-then-delete contract of the consumer: a message may only leave the main
 * magazine once the handler has accepted it or the sideline magazine has taken responsibility for it.
 */
public class MagazineConsumerTaskTest {

    private Magazine<String> magazine;
    private Magazine<String> sidelineMagazine;
    private static final long HANDLER_TIMEOUT_IN_MS = 30_000L;

    private final HandlerExecutor handlerExecutor = new HandlerExecutor(4);
    private QueueMeters meters;

    @BeforeEach
    public void setUp() {
        magazine = Mockito.mock(Magazine.class);
        sidelineMagazine = Mockito.mock(Magazine.class);
        meters = QueueMeters.unmetered("TEST_QUEUE");
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
     * The old depth check used {@code <=}, so a queue holding exactly one full batch was judged
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
        assertTrue(elapsed < 5_000, "a full batch must not wait for the batching deadline; waited " + elapsed + "ms");
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
        assertTrue(elapsed >= 900, "must wait for the deadline, waited only " + elapsed + "ms");
        assertTrue(elapsed < 3_000, "must not overshoot the deadline, waited " + elapsed + "ms");
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
    @Test
    @Timeout(value = 30000, unit = java.util.concurrent.TimeUnit.MILLISECONDS)
    public void testABackloggedBatchingConsumerReturnsAtItsDeadline() {
        when(magazine.fire()).thenAnswer(invocation -> data("endless"));
        final CapturingHandler handler = new CapturingHandler(true);

        final long started = System.currentTimeMillis();
        batchTask(handler, 2, 1, 1_000L).run();
        final long elapsed = System.currentTimeMillis() - started;

        assertTrue(elapsed < 15_000, "a backlogged consumer must hand its thread back; ran for " + elapsed + "ms");
        assertTrue(!handler.batchSizes.isEmpty(), "it must still have done real work before yielding");
    }

    /**
     * The same unbounded drain in the non-batching path, which had no deadline at all: it looped
     * while {@code fire()} kept returning.
     */
    @Test
    @Timeout(value = 30000, unit = java.util.concurrent.TimeUnit.MILLISECONDS)
    public void testABackloggedSingleMessageConsumerReturnsAtItsBudget() {
        when(magazine.fire()).thenAnswer(invocation -> data("endless"));

        final long started = System.currentTimeMillis();
        // A one-second budget rather than the production thirty: the property under test is that
        // the loop terminates on the budget at all, and asserting it against the real value would
        // cost this suite thirty seconds to learn nothing extra.
        new MagazineConsumerTask<>(magazine, sidelineMagazine, handler(true), new ObjectMapper(),
                String.class, null, 1_000L, handlerExecutor, HANDLER_TIMEOUT_IN_MS, meters).run();
        final long elapsed = System.currentTimeMillis() - started;

        assertTrue(elapsed < 15_000, "a backlogged consumer must hand its thread back; ran for " + elapsed + "ms");
        verify(magazine, atLeastOnce()).delete(any());
    }

    /**
     * The production default is what an unconfigured consumer actually gets.
     */
    @Test
    public void testTheDefaultRunBudgetIsTheProductionConstant() {
        assertEquals(30_000L, Constants.CONSUMER_RUN_BUDGET_IN_MS);
    }

    /**
     * The budget bounds the turn; it must never cost a delivery. A message is claimed the moment
     * {@code fire()} returns it, so yielding before consuming it would strand it below the fire
     * pointer with no consumer coming - recoverable only by the sweeper, one sweepDuration later.
     */
    @Test
    @Timeout(value = 30000, unit = java.util.concurrent.TimeUnit.MILLISECONDS)
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
                String.class, BatchingConfig.builder()
                .maxBatchSize(maxBatchSize).maxWaitTimeInSecs(maxWaitSeconds).build(),
                Constants.CONSUMER_RUN_BUDGET_IN_MS, handlerExecutor, HANDLER_TIMEOUT_IN_MS, meters);
    }

    /**
     * A handler that never returns must not hold the thread, and the batch it was given must be
     * sidelined rather than lost or silently deleted.
     */
    @Test
    @Timeout(value = 60000, unit = java.util.concurrent.TimeUnit.MILLISECONDS)
    public void testATimedOutHandlerSidelinesTheBatchAndReleasesTheThread() throws Exception {
        firesThen("msg1");
        when(sidelineMagazine.load(any())).thenReturn(true);
        final HandlerExecutor executor = new HandlerExecutor(4);
        final CountDownLatch release = new CountDownLatch(1);
        final MessageHandler<String> hanging = new MessageHandler<>() {
            @Override
            public Set<Class<?>> getIgnorableExceptions() {
                return Collections.emptySet();
            }

            @Override
            public boolean handle(String message) throws Exception {
                return handle(List.of(message));
            }

            @Override
            public boolean handle(List<String> messages) throws Exception {
                release.await();
                return true;
            }
        };

        try {
            final long started = System.currentTimeMillis();
            new MagazineConsumerTask<>(magazine, sidelineMagazine, hanging, new ObjectMapper(),
                    String.class, null, 1_000L, executor, 300L, meters).run();
            final long elapsed = System.currentTimeMillis() - started;

            assertTrue(elapsed < 20_000, "the consumer must not wait out a hung handler; waited " + elapsed + "ms");
            verify(sidelineMagazine, atLeastOnce()).load(any());
            verify(magazine, atLeastOnce()).delete(any());
        } finally {
            release.countDown();
            executor.stop();
        }
    }

    /**
     * A timeout must never be treated as ignorable, whatever the handler declares. "Ignorable"
     * means "delete without sidelining", and a timeout says nothing about whether the work was
     * done - the handler may still be running. Applying it would destroy the only copy on a guess.
     */
    @Test
    @Timeout(value = 60000, unit = java.util.concurrent.TimeUnit.MILLISECONDS)
    public void testATimeoutIsNeverIgnorableEvenWhenTheHandlerSaysSo() throws Exception {
        firesThen("msg1");
        when(sidelineMagazine.load(any())).thenReturn(true);
        final HandlerExecutor executor = new HandlerExecutor(4);
        final CountDownLatch release = new CountDownLatch(1);
        final MessageHandler<String> hanging = new MessageHandler<>() {
            @Override
            public Set<Class<?>> getIgnorableExceptions() {
                // Declares the broadest possible ignorable set.
                return Set.of(Exception.class, RuntimeException.class);
            }

            @Override
            public boolean handle(String message) throws Exception {
                return handle(List.of(message));
            }

            @Override
            public boolean handle(List<String> messages) throws Exception {
                release.await();
                return true;
            }
        };

        try {
            new MagazineConsumerTask<>(magazine, sidelineMagazine, hanging, new ObjectMapper(),
                    String.class, null, 1_000L, executor, 300L, meters).run();

            verify(sidelineMagazine, atLeastOnce()).load(any());
        } finally {
            release.countDown();
            executor.stop();
        }
    }

    private MagazineConsumerTask<String> batchTask(final MessageHandler<String> handler,
                                                   final int maxBatchSize, final int maxWaitSeconds,
                                                   final long runBudgetMillis) {
        return new MagazineConsumerTask<>(magazine, sidelineMagazine, handler, new ObjectMapper(),
                String.class, BatchingConfig.builder()
                .maxBatchSize(maxBatchSize).maxWaitTimeInSecs(maxWaitSeconds).build(), runBudgetMillis,
                handlerExecutor, HANDLER_TIMEOUT_IN_MS, meters);
    }

    /**
     * The run budget must never cut a linger short. A consumer may be configured to wait up to 300
     * seconds for a batch to fill, which is far beyond the default budget, and the caller's stated
     * latency has to win - so the budget is the greater of the two, not the batching wait alone.
     */
    @Test
    @Timeout(value = 30000, unit = java.util.concurrent.TimeUnit.MILLISECONDS)
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
        assertTrue(elapsed >= 1_900, "the run budget must not truncate the caller's batching wait; waited only "
                + elapsed + "ms");
    }

    /**
     * Moved from IgnisMQManagerTest, which built a manager, a scheduler pool and an Aerospike
     * container per test to exercise a task that needs none of them. These use one mock as both the
     * main and the sideline magazine, so a load and a delete land on the same counter.
     */
    @Test
    public void testAMixedDrainDeletesAcceptedAndSidelinesRejectedMessages() {
        final Magazine<String> selfSidelining = Mockito.mock(Magazine.class);
        when(selfSidelining.load(any())).thenReturn(true);
        when(selfSidelining.fire())
                .thenReturn(data("true"), data("false"), data(null), null);

        selfSidelinedTask(selfSidelining, new TestMessageHandler(), String.class).run();

        verify(selfSidelining, times(4)).fire();
        verify(selfSidelining, times(1)).load(any());
        verify(selfSidelining, times(3)).delete(any());
    }

    /**
     * A non-String type goes through the mapper on the way in, which the String path skips
     * entirely.
     */
    @Test
    public void testANonStringMessageIsDeserialisedBeforeReachingTheHandler() {
        final Magazine<String> selfSidelining = Mockito.mock(Magazine.class);
        when(selfSidelining.load(any())).thenReturn(true);
        when(selfSidelining.fire())
                .thenReturn(data("1"), data("1"), data("0"), data(null), null);

        final MessageHandler<Integer> positiveOnly = new MessageHandler<>() {
            @Override
            public Set<Class<?>> getIgnorableExceptions() {
                return Set.of();
            }

            @Override
            public boolean handle(final Integer message) {
                return message > 0;
            }

            @Override
            public boolean handle(final List<Integer> messages) {
                return messages.stream().allMatch(this::handle);
            }
        };

        selfSidelinedTask(selfSidelining, positiveOnly, Integer.class).run();

        verify(selfSidelining, times(5)).fire();
        verify(selfSidelining, times(1)).load(any());
        verify(selfSidelining, times(4)).delete(any());
    }

    /**
     * Seven messages at a batch size of three: two full batches and a remainder held until the
     * batching deadline.
     * <p>
     * C2 changed how the consumer decides it is ready. It no longer reads magazine depth first -
     * the messages themselves are the signal - so the exact number of {@code fire()} calls is now a
     * function of the poll interval and not something worth pinning. What the messages did is.
     */
    @Test
    public void testABatchedDrainSidelinesOnlyTheRejectedBatch() {
        final Magazine<String> selfSidelining = Mockito.mock(Magazine.class);
        when(selfSidelining.load(any())).thenReturn(true);
        when(selfSidelining.fire()).thenReturn(data("true"), data("false"), data("true"),
                data("true"), data("true"), data("true"), data(null), null);

        new MagazineConsumerTask<>(selfSidelining, selfSidelining, new TestMessageHandler(),
                new ObjectMapper(), String.class,
                BatchingConfig.builder().maxBatchSize(3).maxWaitTimeInSecs(1).build(),
                Constants.CONSUMER_RUN_BUDGET_IN_MS, handlerExecutor, HANDLER_TIMEOUT_IN_MS,
                meters).run();

        // The rejected batch is sidelined message by message; both accepted batches are deleted.
        verify(selfSidelining, times(3)).load(any());
        verify(selfSidelining, times(7)).delete(any());
        // Depth is never consulted: that read was one per consumer per wait, with nothing to do.
        verify(selfSidelining, never()).getMetaData();
    }

    /**
     * Anything other than NOTHING_TO_FIRE means the magazine could not answer, so the drain stops
     * and the next scheduled run retries rather than spinning on a failing backend.
     */
    @Test
    public void testAFiringFailureStopsTheDrain() {
        when(magazine.fire())
                .thenThrow(new MagazineException(ErrorCode.INTERNAL_ERROR, "some error", null))
                .thenReturn(null);

        task(handler(true)).run();

        verify(magazine, times(1)).fire();
    }

    /** The same stop, for a failure that is not a MagazineException at all. */
    @Test
    public void testAnUnexpectedFiringFailureAlsoStopsTheDrain() {
        when(magazine.fire())
                .thenThrow(new RuntimeException("generic error"))
                .thenReturn(null);

        task(handler(true)).run();

        verify(magazine, times(1)).fire();
    }

    /**
     * B2: RETRIES_EXHAUSTED means the claim itself may not have completed, so there is no message
     * in hand to retire - deleting anything here would destroy a record nobody has seen.
     */
    @Test
    public void testRetriesExhaustedStopsTheDrainWithoutDeletingData() {
        when(magazine.fire()).thenThrow(new MagazineException(ErrorCode.RETRIES_EXHAUSTED,
                "data may remain", null));

        task(handler(true)).run();

        verify(magazine).fire();
        verify(magazine, never()).delete(any());
    }

    /** The accepting counterpart of testThrowingHandlerDoesNotDeleteWhenSidelineRefuses. */
    @Test
    public void testThrowingHandlerDeletesOnceTheSidelineHasAccepted() {
        firesThen("msg1");
        when(sidelineMagazine.load("msg1")).thenReturn(true);

        task(throwingHandler(Collections.emptySet())).run();

        verify(sidelineMagazine, times(1)).load(any());
        verify(magazine, times(1)).delete(any());
    }

    /**
     * A payload that will not deserialise is handled per message, before the batch reaches the
     * handler. The handler declares JsonProcessingException ignorable, so the message is deleted
     * there and then filtered out - and the now-empty batch is accepted, which deletes it a second
     * time. Deleting an already-deleted record is a no-op, but the second call is real and pinning
     * it is what makes a change to that path visible.
     */
    @Test
    public void testAnUndeserialisableMessageIsDroppedBeforeTheHandlerSeesIt() {
        final Magazine<String> selfSidelining = Mockito.mock(Magazine.class);
        when(selfSidelining.fire()).thenReturn(data("not-valid-json{{{"), (MagazineData<String>) null);

        final MessageHandler<Integer> handler = new MessageHandler<>() {
            @Override
            public Set<Class<?>> getIgnorableExceptions() {
                return Set.of(JsonProcessingException.class);
            }

            @Override
            public boolean handle(final Integer message) {
                return true;
            }

            @Override
            public boolean handle(final List<Integer> messages) {
                return true;
            }
        };

        selfSidelinedTask(selfSidelining, handler, Integer.class).run();

        verify(selfSidelining, times(2)).delete(any());
        verify(selfSidelining, never()).load(any());
    }

    private <T> MagazineConsumerTask<T> selfSidelinedTask(final Magazine<String> selfSidelining,
                                                          final MessageHandler<T> handler,
                                                          final Class<T> clazz) {
        return new MagazineConsumerTask<>(selfSidelining, selfSidelining, handler, new ObjectMapper(),
                clazz, null, Constants.CONSUMER_RUN_BUDGET_IN_MS, handlerExecutor,
                HANDLER_TIMEOUT_IN_MS, meters);
    }

    /**
     * Records the shape of every batch the consumer hands over.
     */
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
                String.class, null, Constants.CONSUMER_RUN_BUDGET_IN_MS,
                handlerExecutor, HANDLER_TIMEOUT_IN_MS, meters);
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
