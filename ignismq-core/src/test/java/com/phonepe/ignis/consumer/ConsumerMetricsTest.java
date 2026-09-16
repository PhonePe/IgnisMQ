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
import com.phonepe.ignis.metric.IgnisMetrics;
import com.phonepe.ignis.metric.QueueMeters;
import com.phonepe.ignis.scheduler.HandlerExecutor;
import com.phonepe.ignis.utils.Constants;
import com.phonepe.magazine.Magazine;
import com.phonepe.magazine.entity.MagazineData;
import com.phonepe.magazine.exception.ErrorCode;
import com.phonepe.magazine.exception.MagazineException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * The sideline and handler-timeout meters.
 * <p>
 * All three sideline reasons look identical in the data - a message moved to the sideline - and call
 * for completely different responses: a handler rejecting work is a business outcome, a handler
 * throwing is a bug, and a handler timing out means C9's bound fired and a thread may still be
 * running. Without the {@code reason} tag an operator sees one undifferentiated rate.
 */
class ConsumerMetricsTest {

    private static final String QUEUE = "TEST_QUEUE";
    private static final long HANDLER_TIMEOUT_IN_MS = 300L;

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final IgnisMetrics metrics = new IgnisMetrics(registry);
    private final HandlerExecutor handlerExecutor = new HandlerExecutor(4);
    private Magazine<String> magazine;
    private Magazine<String> sidelineMagazine;

    @BeforeEach
    void setUp() {
        magazine = Mockito.mock(Magazine.class);
        sidelineMagazine = Mockito.mock(Magazine.class);
        when(magazine.getMagazineIdentifier()).thenReturn(QUEUE);
    }

    @AfterEach
    void tearDown() {
        handlerExecutor.stop();
    }

    @Test
    @DisplayName("a handler that rejects a message sidelines it as rejected")
    void rejectionIsTaggedAsRejected() {
        firesThen("msg");
        when(sidelineMagazine.load(any())).thenReturn(true);

        task(handler(false)).run();

        assertEquals(1.0, sidelined("rejected"));
        assertEquals(0.0, sidelined("exception"));
    }

    @Test
    @DisplayName("a handler that throws sidelines as exception, not as rejection")
    void anExceptionIsTaggedAsException() {
        firesThen("msg");
        when(sidelineMagazine.load(any())).thenReturn(true);

        task(throwingHandler(new IllegalStateException("boom"))).run();

        assertEquals(1.0, sidelined("exception"));
        assertEquals(0.0, sidelined("rejected"));
    }

    /**
     * The one C9 shipped without a meter. A timeout is a distinct sideline reason and also its own
     * counter, because the handler thread may still be running: this is the signal that a queue is
     * leaking handler threads.
     */
    @Test
    @DisplayName("a handler that times out is counted as a timeout and sidelined as one")
    void aTimeoutIsCountedAndTagged() {
        firesThen("msg");
        when(sidelineMagazine.load(any())).thenReturn(true);

        task(throwingHandler(null)).run();

        assertEquals(1.0, counter(IgnisMetrics.HANDLER_TIMEOUTS, Tags.of(IgnisMetrics.TAG_QUEUE, QUEUE)),
                "a handler timeout must be counted, or C9's bound is invisible whether it fires or not");
        assertEquals(1.0, sidelined("timeout"));
        assertEquals(0.0, sidelined("exception"),
                "a timeout is not an ordinary exception; the operator response is different");
    }

    /**
     * The sideline itself refusing the message is the data-loss-adjacent case: nothing was moved and
     * the source record is deliberately left for the sweeper. It must not be counted as a successful
     * sideline.
     */
    @Test
    @DisplayName("a refused sideline is counted separately from a completed one")
    void aRefusedSidelineIsItsOwnReason() {
        firesThen("msg");
        when(sidelineMagazine.load(any())).thenReturn(false);

        task(handler(false)).run();

        assertEquals(1.0, sidelined("sideline_refused"));
        assertEquals(0.0, sidelined("rejected"),
                "nothing reached the sideline, so it must not read as a completed sideline");
    }

    @Test
    @DisplayName("a delivered message is counted by what finally happened to it")
    void messagesAreCountedByDisposition() {
        firesThen("msg");
        when(sidelineMagazine.load(any())).thenReturn(true);

        task(handler(true)).run();
        assertEquals(1.0, messages(IgnisMetrics.ACKED));

        // fire() is stubbed as a finite sequence, so the second drain needs its own.
        firesThen("msg");
        task(handler(false)).run();
        assertEquals(1.0, messages(IgnisMetrics.SIDELINED));
    }

    /**
     * Empty polls are how an operator sees over-provisioned consumers: threads being handed out to
     * do nothing. Every other queueing system reports the equivalent.
     */
    @Test
    @DisplayName("polls are split by whether they returned work")
    void pollsAreCountedByResult() {
        firesThen("msg");

        task(handler(true)).run();

        assertEquals(1.0, poll(IgnisMetrics.MESSAGE));
        assertEquals(1.0, poll(IgnisMetrics.EMPTY),
                "the drain ends on an empty poll, and that is the signal worth counting");
    }

    /**
     * The handler timer is deliberately narrower than the consume timer, which also covers
     * deserialisation, the delete and any sideline. When a queue slows down, those two together
     * separate "the handler got slower" from "storage got slower"; one timer cannot.
     */
    @Test
    @DisplayName("handler duration is recorded apart from the consume timer")
    void handlerDurationIsRecorded() {
        firesThen("msg");

        task(handler(true)).run();

        assertEquals(1L, registry.get(IgnisMetrics.HANDLER_DURATION)
                .tags(IgnisMetrics.TAG_QUEUE, QUEUE, IgnisMetrics.TAG_OUTCOME, IgnisMetrics.SUCCESS)
                .timer().count());
    }

    /**
     * A single-message consumer hands over exactly one message every time, so a batch-size summary
     * would read a constant 1 - indistinguishable from a batching consumer that never fills. Its
     * absence is the honest signal that this queue does not batch.
     */
    @Test
    @DisplayName("batch size is not published for a consumer that does not batch")
    void batchSizeIsAbsentWithoutBatching() {
        firesThen("msg");

        task(handler(true)).run();

        assertNull(registry.find(IgnisMetrics.HANDLER_BATCH_SIZE)
                .tags(IgnisMetrics.TAG_QUEUE, QUEUE).summary());
    }

    @Test
    @DisplayName("a batching consumer publishes the size of each batch handed over")
    void batchSizeIsRecordedWhenBatching() {
        Mockito.doReturn(data("m1"), data("m2"), data("m3"))
                .doThrow(new MagazineException(ErrorCode.NOTHING_TO_FIRE, "nothing", null))
                .when(magazine).fire();

        batchTask(handler(true), 2).run();

        final DistributionSummary batches = registry.get(IgnisMetrics.HANDLER_BATCH_SIZE)
                .tags(IgnisMetrics.TAG_QUEUE, QUEUE).summary();
        assertEquals(2L, batches.count(), "a full batch of two, then the remainder");
        assertEquals(3.0, batches.totalAmount());
    }

    @Test
    @DisplayName("an ignorable exception is counted as ignored, not as sidelined")
    void anIgnoredMessageIsCountedSeparately() {
        firesThen("msg");

        task(ignorableHandler()).run();

        assertEquals(1.0, messages(IgnisMetrics.IGNORED));
        assertEquals(0.0, messages(IgnisMetrics.SIDELINED),
                "an ignorable exception deletes without sidelining; counting it as sidelined "
                        + "would overstate the sideline rate an operator alerts on");
    }

    /**
     * Saturation means the handler never ran, so the batch is intact and must be preserved, not
     * deleted. Counted under its own reason: "we are out of handler threads" and "your handler
     * rejected this message" demand completely different responses.
     */
    @Test
    @DisplayName("a batch refused for want of a handler thread is sidelined as saturated")
    void saturationIsSidelinedUnderItsOwnReason() throws Exception {
        final HandlerExecutor full = new HandlerExecutor(1);
        final CountDownLatch release = new CountDownLatch(1);
        final Thread hog = occupy(full, release);
        try {
            firesThen("msg");
            when(sidelineMagazine.load(any())).thenReturn(true);

            new MagazineConsumerTask<>(magazine, sidelineMagazine, handler(true), new ObjectMapper(),
                    String.class, null, Constants.CONSUMER_RUN_BUDGET_IN_MS, full,
                    HANDLER_TIMEOUT_IN_MS, new QueueMeters(metrics, QUEUE)).run();

            assertEquals(1.0, sidelined("saturated"));
            assertEquals(0.0, sidelined("rejected"));
            assertEquals(0.0, sidelined("timeout"));
            verify(magazine).delete(any());
        } finally {
            release.countDown();
            hog.join(5_000);
            full.stop();
        }
    }

    private Thread occupy(final HandlerExecutor executor, final CountDownLatch release)
            throws InterruptedException {
        final CountDownLatch occupied = new CountDownLatch(1);
        final Thread hog = new Thread(() -> {
            try {
                executor.call(() -> {
                    occupied.countDown();
                    release.await();
                    return true;
                }, 60_000);
            } catch (Exception ignored) {
                // the hog's own outcome is not what this test is about
            }
        });
        hog.start();
        assertTrue(occupied.await(10, java.util.concurrent.TimeUnit.SECONDS));
        return hog;
    }

    private double messages(final String outcome) {
        return counter(IgnisMetrics.MESSAGES,
                Tags.of(IgnisMetrics.TAG_QUEUE, QUEUE, IgnisMetrics.TAG_OUTCOME, outcome));
    }

    private double poll(final String result) {
        return counter(IgnisMetrics.POLL,
                Tags.of(IgnisMetrics.TAG_QUEUE, QUEUE, IgnisMetrics.TAG_RESULT, result));
    }

    private static MessageHandler<String> ignorableHandler() {
        return new MessageHandler<>() {
            @Override
            public Set<Class<?>> getIgnorableExceptions() {
                return Set.of(IllegalStateException.class);
            }

            @Override
            public boolean handle(final String message) {
                throw new IllegalStateException("ignorable");
            }

            @Override
            public boolean handle(final List<String> messages) {
                throw new IllegalStateException("ignorable");
            }
        };
    }

    private double sidelined(final String reason) {
        return counter(IgnisMetrics.SIDELINE,
                Tags.of(IgnisMetrics.TAG_QUEUE, QUEUE, IgnisMetrics.TAG_REASON, reason));
    }

    private double counter(final String name, final Tags tags) {
        final Counter counter = registry.find(name).tags(tags).counter();
        return counter == null ? 0.0 : counter.count();
    }

    /**
     * Stubbed with doReturn rather than when(...), so a test that drains twice can re-stub without
     * the re-stubbing call itself triggering the NOTHING_TO_FIRE it just installed.
     */
    private void firesThen(final String message) {
        Mockito.doReturn(MagazineData.<String>builder().magazineIdentifier(QUEUE).shard(1)
                        .data(message).firePointer(1).build())
                .doThrow(new MagazineException(ErrorCode.NOTHING_TO_FIRE, "nothing", null))
                .when(magazine).fire();
    }

    private MagazineConsumerTask<String> batchTask(final MessageHandler<String> handler,
                                                   final int maxBatchSize) {
        return new MagazineConsumerTask<>(magazine, sidelineMagazine, handler, new ObjectMapper(),
                String.class, BatchingConfig.builder().maxBatchSize(maxBatchSize)
                .maxWaitTimeInSecs(1).build(), Constants.CONSUMER_RUN_BUDGET_IN_MS, handlerExecutor,
                HANDLER_TIMEOUT_IN_MS, new QueueMeters(metrics, QUEUE));
    }

    private static MagazineData<String> data(final String message) {
        return MagazineData.<String>builder().magazineIdentifier(QUEUE).shard(1)
                .data(message).firePointer(1).build();
    }

    private MagazineConsumerTask<String> task(final MessageHandler<String> handler) {
        return new MagazineConsumerTask<>(magazine, sidelineMagazine, handler, new ObjectMapper(),
                String.class, null, Constants.CONSUMER_RUN_BUDGET_IN_MS, handlerExecutor,
                HANDLER_TIMEOUT_IN_MS, new QueueMeters(metrics, QUEUE));
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

    /** A null failure means "hang past the timeout" rather than "throw". */
    private static MessageHandler<String> throwingHandler(final RuntimeException failure) {
        return new MessageHandler<>() {
            @Override
            public Set<Class<?>> getIgnorableExceptions() {
                return Collections.emptySet();
            }

            @Override
            public boolean handle(final String message) {
                return handle(List.of(message));
            }

            @Override
            public boolean handle(final List<String> messages) {
                if (failure != null) {
                    throw failure;
                }
                try {
                    Thread.sleep(HANDLER_TIMEOUT_IN_MS * 20);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return true;
            }
        };
    }
}
