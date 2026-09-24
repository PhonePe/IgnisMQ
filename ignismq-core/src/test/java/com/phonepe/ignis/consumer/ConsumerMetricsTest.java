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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
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

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * The sideline and handler-timeout meters.
 * <p>
 * All three sideline reasons look identical in the data - a message moved to the sideline - and call
 * for completely different responses: a handler rejecting work is a business outcome, a handler
 * throwing is a bug, and a handler timing out means the handler-execution bound fired and a thread
 * may still be running. Without the {@code reason} tag an operator sees one undifferentiated rate.
 */
class ConsumerMetricsTest {

    private static final String QUEUE = "TEST_QUEUE";
    private static final long HANDLER_TIMEOUT_IN_MS = 300L;

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final IgnisMetrics metrics = new IgnisMetrics(registry);
    private final HandlerExecutor handlerExecutor = new HandlerExecutor(4);
    private Magazine<String> magazine;
    private Magazine<String> sidelineMagazine;
    /** Holds {@link #hangingHandler()} open; released in tearDown so no handler thread outlives a test. */
    private final CountDownLatch hold = new CountDownLatch(1);

    @BeforeEach
    void setUp() {
        magazine = mock(Magazine.class);
        sidelineMagazine = mock(Magazine.class);
        when(magazine.getMagazineIdentifier()).thenReturn(QUEUE);
    }

    @AfterEach
    void tearDown() {
        // Before stop(), so a hung handler is freed by this test rather than by the interrupt that
        // stop() issues. If interruption ever stopped working, that is a failing assertion
        // elsewhere rather than a thread leaking into the rest of the suite.
        hold.countDown();
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
     * The one sideline reason that shipped without a meter. A timeout is a distinct sideline reason
     * and also its own counter, because the handler thread may still be running: this is the signal
     * that a queue is leaking handler threads.
     */
    @Test
    @DisplayName("a handler that times out is counted as a timeout and sidelined as one")
    void aTimeoutIsCountedAndTagged() {
        firesThen("msg");
        when(sidelineMagazine.load(any())).thenReturn(true);

        task(hangingHandler()).run();

        assertEquals(1.0, counter(IgnisMetrics.HANDLER_TIMEOUTS, Tags.of(IgnisMetrics.TAG_QUEUE, QUEUE)),
                "a handler timeout must be counted, or the bound is invisible whether it fires or not");
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
        doReturn(stringData("m1"), stringData("m2"), stringData("m3"))
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

    /**
     * A payload nobody can read and a handler that threw are both failures, and until now both were
     * counted as {@code reason=exception}. They mean different things: an unreadable payload is
     * almost always a publisher and consumer disagreeing about the format - a deploy skew, fixed by
     * rolling one of them - whereas a handler that threw is a bug in the handler. An operator
     * alerting on one rate cannot tell which they are looking at.
     */
    @Test
    @DisplayName("a payload that will not deserialise is sidelined as unreadable, not as an exception")
    void anUnreadablePayloadIsSidelinedUnderItsOwnReason() {
        firesUnreadableThen();
        when(sidelineMagazine.load(any())).thenReturn(true);

        integerTask(integerHandler()).run();

        assertEquals(1.0, sidelined(IgnisMetrics.REASON_UNREADABLE));
        assertEquals(0.0, sidelined(IgnisMetrics.REASON_EXCEPTION),
                "the handler never ran, so nothing here is a handler exception");
    }

    /**
     * The converse, and the reason the reason cannot be derived from the exception type: a handler
     * is free to do its own parsing and throw {@link com.fasterxml.jackson.core.JsonProcessingException}
     * from inside {@code handle}. That is a handler bug like any other, and keying the reason off the
     * exception class would relabel it as a payload ignisMQ could not read.
     */
    @Test
    @DisplayName("a handler that throws a Jackson exception is still an exception, not unreadable")
    void aHandlerThrowingAJacksonExceptionIsStillCountedAsAnException() {
        firesThen("msg");
        when(sidelineMagazine.load(any())).thenReturn(true);

        task(jacksonThrowingHandler()).run();

        assertEquals(1.0, sidelined(IgnisMetrics.REASON_EXCEPTION));
        assertEquals(0.0, sidelined(IgnisMetrics.REASON_UNREADABLE),
                "ignisMQ read this payload perfectly well; the handler is what failed");
    }

    /**
     * The new reason must not quietly turn an ignorable exception into a sideline. A handler that
     * declares the deserialisation failure ignorable is asking for the message to be dropped, and
     * that answer is given before any reason is chosen.
     */
    @Test
    @DisplayName("an unreadable payload the handler declares ignorable is still dropped, not sidelined")
    void anIgnorableUnreadablePayloadIsStillDropped() {
        firesUnreadableThen();

        integerTask(ignorableIntegerHandler()).run();

        assertEquals(1.0, messages(IgnisMetrics.IGNORED));
        assertEquals(0.0, sidelined(IgnisMetrics.REASON_UNREADABLE));
        verify(sidelineMagazine, never()).load(any());
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

    private void firesUnreadableThen() {
        doReturn(data("not-valid-json{{{"))
                .doThrow(new MagazineException(ErrorCode.NOTHING_TO_FIRE, "nothing", null))
                .when(magazine).fire();
    }

    /**
     * Integer-typed, because a String-typed consumer hands the payload over raw and has nothing to
     * fail at.
     */
    private MagazineConsumerTask<Integer> integerTask(final MessageHandler<Integer> handler) {
        return new MagazineConsumerTask<>(magazine, sidelineMagazine, handler, new ObjectMapper(),
                Integer.class, null, Constants.CONSUMER_RUN_BUDGET_IN_MS, handlerExecutor,
                HANDLER_TIMEOUT_IN_MS, new QueueMeters(metrics, QUEUE));
    }

    private static MessageHandler<Integer> integerHandler() {
        return integerHandler(Collections.emptySet());
    }

    private static MessageHandler<Integer> ignorableIntegerHandler() {
        return integerHandler(Set.of(JsonProcessingException.class));
    }

    private static MessageHandler<Integer> integerHandler(final Set<Class<?>> ignorable) {
        return new MessageHandler<>() {
            @Override
            public Set<Class<?>> getIgnorableExceptions() {
                return ignorable;
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
    }

    /**
     * Throws Jackson's own exception from inside the handler, where ignisMQ's reader is not involved.
     * <p>
     * {@code handle} declares no checked exception, so this needs a sneaky throw to escape - which is
     * exactly what {@code @SneakyThrows} on a handler that does its own {@code readValue} produces,
     * and that is a common shape rather than a contrived one.
     */
    private static MessageHandler<String> jacksonThrowingHandler() {
        return new MessageHandler<>() {
            @Override
            public Set<Class<?>> getIgnorableExceptions() {
                return Collections.emptySet();
            }

            @Override
            public boolean handle(final String message) {
                return sneakyThrow(new JsonParseException(null, "the handler parsed, and failed"));
            }

            @Override
            public boolean handle(final List<String> messages) {
                return sneakyThrow(new JsonParseException(null, "the handler parsed, and failed"));
            }
        };
    }

    @SuppressWarnings("unchecked")
    private static <E extends Throwable> boolean sneakyThrow(final Throwable t) throws E {
        throw (E) t;
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
        doReturn(stringData(message))
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

    /**
     * A record holding exactly the text given. This is the raw storage form - use it for payloads
     * already in their stored shape (a JSON number for an Integer queue, or deliberately corrupt
     * text) and {@link #stringData} for a String queue, whose stored form is quoted.
     */
    private static MagazineData<String> data(final String message) {
        return MagazineData.<String>builder().magazineIdentifier(QUEUE).shard(1)
                .data(message).firePointer(1).build();
    }

    /**
     * A String-queue record in the form {@code publish()} would actually have stored it: JSON
     * encoded, so {@code m1} is held as {@code "m1"}. The consumer decodes every payload, so bare
     * text staged here would be read as corruption rather than as a message.
     */
    private static MagazineData<String> stringData(final String message) {
        try {
            return data(new ObjectMapper().writeValueAsString(message));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("could not stage " + message, e);
        }
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
                throw failure;
            }
        };
    }

    /** Blocks past {@link #HANDLER_TIMEOUT_IN_MS} so the executor's bound is the thing that fires. */
    private MessageHandler<String> hangingHandler() {
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
                try {
                    hold.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return true;
            }
        };
    }
}
