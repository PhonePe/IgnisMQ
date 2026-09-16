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

package com.phonepe.ignis.metric;

import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * The per-queue meters, and the cost of recording one event.
 */
class QueueMetersTest {

    private static final String QUEUE = "Q";

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();

    @Test
    @DisplayName("recording an event resolves no meter, because every meter is resolved at construction")
    void recordingResolvesNothing() {
        final IgnisMetrics metrics = Mockito.spy(new IgnisMetrics(registry));
        final QueueMeters meters = new QueueMeters(metrics, QUEUE);
        // The batch-size summary is deliberately resolved on first use rather than at construction,
        // so that a queue which never batches never publishes it. One call settles it.
        meters.handlerBatch(1);
        Mockito.clearInvocations(metrics);

        // Everything on the message path, called the way the consumer calls it.
        for (int i = 0; i < 3; i++) {
            meters.recordPublish(1L, false);
            meters.recordPublish(1L, true);
            meters.recordConsume(() -> {
            });
            meters.polled(true);
            meters.polled(false);
            meters.acked(2);
            meters.ignored();
            meters.budgetExhausted();
            meters.handlerBatch(2);
            meters.handlerTimedOut();
            meters.shovelMoved();
            meters.sidelined(IgnisMetrics.REASON_REJECTED);
            meters.sidelined(IgnisMetrics.REASON_EXCEPTION);
            meters.sidelined(IgnisMetrics.REASON_TIMEOUT);
            meters.sidelined(IgnisMetrics.REASON_SATURATED);
            meters.sidelineRefused(IgnisMetrics.REASON_SIDELINE_REFUSED);
            meters.recordHandler(1L, IgnisMetrics.SUCCESS);
            meters.recordHandler(1L, IgnisMetrics.FAILURE);
            meters.recordHandler(1L, IgnisMetrics.REASON_TIMEOUT);
            meters.recordHandler(1L, IgnisMetrics.REASON_SATURATED);
            meters.recordShovel(1L, IgnisMetrics.SUCCESS);
            meters.recordShovel(1L, IgnisMetrics.FAILURE);
        }

        verify(metrics, never()).counter(anyString(), any());
        verify(metrics, never()).timer(anyString(), any());
        verify(metrics, never()).summary(anyString(), any());
    }

    @Test
    @DisplayName("a reason outside the known set still records, rather than failing or being dropped")
    void anUnknownReasonStillRecords() {
        final QueueMeters meters = new QueueMeters(new IgnisMetrics(registry), QUEUE);

        meters.sidelined("something_nobody_declared");
        meters.recordHandler(5L, "an_outcome_nobody_declared");

        assertEquals(1.0, registry.get(IgnisMetrics.SIDELINE)
                .tags(IgnisMetrics.TAG_QUEUE, QUEUE, IgnisMetrics.TAG_REASON, "something_nobody_declared")
                .counter().count());
        assertEquals(5.0, registry.get(IgnisMetrics.HANDLER_DURATION)
                .tags(IgnisMetrics.TAG_QUEUE, QUEUE, IgnisMetrics.TAG_OUTCOME, "an_outcome_nobody_declared")
                .timer().totalTime(TimeUnit.NANOSECONDS));
    }

    @Test
    @DisplayName("every meter still carries its queue tag and its own value")
    void meterValuesAreUnchanged() {
        final QueueMeters meters = new QueueMeters(new IgnisMetrics(registry), QUEUE);

        meters.polled(true);
        meters.polled(false);
        meters.polled(false);
        meters.acked(4);
        meters.ignored();
        meters.budgetExhausted();
        meters.handlerBatch(7);
        meters.handlerTimedOut();
        meters.shovelMoved();
        meters.sidelined(IgnisMetrics.REASON_REJECTED);
        meters.sidelineRefused(IgnisMetrics.REASON_SIDELINE_REFUSED);

        final Tags queue = Tags.of(IgnisMetrics.TAG_QUEUE, QUEUE);
        assertEquals(1.0, registry.get(IgnisMetrics.POLL).tags(queue)
                .tags(IgnisMetrics.TAG_RESULT, IgnisMetrics.MESSAGE).counter().count());
        assertEquals(2.0, registry.get(IgnisMetrics.POLL).tags(queue)
                .tags(IgnisMetrics.TAG_RESULT, IgnisMetrics.EMPTY).counter().count());
        assertEquals(4.0, registry.get(IgnisMetrics.MESSAGES).tags(queue)
                .tags(IgnisMetrics.TAG_OUTCOME, IgnisMetrics.ACKED).counter().count());
        assertEquals(1.0, registry.get(IgnisMetrics.MESSAGES).tags(queue)
                .tags(IgnisMetrics.TAG_OUTCOME, IgnisMetrics.IGNORED).counter().count());
        assertEquals(1.0, registry.get(IgnisMetrics.MESSAGES).tags(queue)
                .tags(IgnisMetrics.TAG_OUTCOME, IgnisMetrics.SIDELINED).counter().count());
        assertEquals(1.0, registry.get(IgnisMetrics.BUDGET_EXHAUSTED).tags(queue).counter().count());
        assertEquals(7.0, registry.get(IgnisMetrics.HANDLER_BATCH_SIZE).tags(queue).summary().totalAmount());
        assertEquals(1.0, registry.get(IgnisMetrics.HANDLER_TIMEOUTS).tags(queue).counter().count());
        assertEquals(1.0, registry.get(IgnisMetrics.SHOVEL_MOVED).tags(queue).counter().count());
        assertEquals(1.0, registry.get(IgnisMetrics.SIDELINE).tags(queue)
                .tags(IgnisMetrics.TAG_REASON, IgnisMetrics.REASON_REJECTED).counter().count());
        assertEquals(1.0, registry.get(IgnisMetrics.SIDELINE).tags(queue)
                .tags(IgnisMetrics.TAG_REASON, IgnisMetrics.REASON_SIDELINE_REFUSED).counter().count());
    }

    @Test
    @DisplayName("a disabled queue publishes nothing, including the meters resolved at construction")
    void disabledPublishesNothing() {
        final QueueMeters meters = new QueueMeters(new IgnisMetrics(registry, false), QUEUE);

        meters.polled(true);
        meters.acked(1);
        meters.handlerBatch(3);
        meters.sidelined(IgnisMetrics.REASON_REJECTED);
        meters.recordHandler(1L, IgnisMetrics.SUCCESS);

        assertEquals(0, registry.getMeters().size(),
                "resolving meters eagerly must not resolve them against the caller's registry");
    }
}
