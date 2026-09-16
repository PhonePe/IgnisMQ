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
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;

import java.util.concurrent.TimeUnit;
import java.util.function.ToDoubleFunction;

/**
 * Every meter belonging to one queue, resolved once at construction.
 * <p>
 * The queue's collaborators take this rather than a registry and a name, so none of them builds a
 * meter, holds a tag set or decides whether instrumentation is on. {@code Tags.and} allocates and
 * sorts, which is why the per-message tag sets are fields.
 */
public final class QueueMeters {

    private final IgnisMetrics metrics;
    private final Tags queue;
    private final Timer publishSuccess;
    private final Timer publishFailure;
    private final Timer consume;
    private final Tags pollMessage;
    private final Tags pollEmpty;
    private final Tags acked;
    private final Tags sidelined;
    private final Tags ignored;

    public QueueMeters(final IgnisMetrics metrics, final String queueName) {
        this.metrics = metrics;
        this.queue = Tags.of(IgnisMetrics.TAG_QUEUE, queueName);
        this.publishSuccess = metrics.timer(IgnisMetrics.PUBLISH,
                queue.and(IgnisMetrics.TAG_OUTCOME, IgnisMetrics.SUCCESS));
        this.publishFailure = metrics.timer(IgnisMetrics.PUBLISH,
                queue.and(IgnisMetrics.TAG_OUTCOME, IgnisMetrics.FAILURE));
        this.consume = metrics.timer(IgnisMetrics.CONSUME, queue);
        this.pollMessage = queue.and(IgnisMetrics.TAG_RESULT, IgnisMetrics.MESSAGE);
        this.pollEmpty = queue.and(IgnisMetrics.TAG_RESULT, IgnisMetrics.EMPTY);
        this.acked = queue.and(IgnisMetrics.TAG_OUTCOME, IgnisMetrics.ACKED);
        this.sidelined = queue.and(IgnisMetrics.TAG_OUTCOME, IgnisMetrics.SIDELINED);
        this.ignored = queue.and(IgnisMetrics.TAG_OUTCOME, IgnisMetrics.IGNORED);
    }

    /** For collaborators built outside a manager, which in practice means tests. */
    public static QueueMeters unmetered(final String queueName) {
        return new QueueMeters(new IgnisMetrics(new CompositeMeterRegistry(), false), queueName);
    }

    public void recordPublish(final long nanos, final boolean failed) {
        (failed ? publishFailure : publishSuccess).record(nanos, TimeUnit.NANOSECONDS);
    }

    public void recordConsume(final Runnable consumption) {
        consume.record(consumption);
    }

    public void polled(final boolean gotMessage) {
        metrics.counter(IgnisMetrics.POLL, gotMessage ? pollMessage : pollEmpty).increment();
    }

    public void acked(final int count) {
        metrics.counter(IgnisMetrics.MESSAGES, acked).increment(count);
    }

    public void ignored() {
        metrics.counter(IgnisMetrics.MESSAGES, ignored).increment();
    }

    public void sidelined(final String reason) {
        metrics.counter(IgnisMetrics.SIDELINE, queue.and(IgnisMetrics.TAG_REASON, reason)).increment();
        metrics.counter(IgnisMetrics.MESSAGES, sidelined).increment();
    }

    /** The payload is still in the main magazine, so this is not a completed sideline. */
    public void sidelineRefused(final String reason) {
        metrics.counter(IgnisMetrics.SIDELINE, queue.and(IgnisMetrics.TAG_REASON, reason)).increment();
    }

    public void budgetExhausted() {
        metrics.counter(IgnisMetrics.BUDGET_EXHAUSTED, queue).increment();
    }

    public void handlerBatch(final int size) {
        metrics.summary(IgnisMetrics.HANDLER_BATCH_SIZE, queue).record(size);
    }

    public void handlerTimedOut() {
        metrics.counter(IgnisMetrics.HANDLER_TIMEOUTS, queue).increment();
    }

    public void recordHandler(final long nanos, final String outcome) {
        metrics.timer(IgnisMetrics.HANDLER_DURATION, queue.and(IgnisMetrics.TAG_OUTCOME, outcome))
                .record(nanos, TimeUnit.NANOSECONDS);
    }

    public void shovelMoved() {
        metrics.counter(IgnisMetrics.SHOVEL_MOVED, queue).increment();
    }

    public void recordShovel(final long nanos, final String outcome) {
        metrics.timer(IgnisMetrics.SHOVEL, queue.and(IgnisMetrics.TAG_OUTCOME, outcome))
                .record(nanos, TimeUnit.NANOSECONDS);
    }

    /** The gauge is pulled, so the source must be the live object rather than a snapshot. */
    public <T> void gaugeConsumers(final T source, final ToDoubleFunction<T> count) {
        metrics.gauge(IgnisMetrics.QUEUE_CONSUMERS, queue, source, count);
    }

    public boolean isEnabled() {
        return metrics.isEnabled();
    }
}
