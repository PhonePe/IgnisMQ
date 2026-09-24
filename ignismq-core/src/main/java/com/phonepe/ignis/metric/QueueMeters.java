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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.ToDoubleFunction;

/**
 * Every meter belonging to one queue, resolved once at construction.
 * <p>
 * The queue's collaborators take this rather than a registry and a name, so none of them builds a
 * meter, holds a tag set or decides whether instrumentation is on. Resolving a meter costs a tag
 * set, a {@code Meter.Id} and a registry lookup, and the message path records several per message,
 * so nothing here resolves anything after construction. The two string-keyed dimensions are closed
 * sets, pre-resolved for their known values and falling back to the registry for anything else.
 */
public final class QueueMeters {

    private static final String[] SIDELINE_REASONS = {
            IgnisMetrics.REASON_REJECTED, IgnisMetrics.REASON_EXCEPTION, IgnisMetrics.REASON_TIMEOUT,
            IgnisMetrics.REASON_SATURATED, IgnisMetrics.REASON_UNREADABLE,
            IgnisMetrics.REASON_SIDELINE_REFUSED};
    private static final String[] HANDLER_OUTCOMES = {
            IgnisMetrics.SUCCESS, IgnisMetrics.FAILURE, IgnisMetrics.REASON_TIMEOUT,
            IgnisMetrics.REASON_SATURATED};
    private static final String[] SHOVEL_OUTCOMES = {IgnisMetrics.SUCCESS, IgnisMetrics.FAILURE};

    private final IgnisMetrics metrics;
    private final Tags queue;
    private final Timer publishSuccess;
    private final Timer publishFailure;
    private final Timer consume;
    private final Counter pollMessage;
    private final Counter pollEmpty;
    private final Counter ackedMessages;
    private final Counter sidelinedMessages;
    private final Counter ignoredMessages;
    private final Counter budgetExhaustedRuns;
    private final Counter handlerTimeouts;
    private final Counter shovelMovedMessages;
    /**
     * The one meter not resolved at construction: a queue that does not batch must not publish it at
     * all, and only the first recording proves the queue batches. Resolved once, on that first call.
     */
    private final AtomicReference<DistributionSummary> handlerBatchSize = new AtomicReference<>();
    private final Map<String, Counter> sidelineReasons = new ConcurrentHashMap<>();
    private final Map<String, Timer> handlerOutcomes = new ConcurrentHashMap<>();
    private final Map<String, Timer> shovelOutcomes = new ConcurrentHashMap<>();

    public QueueMeters(final IgnisMetrics metrics, final String queueName) {
        this.metrics = metrics;
        this.queue = Tags.of(IgnisMetrics.TAG_QUEUE, queueName);
        this.publishSuccess = metrics.timer(IgnisMetrics.PUBLISH,
                queue.and(IgnisMetrics.TAG_OUTCOME, IgnisMetrics.SUCCESS));
        this.publishFailure = metrics.timer(IgnisMetrics.PUBLISH,
                queue.and(IgnisMetrics.TAG_OUTCOME, IgnisMetrics.FAILURE));
        this.consume = metrics.timer(IgnisMetrics.CONSUME, queue);
        this.pollMessage = metrics.counter(IgnisMetrics.POLL,
                queue.and(IgnisMetrics.TAG_RESULT, IgnisMetrics.MESSAGE));
        this.pollEmpty = metrics.counter(IgnisMetrics.POLL,
                queue.and(IgnisMetrics.TAG_RESULT, IgnisMetrics.EMPTY));
        this.ackedMessages = metrics.counter(IgnisMetrics.MESSAGES,
                queue.and(IgnisMetrics.TAG_OUTCOME, IgnisMetrics.ACKED));
        this.sidelinedMessages = metrics.counter(IgnisMetrics.MESSAGES,
                queue.and(IgnisMetrics.TAG_OUTCOME, IgnisMetrics.SIDELINED));
        this.ignoredMessages = metrics.counter(IgnisMetrics.MESSAGES,
                queue.and(IgnisMetrics.TAG_OUTCOME, IgnisMetrics.IGNORED));
        this.budgetExhaustedRuns = metrics.counter(IgnisMetrics.BUDGET_EXHAUSTED, queue);
        this.handlerTimeouts = metrics.counter(IgnisMetrics.HANDLER_TIMEOUTS, queue);
        this.shovelMovedMessages = metrics.counter(IgnisMetrics.SHOVEL_MOVED, queue);
        for (final String reason : SIDELINE_REASONS) {
            sidelineReasons.put(reason, sidelineCounter(reason));
        }
        for (final String outcome : HANDLER_OUTCOMES) {
            handlerOutcomes.put(outcome, handlerTimer(outcome));
        }
        for (final String outcome : SHOVEL_OUTCOMES) {
            shovelOutcomes.put(outcome, shovelTimer(outcome));
        }
    }

    /**
     * For collaborators built outside a manager, which in practice means tests.
     */
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
        (gotMessage ? pollMessage : pollEmpty).increment();
    }

    public void acked(final int count) {
        ackedMessages.increment(count);
    }

    public void ignored() {
        ignoredMessages.increment();
    }

    public void sidelined(final String reason) {
        sidelineReasons.computeIfAbsent(reason, this::sidelineCounter).increment();
        sidelinedMessages.increment();
    }

    /**
     * The payload is still in the main magazine, so this is not a completed sideline.
     */
    public void sidelineRefused(final String reason) {
        sidelineReasons.computeIfAbsent(reason, this::sidelineCounter).increment();
    }

    public void budgetExhausted() {
        budgetExhaustedRuns.increment();
    }

    public void handlerBatch(final int size) {
        DistributionSummary summary = handlerBatchSize.get();
        if (Objects.isNull(summary)) {
            // Registration is idempotent, so a race costs a duplicate lookup and nothing else.
            summary = metrics.summary(IgnisMetrics.HANDLER_BATCH_SIZE, queue);
            handlerBatchSize.set(summary);
        }
        summary.record(size);
    }

    public void handlerTimedOut() {
        handlerTimeouts.increment();
    }

    public void recordHandler(final long nanos, final String outcome) {
        handlerOutcomes.computeIfAbsent(outcome, this::handlerTimer).record(nanos, TimeUnit.NANOSECONDS);
    }

    public void shovelMoved() {
        shovelMovedMessages.increment();
    }

    public void recordShovel(final long nanos, final String outcome) {
        shovelOutcomes.computeIfAbsent(outcome, this::shovelTimer).record(nanos, TimeUnit.NANOSECONDS);
    }

    /**
     * The gauge is pulled, so the source must be the live object rather than a snapshot.
     */
    public <T> void gaugeConsumers(final T source, final ToDoubleFunction<T> count) {
        metrics.gauge(IgnisMetrics.QUEUE_CONSUMERS, queue, source, count);
    }

    public boolean isEnabled() {
        return metrics.isEnabled();
    }

    private Counter sidelineCounter(final String reason) {
        return metrics.counter(IgnisMetrics.SIDELINE, queue.and(IgnisMetrics.TAG_REASON, reason));
    }

    private Timer handlerTimer(final String outcome) {
        return metrics.timer(IgnisMetrics.HANDLER_DURATION, queue.and(IgnisMetrics.TAG_OUTCOME, outcome));
    }

    private Timer shovelTimer(final String outcome) {
        return metrics.timer(IgnisMetrics.SHOVEL, queue.and(IgnisMetrics.TAG_OUTCOME, outcome));
    }
}
