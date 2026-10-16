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
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;

/**
 * Every meter belonging to one queue.
 * <p>
 * The queue's collaborators take this rather than a registry and a name, so none of them builds a
 * meter, holds a tag set or decides whether instrumentation is on.
 * <p>
 * Publish, consume, poll and ack meters are created up front; the rest on first use.
 * {@link #close} removes all of them, including the Magazine meters underneath.
 */
public final class QueueMeters {

    private static final String PUBLISH_FAILURE = "publish.failure";
    private static final String SIDELINED_MESSAGES = "messages.sidelined";
    private static final String IGNORED_MESSAGES = "messages.ignored";
    private static final String BUDGET_EXHAUSTED = "budget.exhausted";
    private static final String HANDLER_TIMEOUTS = "handler.timeouts";
    private static final String SHOVEL_MOVED = "shovel.moved";
    private static final String HANDLER_BATCH_SIZE = "handler.batch.size";

    /** Swapped for a discarding registry on {@link #close}, so late recordings register nothing. */
    private final AtomicReference<IgnisMetrics> metrics = new AtomicReference<>();
    private final String queueName;
    private final Tags queue;
    private final Timer publishSuccess;
    private final Timer consume;
    private final Counter pollMessage;
    private final Counter pollEmpty;
    private final Counter ackedMessages;
    /** Single meters registered on first use, keyed by a constant of this class. */
    private final Map<String, Meter> lazy = new ConcurrentHashMap<>();
    /** Tagged dimensions, keyed by tag value so lookups allocate nothing. */
    private final Map<String, Meter> sidelineReasons = new ConcurrentHashMap<>();
    private final Map<String, Meter> handlerOutcomes = new ConcurrentHashMap<>();
    private final Map<String, Meter> shovelOutcomes = new ConcurrentHashMap<>();
    /** Fields, so per-message lookups allocate no lambda. */
    private final Function<String, Meter> newSidelineReason =
            value -> metrics.get().counter(IgnisMetrics.SIDELINE, tags(IgnisMetrics.TAG_REASON, value));
    private final Function<String, Meter> newHandlerOutcome =
            value -> metrics.get().timer(IgnisMetrics.HANDLER_DURATION, tags(IgnisMetrics.TAG_OUTCOME, value));
    private final Function<String, Meter> newShovelOutcome =
            value -> metrics.get().timer(IgnisMetrics.SHOVEL, tags(IgnisMetrics.TAG_OUTCOME, value));

    public QueueMeters(final IgnisMetrics metrics, final String queueName) {
        this.metrics.set(metrics);
        this.queueName = queueName;
        this.queue = Tags.of(IgnisMetrics.TAG_QUEUE, queueName);
        this.publishSuccess = metrics.timer(IgnisMetrics.PUBLISH,
                queue.and(IgnisMetrics.TAG_OUTCOME, IgnisMetrics.SUCCESS));
        this.consume = metrics.timer(IgnisMetrics.CONSUME, queue);
        this.pollMessage = metrics.counter(IgnisMetrics.POLL,
                queue.and(IgnisMetrics.TAG_RESULT, IgnisMetrics.MESSAGE));
        this.pollEmpty = metrics.counter(IgnisMetrics.POLL,
                queue.and(IgnisMetrics.TAG_RESULT, IgnisMetrics.EMPTY));
        this.ackedMessages = metrics.counter(IgnisMetrics.MESSAGES,
                queue.and(IgnisMetrics.TAG_OUTCOME, IgnisMetrics.ACKED));
    }

    /**
     * For collaborators built outside a manager, which in practice means tests.
     */
    public static QueueMeters unmetered(final String queueName) {
        return new QueueMeters(discarding(), queueName);
    }

    public void recordPublish(final long nanos, final boolean failed) {
        final Timer timer = failed
                ? timer(PUBLISH_FAILURE, IgnisMetrics.PUBLISH, IgnisMetrics.TAG_OUTCOME, IgnisMetrics.FAILURE)
                : publishSuccess;
        timer.record(nanos, TimeUnit.NANOSECONDS);
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
        counter(IGNORED_MESSAGES, IgnisMetrics.MESSAGES, IgnisMetrics.TAG_OUTCOME, IgnisMetrics.IGNORED).increment();
    }

    public void sidelined(final String reason) {
        sidelineReason(reason).increment();
        counter(SIDELINED_MESSAGES, IgnisMetrics.MESSAGES, IgnisMetrics.TAG_OUTCOME, IgnisMetrics.SIDELINED)
                .increment();
    }

    /**
     * The payload is still in the main magazine, so this is not a completed sideline.
     */
    public void sidelineRefused(final String reason) {
        sidelineReason(reason).increment();
    }

    public void budgetExhausted() {
        counter(BUDGET_EXHAUSTED, IgnisMetrics.BUDGET_EXHAUSTED, null, null).increment();
    }

    /** Recorded only by batching queues. */
    public void handlerBatch(final int size) {
        resolve(lazy, HANDLER_BATCH_SIZE, key -> metrics.get().summary(IgnisMetrics.HANDLER_BATCH_SIZE, queue),
                DistributionSummary.class).record(size);
    }

    public void handlerTimedOut() {
        counter(HANDLER_TIMEOUTS, IgnisMetrics.HANDLER_TIMEOUTS, null, null).increment();
    }

    public void recordHandler(final long nanos, final String outcome) {
        resolve(handlerOutcomes, outcome, newHandlerOutcome, Timer.class).record(nanos, TimeUnit.NANOSECONDS);
    }

    public void shovelMoved() {
        counter(SHOVEL_MOVED, IgnisMetrics.SHOVEL_MOVED, null, null).increment();
    }

    public void recordShovel(final long nanos, final String outcome) {
        resolve(shovelOutcomes, outcome, newShovelOutcome, Timer.class).record(nanos, TimeUnit.NANOSECONDS);
    }

    /**
     * The gauge is pulled, so the source must be the live object rather than a snapshot.
     */
    public <T> void gaugeConsumers(final T source, final ToDoubleFunction<T> count) {
        metrics.get().gauge(IgnisMetrics.QUEUE_CONSUMERS, queue, source, count);
    }

    public boolean isEnabled() {
        return metrics.get().isEnabled();
    }

    /**
     * Unregisters this queue's meters and those of {@code magazineIdentifiers}; later recordings are discarded.
     *
     * @return number of meters removed.
     */
    public int close(final Collection<String> magazineIdentifiers) {
        final IgnisMetrics registered = metrics.getAndSet(discarding());
        lazy.clear();
        sidelineReasons.clear();
        handlerOutcomes.clear();
        shovelOutcomes.clear();
        final Set<String> magazines = new HashSet<>(magazineIdentifiers);
        return registered.removeTagged(IgnisMetrics.TAG_QUEUE, Set.of(queueName))
                + registered.removeTagged(IgnisMetrics.TAG_MAGAZINE, magazines);
    }

    private Counter sidelineReason(final String reason) {
        return resolve(sidelineReasons, reason, newSidelineReason, Counter.class);
    }

    /** {@code tagKey} null means the queue tag alone. */
    private Counter counter(final String key, final String name, final String tagKey, final String tagValue) {
        return resolve(lazy, key, ignored -> metrics.get().counter(name, tags(tagKey, tagValue)), Counter.class);
    }

    private Timer timer(final String key, final String name, final String tagKey, final String tagValue) {
        return resolve(lazy, key, ignored -> metrics.get().timer(name, tags(tagKey, tagValue)), Timer.class);
    }

    /** Plain get first, so an existing meter costs no allocation. */
    private static <M extends Meter> M resolve(final Map<String, Meter> meters, final String key,
                                               final Function<String, Meter> create, final Class<M> type) {
        final Meter existing = meters.get(key);
        return type.cast(Objects.nonNull(existing) ? existing : meters.computeIfAbsent(key, create));
    }

    private Tags tags(final String tagKey, final String tagValue) {
        return Objects.isNull(tagKey) ? queue : queue.and(tagKey, tagValue);
    }

    private static IgnisMetrics discarding() {
        return new IgnisMetrics(new CompositeMeterRegistry(), false);
    }
}
