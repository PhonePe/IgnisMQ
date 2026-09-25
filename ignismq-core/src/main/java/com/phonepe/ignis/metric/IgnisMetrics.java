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

import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import lombok.Getter;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.ToDoubleFunction;

/**
 * Every meter ignisMQ publishes, and the only place their names are written down.
 * <p>
 * {@code ignismq.<area>.<measurement>}, with every dimension carried as a tag rather than baked
 * into the name. Tag values are closed sets, so cardinality is queue count x a constant.
 */
public final class IgnisMetrics {

    // --- message path ---------------------------------------------------------------------
    public static final String PUBLISH = "ignismq.publish";
    public static final String CONSUME = "ignismq.consume";
    /**
     * Terminal disposition of a delivered message: acked, sidelined or ignored.
     */
    public static final String MESSAGES = "ignismq.messages";
    /**
     * Whether a poll returned work. A high empty rate means consumers are over-provisioned.
     */
    public static final String POLL = "ignismq.poll";

    // --- handler --------------------------------------------------------------------------
    /** The handler call alone, excluding deserialisation, delete and sideline - unlike CONSUME. */
    public static final String HANDLER_DURATION = "ignismq.handler.duration";
    public static final String HANDLER_BATCH_SIZE = "ignismq.handler.batch.size";
    public static final String HANDLER_EXECUTIONS = "ignismq.handler.executions";
    public static final String HANDLER_TIMEOUTS = "ignismq.handler.timeouts";
    public static final String HANDLER_THREADS_ACTIVE = "ignismq.handler.threads.active";

    // --- queue state ----------------------------------------------------------------------
    /** Backlog: published minus consumed. */
    public static final String QUEUE_DEPTH = "ignismq.queue.depth";
    public static final String QUEUE_PUBLISHED = "ignismq.queue.published";
    public static final String QUEUE_CONSUMED = "ignismq.queue.consumed";
    public static final String QUEUE_SIDELINED = "ignismq.queue.sidelined";
    public static final String QUEUE_SHOVELLED = "ignismq.queue.shovelled";
    public static final String QUEUE_CONSUMERS = "ignismq.queue.consumers";

    // --- control plane --------------------------------------------------------------------
    public static final String QUEUE_CREATE = "ignismq.queue.create";
    public static final String QUEUE_REFRESH = "ignismq.queue.refresh";
    public static final String SWEEP = "ignismq.sweep";
    public static final String SWEEP_REHOMED = "ignismq.sweep.rehomed";
    public static final String SHOVEL = "ignismq.shovel";
    public static final String SHOVEL_MOVED = "ignismq.shovel.moved";
    public static final String SIDELINE = "ignismq.sideline";

    // --- pools ----------------------------------------------------------------------------
    public static final String POOL_THREADS = "ignismq.pool.threads";
    public static final String POOL_THREADS_MAX = "ignismq.pool.threads.max";
    public static final String POOL_TASKS_ACTIVE = "ignismq.pool.tasks.active";
    public static final String POOL_TASKS_DUE = "ignismq.pool.tasks.due";
    public static final String POOL_TASK_WAIT = "ignismq.pool.task.wait";
    public static final String POOL_TASK_FAILURES = "ignismq.pool.task.failures";
    /** A consumer that handed its thread back with work still waiting. */
    public static final String BUDGET_EXHAUSTED = "ignismq.consumer.budget.exhausted";

    public static final String TAG_QUEUE = "queue";
    public static final String TAG_OUTCOME = "outcome";
    public static final String TAG_MAGAZINE = "magazine";
    public static final String TAG_POOL = "pool";
    public static final String TAG_MODE = "mode";
    public static final String TAG_REASON = "reason";
    public static final String TAG_RESULT = "result";

    public static final String SUCCESS = "success";
    public static final String FAILURE = "failure";
    public static final String MAIN = "main";
    public static final String SIDELINE_MAGAZINE = "sideline";
    public static final String POOLED = "pooled";
    public static final String REFUSED = "refused";
    public static final String ACKED = "acked";
    public static final String SIDELINED = "sidelined";
    public static final String IGNORED = "ignored";
    public static final String MESSAGE = "message";
    public static final String EMPTY = "empty";

    /** Why a message left the main magazine for the sideline. A closed set, so meters are pre-resolved. */
    public static final String REASON_REJECTED = "rejected";
    public static final String REASON_EXCEPTION = "exception";
    public static final String REASON_TIMEOUT = "timeout";
    public static final String REASON_SATURATED = "saturated";
    /**
     * ignisMQ could not turn the stored payload into the consumer's message type, so the handler
     * never saw it. Distinct from {@link #REASON_EXCEPTION} because the fix is different: this is
     * publisher and consumer disagreeing about the format, not a failing handler.
     */
    public static final String REASON_UNREADABLE = "unreadable";
    /** The sideline would not take it, so the payload is still in the main magazine. */
    public static final String REASON_SIDELINE_REFUSED = "sideline_refused";

    /** The caller's registry when enabled, a discarding sink when not. */
    @Getter
    private final MeterRegistry registry;
    @Getter
    private final boolean enabled;

    /** Enabled. */
    public IgnisMetrics(final MeterRegistry registry) {
        this(registry, true);
    }

    public IgnisMetrics(final MeterRegistry registry, final boolean enabled) {
        Objects.requireNonNull(registry, "Meter registry is required.");
        this.enabled = enabled;
        // A composite with no children accepts every registration and discards every recording, so
        // callers need no null checks and no branches on the hot path.
        this.registry = enabled ? registry : new CompositeMeterRegistry();
    }


    public Timer timer(final String name, final Tags tags) {
        return registry.timer(name, tags);
    }

    public Counter counter(final String name, final Tags tags) {
        return registry.counter(name, tags);
    }

    public DistributionSummary summary(final String name, final Tags tags) {
        return DistributionSummary.builder(name).tags(tags).register(registry);
    }

    public <T> void gauge(final String name, final Tags tags, final T source,
                          final ToDoubleFunction<T> value) {
        registry.gauge(name, tags, source, value);
    }

    /** Removes a meter, so a deactivated queue stops reporting rather than freezing at its last value. */
    public void remove(final String name, final Tags tags) {
        final Meter meter = registry.find(name).tags(tags).meter();
        if (Objects.nonNull(meter)) {
            registry.remove(meter);
        }
    }

    /**
     * Times an operation and tags the result {@code outcome=success|failure}.
     */
    public <T> T time(final String name, final Tags tags, final ThrowingSupplier<T> operation) throws Exception {
        if (!enabled) {
            return operation.get();
        }
        final long startNanos = System.nanoTime();
        String outcome = SUCCESS;
        try {
            return operation.get();
        } catch (Exception e) {
            outcome = FAILURE;
            throw e;
        } finally {
            registry.timer(name, tags.and(TAG_OUTCOME, outcome))
                    .record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
        }
    }

    public void time(final String name, final Tags tags, final ThrowingRunnable operation) throws Exception {
        time(name, tags, () -> {
            operation.run();
            return null;
        });
    }

    @FunctionalInterface
    @SuppressWarnings("java:S112")
    public interface ThrowingSupplier<T> {
        T get() throws Exception;
    }

    @FunctionalInterface
    @SuppressWarnings("java:S112")
    public interface ThrowingRunnable {
        void run() throws Exception;
    }
}
