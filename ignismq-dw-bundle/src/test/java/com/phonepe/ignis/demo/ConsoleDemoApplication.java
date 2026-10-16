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

package com.phonepe.ignis.demo;

import com.phonepe.ignis.IQueue;
import com.phonepe.ignis.IgnisMQManager;
import com.phonepe.ignis.IgnisMQSettings;
import com.phonepe.ignis.MagazineQueue;
import com.phonepe.ignis.common.QueueMetaData;
import com.phonepe.ignis.common.ShardDepth;
import com.phonepe.ignis.entity.QueueEntity;
import com.phonepe.ignis.exception.ErrorCode;
import com.phonepe.ignis.exception.IgnisMQException;
import com.phonepe.ignis.exception.IgnisMQExceptionMapper;
import com.phonepe.ignis.metric.IgnisMetrics;
import com.phonepe.ignis.metric.QueueMeters;
import com.phonepe.ignis.request.ShovelConfig;
import com.phonepe.ignis.resource.GrantRoleFilter;
import com.phonepe.ignis.resource.IgnisMQResource;
import com.phonepe.ignis.service.IgnisMQService;
import com.phonepe.ignis.utils.Constants;
import com.phonepe.magazine.metrics.MagazineMetrics;
import com.phonepe.magazine.metrics.StorageOperation;
import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test-only application for looking at the console without an Aerospike cluster.
 * <p>
 * About forty queues with a mix of handlers, ages and states, simulated once a second. Traffic is
 * recorded through the real {@link QueueMeters} and Magazine's real {@link MagazineMetrics}, with
 * the same {@code magazine.*} collapse filter the manager installs, so the instance tab shows the
 * meters a real process registers and their values move. Three queues show specific states:
 * {@code order-events} (one shard drifting behind), {@code payment-retries} (active but not
 * adopted here) and {@code legacy-imports} (deactivated). The manager is a stateful mock.
 *
 * <pre>
 * mvn -B -pl ignismq-dw-bundle test-compile
 * mvn -B -pl ignismq-dw-bundle exec:java -Dexec.classpathScope=test \
 *   -Dexec.mainClass=com.phonepe.ignis.demo.ConsoleDemoApplication -Dexec.args=server
 * </pre>
 * Then open <a href="http://localhost:8080/ignisConsole/">http://localhost:8080/ignisConsole/</a>.
 */
public final class ConsoleDemoApplication extends Application<ConsoleDemoConfiguration> {

    private static final String SERVED_HERE = "order-events";
    private static final String SERVED_ELSEWHERE = "payment-retries";
    private static final String DEACTIVATED = "legacy-imports";
    private static final String[] HANDLERS = {"communicationHandler", "orderHandler", "refundHandler",
            "notificationHandler", "ledgerHandler"};
    private static final long DAY = 86_400_000L;

    private final Random random = new Random(42);
    private final Map<String, QueueEntity> stored = new ConcurrentHashMap<>();
    private final Map<String, IQueue<?>> local = new ConcurrentHashMap<>();
    private final Map<String, DemoQueue> simulated = new ConcurrentHashMap<>();
    private final MeterRegistry registry = new SimpleMeterRegistry();
    private final IgnisMetrics metrics;
    private final MagazineMetrics storageMetrics;
    private final AtomicInteger busyThreads = new AtomicInteger();
    private final AtomicInteger maxThreads = new AtomicInteger(256);

    public ConsoleDemoApplication() {
        // Folded like the manager's default, so Magazine's meters are per process.
        this.metrics = new IgnisMetrics(registry, true, true);
        this.storageMetrics = new MagazineMetrics(registry);
    }

    public static void main(final String[] args) throws Exception {
        new ConsoleDemoApplication().run(args.length == 0 ? new String[]{"server"} : args);
    }

    @Override
    public String getName() {
        return "ignismq-console-demo";
    }

    @Override
    public void initialize(final Bootstrap<ConsoleDemoConfiguration> bootstrap) {
        bootstrap.addBundle(new AssetsBundle("/ignisAssets/", "/ignisConsole", "ignisIndex.html", "ignisAssets"));
    }

    @Override
    public void run(final ConsoleDemoConfiguration configuration, final Environment environment) {
        // Demo only: both roles are granted unconditionally so the actions can be tried.
        environment.jersey().register(new GrantRoleFilter(
                IgnisMQResource.OPERATE_ROLE, IgnisMQResource.DEACTIVATE_ROLE));
        environment.jersey().register(RolesAllowedDynamicFeature.class);
        environment.jersey().register(new IgnisMQExceptionMapper());
        final IgnisMQManager manager = manager();
        environment.jersey().register(new IgnisMQResource(new IgnisMQService(manager,
                "billing-service", "farm-1", IgnisMQSettings.defaults(), registry, 0)));

        registry.gauge(IgnisMetrics.POOL_THREADS, Tags.of(IgnisMetrics.TAG_POOL, "worker"), busyThreads,
                AtomicInteger::get);
        registry.gauge(IgnisMetrics.POOL_THREADS_MAX, Tags.of(IgnisMetrics.TAG_POOL, "worker"), maxThreads,
                AtomicInteger::get);
        // A few minutes of history up front, so nothing reads zero on first load; then live.
        for (int second = 0; second < 300; second++) {
            tick(second);
        }
        final ScheduledExecutorService ticker = Executors.newSingleThreadScheduledExecutor(runnable -> {
            final Thread thread = new Thread(runnable, "console-demo-traffic");
            thread.setDaemon(true);
            return thread;
        });
        final AtomicLong seconds = new AtomicLong(300);
        ticker.scheduleAtFixedRate(() -> tick(seconds.incrementAndGet()), 1, 1, TimeUnit.SECONDS);
        environment.lifecycle().manage(new io.dropwizard.lifecycle.Managed() {
            @Override
            public void start() {
                // Started above, so the first page load already has data.
            }

            @Override
            public void stop() {
                ticker.shutdownNow();
            }
        });
    }

    private synchronized void tick(final long second) {
        busyThreads.set(simulated.values().stream().mapToInt(queue -> queue.consumers.get()).sum());
        simulated.values().forEach(queue -> queue.tick(second));
        if (second % 30 == 0) {
            registry.timer(IgnisMetrics.QUEUE_REFRESH, IgnisMetrics.TAG_OUTCOME, IgnisMetrics.SUCCESS)
                    .record(Duration.ofMillis(40 + random.nextInt(60)));
        }
        if (second % 7 == 0) {
            registry.timer(IgnisMetrics.POOL_TASK_WAIT, IgnisMetrics.TAG_POOL, "worker")
                    .record(Duration.ofNanos(50_000 + random.nextInt(400_000)));
        }
    }

    private IgnisMQManager manager() {
        final long now = System.currentTimeMillis();
        add(SERVED_HERE, "orderHandler", 8, true, true, now - 21 * DAY, 10, 4);
        add(SERVED_ELSEWHERE, "refundHandler", 4, true, false, now - 3 * DAY, 0, 0);
        add(DEACTIVATED, "ledgerHandler", 2, false, false, now - 40 * DAY, 0, 0);
        add("invoice-mailer", "notificationHandler", 8, true, true, now - 9 * DAY, 6, 3);
        add("refund-sync", "refundHandler", 4, true, true, now - 5 * DAY, 2, 2);
        add("ledger-postings", "ledgerHandler", 16, true, true, now - 12 * DAY, 25, 8);
        for (int index = 0; index < 34; index++) {
            final String name = "SEND_COMMUNICATION_" + (2610150600L + random.nextInt(99_999_999)) + "1";
            // Campaign queues: mostly young, a quarter finished and deactivated, some idle.
            final long age = index < 6 ? random.nextLong(3_600_000L) : random.nextLong(30L * DAY);
            final boolean active = index % 4 != 3;
            final double rate = !active || index % 5 == 0 ? 0 : random.nextDouble() * 12;
            add(name, HANDLERS[index % 3 == 0 ? 0 : index % HANDLERS.length], 8, active, active, now - age,
                    rate, 2);
        }

        final IgnisMQManager manager = mock(IgnisMQManager.class);
        when(manager.getAllQueues()).thenReturn(local);
        when(manager.getStoredQueues(anyBoolean())).thenAnswer(invocation -> stored.entrySet().stream()
                .filter(entry -> entry.getValue().isActive() == (boolean) invocation.getArgument(0))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
        when(manager.getStoredQueue(anyString()))
                .thenAnswer(invocation -> Optional.ofNullable(stored.get(invocation.<String>getArgument(0))));
        doAnswer(invocation -> {
            final DemoQueue queue = simulated.get(invocation.<String>getArgument(0));
            // Enforced here so the demo shows the real manager's 409 with its error code.
            if (queue.consumers.get() + (int) invocation.getArgument(1) > Constants.MAX_CONSUMERS_ALLOWED) {
                throw IgnisMQException.builder().errorCode(ErrorCode.MAX_ALLOWED_CONSUMERS_EXCEEDED)
                        .message("A queue may run at most " + Constants.MAX_CONSUMERS_ALLOWED + " consumers")
                        .build();
            }
            return queue.consumers.addAndGet(invocation.getArgument(1));
        }).when(manager).increaseConsumers(anyString(), anyInt());
        doAnswer(invocation -> simulated.get(invocation.<String>getArgument(0)).consumers
                .addAndGet(-(int) invocation.getArgument(1)))
                .when(manager).decreaseConsumers(anyString(), anyInt());
        doAnswer(invocation -> {
            final String name = invocation.getArgument(0);
            stored.get(name).setActive(false);
            local.remove(name);
            final DemoQueue queue = simulated.remove(name);
            if (queue != null) {
                // What the real manager does on deactivation.
                queue.meters.close(List.of(name, name + IgnisMetrics.SIDELINE_SUFFIX));
            }
            return null;
        }).when(manager).deactivateQueue(anyString());
        return manager;
    }

    private void add(final String name, final String handler, final int shards, final boolean active,
                     final boolean runsHere, final long createdAt, final double publishRate, final int consumers) {
        stored.put(name, QueueEntity.builder()
                .active(active)
                .shards(shards)
                .concurrency(Math.max(consumers, 1))
                .messageHandlerType(handler)
                .createdAt(createdAt)
                .messageExpiry(name.startsWith("SEND_") ? 86_400 : 172_800)
                .queueExpiry(name.startsWith("SEND_") ? 604_800 : 2_592_000)
                .sweepDuration(1_200_000L)
                .handlerTimeout(600_000L)
                .shovelConcurrency(name.equals(SERVED_HERE) ? 4 : -1)
                .shovelTimeIntervalInSecs(name.equals(SERVED_HERE) ? 600 : -1)
                .build());
        if (runsHere) {
            final DemoQueue queue = new DemoQueue(name, shards, publishRate, consumers, name.equals(SERVED_HERE));
            simulated.put(name, queue);
            local.put(name, queue.mock());
        }
    }

    /**
     * One queue's traffic. Every event goes through the same meter calls the real consumer,
     * publisher, shovel and Magazine make, so the numbers relate to each other the way they do in
     * production.
     */
    private final class DemoQueue {
        private final String name;
        private final String sideline;
        private final int shards;
        private final double publishRate;
        private final boolean drifting;
        private final AtomicInteger consumers;
        private final AtomicLong published = new AtomicLong();
        private final AtomicLong consumed = new AtomicLong();
        private final AtomicLong sidelined = new AtomicLong();
        private final AtomicLong shovelled = new AtomicLong();
        private final AtomicLong depth = new AtomicLong();
        private final QueueMeters meters;

        DemoQueue(final String name, final int shards, final double publishRate, final int consumers,
                  final boolean drifting) {
            this.name = name;
            this.sideline = name + IgnisMetrics.SIDELINE_SUFFIX;
            this.shards = shards;
            this.publishRate = publishRate;
            this.drifting = drifting;
            this.consumers = new AtomicInteger(consumers);
            this.meters = new QueueMeters(metrics, name);
            final Tags queue = Tags.of(IgnisMetrics.TAG_QUEUE, name);
            registry.gauge(IgnisMetrics.QUEUE_DEPTH, queue, depth, AtomicLong::get);
            registry.gauge(IgnisMetrics.QUEUE_PUBLISHED, queue, published, AtomicLong::get);
            registry.gauge(IgnisMetrics.QUEUE_CONSUMED, queue, consumed, AtomicLong::get);
            registry.gauge(IgnisMetrics.QUEUE_SIDELINED, queue, sidelined, AtomicLong::get);
            registry.gauge(IgnisMetrics.QUEUE_SHOVELLED, queue, shovelled, AtomicLong::get);
            meters.gaugeConsumers(this.consumers, AtomicInteger::get);
            if (drifting) {
                // A standing backlog, so the shard view has something to show from the first second.
                publish(3_000);
            }
        }

        void tick(final long second) {
            // Traffic comes in waves over a few minutes, so the charts have a shape.
            final double wave = 1 + 0.6 * Math.sin(second / 45.0 + name.hashCode());
            publish((int) Math.round(publishRate * wave * (70 + random.nextInt(61)) / 100.0));
            final int capacity = (int) Math.round(consumers.get() * (260 + random.nextInt(101)) / 100.0);
            final int available = (int) Math.min(depth.get(), capacity);
            for (int index = 0; index < available; index++) {
                consumeOne();
            }
            for (int index = available; index < Math.min(capacity, available + 3); index++) {
                meters.polled(false);
                storageMetrics.fireOutcome(name, MagazineMetrics.FireOutcome.EMPTY);
                storageMetrics.recordFire(System.nanoTime() - 30_000, name);
            }
            if (depth.get() > capacity * 20L && random.nextInt(4) == 0) {
                meters.budgetExhausted();
            }
            if (second % 5 == 0 && (publishRate > 0 || depth.get() > 0)) {
                storageMetrics.aerospikeCall(name, StorageOperation.REFRESH_ACTIVE_SHARDS);
            }
            if (second % 150 == 0 && publishRate > 0) {
                storageMetrics.aerospikeCall(name, StorageOperation.WRITE_FIRE_HISTORY);
            }
            if (second % 60 == 0 && sidelined.get() > 0) {
                shovel();
            }
        }

        private void publish(final int count) {
            for (int index = 0; index < count; index++) {
                final boolean failed = random.nextInt(2_000) == 0;
                meters.recordPublish(nanos(0.4, 1.8), failed);
                if (failed) {
                    storageMetrics.loadOutcome(name, MagazineMetrics.LoadOutcome.FAILED);
                    continue;
                }
                storageMetrics.aerospikeCall(name, StorageOperation.INCREMENT_LOAD_POINTER);
                storageMetrics.aerospikeCall(name, StorageOperation.WRITE_DATA);
                storageMetrics.aerospikeCall(name, StorageOperation.INCREMENT_LOAD_COUNTER);
                storageMetrics.loadOutcome(name, MagazineMetrics.LoadOutcome.LOADED);
                storageMetrics.recordLoad(System.nanoTime() - nanos(0.3, 1.2), name);
                published.incrementAndGet();
                depth.incrementAndGet();
            }
        }

        private void consumeOne() {
            meters.polled(true);
            storageMetrics.aerospikeCall(name, StorageOperation.CLAIM_FIRE_POINTER);
            storageMetrics.fireClaim(name, MagazineMetrics.ClaimOutcome.WON);
            storageMetrics.aerospikeCall(name, StorageOperation.READ_DATA);
            storageMetrics.aerospikeCall(name, StorageOperation.INCREMENT_FIRE_COUNTER);
            storageMetrics.fireOutcome(name, MagazineMetrics.FireOutcome.DELIVERED);
            storageMetrics.recordFire(System.nanoTime() - nanos(0.2, 0.8), name);
            depth.decrementAndGet();
            consumed.incrementAndGet();

            final int roll = random.nextInt(1_000);
            final long handler = nanos(4, 60);
            registry.timer(IgnisMetrics.CONSUME, IgnisMetrics.TAG_QUEUE, name)
                    .record(handler + nanos(0.5, 2), TimeUnit.NANOSECONDS);
            if (roll < 6) {
                final String reason = roll < 4 ? IgnisMetrics.REASON_EXCEPTION
                        : roll < 5 ? IgnisMetrics.REASON_TIMEOUT : IgnisMetrics.REASON_REJECTED;
                meters.recordHandler(handler, IgnisMetrics.REASON_TIMEOUT.equals(reason)
                        ? IgnisMetrics.REASON_TIMEOUT : IgnisMetrics.FAILURE);
                if (IgnisMetrics.REASON_TIMEOUT.equals(reason)) {
                    meters.handlerTimedOut();
                }
                meters.sidelined(reason);
                storageMetrics.aerospikeCall(sideline, StorageOperation.INCREMENT_LOAD_POINTER);
                storageMetrics.aerospikeCall(sideline, StorageOperation.WRITE_DATA);
                storageMetrics.aerospikeCall(sideline, StorageOperation.INCREMENT_LOAD_COUNTER);
                storageMetrics.loadOutcome(sideline, MagazineMetrics.LoadOutcome.LOADED);
                sidelined.incrementAndGet();
            } else {
                meters.recordHandler(handler, IgnisMetrics.SUCCESS);
                meters.acked(1);
            }
            storageMetrics.aerospikeCall(name, StorageOperation.DELETE_DATA);
        }

        private void shovel() {
            final long moving = sidelined.getAndSet(0);
            for (long index = 0; index < moving; index++) {
                storageMetrics.aerospikeCall(sideline, StorageOperation.CLAIM_FIRE_POINTER);
                storageMetrics.aerospikeCall(sideline, StorageOperation.READ_DATA);
                storageMetrics.aerospikeCall(sideline, StorageOperation.DELETE_DATA);
                meters.shovelMoved();
                publish(1);
            }
            shovelled.addAndGet(moving);
            meters.recordShovel(nanos(1, 6) * Math.max(moving, 1), IgnisMetrics.SUCCESS);
        }

        private long nanos(final double fromMillis, final double toMillis) {
            final long from = (long) (fromMillis * 1_000_000);
            return from + random.nextLong(Math.max(1L, (long) (toMillis * 1_000_000) - from));
        }

        /** Publishing picks a shard at random, so the backlog spreads evenly - except a drifting shard. */
        private List<ShardDepth> shardDepths() {
            final List<ShardDepth> result = new ArrayList<>();
            final double weights = shards + (drifting ? 2 : 0);
            for (int shard = 0; shard < shards; shard++) {
                final double weight = (drifting && shard == 6 ? 3 : 1) / weights;
                final long pending = Math.round(depth.get() * weight);
                final long out = Math.round(consumed.get() / (double) shards);
                result.add(ShardDepth.builder().shard("SHARD_" + shard).published(out + pending)
                        .consumed(out).pending(pending).build());
            }
            return result;
        }

        IQueue<?> mock() {
            final MagazineQueue<String> queue = org.mockito.Mockito.mock(MagazineQueue.class);
            when(queue.getShardDepths()).thenAnswer(invocation -> shardDepths());
            when(queue.getMetaData()).thenAnswer(invocation -> QueueMetaData.builder()
                    .published(published.get()).consumed(consumed.get())
                    .sidelined(sidelined.get()).shovelled(shovelled.get()).build());
            when(queue.getNoOfConsumers()).thenAnswer(invocation -> consumers.get());
            when(queue.getNoOfShovelConsumers()).thenReturn(drifting ? 1 : 0);
            when(queue.getShovelConfig()).thenReturn(drifting
                    ? ShovelConfig.builder().concurrency(4).timeIntervalInSecs(600).build() : null);
            return queue;
        }
    }
}
