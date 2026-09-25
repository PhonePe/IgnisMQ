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
import com.phonepe.ignis.resource.IgnisMQResource;
import com.phonepe.ignis.service.IgnisMQService;
import com.phonepe.ignis.resource.GrantRoleFilter;
import com.phonepe.ignis.exception.ErrorCode;
import com.phonepe.ignis.utils.Constants;
import com.phonepe.ignis.exception.IgnisMQException;
import com.phonepe.ignis.exception.IgnisMQExceptionMapper;
import com.phonepe.ignis.entity.QueueEntity;
import com.phonepe.ignis.request.ShovelConfig;
import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
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
 * The three queues are chosen to show the states that are easy to get wrong: one served here with a
 * deliberately uneven shard spread, one that exists but is served by other instances, and one that
 * has been deactivated. The manager underneath is a mock, but a stateful one - scaling consumers and
 * deactivating a queue really do change what the console then shows.
 *
 * <pre>
 * mvn -B -pl ignismq-dw-bundle test-compile
 * mvn -B -pl ignismq-dw-bundle exec:java -Dexec.classpathScope=test \
 *   -Dexec.mainClass=com.phonepe.ignis.demo.ConsoleDemoApplication -Dexec.args=server
 * </pre>
 * <p>
 * Then open <a href="http://localhost:8080/ignisConsole/">http://localhost:8080/ignisConsole/</a>.
 */
public final class ConsoleDemoApplication extends Application<ConsoleDemoConfiguration> {

    private static final String SERVED_HERE = "order-events";
    private static final String SERVED_ELSEWHERE = "payment-retries";
    private static final String DEACTIVATED = "legacy-imports";

    private final Map<String, QueueEntity> stored = new LinkedHashMap<>();
    private final Map<String, IQueue<?>> local = new LinkedHashMap<>();
    private final AtomicInteger consumers = new AtomicInteger(4);
    private final Map<String, AtomicInteger> gaugeSources = Map.of(
            "threads", new AtomicInteger(18), "depth", new AtomicInteger(31_750));

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
        // Demo only: the role is granted unconditionally so the actions can be tried. A real
        // deployment registers its own authentication and grants the role selectively.
        environment.jersey().register(new GrantRoleFilter(
                IgnisMQResource.OPERATE_ROLE, IgnisMQResource.DEACTIVATE_ROLE));
        environment.jersey().register(RolesAllowedDynamicFeature.class);
        // Registered by IgnisMQBundle in a real application; here by hand, because the demo builds the
        // resource itself and would otherwise report failures differently from what it is demonstrating.
        environment.jersey().register(new IgnisMQExceptionMapper());
        environment.jersey().register(new IgnisMQResource(new IgnisMQService(manager(),
                "billing-service", "farm-1", IgnisMQSettings.defaults(), demoMeters(), 0)));
    }

    /**
     * A handful of meters with plausible values, so the instance tab has something to show. A real
     * deployment's registry is the application's own and is populated by traffic.
     */
    private MeterRegistry demoMeters() {
        final MeterRegistry registry = new SimpleMeterRegistry();
        registry.counter("ignismq.messages", "queue", SERVED_HERE, "outcome", "acked").increment(128_400);
        registry.counter("ignismq.messages", "queue", SERVED_HERE, "outcome", "sidelined").increment(37);
        registry.counter("ignismq.poll", "queue", SERVED_HERE, "result", "empty").increment(9_812);
        registry.counter("ignismq.poll", "queue", SERVED_HERE, "result", "message").increment(128_437);
        registry.timer("ignismq.consume", "queue", SERVED_HERE)
                .record(java.time.Duration.ofMillis(12));
        registry.timer("ignismq.handler.duration", "queue", SERVED_HERE, "outcome", "success")
                .record(java.time.Duration.ofMillis(9));
        // Held in a field: Micrometer keeps only a weak reference to a gauge's source, so a literal
        // would be collected and the gauge would start reporting NaN.
        registry.gauge("ignismq.pool.threads", io.micrometer.core.instrument.Tags.of("pool", "worker"),
                gaugeSources.get("threads"), AtomicInteger::get);
        registry.gauge("ignismq.queue.depth", io.micrometer.core.instrument.Tags.of("queue", SERVED_HERE),
                gaugeSources.get("depth"), AtomicInteger::get);
        registry.counter("magazine.aerospike.calls", "magazine", SERVED_HERE, "operation", "fire")
                .increment(138_249);
        // The sideline is a magazine of its own, so its meters carry their own tag value. They belong
        // under the queue they came from, which is what the grouping has to work out.
        registry.counter("magazine.aerospike.calls", "magazine", SERVED_HERE + "_SIDELINE",
                "operation", "load").increment(37);
        registry.counter("ignismq.shovel.moved", "queue", SERVED_HERE).increment(12);
        return registry;
    }

    private IgnisMQManager manager() {
        stored.put(SERVED_HERE, entity(true, 8, 6));
        stored.put(SERVED_ELSEWHERE, entity(true, 4, 2));
        stored.put(DEACTIVATED, entity(false, 2, 1));
        local.put(SERVED_HERE, queue());

        final IgnisMQManager manager = mock(IgnisMQManager.class);
        when(manager.getAllQueues()).thenReturn(local);
        when(manager.getStoredQueues(anyBoolean())).thenAnswer(invocation -> stored.entrySet().stream()
                .filter(entry -> entry.getValue().isActive() == (boolean) invocation.getArgument(0))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
        when(manager.getStoredQueue(anyString()))
                .thenAnswer(invocation -> Optional.ofNullable(stored.get(invocation.<String>getArgument(0))));
        doAnswer(invocation -> {
            // The cap is enforced here so the demo shows what a real manager does with it: a 409
            // naming the error code, rather than the 500 this used to produce.
            if (consumers.get() + (int) invocation.getArgument(1) > Constants.MAX_CONSUMERS_ALLOWED) {
                throw IgnisMQException.builder().errorCode(ErrorCode.MAX_ALLOWED_CONSUMERS_EXCEEDED)
                        .message("A queue may run at most " + Constants.MAX_CONSUMERS_ALLOWED + " consumers")
                        .build();
            }
            return consumers.addAndGet(invocation.getArgument(1));
        }).when(manager).increaseConsumers(anyString(), anyInt());
        doAnswer(invocation -> consumers.addAndGet(-(int) invocation.getArgument(1)))
                .when(manager).decreaseConsumers(anyString(), anyInt());
        doAnswer(invocation -> {
            final String name = invocation.getArgument(0);
            stored.get(name).setActive(false);
            local.remove(name);
            return null;
        }).when(manager).deactivateQueue(anyString());
        return manager;
    }

    /**
     * Publishing picks a shard at random, so the shape to show is the normal one: a backlog spread
     * roughly evenly with ordinary sampling variance. The deliberately odd shard is shard 6, which
     * is drifting behind the rest - the only thing this view can honestly diagnose.
     */
    private IQueue<?> queue() {
        final MagazineQueue<String> queue = mock(MagazineQueue.class);
        final List<ShardDepth> shards = new ArrayList<>();
        final long[] loaded = {16_040, 15_930, 16_105, 15_988, 16_012, 15_961, 16_074, 15_890};
        final long[] pending = {412, 388, 401, 377, 394, 383, 1_240, 399};
        for (int index = 0; index < loaded.length; index++) {
            // Magazine names its shards SHARD_<n>; the demo uses the same ids a real queue reports.
            shards.add(shard("SHARD_" + index, loaded[index], loaded[index] - pending[index]));
        }
        final long published = shards.stream().mapToLong(ShardDepth::getPublished).sum();
        final long consumed = shards.stream().mapToLong(ShardDepth::getConsumed).sum();

        when(queue.getShardDepths()).thenReturn(shards);
        when(queue.getMetaData()).thenReturn(QueueMetaData.builder()
                .published(published).consumed(consumed).sidelined(37).shovelled(12).build());
        when(queue.getNoOfConsumers()).thenAnswer(invocation -> consumers.get());
        when(queue.getNoOfShovelConsumers()).thenReturn(1);
        when(queue.getShovelConfig()).thenReturn(ShovelConfig.builder()
                .concurrency(4).timeIntervalInSecs(600).build());
        return queue;
    }

    private static ShardDepth shard(final String name, final long published, final long consumed) {
        return ShardDepth.builder()
                .shard(name)
                .published(published)
                .consumed(consumed)
                .pending(Math.max(published - consumed, 0L))
                .build();
    }

    private static QueueEntity entity(final boolean active, final int shards, final int concurrency) {
        return QueueEntity.builder()
                .active(active)
                .shards(shards)
                .concurrency(concurrency)
                .messageHandlerType("orderHandler")
                .createdAt(System.currentTimeMillis() - 86_400_000L)
                .messageExpiry(172_800)
                .queueExpiry(172_800)
                .sweepDuration(1_200_000L)
                .handlerTimeout(600_000L)
                .shovelConcurrency(-1)
                .shovelTimeIntervalInSecs(-1)
                .build();
    }
}
