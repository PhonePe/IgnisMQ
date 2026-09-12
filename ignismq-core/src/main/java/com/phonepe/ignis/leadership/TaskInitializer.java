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

package com.phonepe.ignis.leadership;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.phonepe.ignis.common.LoadBalancer;
import com.phonepe.ignis.client.StorageClient;
import com.phonepe.ignis.common.MagazineRegistry;
import com.phonepe.ignis.sweep.Sweeper;
import com.phonepe.ignis.service.QueueService;
import com.phonepe.ignis.storage.BaseStorage;
import com.phonepe.ignis.scheduler.IgnisSchedulerCommands;
import com.phonepe.ignis.utils.Constants;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;

/**
 * @author shantanu.tiwari
 * Created on 14/03/22
 */
@Slf4j
public class TaskInitializer {
    public static final int DELAY_FOR_SWEEPER_TASK = 15 * 60 * 1000; // 15 minutes
    private static final int INITIAL_DELAY_FOR_SWEEPER_TASK = 10 * 60 * 1000; // 10 minutes

    private final CuratorFramework curatorFramework;
    private final QueueService queueService;
    private final String clientId;
    private final BaseStorage storage;
    private final StorageClient client;
    private LeaderElector leaderElector;
    private ScheduledFuture<?> sweeperTask;
    private final String farmId;
    private final MeterRegistry meterRegistry;
    private final IgnisSchedulerCommands scheduler;
    /** True when this initializer created the scheduler and must therefore shut it down. */
    private final boolean ownsScheduler;
    private final MagazineRegistry magazineRegistry;

    public TaskInitializer(final CuratorFramework curatorFramework,
                           final QueueService queueService,
                           final String clientId,
                           final BaseStorage storage,
                           final StorageClient client,
                           final String farmId,
                           final MeterRegistry meterRegistry) {
        this(curatorFramework, queueService, clientId, storage, client, farmId, meterRegistry, null, null);
    }

    public TaskInitializer(final CuratorFramework curatorFramework,
                           final QueueService queueService,
                           final String clientId,
                           final BaseStorage storage,
                           final StorageClient client,
                           final String farmId,
                           final MeterRegistry meterRegistry,
                           final IgnisSchedulerCommands scheduler,
                           final MagazineRegistry magazineRegistry) {
        this.magazineRegistry = magazineRegistry;
        this.ownsScheduler = Objects.isNull(scheduler);
        // Standalone, this is the control plane in its own right, so it gets the control pool's
        // fixed shape rather than a growable worker pool it would never grow.
        this.scheduler = ownsScheduler
                ? new IgnisSchedulerCommands("ignismq-control", Constants.SCHEDULER_CONTROL_THREADS,
                        Constants.SCHEDULER_CONTROL_THREADS)
                : scheduler;
        this.curatorFramework = curatorFramework;
        this.queueService = queueService;
        this.clientId = clientId;
        this.storage = storage;
        this.client = client;
        this.farmId = farmId;
        this.meterRegistry = Objects.requireNonNull(meterRegistry, "Meter registry is required.");
    }

    public void start() throws Exception {
        if (Objects.nonNull(leaderElector)) {
            log.info("Already initialised... Gracefully ignoring...");
            return;
        }

        final Sweeper sweeperTask = new Sweeper(queueService, clientId, storage, client, farmId,
                meterRegistry, magazineRegistry);
        final Map<Integer, Set<LoadBalancer>> workers = ImmutableMap.of(1, ImmutableSet.of(sweeperTask));
        leaderElector = new LeaderElector(clientId, curatorFramework, workers);
        leaderElector.start();
        scheduleSweeperTask(sweeperTask);
    }

    /**
     * Safe to call without a preceding {@link #start()}, and safe to call twice: a framework
     * lifecycle will invoke stop even when startup failed part way through.
     */
    public void stop() {
        if (Objects.nonNull(sweeperTask)) {
            scheduler.cancelRepeating(sweeperTask);
            sweeperTask = null;
        }
        if (ownsScheduler) {
            scheduler.stop();
        }
        if (Objects.isNull(leaderElector)) {
            log.info("Task initializer was never started, nothing to stop.");
            return;
        }
        leaderElector.stop();
        leaderElector = null;
    }

    private void scheduleSweeperTask(final Sweeper sweeper) {
        sweeperTask = scheduler.scheduleRepeating(sweeper,
                INITIAL_DELAY_FOR_SWEEPER_TASK, DELAY_FOR_SWEEPER_TASK);
    }
}
