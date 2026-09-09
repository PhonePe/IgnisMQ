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
import com.phonepe.ignis.client.StorageClient;
import com.phonepe.ignis.sweep.Sweeper;
import com.phonepe.ignis.service.QueueService;
import com.phonepe.ignis.storage.BaseStorage;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Timer;

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
    private Timer sweeperTimer;
    private final String farmId;
    private final MeterRegistry meterRegistry;

    public TaskInitializer(final CuratorFramework curatorFramework,
                           final QueueService queueService,
                           final String clientId,
                           final BaseStorage storage,
                           final StorageClient client,
                           final String farmId,
                           final MeterRegistry meterRegistry) {
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

        final Sweeper sweeperTask = new Sweeper(queueService, clientId, storage, client, farmId, meterRegistry);
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
        if (Objects.nonNull(sweeperTimer)) {
            sweeperTimer.cancel();
            sweeperTimer = null;
        }
        if (Objects.isNull(leaderElector)) {
            log.info("Task initializer was never started, nothing to stop.");
            return;
        }
        leaderElector.stop();
        leaderElector = null;
    }

    private void scheduleSweeperTask(final Sweeper sweeperTask) {
        // Run it as a daemon
        sweeperTimer = new Timer();
        sweeperTimer.schedule(
                sweeperTask,
                INITIAL_DELAY_FOR_SWEEPER_TASK,
                DELAY_FOR_SWEEPER_TASK
        );
    }
}
