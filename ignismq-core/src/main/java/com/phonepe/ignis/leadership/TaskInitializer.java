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
import io.dropwizard.lifecycle.Managed;
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
public class TaskInitializer implements Managed {
    public static final int DELAY_FOR_SWEEPER_TASK = 15 * 60 * 1000; // 15 minutes
    private static final int INITIAL_DELAY_FOR_SWEEPER_TASK = 10 * 60 * 1000; // 10 minutes

    private final CuratorFramework curatorFramework;
    private final QueueService queueService;
    private final String clientId;
    private final BaseStorage storage;
    private final StorageClient client;
    private LeaderElector leaderElector;
    private final String farmId;

    public TaskInitializer(final CuratorFramework curatorFramework,
                           final QueueService queueService,
                           final String clientId,
                           final BaseStorage storage,
                           final StorageClient client,
                           final String farmId) {
        this.curatorFramework = curatorFramework;
        this.queueService = queueService;
        this.clientId = clientId;
        this.storage = storage;
        this.client = client;
        this.farmId = farmId;
    }

    @Override
    public void start() throws Exception {
        if (Objects.nonNull(leaderElector)) {
            log.info("Already initialised... Gracefully ignoring...");
            return;
        }

        final Sweeper sweeperTask = new Sweeper(queueService, clientId, storage, client, farmId);
        final Map<Integer, Set<LoadBalancer>> workers = ImmutableMap.of(1, ImmutableSet.of(sweeperTask));
        leaderElector = new LeaderElector(clientId, curatorFramework, workers);
        leaderElector.start();
        scheduleSweeperTask(sweeperTask);
    }

    @Override
    public void stop() {
        leaderElector.stop();
    }

    private void scheduleSweeperTask(final Sweeper sweeperTask) {
        // Run it as a daemon
        final Timer sweeperTimer = new Timer();
        sweeperTimer.schedule(
                sweeperTask,
                INITIAL_DELAY_FOR_SWEEPER_TASK,
                DELAY_FOR_SWEEPER_TASK
        );
    }
}
