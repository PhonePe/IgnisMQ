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

package com.phonepe.ignis.sweep;

import com.phonepe.ignis.client.StorageClient;
import com.phonepe.ignis.utils.Utils;
import com.phonepe.ignis.service.QueueService;
import com.phonepe.ignis.entity.QueueEntity;
import com.phonepe.ignis.leadership.LoadBalancer;
import com.phonepe.ignis.storage.BaseStorage;
import lombok.extern.slf4j.Slf4j;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author shantanu.tiwari
 * Created on 12/03/22
 */
@Slf4j
public class Sweeper extends TimerTask implements LoadBalancer {
    private final AtomicBoolean active = new AtomicBoolean(false);
    private final QueueService queueService;
    private final QueueSweeper queueSweeper;

    public Sweeper(final QueueService queueService, final String clientId,
                   final BaseStorage storage, final StorageClient client,
                   final String farmId, final MeterRegistry meterRegistry) {
        this.queueService = queueService;
        this.queueSweeper = new QueueSweeper(queueService, clientId, storage, client, farmId, meterRegistry);
    }

    @Override
    public void run() {
        if (active.get()) {
            try {
                log.info("Starting IGNIS sweeping!");
                final Map<String, QueueEntity> queues = queueService.getQueues(true);
                final List<Future<Boolean>> futureList = new ArrayList<>();
                queues.forEach((queueName, queueEntity) ->
                        futureList.add(Utils.executorService.submit(() -> {
                            queueSweeper.sweepQueue(queueName, queueEntity);
                            return true;
                        })));
                Utils.waitForRequestsCompletion(futureList);
            } catch (Exception e) {
                log.error("Error sweeping queues, should get swept in next sweep cycle", e);
            }
        }
    }

    @Override
    public void activate() {
        active.set(true);
    }

    @Override
    public void deactivate() {
        active.set(false);
    }
}
