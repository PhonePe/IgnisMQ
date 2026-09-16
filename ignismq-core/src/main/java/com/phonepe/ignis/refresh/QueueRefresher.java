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

package com.phonepe.ignis.refresh;

import com.phonepe.ignis.entity.QueueEntity;
import com.phonepe.ignis.service.QueueService;
import com.phonepe.ignis.utils.Constants;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Reconciles the queues a process is running with what the store says they should be.
 * <p>
 * Runs on the watcher schedule and on handler registration, so it is the path by which a queue
 * created, expired, deactivated or resized on another instance takes effect here.
 */
@Slf4j
public final class QueueRefresher {

    private final QueueService queueService;
    private final QueueLifecycle lifecycle;

    public QueueRefresher(final QueueService queueService, final QueueLifecycle lifecycle) {
        this.queueService = queueService;
        this.lifecycle = lifecycle;
    }

    public void refresh() {
        final Map<String, QueueEntity> active = queueService.getQueues(true);
        final Map<String, QueueEntity> inactive = queueService.getQueues(false);
        final Set<String> liveBefore = lifecycle.liveQueueNames();

        retireExpired(active, inactive);
        adopt(active, liveBefore);
        retireDeactivated(inactive, liveBefore);
        resize(active, liveBefore);
    }

    /** Expiry is evaluated here rather than by the store, which has no clock of its own. */
    private void retireExpired(final Map<String, QueueEntity> active,
                               final Map<String, QueueEntity> inactive) {
        final Map<String, QueueEntity> all = new HashMap<>(inactive);
        all.putAll(active);
        all.entrySet().stream()
                .filter(entry -> entry.getValue().isActive())
                .filter(entry -> hasExpired(entry.getValue()))
                .forEach(entry -> {
                    lifecycle.deactivate(entry.getKey());
                    active.remove(entry.getKey());
                });
    }

    /** A queue created on another instance is adopted here; one that fails is retried next pass. */
    private void adopt(final Map<String, QueueEntity> active, final Set<String> liveBefore) {
        active.entrySet().stream()
                .filter(entry -> !liveBefore.contains(entry.getKey()))
                .forEach(entry -> {
                    try {
                        lifecycle.adopt(entry.getKey(), entry.getValue());
                    } catch (Exception e) {
                        log.error("Error creating queue '{}' in watcher", entry.getKey(), e);
                    }
                });
    }

    private void retireDeactivated(final Map<String, QueueEntity> inactive, final Set<String> liveBefore) {
        inactive.keySet().stream()
                .filter(liveBefore::contains)
                .forEach(lifecycle::deactivate);
    }

    /** Concurrency and shovel configuration are the two things editable on a running queue. */
    private void resize(final Map<String, QueueEntity> active, final Set<String> liveBefore) {
        liveBefore.stream()
                .filter(active::containsKey)
                .forEach(queueName -> {
                    final RefreshableQueue queue = lifecycle.queue(queueName);
                    if (Objects.isNull(queue)) {
                        return;
                    }
                    applyConcurrency(queue, active.get(queueName).getConcurrency());
                    applyShovelConfig(queue, active.get(queueName));
                });
    }

    private void applyConcurrency(final RefreshableQueue queue, final int wanted) {
        final int current = queue.getNoOfConsumers();
        if (current < wanted) {
            queue.createConsumers(wanted - current);
        } else if (current > wanted) {
            queue.stopConsumers(current - wanted);
        }
    }

    private void applyShovelConfig(final RefreshableQueue queue, final QueueEntity entity) {
        if (!shovelConfigChanged(queue, entity)) {
            return;
        }
        queue.stopShovelConsumers(Constants.MAX_CONSUMERS_ALLOWED);
        queue.scheduleShoveling(entity.getShovelConcurrency(), entity.getShovelTimeIntervalInSecs());
    }

    private boolean shovelConfigChanged(final RefreshableQueue queue, final QueueEntity entity) {
        if (Objects.isNull(queue.getShovelConfig())) {
            return entity.getShovelConcurrency() > 0 && entity.getShovelTimeIntervalInSecs() > 0;
        }
        return queue.getShovelConfig().getConcurrency() != entity.getShovelConcurrency()
                || queue.getShovelConfig().getTimeIntervalInSecs() != entity.getShovelTimeIntervalInSecs();
    }

    private static boolean hasExpired(final QueueEntity entity) {
        return entity.getCreatedAt() + (entity.getQueueExpiry() * 1000L) <= System.currentTimeMillis();
    }

    /**
     * What the refresher may do to the process's set of queues. Implemented by the manager, which
     * owns that set, so that a refresh pass never reaches into it directly.
     */
    public interface QueueLifecycle {

        Set<String> liveQueueNames();

        /** Null when the queue is not live here, which a concurrent deactivation can cause. */
        RefreshableQueue queue(String queueName);

        /** Builds the queue, starts it and adds it to the live set. */
        void adopt(String queueName, QueueEntity entity) throws Exception;

        void deactivate(String queueName);
    }
}
