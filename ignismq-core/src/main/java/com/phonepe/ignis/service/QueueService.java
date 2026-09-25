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

package com.phonepe.ignis.service;

import com.phonepe.ignis.entity.QueueEntity;

import java.util.Map;
import java.util.Optional;

/**
 * @author shantanu.tiwari
 */
public sealed interface QueueService permits AerospikeQueueService {
    boolean exists(final String name);

    Optional<QueueEntity> get(final String name);

    void store(final String name, final QueueEntity entity, final int ttl);

    void updateState(final String name, final boolean active);

    void updateConcurrency(final String name, final int concurrency);

    void updateShovelConfig(final String name, final int shovelConcurrency, final int shovelTimeInterval);

    Map<String, QueueEntity> getQueues(final boolean active);

    /**
     * Persists how far a sweep has progressed through each shard.
     * <p>
     * The pointers are absolute - the next slot to examine - rather than deltas, so a retried or
     * overlapping pass converges on the same value instead of compounding.
     *
     * @param shardPointers  shard id to the next slot to sweep, keyed as Magazine keys its shards.
     * @param sweptCounter   running total of messages re-homed, stored as an absolute value.
     */
    void updateSweepProgress(final String queueName, final boolean isSideline,
                             final Map<String, Long> shardPointers, final long sweptCounter);
}
