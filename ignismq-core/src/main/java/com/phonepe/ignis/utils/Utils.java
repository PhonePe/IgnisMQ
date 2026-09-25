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

package com.phonepe.ignis.utils;

import com.phonepe.ignis.exception.IgnisMQException;
import com.phonepe.magazine.entity.MetaData;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;

/**
 * @author shantanu.tiwari
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Utils {
    public static final ExecutorService executorService = Executors.newFixedThreadPool(Constants.PARALLEL_FACTOR);

    public static String getSidelineQueueName(final String name) {
        return String.format("%s_SIDELINE", name);
    }

    public static String getMagazineSet(final String clientId, final String setName) {
        return String.format(Constants.MAGAZINE_SET_FORMAT, clientId, setName);
    }

    public static String getShardId(int shard) {
        return String.format(Constants.MAGAZINE_SHARD_FORMAT, Constants.MAGAZINE_SHARD_PREFIX, shard);
    }

    /**
     * Window width Magazine should use for one delivery-time checkpoint, given how far back this
     * queue's sweep needs to see.
     * <p>
     * Reach is traded for resolution, and the map size pays for neither: eviction is by count, so
     * the record carries {@link Constants#FIRE_HISTORY_ENTRIES} checkpoints whatever this returns.
     * Dividing the sweep duration into {@link Constants#FIRE_HISTORY_WINDOWS_PER_SWEEP_DURATION}
     * windows spans several sweep durations, so the only question the sweeper asks - "where was the
     * pointer one sweep duration ago" - is never near the eviction edge, while an answer is still
     * precise to an eighth of that duration.
     */
    public static int fireHistoryWindowSeconds(final long sweepDurationInMillis) {
        final long bounded = Math.min(Math.max(sweepDurationInMillis, 0L), Constants.MAX_SWEEP_DURATION_IN_MS);
        return (int) Math.max(1L,
                bounded / 1000L / Constants.FIRE_HISTORY_WINDOWS_PER_SWEEP_DURATION);
    }

    /**
     * Clamps a configured handler timeout to half the sweep duration, which is the correctness
     * bound: past sweepDuration the sweeper sidelines and deletes a record whose handler is still
     * running.
     */
    public static long handlerTimeoutMillis(final long configuredTimeoutInMillis,
                                            final long sweepDurationInMillis) {
        final long boundedSweep = Math.min(Math.max(sweepDurationInMillis, 0L),
                Constants.MAX_SWEEP_DURATION_IN_MS);
        final long ceiling = boundedSweep / Constants.HANDLER_TIMEOUT_SWEEP_DIVISOR;
        return Math.max(1L, Math.min(configuredTimeoutInMillis, ceiling));
    }

    public static void waitForRequestsCompletion(final List<Future<Boolean>> futureList) {
        for (Future<Boolean> future : futureList) {
            try {
                future.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw IgnisMQException.propagate(e);
            } catch (ExecutionException e) {
                throw IgnisMQException.propagate(e);
            }
        }
        futureList.clear();
    }

    public static long getMagazineCount(final Collection<MetaData> allShardsMetaData,
                                        final Function<MetaData, Long> mappingFunction) {
        return allShardsMetaData.stream()
                .map(mappingFunction)
                .reduce(Long::sum)
                .orElse(0L);
    }
}
