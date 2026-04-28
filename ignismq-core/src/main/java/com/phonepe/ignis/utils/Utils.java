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

import com.phonepe.ignis.MagazineQueue.MagazineStorageVisitor;
import com.phonepe.ignis.client.StorageClient;
import com.phonepe.ignis.entity.QueueEntity;
import com.phonepe.ignis.exception.IgnisMQException;
import com.phonepe.ignis.service.QueueService;
import com.phonepe.ignis.storage.BaseStorage;
import com.phonepe.magazine.Magazine;
import com.phonepe.magazine.common.MetaData;
import io.appform.functionmetrics.MonitoredFunction;
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
import java.util.stream.IntStream;

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

    public static String createMagazineAerospikeKey(final long firePointer, final int shard,
                                                    final String queueName) {
        return String.format(Constants.MAGAZINE_DATA_KEY_FORMAT, queueName, shard, firePointer);
    }

    public static String createFirePointerKey(final String queueName, final int shard) {
        return String.format(Constants.MAGAZINE_META_KEY_FORMAT,
                queueName, shard, com.phonepe.magazine.common.Constants.POINTERS);
    }

    public static String getMagazineSet(final String clientId, final String setName) {
        return String.format(Constants.MAGAZINE_SET_FORMAT, clientId, setName);
    }

    public static String getShardId(int shard) {
        return String.format(Constants.MAGAZINE_SHARD_FORMAT, com.phonepe.magazine.common.Constants.SHARD_PREFIX, shard);
    }

    @MonitoredFunction
    public static void sweepQueue(
            final QueueService queueService, final String clientId, final StorageClient client,
            final BaseStorage storage, final String queueName, final QueueEntity queueEntity,
            final String farmId) {
        try {
            long sweepTillFireTS = System.currentTimeMillis() - queueEntity.getSweepDuration();
            final Magazine<String> sidelineMagazine = Magazine.<String>builder()
                    .baseMagazineStorage(storage.accept(new MagazineStorageVisitor(
                            clientId, queueEntity.getMessageExpiry(),
                            queueEntity.getQueueExpiry() * Constants.TTL_FACTOR_FOR_QUEUE_EXPIRY,
                            client, queueEntity.getShards(), farmId)))
                    .magazineIdentifier(Utils.getSidelineQueueName(queueName))
                    .build();
            log.info("Sweeping queue {}", queueName);
            IntStream.range(0, queueEntity.getShards()).boxed()
                    .forEach(shard -> {
                        try {
                            queueService.sweep(queueName, shard, sweepTillFireTS, sidelineMagazine);
                        } catch (Exception e) {
                            log.error("Sweeping failed for queue {}, shard {}", queueName, shard, e);
                        }
                    });
        } catch (Exception e) {
            log.error("Sweeping failed!", e);
        }
    }

    public static void waitForRequestsCompletion(final List<Future<Boolean>> futureList) {
        for (Future<Boolean> future : futureList) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw IgnisMQException.propagate(e);
            }
        }
        futureList.clear();
    }

    @MonitoredFunction
    public static long getMagazineCount(final Collection<MetaData> allShardsMetaData,
                                        final Function<MetaData, Long> mappingFunction) {
        return allShardsMetaData.stream()
                .map(mappingFunction)
                .reduce(Long::sum)
                .orElse(0L);
    }
}
