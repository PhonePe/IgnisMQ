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

package com.phonepe.ignis.storage;

import com.aerospike.client.IAerospikeClient;
import com.phonepe.ignis.client.StorageClient;
import com.phonepe.ignis.utils.Constants;
import com.phonepe.ignis.utils.Utils;
import com.phonepe.magazine.core.BaseMagazineStorage;
import com.phonepe.magazine.entity.MagazineScope;
import com.phonepe.magazine.impl.aerospike.AerospikeStorageConfig;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.AllArgsConstructor;

/**
 * Builds the Magazine storage every ignisMQ magazine sits on, from a queue's stored configuration.
 * <p>
 * Lives here rather than nested in {@code MagazineQueue} because the sweeper needs it too, and a
 * queue type owning the storage recipe made the sweep package depend back on the queue - a genuine
 * package cycle, caught by the architecture rules.
 */
@AllArgsConstructor
public final class MagazineStorageVisitor implements StorageVisitor<BaseMagazineStorage<String>> {
    private final String clientId;
    private final int recordTtlInSeconds;
    private final int metaDataTtlInSeconds;
    private final StorageClient client;
    private final int shards;
    private final String farmId;
    private final MeterRegistry meterRegistry;
    private final long sweepDurationInMillis;

    @Override
    public BaseMagazineStorage<String> visit(final AerospikeStorage storage) {
        return com.phonepe.magazine.impl.aerospike.AerospikeStorage.<String>builder()
                .clazz(String.class)
                .storageConfig(AerospikeStorageConfig.builder()
                        .dataSetName(Utils.getMagazineSet(clientId, Constants.AEROSPIKE_DATA_SET))
                        .metaSetName(Utils.getMagazineSet(clientId, Constants.AEROSPIKE_META_SET))
                        .namespace(storage.getNamespace())
                        .shards(shards)
                        .recordTtl(recordTtlInSeconds)
                        .metaDataTtl(metaDataTtlInSeconds)
                        // Delivery-time watermarking. Every magazine ignisMQ builds records it,
                        // including the ones the consumers hold: checkpoints are written from
                        // Magazine's own active-shard refresh, so a magazine that is only
                        // published to and consumed from is exactly the one that must be
                        // recording. Switching it on only for the sweeper's own handles would
                        // leave nothing for the sweeper to read.
                        // Explicit, not inherited: this is the worst-case publish-to-consume
                        // delay on an idle queue and the cadence at which delivery-time
                        // checkpoints are recorded. See the constant.
                        .activeShardRefreshSeconds(Constants.ACTIVE_SHARD_REFRESH_SECONDS)
                        .fireHistoryEnabled(true)
                        .fireHistoryWindowSeconds(Utils.fireHistoryWindowSeconds(sweepDurationInMillis))
                        .fireHistoryEntries(Constants.FIRE_HISTORY_ENTRIES)
                        .build())
                .aerospikeClient((IAerospikeClient) client.getClient())
                .enableDeDupe(false)
                .farmId(farmId)
                .scope(MagazineScope.LOCAL)
                .clientId(clientId)
                .meterRegistry(meterRegistry)
                .build();
    }
}
