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

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * @author shantanu.tiwari
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Constants {
    public static final int DEFAULT_SHARDS = 32;

    public static final String AEROSPIKE_DATA_SET = "data_set";
    public static final String AEROSPIKE_META_SET = "meta_set";

    public static final String MAGAZINE_SET_FORMAT = "%s_%s";
    public static final String MAGAZINE_LOCAL_SET_FORMAT = "%s_%s";
    public static final String MAGAZINE_SHARD_FORMAT = "%s_%d";
    public static final String MAGAZINE_DATA_KEY_FORMAT = "%s_SHARD_%d_%d";
    public static final String MAGAZINE_UNSHARDED_DATA_KEY_FORMAT = "%s_%d";
    public static final String MAGAZINE_META_KEY_FORMAT = "%s_SHARD_%d_%s";
    public static final String MAGAZINE_UNSHARDED_META_KEY_FORMAT = "%s_%s";
    public static final String MAGAZINE_SHARD_CONFIGURATION_KEY_FORMAT = "%s_SHARDS";
    public static final String MAGAZINE_SHARD_PREFIX = "SHARD";
    public static final String MAGAZINE_LEGACY_METADATA_SUFFIX = "POINTERS";
    public static final String MAGAZINE_UNIFIED_METADATA_SUFFIX = "METADATA";
    public static final String MAGAZINE_METADATA_SCHEMA_VERSION_BIN = "META_VERSION";
    public static final String MAGAZINE_SHARDS_BIN = "SHARDS";
    public static final String MAGAZINE_FIRE_POINTER_BIN = "FIRE_POINTER";
    public static final String MAGAZINE_DATA_BIN = "data";
    public static final int MAGAZINE_UNIFIED_METADATA_SCHEMA_VERSION = 2;

    public static final int PARALLEL_FACTOR = 64;

    public static final int INITIAL_DELAY_IN_MS = 2 * 60 * 1000; // 2 minutes
    public static final int WATCHER_INITIAL_DELAY_IN_MS = 60 * 1000; // 1 minute
    public static final int DELAY_PERIOD_IN_MS = 1000; // 1 second
    public static final int SHOVEL_DELAY_IN_MS = 10 * 1000; // 10 seconds
    public static final int DEFAULT_DURATION_DAY = 2;

    public static final int MAX_ALLOWED_SHOVEL_TIME_INTERVAL_IN_SECONDS = 24 * 60 * 60; // 1 day
    public static final int MAX_DURATION_ALLOWED_IN_SECONDS = 365 * 24 * 60 * 60; // 1 year
    public static final int REFRESH_INTERVAL_IN_MS = 5 * 60 * 1000; // 5 minutes
    public static final int MAX_CONSUMERS_ALLOWED = 100;
    public static final int TTL_FACTOR_FOR_QUEUE_EXPIRY = 2;
}
