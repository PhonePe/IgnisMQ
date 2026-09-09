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
    public static final String MAGAZINE_SHARD_FORMAT = "%s_%d";
    public static final String MAGAZINE_SHARD_PREFIX = "SHARD";
    public static final int SWEEP_BATCH_SIZE = 1000;

    /**
     * Checkpoints Magazine retains per shard. Fixed rather than derived: this map rides the fire
     * pointer record, which is rewritten on every claim, so its size is a hot-path cost and must
     * not scale with anything.
     */
    public static final int FIRE_HISTORY_ENTRIES = 32;

    /**
     * Checkpoint windows per sweep duration. With {@link #FIRE_HISTORY_ENTRIES} retained, the
     * nominal span is {@code entries / divisor} sweep durations - four here - so a sweep asking
     * about {@code now - sweepDuration} is comfortably inside the retained history and can never
     * trip Magazine's evicted-history failure.
     */
    public static final int FIRE_HISTORY_WINDOWS_PER_SWEEP_DURATION = 8;

    /**
     * Hard ceiling on how far back a sweep may look, per the delivery-time watermark design. The
     * request-level {@code sweepDurationInMins} bound is stricter still; this is the invariant the
     * history sizing is allowed to assume.
     */
    public static final long MAX_SWEEP_DURATION_IN_MS = 12 * 60 * 60 * 1000L;

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
