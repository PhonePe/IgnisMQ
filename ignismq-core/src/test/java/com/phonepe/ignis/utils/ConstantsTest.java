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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ConstantsTest {

    @Test
    public void testConstants() {
        assertEquals(8, Constants.DEFAULT_SHARDS);
        assertEquals(64, Constants.PARALLEL_FACTOR);
        assertEquals(100, Constants.MAX_CONSUMERS_ALLOWED);
        assertEquals(2, Constants.TTL_FACTOR_FOR_QUEUE_EXPIRY);
        assertEquals(1000, Constants.INITIAL_DELAY_IN_MS);
        assertEquals(1000, Constants.DELAY_PERIOD_IN_MS);
        assertEquals(10 * 1000, Constants.SHOVEL_DELAY_IN_MS);
        assertEquals(24 * 60 * 60, Constants.MAX_ALLOWED_SHOVEL_TIME_INTERVAL_IN_SECONDS);
        assertEquals(365 * 24 * 60 * 60, Constants.MAX_DURATION_ALLOWED_IN_SECONDS);
        assertEquals(5 * 60 * 1000, Constants.REFRESH_INTERVAL_IN_MS);
        assertEquals(60 * 1000, Constants.WATCHER_INITIAL_DELAY_IN_MS);
        assertEquals(2, Constants.DEFAULT_DURATION_DAY);

        assertNotNull(Constants.AEROSPIKE_DATA_SET);
        assertNotNull(Constants.AEROSPIKE_META_SET);
        assertNotNull(Constants.MAGAZINE_SET_FORMAT);
        assertNotNull(Constants.MAGAZINE_SHARD_FORMAT);
        assertNotNull(Constants.MAGAZINE_SHARD_PREFIX);
        assertEquals(1000, Constants.SWEEP_BATCH_SIZE);
        assertEquals(32, Constants.FIRE_HISTORY_ENTRIES);
        assertEquals(8, Constants.FIRE_HISTORY_WINDOWS_PER_SWEEP_DURATION);
        assertEquals(12 * 60 * 60 * 1000L, Constants.MAX_SWEEP_DURATION_IN_MS);
    }
}
