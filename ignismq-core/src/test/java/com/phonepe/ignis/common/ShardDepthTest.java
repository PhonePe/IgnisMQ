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

package com.phonepe.ignis.common;

import com.phonepe.magazine.entity.MetaData;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The per-shard view exists to make an uneven spread visible, so the ordering and the arithmetic
 * are the whole contract.
 */
class ShardDepthTest {

    @Test
    void aShardReportsItsOwnPublishedConsumedAndPending() {
        final List<ShardDepth> depths = ShardDepth.from(Map.of("q_0", metaData(10, 4)));

        assertEquals(1, depths.size());
        assertEquals("q_0", depths.get(0).getShard());
        assertEquals(10, depths.get(0).getPublished());
        assertEquals(4, depths.get(0).getConsumed());
        assertEquals(6, depths.get(0).getPending());
    }

    /**
     * The same floor {@code getUnconsumedCount} applies. A fire pointer ahead of its load pointer is
     * a transient of the two reads, not a negative backlog.
     */
    @Test
    void aConsumedCountAheadOfThePublishedCountReportsNoBacklogRatherThanANegativeOne() {
        final List<ShardDepth> depths = ShardDepth.from(Map.of("q_0", metaData(4, 10)));

        assertEquals(0, depths.get(0).getPending());
    }

    /**
     * Lexicographic ordering would put shard 10 between 1 and 2, which makes a table of 32 shards
     * unreadable for exactly the case the view is for.
     */
    @Test
    void shardsAreOrderedNumericallyRatherThanLexicographically() {
        final Map<String, MetaData> metaData = new LinkedHashMap<>();
        metaData.put("q_10", metaData(1, 0));
        metaData.put("q_2", metaData(1, 0));
        metaData.put("q_1", metaData(1, 0));

        assertEquals(List.of("q_1", "q_2", "q_10"),
                ShardDepth.from(metaData).stream().map(ShardDepth::getShard).toList());
    }

    /**
     * An unsharded magazine names its single shard after itself, with no numeric suffix at all.
     */
    @Test
    void aShardIdWithoutANumericSuffixIsStillReported() {
        final Map<String, MetaData> metaData = new LinkedHashMap<>();
        metaData.put("orders", metaData(3, 1));
        metaData.put("q_1", metaData(1, 0));

        final List<String> shards = ShardDepth.from(metaData).stream().map(ShardDepth::getShard).toList();

        assertEquals(2, shards.size());
        assertTrue(shards.containsAll(List.of("orders", "q_1")));
    }

    @Test
    void aMagazineWithNoMetadataReportsNoShards() {
        assertEquals(List.of(), ShardDepth.from(Map.of()));
    }

    private static MetaData metaData(final long loadPointer, final long firePointer) {
        return MetaData.builder()
                .loadPointer(loadPointer)
                .firePointer(firePointer)
                .loadCounter(loadPointer)
                .fireCounter(firePointer)
                .build();
    }
}
