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
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * One shard's share of a queue, using the same pointers {@link QueueMetaData} totals.
 *
 * @author shantanu.tiwari
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ShardDepth {

    private String shard;
    private long published;
    private long consumed;
    private long pending;

    /**
     * @param shardMetaData a magazine's metadata, shard id to counters.
     * @return one entry per shard, ordered by the numeric suffix of the shard id where it has one.
     */
    public static List<ShardDepth> from(final Map<String, MetaData> shardMetaData) {
        return shardMetaData.entrySet().stream()
                .sorted(Map.Entry.comparingByKey(ShardDepth::compareShardIds))
                .map(entry -> of(entry.getKey(), entry.getValue()))
                .toList();
    }

    private static ShardDepth of(final String shard, final MetaData metaData) {
        return ShardDepth.builder()
                .shard(shard)
                .published(metaData.getLoadPointer())
                .consumed(metaData.getFirePointer())
                .pending(Math.max(metaData.getLoadPointer() - metaData.getFirePointer(), 0L))
                .build();
    }

    /**
     * Shard ids end in an index, and comparing those as text puts 10 between 1 and 2 - which is
     * unreadable at the shard counts this is most often looked at with.
     */
    private static int compareShardIds(final String left, final String right) {
        final int leftIndex = shardIndex(left);
        final int rightIndex = shardIndex(right);
        return leftIndex >= 0 && rightIndex >= 0
                ? Integer.compare(leftIndex, rightIndex)
                : left.compareTo(right);
    }

    private static int shardIndex(final String shard) {
        final int separator = shard.lastIndexOf('_');
        if (separator < 0 || separator == shard.length() - 1) {
            return -1;
        }
        for (int position = separator + 1; position < shard.length(); position++) {
            if (!Character.isDigit(shard.charAt(position))) {
                return -1;
            }
        }
        try {
            return Integer.parseInt(shard, separator + 1, shard.length(), 10);
        } catch (NumberFormatException e) {
            return -1;
        }
    }
}
