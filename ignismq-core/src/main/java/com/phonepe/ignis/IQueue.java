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

package com.phonepe.ignis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.phonepe.ignis.common.QueueMetaData;
import com.phonepe.ignis.common.ShardDepth;

import java.util.List;

/**
 * @author shantanu.tiwari
 */
public sealed interface IQueue<M> permits MagazineQueue {
    /**
     * @param message to be published in queue.
     * @return true if successfully published in the queue.
     */
    boolean publish(final M message) throws JsonProcessingException;

    /**
     * @return remaining messages count that are yet to be consumed i.e publishedCount - consumedCount.
     */
    long getUnconsumedCount();

    /**
     * publishedCount: No of messages published in the queue.
     * consumedCount: No of consumed messages.
     *
     * @return QueueMetaData
     */
    QueueMetaData getMetaData();

    /**
     * The same published and consumed counts as {@link #getMetaData()}, split by shard of the main
     * magazine.
     * <p>
     * Publishing picks a shard at random, so an even spread is the expected shape; a shard sitting
     * far from the others is one whose consumers are not keeping up with it.
     *
     * @return one entry per shard, ordered by shard index.
     */
    List<ShardDepth> getShardDepths();

    /**
     * To shovel the messages from sideline magazine to main magazine explicitly without any delay.
     * It will stop as soon as there is no data remaining in the magazine
     * <p>
     * This will be running on one instance of any distributed system. Watcher won't create shovel tasks across instances.
     *
     * @param concurrency: Number of consumers parallelly running to shovel the messages
     */
    void shovel(final int concurrency);
}
