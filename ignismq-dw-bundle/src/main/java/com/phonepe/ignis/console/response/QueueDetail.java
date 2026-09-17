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

package com.phonepe.ignis.console.response;
import com.phonepe.ignis.common.ShardDepth;

import java.util.List;

/**
 * @param depth      null when this process does not hold the queue: depth is read through the live
 *                   magazines, which only an instance serving the queue has.
 * @param shards     null for the same reason.
 * @param instance   null when this process does not hold the queue.
 */
public record QueueDetail(QueueSummary queue,
                          QueueDepth depth,
                          List<ShardDepth> shards,
                          InstanceQueue instance) {
}
