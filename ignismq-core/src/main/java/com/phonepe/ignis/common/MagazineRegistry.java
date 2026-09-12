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

import com.phonepe.magazine.Magazine;

/**
 * Finds the magazines this process already holds for a queue.
 * <p>
 * Exists so the sweeper can reuse them instead of constructing its own pair on every pass. That is
 * not only about allocation: Magazine suppresses duplicate delivery-time checkpoint writes through a
 * claim held <em>on the storage instance</em>, so a storage built fresh for one sweep has claimed
 * nothing and re-issues a write per shard that the long-lived consumer magazine already made.
 * <p>
 * Declared here rather than on the manager because the sweep package must not depend on the manager
 * - that pairing was a package cycle, and the architecture rules now reject it.
 */
@FunctionalInterface
public interface MagazineRegistry {

    /**
     * @param queueName the queue to look up.
     * @return the live pair, or null when this process does not hold one. Null is an ordinary
     *         answer, not a failure: the queue watcher refreshes on a five-minute cycle, so a queue
     *         created on another instance is genuinely absent here for a while, and the caller is
     *         expected to fall back to building its own rather than skip the queue.
     */
    QueueMagazines find(String queueName);

    /** A queue's main magazine and the sideline it spills into. */
    record QueueMagazines(Magazine<String> main, Magazine<String> sideline) {
    }
}
