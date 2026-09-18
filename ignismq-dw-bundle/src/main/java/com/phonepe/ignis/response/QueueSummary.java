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

package com.phonepe.ignis.response;
/**
 * One queue as storage describes it, plus whether this process is one of the instances serving it.
 * <p>
 * Everything here except {@code heldHere} is true of the cluster. {@code heldHere} is true of this
 * process only, and is present because it decides which of the other views can answer at all.
 */
public record QueueSummary(String name,
                           boolean active,
                           boolean heldHere,
                           int shards,
                           int concurrency,
                           long createdAt,
                           int messageExpirySeconds,
                           int queueExpirySeconds,
                           long sweepDurationMillis,
                           long handlerTimeoutMillis,
                           Integer shovelConcurrency,
                           Integer shovelIntervalSeconds,
                           Integer maxBatchSize,
                           Integer maxWaitTimeSeconds,
                           String messageHandlerType) {
}
