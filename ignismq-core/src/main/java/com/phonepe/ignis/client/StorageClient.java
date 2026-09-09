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

package com.phonepe.ignis.client;

/**
 * A connection holder for a storage backend.
 * <p>
 * Implementations connect eagerly on construction. {@link #stop()} exists so that whoever created
 * the client can release its connection pool; it is deliberately a no-op by default so that
 * callers who pass in an externally owned client are not forced to implement teardown.
 *
 * @author shantanu.tiwari
 */
public interface StorageClient<T> {
    T getClient();

    /**
     * Releases any resources held by this client. Must be idempotent.
     */
    default void stop() {
        // No-op: externally managed clients are torn down by their owner.
    }
}
