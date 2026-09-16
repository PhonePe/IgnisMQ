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

import com.phonepe.ignis.utils.Constants;
import lombok.Builder;
import lombok.Value;

/** Tunables for an {@link IgnisMQManager}. Everything here has a working default. */
@Value
@Builder(toBuilder = true)
public class IgnisMQSettings {

    /**
     * Ceiling on threads running consumers and shovels, and on those running message handlers. Size
     * it as concurrently-active queues x their concurrency, not total configured consumers. A
     * queue's {@code concurrency} is a target rather than a guarantee.
     */
    @Builder.Default
    int workerThreads = Constants.DEFAULT_WORKER_THREADS;

    /**
     * Makes every {@code ignismq.*} meter a no-op when false. Magazine's meters are unaffected -
     * those are switched off through Magazine's own configuration.
     */
    @Builder.Default
    boolean metricsEnabled = Constants.DEFAULT_METRICS_ENABLED;

    public static IgnisMQSettings defaults() {
        return IgnisMQSettings.builder().build();
    }
}
