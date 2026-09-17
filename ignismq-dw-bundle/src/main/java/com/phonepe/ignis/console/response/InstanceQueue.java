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
/**
 * What this process is running for one queue. Consumer and shovel counts are per-instance by
 * construction - the cluster total is the sum over instances, which no instance can see.
 */
public record InstanceQueue(String name,
                            int consumers,
                            int shovels,
                            Integer shovelConcurrency,
                            Integer shovelIntervalSeconds) {
}
