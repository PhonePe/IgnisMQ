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

import java.util.List;

/**
 * Meters held by this process, and nothing else. There is no cross-instance total here and there
 * cannot be: ignisMQ has no register of instances. Scrape and aggregate in a metrics backend.
 */
public record InstanceMetrics(String clientId, String farmId, boolean metricsEnabled,
                              int meterCount, List<MeterSample> meters) {
}
