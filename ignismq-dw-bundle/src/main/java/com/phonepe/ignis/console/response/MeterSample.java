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

import java.util.Map;

/**
 * One meter as this process currently holds it.
 *
 * @param measurements statistic name to value, as Micrometer reports them. A counter has one, a
 *                     timer has several.
 */
public record MeterSample(String name, Map<String, String> tags, String type,
                          Map<String, Double> measurements) {
}
