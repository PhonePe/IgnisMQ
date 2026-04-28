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

package com.phonepe.ignis.request;

import com.phonepe.ignis.utils.Constants;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

/**
 * @author shantanu.tiwari
 */
@Data
@Builder
@AllArgsConstructor
public class ShovelConfig {
    @Max(Constants.MAX_ALLOWED_SHOVEL_TIME_INTERVAL_IN_SECONDS)
    @Builder.Default
    private int timeIntervalInSecs = 10 * 60; // 10 minutes
    @Min(1)
    @Max(50)
    @Builder.Default
    private int concurrency = 4;
}
