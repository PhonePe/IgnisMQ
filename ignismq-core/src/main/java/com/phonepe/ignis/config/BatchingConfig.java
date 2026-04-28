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

package com.phonepe.ignis.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

/**
 * @author shantanu.tiwari
 * Created on 20/11/22
 */
@Data
@Builder
@AllArgsConstructor
public class BatchingConfig {
    @Min(2)
    @Max(200000)
    @Builder.Default
    private int maxBatchSize = 2; // Maximum allowed batch size for queue consumer
    @Min(1) // 1 second
    @Max(300) // 300 seconds
    @Builder.Default
    private int maxWaitTimeInSecs = 1; // Maximum allowed time till when batching will happen
}
