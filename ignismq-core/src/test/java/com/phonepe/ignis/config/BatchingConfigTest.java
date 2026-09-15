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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BatchingConfigTest {

    @Test
    public void testDefaults() {
        BatchingConfig config = BatchingConfig.builder().build();
        assertEquals(2, config.getMaxBatchSize());
        assertEquals(1, config.getMaxWaitTimeInSecs());
    }

    @Test
    public void testCustomValues() {
        BatchingConfig config = BatchingConfig.builder()
                .maxBatchSize(100)
                .maxWaitTimeInSecs(30)
                .build();

        assertEquals(100, config.getMaxBatchSize());
        assertEquals(30, config.getMaxWaitTimeInSecs());
    }
}
