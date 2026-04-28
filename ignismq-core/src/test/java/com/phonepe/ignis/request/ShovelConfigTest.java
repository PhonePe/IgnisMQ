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

import org.junit.Test;

import static org.junit.Assert.*;

public class ShovelConfigTest {

    @Test
    public void testDefaults() {
        ShovelConfig config = ShovelConfig.builder().build();
        assertEquals(600, config.getTimeIntervalInSecs());
        assertEquals(4, config.getConcurrency());
    }

    @Test
    public void testCustomValues() {
        ShovelConfig config = ShovelConfig.builder()
                .concurrency(10)
                .timeIntervalInSecs(300)
                .build();

        assertEquals(10, config.getConcurrency());
        assertEquals(300, config.getTimeIntervalInSecs());
    }
}
