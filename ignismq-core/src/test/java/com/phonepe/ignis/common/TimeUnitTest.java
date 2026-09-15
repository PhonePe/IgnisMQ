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

package com.phonepe.ignis.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TimeUnitTest {

    @Test
    public void testMinuteToSeconds() {
        assertEquals(60, TimeUnit.MINUTE.toSeconds(1));
        assertEquals(300, TimeUnit.MINUTE.toSeconds(5));
    }

    @Test
    public void testHourToSeconds() {
        assertEquals(3600, TimeUnit.HOUR.toSeconds(1));
        assertEquals(7200, TimeUnit.HOUR.toSeconds(2));
    }

    @Test
    public void testDayToSeconds() {
        assertEquals(86400, TimeUnit.DAY.toSeconds(1));
        assertEquals(172800, TimeUnit.DAY.toSeconds(2));
    }

    @Test
    public void testAllValues() {
        assertEquals(3, TimeUnit.values().length);
        assertNotNull(TimeUnit.valueOf("MINUTE"));
        assertNotNull(TimeUnit.valueOf("HOUR"));
        assertNotNull(TimeUnit.valueOf("DAY"));
    }
}
