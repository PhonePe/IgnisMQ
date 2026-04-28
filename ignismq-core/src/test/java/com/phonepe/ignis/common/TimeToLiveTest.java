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

import com.phonepe.ignis.utils.Constants;
import org.junit.Test;

import static org.junit.Assert.*;

public class TimeToLiveTest {

    @Test
    public void testToSecondsMinute() {
        TimeToLive ttl = TimeToLive.builder().timeUnit(TimeUnit.MINUTE).duration(5).build();
        assertEquals(300, ttl.toSeconds());
    }

    @Test
    public void testToSecondsHour() {
        TimeToLive ttl = TimeToLive.builder().timeUnit(TimeUnit.HOUR).duration(2).build();
        assertEquals(7200, ttl.toSeconds());
    }

    @Test
    public void testToSecondsDay() {
        TimeToLive ttl = TimeToLive.builder().timeUnit(TimeUnit.DAY).duration(1).build();
        assertEquals(86400, ttl.toSeconds());
    }

    @Test
    public void testIsValidTrue() {
        TimeToLive ttl = TimeToLive.builder().timeUnit(TimeUnit.DAY).duration(365).build();
        assertTrue(ttl.isValid());
    }

    @Test
    public void testIsValidFalse() {
        TimeToLive ttl = TimeToLive.builder().timeUnit(TimeUnit.DAY).duration(366).build();
        assertFalse(ttl.isValid());
    }

    @Test
    public void testIsValidExactly1Year() {
        // 365 days * 86400 = 31536000 which equals MAX_DURATION_ALLOWED_IN_SECONDS
        TimeToLive ttl = TimeToLive.builder().timeUnit(TimeUnit.DAY).duration(365).build();
        assertEquals(Constants.MAX_DURATION_ALLOWED_IN_SECONDS, ttl.toSeconds());
        assertTrue(ttl.isValid());
    }
}
