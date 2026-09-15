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

package com.phonepe.ignis.metric;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class QueueStatTest {

    @Test
    public void testBuilder() {
        QueueStat stat = QueueStat.builder()
                .name("QUEUE_1")
                .active(true)
                .published(100)
                .consumed(50)
                .unConsumed(50)
                .sidelined(10)
                .shovelled(5)
                .build();

        assertEquals("QUEUE_1", stat.getName());
        assertTrue(stat.isActive());
        assertEquals(100, stat.getPublished());
        assertEquals(50, stat.getConsumed());
        assertEquals(50, stat.getUnConsumed());
        assertEquals(10, stat.getSidelined());
        assertEquals(5, stat.getShovelled());
    }
}
