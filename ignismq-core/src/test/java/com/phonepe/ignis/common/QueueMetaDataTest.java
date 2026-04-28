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

import org.junit.Test;

import static org.junit.Assert.*;

public class QueueMetaDataTest {

    @Test
    public void testBuilder() {
        QueueMetaData metaData = QueueMetaData.builder()
                .published(100)
                .consumed(50)
                .sidelined(10)
                .shovelled(5)
                .build();

        assertEquals(100, metaData.getPublished());
        assertEquals(50, metaData.getConsumed());
        assertEquals(10, metaData.getSidelined());
        assertEquals(5, metaData.getShovelled());
    }

    @Test
    public void testDefaultValues() {
        QueueMetaData metaData = QueueMetaData.builder().build();
        assertEquals(0, metaData.getPublished());
        assertEquals(0, metaData.getConsumed());
        assertEquals(0, metaData.getSidelined());
        assertEquals(0, metaData.getShovelled());
    }
}
