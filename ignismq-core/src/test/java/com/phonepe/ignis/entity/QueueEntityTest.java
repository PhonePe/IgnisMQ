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

package com.phonepe.ignis.entity;

import com.phonepe.ignis.config.BatchingConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class QueueEntityTest {

    @Test
    public void testBuilder() {
        QueueEntity entity = QueueEntity.builder()
                .messageHandlerType("handler")
                .shards(32)
                .queueExpiry(600)
                .messageExpiry(300)
                .concurrency(4)
                .shovelConcurrency(2)
                .shovelTimeIntervalInSecs(600)
                .createdAt(1000L)
                .active(true)
                .sweepDuration(20 * 60 * 1000L)
                .sweptCounter(10)
                .sidelineSweptCounter(5)
                .batchingConfig(BatchingConfig.builder().build())
                .build();

        assertEquals("handler", entity.getMessageHandlerType());
        assertEquals(32, entity.getShards());
        assertEquals(600, entity.getQueueExpiry());
        assertEquals(300, entity.getMessageExpiry());
        assertEquals(4, entity.getConcurrency());
        assertEquals(2, entity.getShovelConcurrency());
        assertEquals(600, entity.getShovelTimeIntervalInSecs());
        assertEquals(1000L, entity.getCreatedAt());
        assertTrue(entity.isActive());
        assertEquals(20 * 60 * 1000L, entity.getSweepDuration());
        assertEquals(10, entity.getSweptCounter());
        assertEquals(5, entity.getSidelineSweptCounter());
        assertNotNull(entity.getBatchingConfig());
    }

    @Test
    public void testSetters() {
        QueueEntity entity = QueueEntity.builder().build();
        entity.setActive(true);
        assertTrue(entity.isActive());
        entity.setConcurrency(10);
        assertEquals(10, entity.getConcurrency());
    }
}
