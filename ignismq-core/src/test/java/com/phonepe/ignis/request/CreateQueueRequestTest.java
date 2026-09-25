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

import com.phonepe.ignis.common.TimeToLive;
import com.phonepe.ignis.common.TimeUnit;
import com.phonepe.ignis.config.BatchingConfig;
import com.phonepe.ignis.utils.Constants;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CreateQueueRequestTest {

    @Test
    void testDefaults() {
        CreateQueueRequest request = CreateQueueRequest.builder()
                .name("QUEUE_1")
                .concurrency(5)
                .messageHandlerType("handler")
                .build();

        assertEquals(Constants.DEFAULT_SHARDS, (int) request.getShards());
        assertNotNull(request.getMessageExpiry());
        assertNotNull(request.getQueueExpiry());
        assertEquals(TimeUnit.DAY, request.getMessageExpiry().getTimeUnit());
        assertEquals(Constants.DEFAULT_DURATION_DAY, request.getMessageExpiry().getDuration());
        assertEquals(20, request.getSweepDurationInMins());
        assertNull(request.getShovelConfig());
        assertNull(request.getBatchingConfig());
    }

    @Test
    void testCustomValues() {
        CreateQueueRequest request = CreateQueueRequest.builder()
                .name("QUEUE_1")
                .shards(64)
                .concurrency(10)
                .messageHandlerType("handler")
                .messageExpiry(TimeToLive.builder().timeUnit(TimeUnit.HOUR).duration(1).build())
                .queueExpiry(TimeToLive.builder().timeUnit(TimeUnit.DAY).duration(1).build())
                .sweepDurationInMins(30)
                .batchingConfig(BatchingConfig.builder().maxBatchSize(100).maxWaitTimeInSecs(5).build())
                .shovelConfig(ShovelConfig.builder().concurrency(3).timeIntervalInSecs(300).build())
                .build();

        assertEquals(64, (int) request.getShards());
        assertEquals(10, request.getConcurrency());
        assertEquals(30, request.getSweepDurationInMins());
        assertEquals(100, request.getBatchingConfig().getMaxBatchSize());
        assertEquals(3, request.getShovelConfig().getConcurrency());
    }

    @Test
    void testIsValidSuccess() {
        CreateQueueRequest request = CreateQueueRequest.builder()
                .name("QUEUE_1")
                .concurrency(5)
                .messageHandlerType("handler")
                .messageExpiry(TimeToLive.builder().timeUnit(TimeUnit.HOUR).duration(1).build())
                .queueExpiry(TimeToLive.builder().timeUnit(TimeUnit.DAY).duration(1).build())
                .build();

        assertTrue(request.isValid());
    }

    @Test
    void testIsValidFailsWhenMessageExpiryGreaterThanQueueExpiry() {
        CreateQueueRequest request = CreateQueueRequest.builder()
                .name("QUEUE_1")
                .concurrency(5)
                .messageHandlerType("handler")
                .messageExpiry(TimeToLive.builder().timeUnit(TimeUnit.DAY).duration(2).build())
                .queueExpiry(TimeToLive.builder().timeUnit(TimeUnit.DAY).duration(1).build())
                .build();

        assertFalse(request.isValid());
    }

    @Test
    void testIsValidFailsWhenQueueExpiryExceedsMax() {
        CreateQueueRequest request = CreateQueueRequest.builder()
                .name("QUEUE_1")
                .concurrency(5)
                .messageHandlerType("handler")
                .queueExpiry(TimeToLive.builder().timeUnit(TimeUnit.DAY).duration(400).build())
                .build();

        assertFalse(request.isValid());
    }

    @Test
    void testIsValidFailsWhenMessageExpiryExceedsMax() {
        CreateQueueRequest request = CreateQueueRequest.builder()
                .name("QUEUE_1")
                .concurrency(5)
                .messageHandlerType("handler")
                .messageExpiry(TimeToLive.builder().timeUnit(TimeUnit.DAY).duration(400).build())
                .build();

        assertFalse(request.isValid());
    }

    @Test
    void testGetDefaultTimeToLive() {
        CreateQueueRequest request = CreateQueueRequest.builder()
                .name("QUEUE_1")
                .concurrency(5)
                .messageHandlerType("handler")
                .build();

        TimeToLive defaultTtl = request.getDefaultTimeToLive();
        assertEquals(TimeUnit.DAY, defaultTtl.getTimeUnit());
        assertEquals(Constants.DEFAULT_DURATION_DAY, defaultTtl.getDuration());
    }

    @Test
    void testHandlerTimeoutDefaultsWhenUnset() {
        CreateQueueRequest request = CreateQueueRequest.builder()
                .name("QUEUE_1")
                .concurrency(5)
                .messageHandlerType("handler")
                .build();

        assertEquals(Constants.DEFAULT_HANDLER_TIMEOUT_IN_MINS, request.getHandlerTimeoutInMins());
    }

    @Test
    void testHandlerTimeoutIsOverridable() {
        CreateQueueRequest request = CreateQueueRequest.builder()
                .name("QUEUE_1")
                .concurrency(5)
                .messageHandlerType("handler")
                .handlerTimeoutInMins(3)
                .build();

        assertEquals(3, request.getHandlerTimeoutInMins());
    }
}
