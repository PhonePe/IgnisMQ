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

package com.phonepe.ignis.util;

import com.phonepe.ignis.common.TimeToLive;
import com.phonepe.ignis.common.TimeUnit;
import com.phonepe.ignis.config.BatchingConfig;
import com.phonepe.ignis.request.CreateQueueRequest;
import com.phonepe.ignis.request.ShovelConfig;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class RequestFactory {

    public static CreateQueueRequest createQueueRequest(final String queueName, final String messageHandlerType) {
        return CreateQueueRequest.builder()
                .concurrency(4)
                .messageExpiry(TimeToLive.builder()
                        .duration(5)
                        .timeUnit(TimeUnit.MINUTE)
                        .build())
                .queueExpiry(TimeToLive.builder()
                        .duration(5)
                        .timeUnit(TimeUnit.MINUTE)
                        .build())
                .name(queueName)
                .messageHandlerType(messageHandlerType)
                .shovelConfig(ShovelConfig.builder()
                        .timeIntervalInSecs(1)
                        .build())
                .batchingConfig(BatchingConfig.builder().build())
                .build();
    }
}
