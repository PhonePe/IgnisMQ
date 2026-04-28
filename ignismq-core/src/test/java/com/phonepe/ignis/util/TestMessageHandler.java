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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableSet;
import com.phonepe.ignis.common.MessageHandler;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Set;

@Slf4j
@NoArgsConstructor
public class TestMessageHandler implements MessageHandler<String> {
    @Override
    public Set<Class<?>> getIgnorableExceptions() {
        return ImmutableSet.of(JsonProcessingException.class);
    }

    @Override
    public boolean handle(String message) {
        log.info("Consuming message {}", message);
        return Boolean.parseBoolean(message);
    }

    @Override
    public boolean handle(List<String> messages) {
        return messages.stream().allMatch(this::handle);
    }
}