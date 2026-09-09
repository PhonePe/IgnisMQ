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

package com.phonepe.ignis.guage;

import com.phonepe.ignis.IQueue;
import com.phonepe.ignis.IgnisMQManager;
import com.phonepe.ignis.metric.QueueStat;
import com.phonepe.ignis.service.QueueService;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * @author shantanu.tiwari
 */
@Slf4j
public class QueueStatGuage implements Supplier<List<QueueStat>> {
    private final QueueService queueService;
    private final IgnisMQManager ignisMQManager;

    public QueueStatGuage(final QueueService queueService,
                          final IgnisMQManager ignisMQManager) {
        this.queueService = queueService;
        this.ignisMQManager = ignisMQManager;
    }

    @Override
    public List<QueueStat> get() {
        try {
            Map<String, IQueue<?>> cachedQueues = ignisMQManager.getAllQueues();
            return queueService.getQueues(true).entrySet()
                    .stream()
                    .filter(entry -> cachedQueues.containsKey(entry.getKey()))
                    .map(entry ->
                            QueueStat.builder()
                                    .name(entry.getKey())
                                    .consumed(cachedQueues.get(entry.getKey()).getMetaData().getConsumed())
                                    .published(cachedQueues.get(entry.getKey()).getMetaData().getPublished())
                                    .unConsumed(cachedQueues.get(entry.getKey()).getUnconsumedCount())
                                    .sidelined(cachedQueues.get(entry.getKey()).getMetaData().getSidelined())
                                    .shovelled(cachedQueues.get(entry.getKey()).getMetaData().getShovelled())
                                    .build()
                    ).collect(Collectors.toList());
        } catch (Exception e) {
            log.info("Error calculating metrics", e);
            return Collections.emptyList();
        }
    }
}
