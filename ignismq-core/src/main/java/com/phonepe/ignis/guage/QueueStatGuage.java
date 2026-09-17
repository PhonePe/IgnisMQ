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
import com.phonepe.ignis.common.QueueMetaData;
import com.phonepe.ignis.metric.QueueStat;
import com.phonepe.ignis.service.QueueService;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * @author shantanu.tiwari
 */
@Slf4j
public class QueueStatGuage implements Supplier<List<QueueStat>> {
    private final QueueService queueService;
    private final Supplier<Map<String, IQueue<?>>> localQueues;

    /**
     * @param queueService source of the queues registered in storage
     * @param localQueues  supplier of the queues this process actually holds; only these can be
     *                     reported on, because reporting requires a live magazine handle
     */
    public QueueStatGuage(final QueueService queueService,
                          final Supplier<Map<String, IQueue<?>>> localQueues) {
        this.queueService = queueService;
        this.localQueues = localQueues;
    }

    @Override
    public List<QueueStat> get() {
        try {
            Map<String, IQueue<?>> cachedQueues = localQueues.get();
            return queueService.getQueues(true).keySet()
                    .stream()
                    .map(name -> {
                        final IQueue<?> queue = cachedQueues.get(name);
                        return queue == null ? null : toStat(name, queue);
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            log.info("Error calculating metrics", e);
            return Collections.emptyList();
        }
    }

    private static QueueStat toStat(final String name, final IQueue<?> queue) {
        final QueueMetaData metaData = queue.getMetaData();
        return QueueStat.builder()
                .name(name)
                .active(true)
                .published(metaData.getPublished())
                .consumed(metaData.getConsumed())
                .unConsumed(Math.max(metaData.getPublished() - metaData.getConsumed(), 0L))
                .sidelined(metaData.getSidelined())
                .shovelled(metaData.getShovelled())
                .build();
    }
}
