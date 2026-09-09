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

package com.phonepe.ignis.consumer;

import com.codepoetics.protonpack.StreamUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.phonepe.ignis.common.MessageHandler;
import com.phonepe.ignis.config.BatchingConfig;
import com.phonepe.ignis.utils.Utils;
import com.phonepe.ignis.service.QueueService;
import com.phonepe.magazine.Magazine;
import com.phonepe.magazine.entity.MagazineData;
import com.phonepe.magazine.entity.MetaData;
import com.phonepe.magazine.exception.ErrorCode;
import com.phonepe.magazine.exception.MagazineException;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.List;
import java.util.Objects;
import io.micrometer.core.instrument.Timer;
import java.util.TimerTask;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author shantanu.tiwari
 */
@Slf4j
public final class MagazineConsumerTask<M> extends TimerTask {
    private static final int DEFAULT_WAIT_TIME = 5000;
    private final Magazine<String> magazine;
    private final Magazine<String> sidelineMagazine;
    private final MessageHandler<M> messageHandler;
    private final ObjectMapper mapper;
    private final Class<M> clazz;
    private final Timer consumeMetricTimer;
    private final QueueService queueService;
    private final BatchingConfig batchingConfig;

    public MagazineConsumerTask(final Magazine<String> magazine,
                                final Magazine<String> sidelineMagazine,
                                final MessageHandler<M> messageHandler,
                                final ObjectMapper mapper,
                                final Class<M> clazz,
                                final Timer consumeMetricTimer,
                                final QueueService queueService,
                                final BatchingConfig batchingConfig) {
        this.magazine = magazine;
        this.sidelineMagazine = sidelineMagazine;
        this.messageHandler = messageHandler;
        this.mapper = mapper;
        this.clazz = clazz;
        this.consumeMetricTimer = consumeMetricTimer;
        this.queueService = queueService;
        this.batchingConfig = batchingConfig;
    }

    @Override
    public void run() {
        try {
            log.debug("Started consumer for queue {}", magazine.getMagazineIdentifier());
            if (Objects.nonNull(batchingConfig)) {
                batchConsume(System.currentTimeMillis());
            } else {
                consumeSingleMessage();
            }
            log.debug("Completed consumption for queue {}", magazine.getMagazineIdentifier());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Fatal!!! Consumer task interrupted for queue {}", magazine.getMagazineIdentifier(), e);
        } catch (Exception e) {
            log.error("Fatal!!! Error running consumer task...", e);
        }
    }

    private void consumeSingleMessage() {
        StreamUtils.takeWhile(
                Stream.generate(this::fireFromMagazine),
                Objects::nonNull
        ).forEach(magazineData -> consume(List.of(magazineData)));
    }

    private void batchConsume(final long startTime) throws InterruptedException {
        val allShardsMetaData = magazine.getMetaData().values();
        val loadPointer = Utils.getMagazineCount(allShardsMetaData, MetaData::getLoadPointer);
        val firePointer = Utils.getMagazineCount(allShardsMetaData, MetaData::getFirePointer);

        // If batch size messages aren't present in queue or max wait time is not elapsed then wait for some time and then recheck
        if (loadPointer - firePointer <= batchingConfig.getMaxBatchSize()
                && isMaxWaitTimeNotElapsed(startTime)) {
            log.debug("Ignis queue is waiting for batching to complete or max time to elapse. Waiting for {}ms", DEFAULT_WAIT_TIME);
            Thread.sleep(DEFAULT_WAIT_TIME);
            batchConsume(startTime);
        } else {
            StreamUtils.windowed(
                    StreamUtils.takeWhile(
                            Stream.generate(this::fireFromMagazine),
                            Objects::nonNull
                    ),
                    batchingConfig.getMaxBatchSize(),
                    batchingConfig.getMaxBatchSize(),
                    true
            ).forEach(this::consume);
        }
    }

    private MagazineData<String> fireFromMagazine() {
        try {
            final MagazineData<String> magazineData = magazine.fire();
            if (Objects.isNull(magazineData)) {
                return null;
            }
            queueService.addFireTimestamp(magazineData, System.currentTimeMillis());
            return magazineData;
        } catch (MagazineException e) {
            if (e.getErrorCode().equals(ErrorCode.NOTHING_TO_FIRE)) {
                return null;
            }
            logExceptionInFiring(e);
        } catch (Exception e) {
            logExceptionInFiring(e);
        }
        return null;
    }

    private void consume(final List<MagazineData<String>> magazineDataList) {
        consumeMetricTimer.record(() -> {
            try {
                log.debug("Consuming messages {}", magazineDataList);
                final Boolean success = handle(magazineDataList.stream()
                        .map(magazineData -> {
                            if (Objects.nonNull(magazineData.getData())) {
                                if (clazz.isPrimitive() || clazz == String.class) {
                                    return (M) magazineData.getData();
                                }
                                try {
                                    return mapper.readValue(magazineData.getData(), clazz);
                                } catch (JsonProcessingException e) {
                                    handleException(magazineData, e);
                                }
                            }
                            return null;
                        })
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList()));
                if (Boolean.TRUE.equals(success)) {
                    magazineDataList.forEach(magazine::delete);
                } else {
                    magazineDataList.forEach(this::sidelineThenDelete);
                }
            } catch (Exception e) {
                magazineDataList.forEach(magazineData -> handleException(magazineData, e));
            }
        });
    }

    private void handleException(final MagazineData<String> magazineData,
                                 final Exception e) {
        log.error("Exception in handling the message {}", magazineData.getData(), e);
        if (isExceptionIgnorable(e)) {
            magazine.delete(magazineData);
            return;
        }
        sidelineThenDelete(magazineData);
    }

    /**
     * Removes the message from the main magazine only once the sideline magazine has accepted it.
     * <p>
     * If the transfer fails the record is deliberately left in place. It sits below the fire pointer,
     * so no consumer will see it again, but it still carries a fire timestamp and the sweeper will
     * retry the same transfer on its next pass. Deleting it here instead would remove the only
     * remaining copy.
     */
    private void sidelineThenDelete(final MagazineData<String> magazineData) {
        if (transferToSideline(magazineData.getData())) {
            magazine.delete(magazineData);
        } else {
            log.error("Could not sideline message from queue {}; leaving it in the main magazine for " +
                    "the sweeper rather than deleting it", magazine.getMagazineIdentifier());
        }
    }

    /**
     * @return true when the payload is safely in the sideline magazine, or when there is no payload
     *         to preserve in the first place.
     */
    private boolean transferToSideline(final String message) {
        if (Objects.isNull(message) || message.isEmpty()) {
            return true;
        }
        try {
            log.warn("Sidelining the messages '{}' to sideline magazine...", message);
            return sidelineMagazine.load(message);
        } catch (Exception e) {
            log.error("Exception sidelining message '{}'", message, e);
            return false;
        }
    }

    private void logExceptionInFiring(Exception e) {
        log.error("Magazine exception in consumer task of queue {}. Gracefully ignoring..." +
                " New task will be created once this is completed", magazine.getMagazineIdentifier(), e);
    }

    private boolean isMaxWaitTimeNotElapsed(long startTime) {
        return (System.currentTimeMillis() - startTime) <= batchingConfig.getMaxWaitTimeInSecs() * 1000L;
    }

    private Boolean handle(final List<M> messages) throws Exception {
        return messageHandler.handle(messages);
    }

    private boolean isExceptionIgnorable(final Throwable t) {
        if (Objects.nonNull(messageHandler.getIgnorableExceptions())) {
            return messageHandler.getIgnorableExceptions()
                    .stream()
                .anyMatch(exceptionType -> exceptionType.isAssignableFrom(t.getClass()));
        }
        return false;
    }
}
