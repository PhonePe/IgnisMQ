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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.phonepe.ignis.common.MessageHandler;
import com.phonepe.ignis.config.BatchingConfig;
import com.phonepe.ignis.utils.Constants;
import com.phonepe.magazine.Magazine;
import com.phonepe.magazine.entity.MagazineData;
import com.phonepe.magazine.exception.ErrorCode;
import com.phonepe.magazine.exception.MagazineException;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import io.micrometer.core.instrument.Timer;
import java.util.stream.Collectors;

/**
 * @author shantanu.tiwari
 */
@Slf4j
public final class MagazineConsumerTask<M> implements Runnable {

    private static final long POLL_INTERVAL_IN_MS = 200L;
    private final Magazine<String> magazine;
    private final Magazine<String> sidelineMagazine;
    private final MessageHandler<M> messageHandler;
    private final ObjectMapper mapper;
    private final Class<M> clazz;
    private final Timer consumeMetricTimer;
    private final BatchingConfig batchingConfig;
    private final long runBudgetMillis;

    public MagazineConsumerTask(final Magazine<String> magazine,
                                final Magazine<String> sidelineMagazine,
                                final MessageHandler<M> messageHandler,
                                final ObjectMapper mapper,
                                final Class<M> clazz,
                                final Timer consumeMetricTimer,
                                final BatchingConfig batchingConfig) {
        this(magazine, sidelineMagazine, messageHandler, mapper, clazz, consumeMetricTimer,
                batchingConfig, Constants.CONSUMER_RUN_BUDGET_IN_MS);
    }

    public MagazineConsumerTask(final Magazine<String> magazine,
                                final Magazine<String> sidelineMagazine,
                                final MessageHandler<M> messageHandler,
                                final ObjectMapper mapper,
                                final Class<M> clazz,
                                final Timer consumeMetricTimer,
                                final BatchingConfig batchingConfig,
                                final long runBudgetMillis) {
        this.runBudgetMillis = runBudgetMillis;
        this.magazine = magazine;
        this.sidelineMagazine = sidelineMagazine;
        this.messageHandler = messageHandler;
        this.mapper = mapper;
        this.clazz = clazz;
        this.consumeMetricTimer = consumeMetricTimer;
        this.batchingConfig = batchingConfig;
    }

    @Override
    public void run() {
        try {
            log.debug("Started consumer for queue {}", magazine.getMagazineIdentifier());
            final long startTime = System.currentTimeMillis();
            if (Objects.nonNull(batchingConfig)) {
                batchConsume(startTime);
            } else {
                consumeSingleMessage(startTime + runBudgetMillis);
            }
            log.debug("Completed consumption for queue {}", magazine.getMagazineIdentifier());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Fatal!!! Consumer task interrupted for queue {}", magazine.getMagazineIdentifier(), e);
        } catch (Exception e) {
            log.error("Fatal!!! Error running consumer task...", e);
        }
    }

    /**
     * Consumes one message at a time until the magazine is empty or the run budget expires.
     *
     * @param deadline when this invocation must hand its thread back.
     */
    private void consumeSingleMessage(final long deadline) {
        while (!Thread.currentThread().isInterrupted()) {
            final MagazineData<String> magazineData = fireFromMagazine();
            if (Objects.isNull(magazineData)) {
                return;
            }
            // Consume first, check the budget second. A message is claimed the moment fire()
            // returns it, so abandoning it here would leave it below the fire pointer with no
            // consumer coming - recoverable only by the sweeper, one sweepDuration later. The
            // budget bounds the turn; it must never cost a delivery.
            consume(List.of(magazineData));
            if (budgetExhausted(deadline)) {
                return;
            }
        }
    }

    /**
     * Fills batches from the magazine, flushing on whichever comes first: a full batch, or the
     * configured wait elapsing.
     */
    private void batchConsume(final long startTime) throws InterruptedException {
        final long lingerMillis = batchingConfig.getMaxWaitTimeInSecs() * 1000L;
        final long lingerDeadline = startTime + lingerMillis;
        final long runDeadline = startTime + Math.max(lingerMillis, runBudgetMillis);
        final List<MagazineData<String>> batch = new ArrayList<>(batchingConfig.getMaxBatchSize());

        while (!Thread.currentThread().isInterrupted()) {
            final MagazineData<String> magazineData = fireFromMagazine();
            if (Objects.nonNull(magazineData)) {
                batch.add(magazineData);
                // A full batch goes now. The old code compared depth with <=, so it slept for five
                // more seconds at exactly the moment a complete batch had become available (B5).
                if (batch.size() >= batchingConfig.getMaxBatchSize()) {
                    consume(List.copyOf(batch));
                    batch.clear();
                    // Checked here and not on every fire: a batch already in hand is finished
                    // rather than abandoned, so the budget bounds the turn without splitting a
                    // batch the caller asked for.
                    if (budgetExhausted(runDeadline)) {
                        return;
                    }
                }
                continue;
            }

            // Nothing pending means nothing to wait for. Lingering here would hold the thread for
            // the whole configured wait on an idle queue, which is the opposite of what the setting
            // is for: it bounds how long a *partial* batch may be held open, not how long an empty
            // consumer should block.
            if (batch.isEmpty()) {
                break;
            }
            final long remaining = lingerDeadline - System.currentTimeMillis();
            if (remaining <= 0) {
                break;
            }
            // Never sleep past the deadline. A fixed five-second sleep could overshoot the caller's
            // stated wait by almost its whole length.
            Thread.sleep(Math.min(POLL_INTERVAL_IN_MS, remaining));
        }

        if (!batch.isEmpty()) {
            consume(List.copyOf(batch));
        }
    }

    private boolean budgetExhausted(final long deadline) {
        if (System.currentTimeMillis() < deadline) {
            return false;
        }
        log.debug("Consumer for queue {} reached its run budget; yielding its thread and resuming " +
                "on the next scheduled run", magazine.getMagazineIdentifier());
        return true;
    }

    private MagazineData<String> fireFromMagazine() {
        try {
            final MagazineData<String> magazineData = magazine.fire();
            if (Objects.isNull(magazineData)) {
                return null;
            }
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
     * so no consumer will see it again, but that is precisely the range the sweeper scans once the
     * delivery-time watermark has moved past it, and it will retry the same transfer. Deleting it
     * here instead would remove the only remaining copy.
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
