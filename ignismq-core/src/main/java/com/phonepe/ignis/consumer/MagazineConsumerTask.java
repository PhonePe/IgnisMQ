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
import com.fasterxml.jackson.databind.ObjectReader;
import com.phonepe.ignis.common.MessageHandler;
import com.phonepe.ignis.config.BatchingConfig;
import com.phonepe.ignis.metric.IgnisMetrics;
import com.phonepe.ignis.metric.QueueMeters;
import com.phonepe.ignis.scheduler.HandlerExecutor;
import com.phonepe.ignis.scheduler.HandlerSaturatedException;
import com.phonepe.magazine.Magazine;
import com.phonepe.magazine.entity.MagazineData;
import com.phonepe.magazine.exception.ErrorCode;
import com.phonepe.magazine.exception.MagazineException;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

/**
 * @author shantanu.tiwari
 */
@Slf4j
public final class MagazineConsumerTask<M> implements Runnable {

    private static final long POLL_INTERVAL_IN_MS = 200L;
    private final Magazine<String> magazine;
    private final Magazine<String> sidelineMagazine;
    private final MessageHandler<M> messageHandler;
    private final ObjectReader reader;
    private final boolean rawPayload;
    private final BatchingConfig batchingConfig;
    private final long runBudgetMillis;
    private final HandlerExecutor handlerExecutor;
    private final long handlerTimeoutMillis;
    private final QueueMeters meters;

    public MagazineConsumerTask(final Magazine<String> magazine,
                                final Magazine<String> sidelineMagazine,
                                final MessageHandler<M> messageHandler,
                                final ObjectMapper mapper,
                                final Class<M> clazz,
                                final BatchingConfig batchingConfig,
                                final long runBudgetMillis,
                                final HandlerExecutor handlerExecutor,
                                final long handlerTimeoutMillis,
                                final QueueMeters meters) {
        this.handlerExecutor = Objects.requireNonNull(handlerExecutor, "handlerExecutor");
        this.meters = Objects.requireNonNull(meters, "meters");
        this.handlerTimeoutMillis = handlerTimeoutMillis;
        this.runBudgetMillis = runBudgetMillis;
        this.magazine = magazine;
        this.sidelineMagazine = sidelineMagazine;
        this.messageHandler = messageHandler;
        this.rawPayload = clazz.isPrimitive() || clazz == String.class;
        // Resolved once: readValue(String, Class) looks the deserialiser up on every call, and this
        // one runs per message.
        this.reader = rawPayload ? null : mapper.readerFor(clazz);
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
        List<MagazineData<String>> batch = new ArrayList<>(batchingConfig.getMaxBatchSize());

        while (!Thread.currentThread().isInterrupted()) {
            final MagazineData<String> magazineData = fireFromMagazine();
            if (Objects.nonNull(magazineData)) {
                batch.add(magazineData);
                // A full batch goes now. The old code compared depth with <=, so it slept for five
                // more seconds at exactly the moment a complete batch had become available.
                if (batch.size() >= batchingConfig.getMaxBatchSize()) {
                    consume(batch);
                    batch = new ArrayList<>(batchingConfig.getMaxBatchSize());
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
            consume(batch);
        }
    }

    private boolean budgetExhausted(final long deadline) {
        if (System.currentTimeMillis() < deadline) {
            return false;
        }
        meters.budgetExhausted();
        log.debug("Consumer for queue {} reached its run budget; yielding its thread and resuming " +
                "on the next scheduled run", magazine.getMagazineIdentifier());
        return true;
    }

    private MagazineData<String> fireFromMagazine() {
        try {
            final MagazineData<String> magazineData = magazine.fire();
            if (Objects.isNull(magazineData)) {
                meters.polled(false);
                return null;
            }
            meters.polled(true);
            return magazineData;
        } catch (MagazineException e) {
            if (e.getErrorCode().equals(ErrorCode.NOTHING_TO_FIRE)) {
                meters.polled(false);
                return null;
            }
            logExceptionInFiring(e);
        } catch (Exception e) {
            logExceptionInFiring(e);
        }
        return null;
    }

    private void consume(final List<MagazineData<String>> magazineDataList) {
        meters.recordConsume(() -> {
            try {
                log.debug("Consuming messages {}", magazineDataList);
                final Boolean success = handle(deserialise(magazineDataList));
                if (Boolean.TRUE.equals(success)) {
                    magazineDataList.forEach(magazine::delete);
                    meters.acked(magazineDataList.size());
                } else {
                    magazineDataList.forEach(data -> sidelineThenDelete(data, IgnisMetrics.REASON_REJECTED));
                }
            } catch (Exception e) {
                magazineDataList.forEach(magazineData -> handleException(magazineData, e));
            }
        });
    }

    /**
     * A message whose payload cannot be read is dealt with here and left out of the batch; it stays
     * in the caller's list, so a batch the handler then accepts still deletes it.
     */
    private List<M> deserialise(final List<MagazineData<String>> magazineDataList) {
        final List<M> messages = new ArrayList<>(magazineDataList.size());
        for (final MagazineData<String> magazineData : magazineDataList) {
            final String payload = magazineData.getData();
            if (Objects.isNull(payload)) {
                continue;
            }
            if (rawPayload) {
                messages.add((M) payload);
                continue;
            }
            try {
                messages.add(reader.readValue(payload));
            } catch (JsonProcessingException e) {
                handleException(magazineData, e);
            }
        }
        return messages;
    }

    private void handleException(final MagazineData<String> magazineData,
                                 final Exception e) {
        log.error("Exception in handling the message {}", magazineData.getData(), e);
        if (isExceptionIgnorable(e)) {
            meters.ignored();
            magazine.delete(magazineData);
            return;
        }
        sidelineThenDelete(magazineData, sidelineReason(e));
    }

    /**
     * Removes the message from the main magazine only once the sideline magazine has accepted it.
     * <p>
     * If the transfer fails the record is deliberately left in place. It sits below the fire pointer,
     * so no consumer will see it again, but that is precisely the range the sweeper scans once the
     * delivery-time watermark has moved past it, and it will retry the same transfer. Deleting it
     * here instead would remove the only remaining copy.
     */
    private void sidelineThenDelete(final MagazineData<String> magazineData, final String reason) {
        if (transferToSideline(magazineData.getData())) {
            meters.sidelined(reason);
            magazine.delete(magazineData);
        } else {
            meters.sidelineRefused(IgnisMetrics.REASON_SIDELINE_REFUSED);
            log.error("Could not sideline message from queue {}; leaving it in the main magazine for " +
                    "the sweeper rather than deleting it", magazine.getMagazineIdentifier());
        }
    }

    /**
     * @return true when the payload is safely in the sideline magazine, or when there is no payload
     * to preserve in the first place.
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
        // Only for a batching consumer. A single-message consumer hands over exactly one every
        // time, so the summary would be a constant 1 - and an operator reading it would fairly
        // conclude that batching was configured and permanently underfilling.
        if (Objects.nonNull(batchingConfig)) {
            meters.handlerBatch(messages.size());
        }
        final long startNanos = System.nanoTime();
        String outcome = IgnisMetrics.SUCCESS;
        try {
            return callHandler(messages);
        } catch (HandlerTimeoutException e) {
            outcome = IgnisMetrics.REASON_TIMEOUT;
            throw e;
        } catch (HandlerSaturatedException e) {
            outcome = IgnisMetrics.REASON_SATURATED;
            throw e;
        } catch (Exception e) {
            outcome = IgnisMetrics.FAILURE;
            throw e;
        } finally {
            meters.recordHandler(System.nanoTime() - startNanos, outcome);
        }
    }

    private Boolean callHandler(final List<M> messages) throws Exception {
        try {
            return handlerExecutor.call(() -> messageHandler.handle(messages), handlerTimeoutMillis);
        } catch (TimeoutException e) {
            meters.handlerTimedOut();
            throw new HandlerTimeoutException(magazine.getMagazineIdentifier(), messages.size(),
                    handlerTimeoutMillis, e);
        }
    }

    private static String sidelineReason(final Exception e) {
        if (e instanceof HandlerTimeoutException) {
            return IgnisMetrics.REASON_TIMEOUT;
        }
        return e instanceof HandlerSaturatedException ? IgnisMetrics.REASON_SATURATED : IgnisMetrics.REASON_EXCEPTION;
    }

    private boolean isExceptionIgnorable(final Throwable t) {
        // Never ignorable: "ignorable" means delete without sidelining. A timeout says nothing about
        // whether the work was done, and saturation means the handler never ran at all.
        if (t instanceof HandlerTimeoutException || t instanceof HandlerSaturatedException) {
            return false;
        }
        if (Objects.nonNull(messageHandler.getIgnorableExceptions())) {
            return messageHandler.getIgnorableExceptions()
                    .stream()
                    .anyMatch(exceptionType -> exceptionType.isAssignableFrom(t.getClass()));
        }
        return false;
    }
}
