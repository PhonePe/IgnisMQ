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

package com.phonepe.ignis.shovel;

import com.codepoetics.protonpack.StreamUtils;
import com.phonepe.ignis.metric.IgnisMetrics;
import com.phonepe.ignis.metric.QueueMeters;
import com.phonepe.ignis.scheduler.IgnisSchedulerCommands;
import com.phonepe.ignis.utils.Constants;
import com.phonepe.magazine.Magazine;
import com.phonepe.magazine.entity.MagazineData;
import com.phonepe.magazine.exception.ErrorCode;
import com.phonepe.magazine.exception.MagazineException;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.stream.Stream;

/**
 * @author shantanu.tiwari
 */
@Slf4j
public final class ShovelTask implements Runnable {
    private final Magazine<String> magazine;
    private final Magazine<String> sidelineMagazine;
    private final boolean autoDelete;
    /** Null when this shovel is not scheduled, so a failure has nothing to retry through. */
    private final IgnisSchedulerCommands scheduler;
    private final QueueMeters meters;

    public ShovelTask(final Magazine<String> magazine,
                      final Magazine<String> sidelineMagazine,
                      final boolean autoDelete,
                      final IgnisSchedulerCommands scheduler,
                      final QueueMeters meters) {
        this.magazine = magazine;
        this.sidelineMagazine = sidelineMagazine;
        this.autoDelete = autoDelete;
        this.scheduler = scheduler;
        this.meters = Objects.requireNonNull(meters, "meters");
    }

    @Override
    public void run() {
        final long startNanos = System.nanoTime();
        String outcome = IgnisMetrics.SUCCESS;
        try {
            log.debug("Created shovel task for queue '{}'", magazine.getMagazineIdentifier());
            StreamUtils.takeWhile(
                    Stream.generate(this::fireFromMagazine),
                    Objects::nonNull
            ).forEach(magazineData -> {
                final String message = magazineData.getData();
                if (transferred(message)) {
                    meters.shovelMoved();
                    sidelineMagazine.delete(magazineData);
                } else {
                    // Neither magazine holds a fresh copy, so the record fired out of the sideline is
                    // the only one left. Leave it: it stays below the sideline fire pointer, which is
                    // exactly where the sideline sweep looks, and the transfer is retried from there.
                    log.error("Could not move message into the main magazine nor return it to the " +
                                    "sideline for queue '{}'. Leaving the source record for the sweeper",
                            magazine.getMagazineIdentifier());
                }
            });
            log.debug("Shovel task completed for queue '{}'", magazine.getMagazineIdentifier());
        } catch (Exception e) {
            outcome = IgnisMetrics.FAILURE;
            log.error("Fatal!!! Error running shovel task...", e);
            scheduleNewShovelTask();
        } finally {
            meters.recordShovel(System.nanoTime() - startNanos, outcome);
        }
    }

    private MagazineData<String> fireFromMagazine() {
        try {
            return sidelineMagazine.fire();
        } catch (MagazineException e) {
            if (e.getErrorCode().equals(ErrorCode.NOTHING_TO_FIRE)) {
                return null;
            }
            logExceptionInFiring(e);
        } catch (Exception e) {
            logExceptionInFiring(e);
        }
        scheduleNewShovelTask();
        return null;
    }

    private void logExceptionInFiring(Exception e) {
        log.error("Magazine exception in shovel task of queue {}. Scheduling new task with some delay if autoDelete is set",
                magazine.getMagazineIdentifier(), e);
    }

    /**
     * A one-shot shovel that failed gets one more attempt, on the shared scheduler rather than on a
     * brand new {@code Timer} thread that nothing owned and nothing could shut down.
     */
    private void scheduleNewShovelTask() {
        if (autoDelete && Objects.nonNull(scheduler) && !scheduler.isStopped()) {
            scheduler.scheduleOnce(
                    new ShovelTask(magazine, sidelineMagazine, true, scheduler, meters),
                    Constants.SHOVEL_DELAY_IN_MS);
        }
    }

    /**
     * Attempts to move the payload into the main magazine, falling back to putting it back on the
     * sideline.
     *
     * @return true when a copy of the message is safely stored somewhere and the source record fired
     *         out of the sideline can therefore be deleted. A null payload is nothing to preserve.
     */
    private boolean transferred(final String message) {
        if (Objects.isNull(message)) {
            return true;
        }
        try {
            if (magazine.load(message)) {
                return true;
            }
            log.warn("Non success response, reloading the message '{}' to sideline magazine...", message);
        } catch (Exception e) {
            log.error("Exception loading the message into the main magazine, reloading to sideline magazine", e);
        }
        return reloadMessage(message);
    }

    private boolean reloadMessage(final String message) {
        try {
            return sidelineMagazine.reload(message);
        } catch (Exception e) {
            log.error("Exception reloading the message '{}' to the sideline magazine", message, e);
            return false;
        }
    }
}
