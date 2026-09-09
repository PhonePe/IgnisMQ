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
import com.phonepe.ignis.service.QueueService;
import com.phonepe.ignis.utils.Constants;
import com.phonepe.magazine.Magazine;
import com.phonepe.magazine.entity.MagazineData;
import com.phonepe.magazine.exception.ErrorCode;
import com.phonepe.magazine.exception.MagazineException;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.Timer;
import java.util.TimerTask;
import java.util.stream.Stream;

/**
 * @author shantanu.tiwari
 */
@Slf4j
public final class ShovelTask extends TimerTask {
    private final Magazine<String> magazine;
    private final Magazine<String> sidelineMagazine;
    private final QueueService queueService;
    private final boolean autoDelete;

    public ShovelTask(final Magazine<String> magazine,
                      final Magazine<String> sidelineMagazine,
                      final QueueService queueService,
                      final boolean autoDelete) {
        this.magazine = magazine;
        this.sidelineMagazine = sidelineMagazine;
        this.queueService = queueService;
        this.autoDelete = autoDelete;
    }

    @Override
    public void run() {
        try {
            log.debug("Created shovel task for queue '{}'", magazine.getMagazineIdentifier());
            StreamUtils.takeWhile(
                    Stream.generate(this::fireFromMagazine),
                    Objects::nonNull
            ).forEach(magazineData -> {
                final String message = magazineData.getData();
                if (transferred(message)) {
                    sidelineMagazine.delete(magazineData);
                } else {
                    // Neither magazine holds a fresh copy, so the record fired out of the sideline is
                    // the only one left. Leave it: it stays below the sideline fire pointer with its
                    // fire timestamp intact, and the sideline sweep will retry it.
                    log.error("Could not move message into the main magazine nor return it to the " +
                                    "sideline for queue '{}'. Leaving the source record for the sweeper",
                            magazine.getMagazineIdentifier());
                }
            });
            log.debug("Shovel task completed for queue '{}'", magazine.getMagazineIdentifier());
        } catch (Exception e) {
            log.error("Fatal!!! Error running shovel task...", e);
            scheduleNewShovelTask();
        }
    }

    private MagazineData<String> fireFromMagazine() {
        try {
            final MagazineData<String> magazineData = sidelineMagazine.fire();
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
        scheduleNewShovelTask();
        return null;
    }

    private void logExceptionInFiring(Exception e) {
        log.error("Magazine exception in shovel task of queue {}. Scheduling new task with some delay if autoDelete is set",
                magazine.getMagazineIdentifier(), e);
    }

    private void scheduleNewShovelTask() {
        if (autoDelete) {
            new Timer().schedule(
                    new ShovelTask(magazine, sidelineMagazine, queueService, true),
                    Constants.SHOVEL_DELAY_IN_MS
            );
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
