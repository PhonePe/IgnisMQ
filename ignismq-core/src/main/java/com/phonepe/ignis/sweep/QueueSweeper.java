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

package com.phonepe.ignis.sweep;

import com.phonepe.ignis.storage.MagazineStorageVisitor;
import com.phonepe.ignis.client.StorageClient;
import com.phonepe.ignis.common.MagazineRegistry;
import com.phonepe.ignis.entity.QueueEntity;
import com.phonepe.ignis.service.QueueService;
import com.phonepe.ignis.storage.BaseStorage;
import com.phonepe.ignis.utils.Constants;
import com.phonepe.ignis.utils.Utils;
import com.phonepe.magazine.Magazine;
import com.phonepe.magazine.core.BaseMagazineStorage;
import com.phonepe.magazine.entity.FireCheckpoint;
import com.phonepe.magazine.entity.MagazineData;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Rescues messages that were handed to a consumer and never acknowledged.
 * <p>
 * A shard is a numbered conveyor belt. Slots above the fire pointer are filled but undelivered and
 * must never be touched; slots below it are either empty, meaning delivered and acknowledged, or
 * still full, meaning delivered and abandoned. Only the last kind is the sweeper's business, and
 * only once enough time has passed that the consumer is certainly gone.
 * <p>
 * Knowing <em>when</em> a slot was handed out is the whole problem. It is answered positionally
 * rather than per message: {@link Magazine#firePointerBefore(Instant)} reports where each shard's
 * fire pointer stood at a given moment, so everything at or below that pointer was claimed at least
 * that long ago. A shard is absent from the answer when no checkpoint reaches back that far, which
 * is normal for a young queue and correctly means "sweep nothing here this pass".
 * <p>
 * The watermark bounds the range; it does not say which slots inside it still exist. That is only
 * knowable by looking, so the range is still scanned - but the scan is one batch read per thousand
 * slots against thousands of deliveries per second, and every slot is scanned once ever because the
 * progress marker only moves forward.
 * <p>
 * <strong>Re-homing is at-least-once.</strong> A message is loaded into the sideline and only then
 * deleted from the source, so a crash between the two leaves it in both and the next pass re-homes
 * it again. That is the deliberate direction: the alternative ordering loses messages, which is what
 * B2 was.
 * <p>
 * Stateless and safe to reuse: one instance per process, holding the collaborators every sweep
 * needs.
 */
@Slf4j
public final class QueueSweeper {

    private final QueueService queueService;
    private final String clientId;
    private final BaseStorage storage;
    private final StorageClient client;
    private final String farmId;
    private final MeterRegistry meterRegistry;
    /** Optional: absent means always build, which is what a standalone sweeper does. */
    private final MagazineRegistry magazineRegistry;

    public QueueSweeper(final QueueService queueService,
                        final String clientId,
                        final BaseStorage storage,
                        final StorageClient client,
                        final String farmId,
                        final MeterRegistry meterRegistry) {
        this(queueService, clientId, storage, client, farmId, meterRegistry, null);
    }

    public QueueSweeper(final QueueService queueService,
                        final String clientId,
                        final BaseStorage storage,
                        final StorageClient client,
                        final String farmId,
                        final MeterRegistry meterRegistry,
                        final MagazineRegistry magazineRegistry) {
        this.magazineRegistry = magazineRegistry;
        this.queueService = queueService;
        this.clientId = clientId;
        this.storage = storage;
        this.client = client;
        this.farmId = farmId;
        this.meterRegistry = Objects.requireNonNull(meterRegistry, "Meter registry is required.");
    }

    /**
     * Sweeps a queue, building its two magazines from the stored configuration.
     * <p>
     * Never throws: a queue that cannot be swept must not stop the rest of the sweep cycle, and the
     * next pass will retry it from the same progress marker.
     */
    public void sweepQueue(final String queueName, final QueueEntity queueEntity) {
        try {
            log.info("Sweeping queue {}", queueName);
            final MagazineRegistry.QueueMagazines existing = Objects.isNull(magazineRegistry)
                    ? null : magazineRegistry.find(queueName);
            if (Objects.nonNull(existing)) {
                // Reusing the live pair also reuses its checkpoint claims, so this pass does not
                // repeat a write per shard that the consumers already made this window.
                sweep(queueName, queueEntity, existing.main(), existing.sideline());
                return;
            }
            // Not held locally - a queue created on another instance, before the watcher has caught
            // up. One storage for both magazines: its caches are keyed by MagazineContext, so it is
            // built to serve more than one.
            log.debug("Queue {} is not cached in this process; building magazines for the sweep", queueName);
            final BaseMagazineStorage<String> magazineStorage = buildStorage(queueEntity);
            sweep(queueName, queueEntity,
                    magazine(magazineStorage, queueName),
                    magazine(magazineStorage, Utils.getSidelineQueueName(queueName)));
        } catch (Exception e) {
            log.error("Sweeping failed for queue {}", queueName, e);
        }
    }

    public void sweep(final String queueName,
                      final QueueEntity queueEntity,
                      final Magazine<String> magazine,
                      final Magazine<String> sidelineMagazine) {
        final long now = System.currentTimeMillis();
        final long cutoff = now - queueEntity.getSweepDuration();
        sweepQuietly(queueName, magazine, sidelineMagazine, queueEntity, cutoff, false);

        // A message the shovel has just fired out of the sideline is in flight, not abandoned. Two
        // shovel intervals is the same allowance the previous implementation made.
        final long lastShovelTs = now - (queueEntity.getShovelTimeIntervalInSecs() * 2 * 1000L);
        sweepQuietly(queueName, sidelineMagazine, sidelineMagazine, queueEntity,
                Math.min(cutoff, lastShovelTs), true);
    }

    private void sweepQuietly(final String queueName,
                              final Magazine<String> magazine,
                              final Magazine<String> sidelineMagazine,
                              final QueueEntity queueEntity,
                              final long cutoffMillis,
                              final boolean isSideline) {
        try {
            sweepMagazine(queueName, magazine, sidelineMagazine, queueEntity, cutoffMillis, isSideline);
        } catch (Exception e) {
            // Includes INVALID_REQUEST from firePointerBefore, which means the retained history no
            // longer reaches back to the cutoff and the sweeper therefore cannot tell an abandoned
            // message from a live one. Refusing to sweep is the only safe answer, and the loud
            // failure is the point: silence would look identical to a healthy queue with nothing to
            // do.
            log.error("Sweep failed for magazine '{}'. Skipping it this pass",
                    magazine.getMagazineIdentifier(), e);
        }
    }

    private void sweepMagazine(final String queueName,
                               final Magazine<String> magazine,
                               final Magazine<String> sidelineMagazine,
                               final QueueEntity queueEntity,
                               final long cutoffMillis,
                               final boolean isSideline) {
        final Map<String, FireCheckpoint> watermarks =
                magazine.firePointerBefore(Instant.ofEpochMilli(cutoffMillis));
        if (watermarks.isEmpty()) {
            log.debug("No checkpoint reaches back to {} for magazine '{}' yet. Nothing to sweep",
                    Instant.ofEpochMilli(cutoffMillis), magazine.getMagazineIdentifier());
            return;
        }

        final Map<Integer, Long> limits = watermarkLimits(magazine, watermarks);
        final Map<Integer, Long> positions = startPositions(limits, isSideline
                ? queueEntity.getSidelineSweepPointers() : queueEntity.getSweepPointers());

        long swept = isSideline ? queueEntity.getSidelineSweptCounter() : queueEntity.getSweptCounter();
        while (!positions.isEmpty()) {
            swept = sweepRound(queueName, magazine, sidelineMagazine, positions, limits, swept, isSideline);
        }
    }

    private static Map<Integer, Long> watermarkLimits(final Magazine<String> magazine,
                                                      final Map<String, FireCheckpoint> watermarks) {
        final Map<Integer, Long> limits = new HashMap<>();
        for (int shard = 0; shard < magazine.getShards(); shard++) {
            final FireCheckpoint checkpoint = watermarks.get(Utils.getShardId(shard));
            if (Objects.nonNull(checkpoint)) {
                limits.put(shard, checkpoint.firePointer());
            }
        }
        return limits;
    }

    private static Map<Integer, Long> startPositions(final Map<Integer, Long> limits,
                                                     final Map<String, Long> storedPointers) {
        final Map<Integer, Long> positions = new HashMap<>();
        limits.forEach((shard, limit) -> {
            final long from = Objects.isNull(storedPointers)
                    ? 0L : Math.max(0L, storedPointers.getOrDefault(Utils.getShardId(shard), 0L));
            if (from <= limit) {
                positions.put(shard, from);
            }
        });
        return positions;
    }

    private long sweepRound(final String queueName,
                            final Magazine<String> magazine,
                            final Magazine<String> sidelineMagazine,
                            final Map<Integer, Long> positions,
                            final Map<Integer, Long> limits,
                            final long sweptSoFar,
                            final boolean isSideline) {
        final Map<Integer, SweepWindow> windows = buildWindows(positions, limits);
        final Map<Integer, List<MagazineData<String>>> orphans =
                groupByShard(magazine.peek(pointersOf(windows)));

        long swept = sweptSoFar;
        final Map<String, Long> progress = new HashMap<>();
        for (Map.Entry<Integer, SweepWindow> entry : windows.entrySet()) {
            final int shard = entry.getKey();
            final SweepWindow window = entry.getValue();
            final ShardOutcome outcome = reHome(magazine, sidelineMagazine, shard, window,
                    orphans.getOrDefault(shard, List.of()));

            swept += outcome.reHomed();
            progress.put(Utils.getShardId(shard), outcome.resumeFrom());
            advance(positions, shard, outcome.resumeFrom(), window.end(), limits.get(shard));
        }

        queueService.updateSweepProgress(queueName, isSideline, progress, swept);
        return swept;
    }

    private static Map<Integer, SweepWindow> buildWindows(final Map<Integer, Long> positions,
                                                          final Map<Integer, Long> limits) {
        final Map<Integer, SweepWindow> windows = new HashMap<>();
        positions.forEach((shard, from) ->
                windows.put(shard, new SweepWindow(from,
                        Math.min(from + Constants.SWEEP_BATCH_SIZE - 1, limits.get(shard)))));
        return windows;
    }

    private static Map<Integer, Set<Long>> pointersOf(final Map<Integer, SweepWindow> windows) {
        final Map<Integer, Set<Long>> pointers = new HashMap<>();
        windows.forEach((shard, window) -> {
            final Set<Long> slots = new LinkedHashSet<>();
            for (long pointer = window.from(); pointer <= window.end(); pointer++) {
                slots.add(pointer);
            }
            pointers.put(shard, slots);
        });
        return pointers;
    }

    private static Map<Integer, List<MagazineData<String>>> groupByShard(
            final Set<MagazineData<String>> peeked) {
        final Map<Integer, List<MagazineData<String>>> byShard = new HashMap<>();
        peeked.forEach(data -> byShard
                // An unsharded magazine has exactly one shard, numbered zero, and may report it as
                // null rather than echoing back the key it was asked for.
                .computeIfAbsent(Objects.isNull(data.getShard()) ? 0 : data.getShard(),
                        shard -> new ArrayList<>())
                .add(data));
        byShard.values().forEach(orphans -> orphans.sort(Comparator.comparingLong(MagazineData::getFirePointer)));
        return byShard;
    }

    private ShardOutcome reHome(final Magazine<String> magazine,
                                final Magazine<String> sidelineMagazine,
                                final int shard,
                                final SweepWindow window,
                                final List<MagazineData<String>> orphans) {
        int reHomed = 0;
        for (MagazineData<String> orphan : orphans) {
            if (!transferred(sidelineMagazine, orphan)) {
                // Leave the source record exactly where it is: below the fire pointer, inside a
                // range the watermark will still cover next pass. Advancing past it would be the
                // one way to lose it, since nowhere else holds a copy.
                log.error("Could not re-home an abandoned message from magazine '{}' shard {} at {}. "
                                + "Leaving it in place and stopping this shard for this pass",
                        magazine.getMagazineIdentifier(), shard, orphan.getFirePointer());
                return new ShardOutcome(orphan.getFirePointer(), reHomed);
            }
            magazine.delete(orphan);
            reHomed++;
        }
        return new ShardOutcome(window.end() + 1, reHomed);
    }

    private static void advance(final Map<Integer, Long> positions,
                                final int shard,
                                final long resumeFrom,
                                final long windowEnd,
                                final long limit) {
        if (resumeFrom > windowEnd && resumeFrom <= limit) {
            positions.put(shard, resumeFrom);
        } else {
            positions.remove(shard);
        }
    }

    private static boolean transferred(final Magazine<String> sidelineMagazine,
                                       final MagazineData<String> orphan) {
        if (Objects.isNull(orphan.getData()) || orphan.getData().isEmpty()) {
            return true;
        }
        try {
            return sidelineMagazine.load(orphan.getData());
        } catch (Exception e) {
            log.error("Exception re-homing an abandoned message into magazine '{}'",
                    sidelineMagazine.getMagazineIdentifier(), e);
            return false;
        }
    }

    private BaseMagazineStorage<String> buildStorage(final QueueEntity queueEntity) {
        return storage.accept(new MagazineStorageVisitor(
                clientId, queueEntity.getMessageExpiry(),
                queueEntity.getQueueExpiry() * Constants.TTL_FACTOR_FOR_QUEUE_EXPIRY,
                client, queueEntity.getShards(), farmId, meterRegistry,
                queueEntity.getSweepDuration()));
    }

    private static Magazine<String> magazine(final BaseMagazineStorage<String> magazineStorage,
                                             final String magazineIdentifier) {
        return Magazine.<String>builder()
                .baseMagazineStorage(magazineStorage)
                .magazineIdentifier(magazineIdentifier)
                .build();
    }

    private record SweepWindow(long from, long end) {
    }

    private record ShardOutcome(long resumeFrom, int reHomed) {
    }
}
