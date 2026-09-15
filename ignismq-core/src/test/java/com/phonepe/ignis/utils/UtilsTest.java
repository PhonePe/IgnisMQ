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

package com.phonepe.ignis.utils;

import com.phonepe.ignis.exception.IgnisMQException;
import com.phonepe.magazine.entity.MetaData;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class UtilsTest {

    @Test
    public void testGetSidelineQueueName() {
        assertEquals("QUEUE_1_SIDELINE", Utils.getSidelineQueueName("QUEUE_1"));
    }

    /**
     * Reach is traded for resolution: a 20-minute sweep is checkpointed every 150 seconds, so the
     * 32 retained entries span several sweep durations and the sweeper's question always lands
     * inside them.
     */
    @Test
    public void testFireHistoryWindowDividesTheSweepDuration() {
        assertEquals(150, Utils.fireHistoryWindowSeconds(20 * 60 * 1000L));
        assertEquals(450, Utils.fireHistoryWindowSeconds(60 * 60 * 1000L));
    }

    /**
     * A window narrower than a second is not expressible, and a sweep duration of zero is a valid
     * request to sweep everything already delivered.
     */
    @Test
    public void testFireHistoryWindowNeverCollapsesBelowASecond() {
        assertEquals(1, Utils.fireHistoryWindowSeconds(0L));
        assertEquals(1, Utils.fireHistoryWindowSeconds(-1L));
        assertEquals(1, Utils.fireHistoryWindowSeconds(1000L));
    }

    /**
     * Sizing is capped at the sweep duration ignisMQ is willing to honour, so a nonsensical value
     * cannot silently produce a window so wide the history stops being useful.
     */
    @Test
    public void testFireHistoryWindowIsCappedAtTheMaximumSweepDuration() {
        assertEquals(Utils.fireHistoryWindowSeconds(Constants.MAX_SWEEP_DURATION_IN_MS),
                Utils.fireHistoryWindowSeconds(Constants.MAX_SWEEP_DURATION_IN_MS * 10));
    }

    @Test
    public void testGetMagazineSet() {
        String set = Utils.getMagazineSet("CLIENT_ID", "data_set");
        assertEquals("CLIENT_ID_data_set", set);
    }

    @Test
    public void testGetShardId() {
        String shardId = Utils.getShardId(5);
        assertTrue(shardId.contains("5"));
    }

    @Test
    public void testGetMagazineCountEmpty() {
        Collection<MetaData> empty = Collections.emptyList();
        assertEquals(0L, Utils.getMagazineCount(empty, MetaData::getLoadPointer));
    }

    @Test
    public void testGetMagazineCountMultiple() {
        List<MetaData> metaDataList = List.of(
                MetaData.builder().loadPointer(10).firePointer(5).build(),
                MetaData.builder().loadPointer(20).firePointer(15).build()
        );
        assertEquals(30L, Utils.getMagazineCount(metaDataList, MetaData::getLoadPointer));
        assertEquals(20L, Utils.getMagazineCount(metaDataList, MetaData::getFirePointer));
    }

    @Test
    public void testWaitForRequestsCompletionSuccess() {
        List<Future<Boolean>> futures = new ArrayList<>();
        futures.add(CompletableFuture.completedFuture(true));
        futures.add(CompletableFuture.completedFuture(true));

        Utils.waitForRequestsCompletion(futures);
        assertTrue(futures.isEmpty());
    }

    @Test
    public void testWaitForRequestsCompletionFailure() {
        List<Future<Boolean>> futures = new ArrayList<>();
        CompletableFuture<Boolean> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("test"));
        futures.add(failedFuture);

        assertThrows(IgnisMQException.class, () -> Utils.waitForRequestsCompletion(futures));
    }

    @Test
    public void testExecutorServiceNotNull() {
        assertNotNull(Utils.executorService);
    }

    @Test
    public void testWaitForRequestsCompletionWithInterruptedException() {
        List<Future<Boolean>> futures = new ArrayList<>();
        Future<Boolean> future = new Future<>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                return false;
            }

            @Override
            public boolean isCancelled() {
                return false;
            }

            @Override
            public boolean isDone() {
                return false;
            }

            @Override
            public Boolean get() throws InterruptedException {
                throw new InterruptedException("interrupted");
            }

            @Override
            public Boolean get(long timeout, TimeUnit unit) throws InterruptedException {
                throw new InterruptedException("interrupted");
            }
        };
        futures.add(future);
        assertThrows(IgnisMQException.class, () -> Utils.waitForRequestsCompletion(futures));
    }

    /**
     * The sweep duration is the correctness bound: a handler still running holds a claimed record,
     * and once it elapses the sweeper sidelines and deletes that record while the handler runs on.
     */
    @Test
    public void testHandlerTimeoutIsClampedToHalfTheSweepDuration() {
        final long fiveMinutes = 5 * 60 * 1000L;
        final long tenMinutes = 10 * 60 * 1000L;
        assertEquals(fiveMinutes / 2, Utils.handlerTimeoutMillis(tenMinutes, fiveMinutes));
    }

    @Test
    public void testAConfiguredTimeoutWithinTheBoundIsHonoured() {
        final long oneMinute = 60 * 1000L;
        final long thirtyMinutes = 30 * 60 * 1000L;
        assertEquals(oneMinute, Utils.handlerTimeoutMillis(oneMinute, thirtyMinutes));
    }

    /** Degenerate input must not yield a zero or negative timeout, which would fail every batch. */
    @Test
    public void testHandlerTimeoutIsAlwaysPositive() {
        assertTrue(Utils.handlerTimeoutMillis(0L, 0L) > 0);
        assertTrue(Utils.handlerTimeoutMillis(-1L, -1L) > 0);
    }
}
