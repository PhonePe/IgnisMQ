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

package com.phonepe.ignis.scheduler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The executor exists to stop a user handler holding a worker thread for ever, and to be honest
 * about the fact that it cannot actually stop the handler.
 */
class HandlerExecutorTest {

    private HandlerExecutor executor;

    @AfterEach
    void tearDown() {
        if (executor != null) {
            executor.stop();
        }
    }

    @Test
    @Timeout(value = 30000, unit = java.util.concurrent.TimeUnit.MILLISECONDS)
    void testAHandlerThatOverrunsItsTimeoutRaisesTimeout() {
        executor = new HandlerExecutor(4);

        // The handler must still be running when the 200ms timeout expires. Released by this test
        // rather than by the executor's interrupt, so a stop() that stopped interrupting would fail
        // the assertion rather than leak the thread for the life of the JVM.
        final CountDownLatch hold = new CountDownLatch(1);
        try {
            assertThrows(TimeoutException.class, () -> executor.call(() -> {
                hold.await();
                return true;
            }, 200), "a handler that overruns its timeout must raise TimeoutException");
        } finally {
            hold.countDown();
        }
    }

    /**
     * The honest limit, asserted rather than assumed: interruption is a request, and a handler that
     * ignores it keeps running after the timeout. Anyone reading this class needs to know the
     * runaway thread is not reclaimed.
     */
    @Test
    @Timeout(value = 30000, unit = java.util.concurrent.TimeUnit.MILLISECONDS)
    void testAnUninterruptibleHandlerSurvivesItsTimeout() throws Exception {
        executor = new HandlerExecutor(4);
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicBoolean finished = new AtomicBoolean();

        assertThrows(TimeoutException.class, () -> executor.call(() -> {
            started.countDown();
            // Deliberately swallows interruption, as a handler blocked in a socket read would.
            while (release.getCount() > 0) {
                try {
                    release.await();
                } catch (InterruptedException ignored) {
                    Thread.interrupted();
                }
            }
            finished.set(true);
            return true;
        }, 200));

        assertTrue(started.await(5, TimeUnit.SECONDS));
        assertFalse(finished.get(), "the handler is still running; the timeout freed the caller, not the thread");
        release.countDown();
    }

    /**
     * A handler that returns in time behaves exactly as an inline call did.
     */
    @Test
    @Timeout(value = 30000, unit = java.util.concurrent.TimeUnit.MILLISECONDS)
    void testAPromptHandlerReturnsItsValue() throws Exception {
        executor = new HandlerExecutor(4);

        assertTrue(executor.call(() -> true, 10_000));
        assertFalse(executor.call(() -> false, 10_000));
    }

    /**
     * Exceptions must arrive unwrapped, or every existing catch block in the consumer would start
     * seeing {@code ExecutionException} instead of what the handler actually threw - silently
     * changing which exceptions count as ignorable.
     */
    @Test
    @Timeout(value = 30000, unit = java.util.concurrent.TimeUnit.MILLISECONDS)
    void testHandlerExceptionsArriveUnwrapped() {
        executor = new HandlerExecutor(4);

        final IllegalStateException thrown = assertThrows(IllegalStateException.class,
                () -> executor.call(() -> {
                    throw new IllegalStateException("handler blew up");
                }, 10_000), "the handler's exception must propagate");
        assertEquals("handler blew up", thrown.getMessage());
    }

    /**
     * Saturation degrades to inline, untimed execution rather than dropping the
     * batch. Losing the timeout is bad; losing the work would be worse.
     */
    @Test
    @Timeout(value = 30000, unit = java.util.concurrent.TimeUnit.MILLISECONDS)
    void testSaturationFallsBackToRunningInline() throws Exception {
        executor = new HandlerExecutor(1);
        final CountDownLatch occupied = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);

        final Thread hog = new Thread(() -> {
            try {
                executor.call(() -> {
                    occupied.countDown();
                    release.await();
                    return true;
                }, 25_000);
            } catch (Exception ignored) {
                // the hog's own outcome is not what this test is about
            }
        });
        hog.start();
        assertTrue(occupied.await(10, TimeUnit.SECONDS));

        final AtomicBoolean handlerRan = new AtomicBoolean();
        assertThrows(HandlerSaturatedException.class,
                () -> executor.call(() -> handlerRan.compareAndSet(false, true), 10_000));

        assertFalse(handlerRan.get(),
                "a refused batch must not have been executed; the caller still owns it");
        assertEquals(1, executor.saturationRefusals());
        release.countDown();
        hog.join(10_000);
    }

    /**
     * A lifecycle may stop this twice, and may stop one that never ran anything.
     */
    @Test
    @Timeout(value = 30000, unit = java.util.concurrent.TimeUnit.MILLISECONDS)
    void testStopIsIdempotentAndSafeWithoutWork() {
        executor = new HandlerExecutor(4);

        assertFalse(executor.isStopped());
        assertTrue(executor.stop());
        assertTrue(executor.stop());
        assertTrue(executor.isStopped());
    }
}
