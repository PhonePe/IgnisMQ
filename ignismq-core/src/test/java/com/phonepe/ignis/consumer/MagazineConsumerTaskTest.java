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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.phonepe.ignis.common.MessageHandler;
import com.phonepe.magazine.Magazine;
import com.phonepe.magazine.entity.MagazineData;
import com.phonepe.magazine.exception.ErrorCode;
import com.phonepe.magazine.exception.MagazineException;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Covers the transfer-then-delete contract of the consumer: a message may only leave the main
 * magazine once the handler has accepted it or the sideline magazine has taken responsibility for it.
 */
public class MagazineConsumerTaskTest {

    private Magazine<String> magazine;
    private Magazine<String> sidelineMagazine;
    private Timer consumeTimer;

    @Before
    public void setUp() {
        magazine = Mockito.mock(Magazine.class);
        sidelineMagazine = Mockito.mock(Magazine.class);
        consumeTimer = new SimpleMeterRegistry().timer("test.consume");
        when(magazine.getMagazineIdentifier()).thenReturn("TEST_QUEUE");
    }

    @Test
    public void testSuccessfulHandlingDeletesWithoutSidelining() {
        firesThen("msg1");

        task(handler(true)).run();

        verify(sidelineMagazine, never()).load(any());
        verify(magazine, times(1)).delete(any());
    }

    @Test
    public void testRejectedMessageIsSidelinedThenDeleted() {
        firesThen("msg1");
        when(sidelineMagazine.load("msg1")).thenReturn(true);

        task(handler(false)).run();

        verify(sidelineMagazine, times(1)).load("msg1");
        verify(magazine, times(1)).delete(any());
    }

    /**
     * B2: the handler rejected the message and the sideline would not take it. The record in the main
     * magazine is the only copy left, so it must survive for the sweeper to retry.
     */
    @Test
    public void testRejectedMessageIsNotDeletedWhenSidelineLoadReturnsFalse() {
        firesThen("msg1");
        when(sidelineMagazine.load("msg1")).thenReturn(false);

        task(handler(false)).run();

        verify(sidelineMagazine, times(1)).load("msg1");
        verify(magazine, never()).delete(any());
    }

    /**
     * B2: same contract when the sideline load throws, which is the reachable failure mode against
     * Magazine 2's Aerospike storage.
     */
    @Test
    public void testRejectedMessageIsNotDeletedWhenSidelineLoadThrows() {
        firesThen("msg1");
        when(sidelineMagazine.load("msg1")).thenThrow(new RuntimeException("sideline down"));

        task(handler(false)).run();

        verify(magazine, never()).delete(any());
    }

    /**
     * B2: a handler that throws takes the same route. Previously the delete happened regardless of
     * whether the sideline had accepted the message.
     */
    @Test
    public void testThrowingHandlerDoesNotDeleteWhenSidelineRefuses() {
        firesThen("msg1");
        when(sidelineMagazine.load("msg1")).thenReturn(false);

        task(throwingHandler(Collections.emptySet())).run();

        verify(magazine, never()).delete(any());
    }

    /**
     * An ignorable exception means the message is deliberately dropped, so no sideline write is
     * attempted and the delete must still happen.
     */
    @Test
    public void testIgnorableExceptionDeletesWithoutSidelining() {
        firesThen("msg1");

        task(throwingHandler(Set.of(IllegalStateException.class))).run();

        verify(sidelineMagazine, never()).load(any());
        verify(magazine, times(1)).delete(any());
    }

    /**
     * One undeliverable message must not prevent the rest of the batch from being retired.
     */
    @Test
    public void testOneFailedSidelineDoesNotBlockTheRestOfTheBatch() {
        when(magazine.fire())
                .thenReturn(data("msg1"), data("msg2"))
                .thenThrow(new MagazineException(ErrorCode.NOTHING_TO_FIRE, "nothing", null));
        when(sidelineMagazine.load("msg1")).thenReturn(false);
        when(sidelineMagazine.load("msg2")).thenReturn(true);

        task(handler(false)).run();

        verify(sidelineMagazine, times(1)).load("msg1");
        verify(sidelineMagazine, times(1)).load("msg2");
        verify(magazine, times(1)).delete(any());
    }

    private void firesThen(final String message) {
        when(magazine.fire())
                .thenReturn(data(message))
                .thenThrow(new MagazineException(ErrorCode.NOTHING_TO_FIRE, "nothing", null));
    }

    private MagazineConsumerTask<String> task(final MessageHandler<String> handler) {
        return new MagazineConsumerTask<>(magazine, sidelineMagazine, handler, new ObjectMapper(),
                String.class, consumeTimer, null);
    }

    private static MagazineData<String> data(final String message) {
        return MagazineData.<String>builder()
                .magazineIdentifier("TEST_QUEUE")
                .shard(1)
                .data(message)
                .firePointer(1)
                .build();
    }

    private static MessageHandler<String> handler(final boolean outcome) {
        return new MessageHandler<>() {
            @Override
            public Set<Class<?>> getIgnorableExceptions() {
                return Collections.emptySet();
            }

            @Override
            public boolean handle(final String message) {
                return outcome;
            }

            @Override
            public boolean handle(final List<String> messages) {
                return outcome;
            }
        };
    }

    private static MessageHandler<String> throwingHandler(final Set<Class<?>> ignorable) {
        return new MessageHandler<>() {
            @Override
            public Set<Class<?>> getIgnorableExceptions() {
                return ignorable;
            }

            @Override
            public boolean handle(final String message) {
                throw new IllegalStateException("boom");
            }

            @Override
            public boolean handle(final List<String> messages) {
                throw new IllegalStateException("boom");
            }
        };
    }
}
