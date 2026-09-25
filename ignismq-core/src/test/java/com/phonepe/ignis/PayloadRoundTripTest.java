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

package com.phonepe.ignis;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.phonepe.ignis.client.StorageClient;
import com.phonepe.ignis.common.MessageHandler;
import com.phonepe.ignis.service.AerospikeQueueService;
import com.phonepe.ignis.util.AerospikeTestBase;
import com.phonepe.ignis.util.RequestFactory;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.curator.framework.CuratorFramework;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * What a consumer receives must be what the publisher published.
 * <p>
 * This is the one property the rest of the suite could not see, because it tests the two halves
 * separately: the consumer tests stage the magazine by hand, and the manager tests publish without
 * ever looking at what came out the other end. Between them sat an encode with no matching decode.
 * {@code publish} JSON-encodes every payload, while the consumer used to hand String and primitive
 * payloads back as the stored text - so {@code hello} was delivered as {@code "hello"}, and a
 * payload containing quotes, newlines or tabs arrived with its JSON escaping still applied.
 * <p>
 * These tests go through the real manager, the real {@code publish}, real Aerospike-backed storage
 * and the real consumer task, because a mock of any of them is a mock of the thing that was wrong.
 */
class PayloadRoundTripTest extends AerospikeTestBase {

    private IgnisMQManager ignisMQManager;

    @BeforeEach
    void setUp() throws Exception {
        final StorageClient<com.aerospike.client.IAerospikeClient> storageClient = mock(StorageClient.class);
        when(storageClient.getClient()).thenReturn(aerospikeClient);

        ignisMQManager = new IgnisMQManager(
                CLIENT_ID, createBaseStorage(), new ObjectMapper(), new SimpleMeterRegistry(),
                storageClient, mock(CuratorFramework.class), FARM_ID, null);

        final AerospikeQueueService queueService = spy(createQueueService());
        final Field field = IgnisMQManager.class.getDeclaredField("queueService");
        field.setAccessible(true);
        field.set(ignisMQManager, queueService);
        doReturn(Collections.emptyMap()).when(queueService).getQueues(anyBoolean());
    }

    @AfterEach
    void stopManager() {
        if (ignisMQManager != null) {
            ignisMQManager.stop();
            ignisMQManager = null;
        }
    }

    @Test
    void aPlainStringArrivesExactlyAsItWasPublished() throws Exception {
        assertRoundTrips(String.class, "hello world");
    }

    /**
     * The case that proves this was never merely a matter of surrounding quotes: JSON escaping was
     * applied on the way in and never undone, so a payload carrying a quote, a newline or a tab came
     * back with backslashes in it that the publisher never wrote.
     */
    @Test
    void aStringCarryingQuotesAndControlCharactersIsNotMangled() throws Exception {
        assertRoundTrips(String.class, "say \"hi\"\nsecond line\ttabbed");
    }

    /**
     * The worst case: a String queue used to carry JSON. Every quote in the document was escaped on
     * publish, so what the handler received was not parseable as the document that was sent.
     */
    @Test
    void aStringHoldingJsonIsDeliveredAsThatSameJson() throws Exception {
        assertRoundTrips(String.class, "{\"a\":1,\"b\":[2,3]}");
    }

    @Test
    void anEmptyStringSurvivesTheRoundTrip() throws Exception {
        assertRoundTrips(String.class, "");
    }

    /**
     * Boxed types always took the Jackson path and must keep working: {@code Integer.class
     * .isPrimitive()} is false, so the raw-payload shortcut never applied to them.
     */
    @Test
    void anIntegerRoundTrips() throws Exception {
        assertRoundTrips(Integer.class, 42);
    }

    @Test
    void aBooleanRoundTrips() throws Exception {
        assertRoundTrips(Boolean.class, Boolean.TRUE);
    }

    /**
     * A POJO is the common case and the one that always worked. It is pinned here so that fixing the
     * String path cannot quietly cost the path everybody else is on.
     */
    @Test
    void aPojoRoundTrips() throws Exception {
        assertRoundTrips(Order.class, new Order("order-1", 7, "note with \"quotes\"\nand a newline"));
    }

    /**
     * Pins the behaviour of a literal primitive class, which is what the old
     * {@code clazz.isPrimitive()} branch actually selected - and the only thing it selected, since
     * the boxed types report false.
     */
    @Test
    void aPrimitiveIntQueueDeliversTheBoxedValueRatherThanThrowing() throws Exception {
        assertRoundTrips(int.class, 42);
    }

    /**
     * Publishes one message and hands back what the handler was actually given.
     * <p>
     * The handler returns true, so an accepted message is deleted; a payload the consumer could not
     * read would instead be sidelined and never recorded here, which is why the wait fails loudly
     * rather than quietly comparing nulls.
     */
    private <M> void assertRoundTrips(final Class<M> clazz, final M published) throws Exception {
        final String handlerType = "handler-" + clazz.getSimpleName();
        final CapturingHandler<M> handler = new CapturingHandler<>();

        final Map<String, Map.Entry<Class, MessageHandler>> handlers = new HashMap<>();
        handlers.put(handlerType, new AbstractMap.SimpleEntry<>(clazz, handler));
        ignisMQManager.initialiseMessageHandlers(handlers);
        ignisMQManager.createQueue(RequestFactory.createQueueRequest("ROUND_TRIP", handlerType));

        Assertions.assertTrue(ignisMQManager.<M>getQueue("ROUND_TRIP").publish(published),
                "the queue must have accepted the message");

        Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> Assertions.assertFalse(handler.received.isEmpty(),
                        "the consumer never delivered the message to the handler"));

        Assertions.assertEquals(published, handler.received.get(0),
                "a consumer must receive exactly what was published");
    }

    /**
     * Records every message handed to it and accepts all of them.
     */
    private static final class CapturingHandler<M> implements MessageHandler<M> {
        private final List<M> received = Collections.synchronizedList(new java.util.ArrayList<>());

        @Override
        public Set<Class<?>> getIgnorableExceptions() {
            return Set.of();
        }

        @Override
        public boolean handle(final M message) {
            received.add(message);
            return true;
        }

        @Override
        public boolean handle(final List<M> messages) {
            received.addAll(messages);
            return true;
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static final class Order {
        private String id;
        private int quantity;
        private String note;
    }
}
