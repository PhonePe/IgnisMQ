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

package com.phonepe.ignis.exception;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Which failures are the caller's fault and which are ours. Every code is decided explicitly, because
 * the default - everything is a 500 - tells a caller to retry a request that can never succeed.
 */
class IgnisMQExceptionMapperTest {

    private final IgnisMQExceptionMapper mapper = new IgnisMQExceptionMapper();

    /**
     * The whole table in one place, so a change to any row is visible as a change to this test rather
     * than as a status an integration test happens not to assert.
     */
    @Test
    @DisplayName("each error code maps to the status its cause deserves")
    void everyErrorCodeMapsToItsAgreedStatus() {
        final Map<ErrorCode, Integer> expected = new EnumMap<>(ErrorCode.class);
        expected.put(ErrorCode.QUEUE_NOT_FOUND, 404);
        expected.put(ErrorCode.QUEUE_ALREADY_EXISTS, 409);
        expected.put(ErrorCode.MAX_ALLOWED_CONSUMERS_EXCEEDED, 409);
        expected.put(ErrorCode.INVALID_REQUEST, 400);
        expected.put(ErrorCode.INVALID_SHOVEL_TIME_INTERVAL, 400);
        expected.put(ErrorCode.INVALID_MESSAGE_HANDLER, 400);
        expected.put(ErrorCode.NOT_IMPLEMENTED, 501);
        expected.put(ErrorCode.AEROSPIKE_ERROR, 503);
        expected.put(ErrorCode.INTERNAL_ERROR, 500);

        expected.forEach((code, status) ->
                assertEquals(status, status(code), code + " must map to " + status));
    }

    /**
     * The guard on the table above. A new {@link ErrorCode} added without a decision would otherwise
     * inherit 500 silently, which is exactly the behaviour this mapper exists to remove.
     */
    @Test
    @DisplayName("no error code can be added without deciding its status")
    void everyErrorCodeIsAccountedFor() {
        Arrays.stream(ErrorCode.values()).forEach(code ->
                assertNotNull(IgnisMQExceptionMapper.STATUSES.get(code),
                        code + " has no decided HTTP status. Add it to the mapper, and to the table in "
                                + "everyErrorCodeMapsToItsAgreedStatus - do not let it default to 500"));
    }

    /**
     * The reported symptom: asking the console to scale past the cap is a request that can never
     * succeed, and answering 500 invites a retry and an incident.
     */
    @Test
    @DisplayName("exceeding the consumer cap is the caller's fault, not a server fault")
    void theConsumerCapIsAClientError() {
        assertEquals(409, status(ErrorCode.MAX_ALLOWED_CONSUMERS_EXCEEDED));
    }

    /**
     * Storage being unavailable is emphatically not the caller's fault, and must stay in the range
     * an availability alert watches.
     */
    @Test
    @DisplayName("a storage failure stays a server error")
    void storageFailuresStayServerErrors() {
        assertTrue(status(ErrorCode.AEROSPIKE_ERROR) >= 500,
                "a 4xx here would hide a broker outage from every 5xx alert");
    }

    @Test
    @DisplayName("the body names the error code, so a client can act on it without parsing prose")
    void theBodyCarriesTheErrorCode() {
        final Response response = mapper.toResponse(IgnisMQException.builder()
                .errorCode(ErrorCode.QUEUE_NOT_FOUND).message("no such queue").build());

        final IgnisMQExceptionMapper.ErrorBody body = (IgnisMQExceptionMapper.ErrorBody) response.getEntity();
        assertEquals("QUEUE_NOT_FOUND", body.errorCode());
        assertEquals("no such queue", body.message());
    }

    /**
     * A 5xx body is the one place a stray internal detail becomes a leak, and the message on an
     * Aerospike failure is a driver string.
     */
    @Test
    @DisplayName("a server-side failure does not echo its internal message to the caller")
    void serverErrorsDoNotLeakTheirInternalMessage() {
        final Response response = mapper.toResponse(IgnisMQException.builder()
                .errorCode(ErrorCode.AEROSPIKE_ERROR)
                .message("Error Code 8: server memory error on node BB9030011AC4202").build());

        final IgnisMQExceptionMapper.ErrorBody body = (IgnisMQExceptionMapper.ErrorBody) response.getEntity();
        assertEquals("AEROSPIKE_ERROR", body.errorCode(), "the code is safe and is what a client keys on");
        assertEquals("The request could not be completed.", body.message(),
                "the driver's own message belongs in the log, not in the response");
    }

    private int status(final ErrorCode code) {
        return mapper.toResponse(IgnisMQException.builder().errorCode(code).message("boom").build())
                .getStatus();
    }
}
