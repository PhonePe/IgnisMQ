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

import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

@Slf4j
@Provider
public final class IgnisMQExceptionMapper implements ExceptionMapper<IgnisMQException> {

    static final Map<ErrorCode, Integer> STATUSES;

    private static final String OPAQUE_MESSAGE = "The request could not be completed.";

    static {
        final Map<ErrorCode, Integer> statuses = new EnumMap<>(ErrorCode.class);
        statuses.put(ErrorCode.QUEUE_NOT_FOUND, 404);
        statuses.put(ErrorCode.QUEUE_ALREADY_EXISTS, 409);
        statuses.put(ErrorCode.MAX_ALLOWED_CONSUMERS_EXCEEDED, 409);
        statuses.put(ErrorCode.INVALID_REQUEST, 400);
        statuses.put(ErrorCode.INVALID_SHOVEL_TIME_INTERVAL, 400);
        statuses.put(ErrorCode.INVALID_MESSAGE_HANDLER, 400);
        statuses.put(ErrorCode.NOT_IMPLEMENTED, 501);
        // Retryable and not the caller's doing, which 503 says and 500 does not.
        statuses.put(ErrorCode.AEROSPIKE_ERROR, 503);
        statuses.put(ErrorCode.INTERNAL_ERROR, 500);
        STATUSES = Collections.unmodifiableMap(statuses);
    }

    @Override
    public Response toResponse(final IgnisMQException exception) {
        final int status = STATUSES.getOrDefault(exception.getErrorCode(), 500);
        if (status >= 500) {
            log.error("ignisMQ request failed with {}", exception.getErrorCode(), exception);
        }
        return Response.status(status)
                .type(MediaType.APPLICATION_JSON)
                .entity(new ErrorBody(exception.getErrorCode().name(),
                        status >= 500 ? OPAQUE_MESSAGE : exception.getMessage()))
                .build();
    }

    /**
     * @param message present only for statuses the caller can act on. A 5xx message is a driver or
     *                storage string and belongs in the log.
     */
    public record ErrorBody(String errorCode, String message) {
    }
}
