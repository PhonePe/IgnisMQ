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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class IgnisMQExceptionTest {

    @Test
    void testBuilder() {
        IgnisMQException exception = IgnisMQException.builder()
                .errorCode(ErrorCode.QUEUE_NOT_FOUND)
                .message("Queue not found")
                .build();

        assertEquals(ErrorCode.QUEUE_NOT_FOUND, exception.getErrorCode());
        assertEquals("Queue not found", exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    void testBuilderWithCause() {
        RuntimeException cause = new RuntimeException("root cause");
        IgnisMQException exception = IgnisMQException.builder()
                .errorCode(ErrorCode.AEROSPIKE_ERROR)
                .message("AS error")
                .cause(cause)
                .build();

        assertEquals(ErrorCode.AEROSPIKE_ERROR, exception.getErrorCode());
        assertEquals(cause, exception.getCause());
    }

    @Test
    void testPropagateWithIgnisMQException() {
        IgnisMQException original = IgnisMQException.builder()
                .errorCode(ErrorCode.QUEUE_ALREADY_EXISTS)
                .build();

        IgnisMQException result = IgnisMQException.propagate(original);
        assertSame(original, result);
    }

    @Test
    void testPropagateWithWrappedIgnisMQException() {
        IgnisMQException original = IgnisMQException.builder()
                .errorCode(ErrorCode.QUEUE_ALREADY_EXISTS)
                .build();
        RuntimeException wrapper = new RuntimeException("wrapper", original);

        IgnisMQException result = IgnisMQException.propagate(wrapper);
        assertSame(original, result);
    }

    @Test
    void testPropagateWithGenericException() {
        RuntimeException generic = new RuntimeException("generic");

        IgnisMQException result = IgnisMQException.propagate(generic);
        assertEquals(ErrorCode.INTERNAL_ERROR, result.getErrorCode());
        assertEquals("Error occurred", result.getMessage());
        assertEquals(generic, result.getCause());
    }

    @Test
    void testPropagateWithMessageAndGenericException() {
        RuntimeException generic = new RuntimeException("generic");

        IgnisMQException result = IgnisMQException.propagate("custom msg", generic);
        assertEquals(ErrorCode.INTERNAL_ERROR, result.getErrorCode());
        assertEquals("custom msg", result.getMessage());
    }

    @Test
    void testPropagateWithErrorCode() {
        RuntimeException generic = new RuntimeException("generic msg");

        IgnisMQException result = IgnisMQException.propagate(ErrorCode.AEROSPIKE_ERROR, generic);
        assertEquals(ErrorCode.AEROSPIKE_ERROR, result.getErrorCode());
        assertEquals("generic msg", result.getMessage());
    }

    @Test
    void testPropagateWithErrorCodeAndIgnisMQException() {
        IgnisMQException original = IgnisMQException.builder()
                .errorCode(ErrorCode.QUEUE_NOT_FOUND)
                .build();

        IgnisMQException result = IgnisMQException.propagate(ErrorCode.AEROSPIKE_ERROR, original);
        assertSame(original, result);
    }

    @Test
    void testPropagateWithErrorCodeAndWrappedIgnisMQException() {
        IgnisMQException original = IgnisMQException.builder()
                .errorCode(ErrorCode.QUEUE_NOT_FOUND)
                .build();
        RuntimeException wrapper = new RuntimeException("wrapper", original);

        IgnisMQException result = IgnisMQException.propagate(ErrorCode.AEROSPIKE_ERROR, wrapper);
        assertSame(original, result);
    }

    @Test
    void testAllErrorCodes() {
        for (ErrorCode code : ErrorCode.values()) {
            IgnisMQException exception = IgnisMQException.builder()
                    .errorCode(code)
                    .build();
            assertEquals(code, exception.getErrorCode());
        }
    }
}
