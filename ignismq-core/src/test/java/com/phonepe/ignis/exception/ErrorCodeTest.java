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

import org.junit.Test;

import static org.junit.Assert.*;

public class ErrorCodeTest {

    @Test
    public void testAllErrorCodes() {
        ErrorCode[] codes = ErrorCode.values();
        assertTrue(codes.length > 0);

        assertNotNull(ErrorCode.valueOf("QUEUE_ALREADY_EXISTS"));
        assertNotNull(ErrorCode.valueOf("QUEUE_NOT_FOUND"));
        assertNotNull(ErrorCode.valueOf("NOT_IMPLEMENTED"));
        assertNotNull(ErrorCode.valueOf("AEROSPIKE_ERROR"));
        assertNotNull(ErrorCode.valueOf("INVALID_REQUEST"));
        assertNotNull(ErrorCode.valueOf("MAX_ALLOWED_CONSUMERS_EXCEEDED"));
        assertNotNull(ErrorCode.valueOf("INVALID_SHOVEL_TIME_INTERVAL"));
        assertNotNull(ErrorCode.valueOf("INVALID_MESSAGE_HANDLER"));
        assertNotNull(ErrorCode.valueOf("INTERNAL_ERROR"));
    }
}
