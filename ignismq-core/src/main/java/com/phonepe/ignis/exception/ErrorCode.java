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

/**
 * @author shantanu.tiwari
 */
public enum ErrorCode {
    QUEUE_ALREADY_EXISTS,
    QUEUE_NOT_FOUND,
    NOT_IMPLEMENTED,
    AEROSPIKE_ERROR,
    INVALID_REQUEST,
    MAX_ALLOWED_CONSUMERS_EXCEEDED,
    INVALID_SHOVEL_TIME_INTERVAL,
    INVALID_MESSAGE_HANDLER,
    INTERNAL_ERROR
}
