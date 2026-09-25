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

/**
 * Raised when a message handler did not finish inside its allotted time. Never ignorable: the batch
 * is sidelined rather than deleted, because a timeout says nothing about whether the work was done.
 */
public class HandlerTimeoutException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public HandlerTimeoutException(final String queueName, final int batchSize,
                                   final long timeoutMillis, final Throwable cause) {
        super(String.format("Handler for queue '%s' did not return within %dms for a batch of %d; "
                        + "the batch is being sidelined and the handler may still be running",
                queueName, timeoutMillis, batchSize), cause);
    }
}
