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

/** Raised when no handler thread was available, so the handler was never run. */
public class HandlerSaturatedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public HandlerSaturatedException(final int maxThreads, final long graceMillis,
                                     final int refusalsSoFar, final Throwable cause) {
        super(String.format("No handler thread available within %dms across a pool of %d; the batch "
                        + "was refused rather than run without a timeout. Refusals so far: %d. "
                        + "This means handlers are ignoring interruption after timing out",
                graceMillis, maxThreads, refusalsSoFar), cause);
    }
}
