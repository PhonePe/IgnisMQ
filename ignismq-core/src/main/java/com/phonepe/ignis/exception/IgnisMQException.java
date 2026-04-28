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

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @author shantanu.tiwari
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class IgnisMQException extends RuntimeException {
    private final ErrorCode errorCode;

    @Builder
    public IgnisMQException(final ErrorCode errorCode, final String message, final Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public static IgnisMQException propagate(final Throwable throwable) {
        return propagate("Error occurred", throwable);
    }

    public static IgnisMQException propagate(final String message, final Throwable throwable) {
        if (throwable instanceof IgnisMQException) {
            return (IgnisMQException) throwable;
        } else if (throwable.getCause() instanceof IgnisMQException) {
            return (IgnisMQException) throwable.getCause();
        }
        return new IgnisMQException(ErrorCode.INTERNAL_ERROR, message, throwable);
    }

    public static IgnisMQException propagate(final ErrorCode errorCode, final Throwable throwable) {
        if (throwable instanceof IgnisMQException) {
            return (IgnisMQException) throwable;
        } else if (throwable.getCause() instanceof IgnisMQException) {
            return (IgnisMQException) throwable.getCause();
        }
        return new IgnisMQException(errorCode, throwable.getMessage(), throwable);
    }
}
