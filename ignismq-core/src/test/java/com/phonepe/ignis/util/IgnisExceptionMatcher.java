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

package com.phonepe.ignis.util;

import com.phonepe.ignis.exception.ErrorCode;
import com.phonepe.ignis.exception.IgnisMQException;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

public class IgnisExceptionMatcher extends TypeSafeMatcher<IgnisMQException> {
    private final ErrorCode expectedErrorCode;
    private ErrorCode foundErrorCode;

    private IgnisExceptionMatcher(ErrorCode expectedErrorCode) {
        this.expectedErrorCode = expectedErrorCode;
    }

    public static IgnisExceptionMatcher hasCode(ErrorCode errorCode) {
        return new IgnisExceptionMatcher(errorCode);
    }

    @Override
    protected boolean matchesSafely(final IgnisMQException exception) {
        foundErrorCode = exception.getErrorCode();
        return foundErrorCode == expectedErrorCode;
    }

    @Override
    public void describeTo(Description description) {
        description.appendValue(foundErrorCode)
                .appendText(" was not found instead of ")
                .appendValue(expectedErrorCode);
    }
}
