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

package com.phonepe.ignis.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.phonepe.ignis.utils.Constants;
import lombok.Builder;
import lombok.Getter;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.AssertTrue;

/**
 * @author shantanu.tiwari
 */
@Getter
public class TimeToLive {
    @NotNull
    private final TimeUnit timeUnit;
    @Min(1)
    private final int duration;

    @Builder
    @JsonCreator
    public TimeToLive(@JsonProperty("timeUnit") final TimeUnit timeUnit,
                      @JsonProperty("duration") final int duration) {
        this.timeUnit = timeUnit;
        this.duration = duration;
    }

    public int toSeconds() {
        return timeUnit.toSeconds(duration);
    }

    @AssertTrue(message = "Time duration is more than allowed")
    @JsonIgnore
    public boolean isValid() {
        return timeUnit.toSeconds(duration) <= Constants.MAX_DURATION_ALLOWED_IN_SECONDS;
    }
}
