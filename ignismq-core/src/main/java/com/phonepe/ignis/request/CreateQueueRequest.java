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

package com.phonepe.ignis.request;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.phonepe.ignis.common.TimeToLive;
import com.phonepe.ignis.common.TimeUnit;
import com.phonepe.ignis.config.BatchingConfig;
import com.phonepe.ignis.utils.Constants;
import com.phonepe.ignis.utils.ErrorMessage;
import io.dropwizard.validation.ValidationMethod;
import lombok.Builder;
import lombok.Data;

import javax.validation.Valid;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.util.Objects;

/**
 * @author shantanu.tiwari
 */
@Data
public class CreateQueueRequest {
    private static final int DEFAULT_SWEEP_DURATION_IN_MINS = 20;
    @NotBlank
    private final String name;
    @Min(1)
    @Max(512)
    private final Integer shards;
    private final TimeToLive messageExpiry;
    private final TimeToLive queueExpiry;
    @Min(1)
    @Max(Constants.MAX_CONSUMERS_ALLOWED)
    private final int concurrency;
    @NotNull
    private final String messageHandlerType;
    @Valid
    private final ShovelConfig shovelConfig;
    @Min(5)
    @Max(300)
    private final int sweepDurationInMins;
    @Valid
    private final BatchingConfig batchingConfig;

    @Builder
    @JsonCreator
    public CreateQueueRequest(@JsonProperty("name") final String name,
                              @JsonProperty("shards") final Integer shards,
                              @JsonProperty("messageExpiry") final TimeToLive messageExpiry,
                              @JsonProperty("queueExpiry") final TimeToLive queueExpiry,
                              @JsonProperty("concurrency") final int concurrency,
                              @JsonProperty("messageHandler") final String messageHandlerType,
                              @JsonProperty("shovelConfig") final ShovelConfig shovelConfig,
                              @JsonProperty("sweepDurationInMins") final Integer sweepDurationInMins,
                              @JsonProperty("batchingConfig") final BatchingConfig batchingConfig) {
        this.name = name;
        this.shards = Objects.nonNull(shards) ? shards : Constants.DEFAULT_SHARDS;
        this.messageExpiry = Objects.nonNull(messageExpiry) ? messageExpiry : getDefaultTimeToLive();
        this.queueExpiry = Objects.nonNull(queueExpiry) ? queueExpiry : getDefaultTimeToLive();
        this.concurrency = concurrency;
        this.messageHandlerType = messageHandlerType;
        this.shovelConfig = shovelConfig;
        this.sweepDurationInMins = Objects.nonNull(sweepDurationInMins)
                ? sweepDurationInMins : DEFAULT_SWEEP_DURATION_IN_MINS;
        this.batchingConfig = batchingConfig;
    }

    public TimeToLive getDefaultTimeToLive() {
        return TimeToLive.builder()
                .timeUnit(TimeUnit.DAY)
                .duration(Constants.DEFAULT_DURATION_DAY)
                .build();
    }

    @ValidationMethod(message = ErrorMessage.QUEUE_EXPIRY_VALIDATION_MESSAGE)
    @JsonIgnore
    public boolean isValid() {
        return queueExpiry.isValid()
                && messageExpiry.isValid()
                && queueExpiry.toSeconds() >= messageExpiry.toSeconds();
    }
}
