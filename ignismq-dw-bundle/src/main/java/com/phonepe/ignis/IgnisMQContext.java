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

package com.phonepe.ignis;

import com.phonepe.ignis.config.ConsoleConfiguration;
import com.phonepe.ignis.storage.BaseStorage;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import org.apache.curator.framework.CuratorFramework;

/**
 * Everything {@link IgnisMQBundle} needs from the application to build a manager.
 * <p>
 * A single extension point rather than one abstract method per value, so that a new input is a new
 * field here instead of a new abstract method every existing subclass must implement.
 */
@Value
@Builder
public class IgnisMQContext {

    @NonNull
    String clientId;

    @NonNull
    String farmId;

    @NonNull
    BaseStorage storage;

    @NonNull
    CuratorFramework curatorFramework;

    /** Defaulted rather than required: every tunable behind it already has a working default. */
    @Builder.Default
    IgnisMQSettings settings = IgnisMQSettings.defaults();

    /**
     * The first input added since this became a context rather than a set of abstract methods, and
     * the reason it is one: a sixth abstract method would have been a source break for every
     * subclass.
     */
    @Builder.Default
    ConsoleConfiguration console = ConsoleConfiguration.defaults();
}
