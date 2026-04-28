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

import com.phonepe.ignis.IgnisMQManager;
import com.phonepe.ignis.storage.BaseStorage;
import io.dropwizard.Configuration;
import io.dropwizard.ConfiguredBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

/**
 * @author shantanu.tiwari
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public abstract class IgnisMQBundle<T extends Configuration> implements ConfiguredBundle<T> {
    @Getter
    private IgnisMQManager ignisMQManager;

    @Override
    public void run(T config, Environment environment) throws Exception {
        this.ignisMQManager = new IgnisMQManager(getClientId(config), getStorage(config),
                environment.getObjectMapper(), environment.metrics(), getCuratorFramework(),
                getFarmId(config));
        environment.lifecycle().manage(ignisMQManager);
        environment.lifecycle().manage(ignisMQManager.getTaskInitializer());
    }

    @Override
    public void initialize(Bootstrap<?> bootstrap) {
        //Nothing to initialise
    }

    protected abstract BaseStorage getStorage(T config);

    protected abstract String getClientId(T config);

    protected abstract String getFarmId(T config);

    protected abstract CuratorFramework getCuratorFramework();
}
