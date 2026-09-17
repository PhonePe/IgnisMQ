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

import com.phonepe.aerospike.config.AerospikeConfiguration;
import com.phonepe.ignis.storage.AerospikeStorage;
import io.dropwizard.Configuration;
import io.dropwizard.setup.Bootstrap;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.curator.framework.CuratorFramework;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;

public class IgnisMQBundleTest {

    @Test
    public void testTheContextCarriesEveryValueTheManagerNeeds() {
        IgnisMQBundle<AppConfig> bundle = createBundle();
        AppConfig appConfig = new AppConfig("SERVICE");

        IgnisMQContext context = bundle.context(appConfig);

        Assertions.assertEquals("SERVICE", context.getClientId());
        Assertions.assertEquals("NB6", context.getFarmId());
        Assertions.assertNotNull(context.getStorage());
        Assertions.assertNotNull(context.getCuratorFramework());
    }

    /** Settings are the one optional input, so omitting them must not mean null. */
    @Test
    public void testAContextWithoutSettingsDefaultsThemRatherThanLeavingThemNull() {
        IgnisMQContext context = IgnisMQContext.builder()
                .clientId("SERVICE")
                .farmId("NB6")
                .storage(storage())
                .curatorFramework(Mockito.mock(CuratorFramework.class))
                .build();

        Assertions.assertEquals(IgnisMQSettings.defaults(), context.getSettings());
    }

    /**
     * The required inputs lost their compile-time enforcement when the abstract methods collapsed
     * into one context, so the builder has to reject them at startup instead.
     */
    @Test
    public void testAContextMissingARequiredValueIsRejected() {
        Assertions.assertThrows(NullPointerException.class, () -> IgnisMQContext.builder()
                .farmId("NB6")
                .storage(storage())
                .curatorFramework(Mockito.mock(CuratorFramework.class))
                .build());
    }

    @Test
    public void testInitialize() {
        IgnisMQBundle<AppConfig> bundle = createBundle();
        Bootstrap<?> bootstrap = Mockito.mock(Bootstrap.class);
        // Should not throw
        bundle.initialize(bootstrap);
    }

    @Test
    public void testGetIgnisMQManagerBeforeRun() {
        IgnisMQBundle<AppConfig> bundle = createBundle();
        Assertions.assertNull(bundle.getIgnisMQManager());
    }

    private static AerospikeStorage storage() {
        return new AerospikeStorage(
                AerospikeConfiguration.builder()
                        .hosts(Collections.emptyList())
                        .user("USER")
                        .password("PASS")
                        .build(),
                "namespace"
        );
    }

    private IgnisMQBundle<AppConfig> createBundle() {
        return new IgnisMQBundle<AppConfig>() {
            @Override
            protected IgnisMQContext context(AppConfig config) {
                return IgnisMQContext.builder()
                        .clientId(config.getServiceName())
                        .farmId("NB6")
                        .storage(storage())
                        .curatorFramework(Mockito.mock(CuratorFramework.class))
                        .build();
            }
        };
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private class AppConfig extends Configuration {
        private String serviceName;
    }
}
