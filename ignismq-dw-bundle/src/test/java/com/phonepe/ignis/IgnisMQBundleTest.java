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

import com.phonepe.ignis.storage.AerospikeStorage;
import com.phonepe.ignis.storage.BaseStorage;
import com.phonepe.aerospike.config.AerospikeConfiguration;
import io.dropwizard.Configuration;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.curator.framework.CuratorFramework;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;

import static org.mockito.Mockito.*;

public class IgnisMQBundleTest {

    @Test
    public void bundleTest() {
        IgnisMQBundle<AppConfig> bundle = createBundle();
        AppConfig appConfig = new AppConfig("SERVICE");
        Assert.assertEquals("SERVICE", bundle.getClientId(appConfig));
    }

    @Test
    public void testGetStorage() {
        IgnisMQBundle<AppConfig> bundle = createBundle();
        AppConfig appConfig = new AppConfig("SERVICE");
        BaseStorage storage = bundle.getStorage(appConfig);
        Assert.assertNotNull(storage);
    }

    @Test
    public void testGetFarmId() {
        IgnisMQBundle<AppConfig> bundle = createBundle();
        AppConfig appConfig = new AppConfig("SERVICE");
        Assert.assertEquals("NB6", bundle.getFarmId(appConfig));
    }

    @Test
    public void testGetCuratorFramework() {
        IgnisMQBundle<AppConfig> bundle = createBundle();
        Assert.assertNotNull(bundle.getCuratorFramework());
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
        Assert.assertNull(bundle.getIgnisMQManager());
    }

    private IgnisMQBundle<AppConfig> createBundle() {
        return new IgnisMQBundle<AppConfig>() {
            @Override
            protected AerospikeStorage getStorage(AppConfig config) {
                return new AerospikeStorage(
                        AerospikeConfiguration.builder()
                                .hosts(Collections.emptyList())
                                .user("USER")
                                .password("PASS")
                                .build(),
                        "namespace"
                );
            }

            @Override
            protected String getClientId(AppConfig config) {
                return config.getServiceName();
            }

            @Override
            protected String getFarmId(AppConfig config) {
                return "NB6";
            }

            @Override
            protected CuratorFramework getCuratorFramework() {
                return Mockito.mock(CuratorFramework.class);
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
