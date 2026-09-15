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

import com.codahale.metrics.CachedGauge;
import com.phonepe.ignis.metric.DropwizardMagazineMetrics;
import com.phonepe.ignis.metric.QueueStat;
import com.phonepe.ignis.utils.Constants;
import com.phonepe.ignis.storage.BaseStorage;
import io.dropwizard.Configuration;
import io.dropwizard.ConfiguredBundle;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author shantanu.tiwari
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public abstract class IgnisMQBundle<T extends Configuration> implements ConfiguredBundle<T> {
    @Getter
    private IgnisMQManager ignisMQManager;

    static final String QUEUE_STATS_METRIC = "ignis.queue.stats";
    private static final int QUEUE_STATS_TTL_MINUTES = 3;

    @Override
    public void run(T config, Environment environment) throws Exception {
        final MeterRegistry meterRegistry = DropwizardMagazineMetrics.bridgedTo(environment.metrics());
        this.ignisMQManager = new IgnisMQManager(getClientId(config), getStorage(config),
                environment.getObjectMapper(), meterRegistry, getCuratorFramework(),
                getFarmId(config), getWorkerThreads(config));

        // Cached because each load issues metadata reads per queue against the storage backend.
        environment.metrics().register(QUEUE_STATS_METRIC,
                new CachedGauge<List<QueueStat>>(QUEUE_STATS_TTL_MINUTES, TimeUnit.MINUTES) {
                    @Override
                    protected List<QueueStat> loadValue() {
                        return ignisMQManager.getQueueStats();
                    }
                });

        environment.lifecycle().manage(new Managed() {
            @Override
            public void start() throws Exception {
                ignisMQManager.start();
                ignisMQManager.getTaskInitializer().start();
            }

            @Override
            public void stop() {
                // Jetty invokes stop even when start failed part way through, so every step
                // here has to tolerate never having been started.
                ignisMQManager.getTaskInitializer().stop();
                ignisMQManager.stop();
                meterRegistry.close();
            }
        });
    }

    @Override
    public void initialize(Bootstrap<?> bootstrap) {
        //Nothing to initialise
    }

    protected abstract BaseStorage getStorage(T config);

    protected abstract String getClientId(T config);

    protected abstract String getFarmId(T config);

    protected abstract CuratorFramework getCuratorFramework();

    /**
     * Ceiling on threads running consumers and shovels, and on those running message handlers.
     * Override to size it from application configuration.
     * <p>
     * {@code concurrency} on a queue is a target, not a guarantee: with more registered consumers
     * than threads they time-share, and no queue gets its stated parallelism.
     */
    protected int getWorkerThreads(T config) {
        return Constants.DEFAULT_WORKER_THREADS;
    }
}
