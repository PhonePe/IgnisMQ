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
import com.phonepe.ignis.config.ConsoleConfiguration;
import com.phonepe.ignis.resource.IgnisMQResource;
import com.phonepe.ignis.service.IgnisMQService;
import com.phonepe.ignis.exception.IgnisMQExceptionMapper;
import com.phonepe.ignis.metric.DropwizardMagazineMetrics;
import com.phonepe.ignis.metric.QueueStat;
import io.dropwizard.Configuration;
import io.dropwizard.ConfiguredBundle;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

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
    private static final String DASHBOARD_PATH = "/ignisConsole";

    @Override
    public void run(T config, Environment environment) throws Exception {
        final IgnisMQContext context = context(config);
        final MeterRegistry meterRegistry = DropwizardMagazineMetrics.bridgedTo(environment.metrics());
        this.ignisMQManager = new IgnisMQManager(context.getClientId(), context.getStorage(),
                environment.getObjectMapper(), meterRegistry, context.getCuratorFramework(),
                context.getFarmId(), context.getSettings());

        // Cached because each load issues metadata reads per queue against the storage backend.
        environment.metrics().register(QUEUE_STATS_METRIC,
                new CachedGauge<List<QueueStat>>(QUEUE_STATS_TTL_MINUTES, TimeUnit.MINUTES) {
                    @Override
                    protected List<QueueStat> loadValue() {
                        return ignisMQManager.getQueueStats();
                    }
                });

        environment.jersey().register(new IgnisMQExceptionMapper());

        registerConsole(config, environment, context, meterRegistry);

        environment.lifecycle().manage(new Managed() {
            @Override
            public void start() throws Exception {
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

    /**
     * The console is two views over two sources - storage for the cluster, this process's own
     * registry for the instance - and it never tries to speak for instances it cannot see. Rolling
     * the instance view up across a deployment is the metrics backend's job.
     */
    private void registerConsole(final T config, final Environment environment, final IgnisMQContext context,
                                 final MeterRegistry meterRegistry) {
        final ConsoleConfiguration console = context.getConsole();
        if (!console.isEnabled()) {
            return;
        }
        environment.jersey().register(RolesAllowedDynamicFeature.class);
        environment.jersey().register(new IgnisMQResource(new IgnisMQService(ignisMQManager,
                context.getClientId(), context.getFarmId(), context.getSettings(), meterRegistry,
                console.getCacheSeconds())));
        log.info("ignisMQ console mounted. Actions require role '{}' and deactivation requires '{}'; "
                + "register your own authentication and grant them, or they stay closed.",
                IgnisMQResource.OPERATE_ROLE, IgnisMQResource.DEACTIVATE_ROLE);
        if (console.isDashboardEnabled()) {
            new AssetsBundle("/ignisAssets/", DASHBOARD_PATH, "ignisIndex.html", "ignisAssets")
                    .run(config, environment);
        }
    }

    @Override
    public void initialize(Bootstrap<?> bootstrap) {
        //Nothing to initialise
    }

    protected abstract IgnisMQContext context(T config);
}
