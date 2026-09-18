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

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.phonepe.aerospike.config.AerospikeConfiguration;
import com.phonepe.aerospike.config.AerospikeHost;
import com.phonepe.ignis.common.MessageHandler;
import com.phonepe.ignis.config.ConsoleConfiguration;
import com.phonepe.ignis.resource.IgnisMQResource;
import com.phonepe.ignis.exception.IgnisMQExceptionMapper;
import com.phonepe.ignis.common.TimeToLive;
import com.phonepe.ignis.common.TimeUnit;
import com.phonepe.ignis.request.CreateQueueRequest;
import com.phonepe.ignis.storage.AerospikeStorage;
import io.dropwizard.Configuration;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.jersey.setup.JerseyEnvironment;
import io.dropwizard.jetty.setup.ServletEnvironment;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import io.dropwizard.setup.Environment;
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature;
import io.appform.testcontainers.aerospike.AerospikeContainerConfiguration;
import io.appform.testcontainers.aerospike.container.AerospikeContainer;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.curator.framework.CuratorFramework;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.servlet.ServletRegistration;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The end-to-end wiring check A1 deliberately left owed, and D8 owes back.
 * <p>
 * Every other metric test in this repository asserts against a {@code SimpleMeterRegistry} held by
 * the test itself. That proves the meters are recorded; it proves nothing about whether a real
 * Dropwizard application ever sees them, which is the only thing an operator cares about. The bridge
 * is three collaborators deep - Micrometer meter, {@code HierarchicalNameMapper}, Dropwizard
 * {@code MetricRegistry} - and any of them could be misconfigured without a single unit test
 * noticing.
 * <p>
 * So this boots the actual bundle against a real Aerospike and asserts the names land in the real
 * {@code environment.metrics()}.
 */
class IgnisMQBundleMetricsTest {

    private static final String NAMESPACE = "ignismq";
    private static final AerospikeContainer CONTAINER;

    static {
        final AerospikeContainerConfiguration config = new AerospikeContainerConfiguration(
                true, "aerospike/aerospike-server:6.2.0.7", NAMESPACE, "localhost", 3000);
        config.setWaitTimeoutInSeconds(300L);
        CONTAINER = new AerospikeContainer(config);
        CONTAINER.start();
    }

    private IgnisMQBundle<AppConfig> bundle;

    @AfterEach
    void tearDown() {
        if (bundle != null && bundle.getIgnisMQManager() != null) {
            bundle.getIgnisMQManager().getTaskInitializer().stop();
            bundle.getIgnisMQManager().stop();
        }
    }

    @Test
    @DisplayName("booting the bundle puts ignisMQ's meters into the application's own MetricRegistry")
    void metersReachTheApplicationRegistry() throws Exception {
        final Environment environment = environment();
        bundle = createBundle();

        bundle.run(new AppConfig("SERVICE"), environment);
        final MetricRegistry metrics = environment.metrics();

        // Pool saturation is process-wide and published the moment the manager exists, with no
        // queue and no traffic required. If these are missing the workerThreads knob is untunable
        // in exactly the deployment shape that matters.
        assertTrue(names(metrics).stream().anyMatch(name -> name.startsWith("ignismq.pool.threads")),
                "pool meters must reach the application registry: " + names(metrics));
        assertTrue(names(metrics).stream().anyMatch(name -> name.contains("ignismq.pool.tasks.due")),
                "worker queue depth must reach the application registry");

        // And the queue-stats gauge the bundle owns.
        assertNotNull(metrics.getGauges().get(IgnisMQBundle.QUEUE_STATS_METRIC));
    }

    @Test
    @DisplayName("creating and using a queue publishes its per-queue meters through the bridge")
    void queueMetersReachTheApplicationRegistry() throws Exception {
        final Environment environment = environment();
        bundle = createBundle();
        bundle.run(new AppConfig("SERVICE"), environment);

        publishThrough("BUNDLE_METRIC_QUEUE");

        final Set<String> names = names(environment.metrics());
        assertTrue(names.stream().anyMatch(name -> name.contains("ignismq.queue.create")),
                "createQueue is one of the timers A1 deleted and D8 owes back: " + names);
        assertTrue(names.stream().anyMatch(name -> name.contains("ignismq.publish")),
                "the per-queue publish timer must survive the bridge: " + names);
        assertFalse(names.stream().anyMatch(name -> name.startsWith("commands.")),
                "the flat commands.* shape was replaced; leaving both would be two schemes at once");
    }

    /**
     * The switch, asserted where it actually matters. A flag that is only unit-tested against an
     * IgnisMetrics instance proves nothing about whether the bundle honours it.
     */
    @Test
    @DisplayName("disabling metrics leaves the application registry free of ignismq meters")
    void metricsCanBeDisabled() throws Exception {
        final Environment environment = environment();
        bundle = createBundle(false);

        bundle.run(new AppConfig("SERVICE"), environment);
        // Publishing is what registers Magazine's meters, so a bundle that merely booted would
        // satisfy the magazine assertion below without proving anything.
        publishThrough("BUNDLE_DISABLED_QUEUE");

        assertTrue(names(environment.metrics()).stream().noneMatch(name -> name.startsWith("ignismq.")),
                "nothing under ignismq.* may be published when instrumentation is off: "
                        + names(environment.metrics()));
        // Magazine is an implementation detail this bundle's users never configure, so the switch
        // has to reach it too. Otherwise "metrics off" still publishes a whole namespace.
        assertTrue(names(environment.metrics()).stream().noneMatch(name -> name.startsWith("magazine")),
                "nothing under magazine.* may be published either, because a caller cannot switch "
                        + "off a library it does not know it depends on: " + names(environment.metrics()));
        // The bundle's own queue-stats gauge is not part of D8's instrumentation and stays.
        assertNotNull(environment.metrics().getGauges().get(IgnisMQBundle.QUEUE_STATS_METRIC));
    }

    /**
     * The console is part of the bundle's wiring, not something a user assembles, so the wiring is
     * what is worth asserting: a resource and the role feature that closes its mutations.
     */
    @Test
    @DisplayName("booting the bundle mounts the console and the feature that enforces its role")
    void theConsoleIsMountedByDefault() throws Exception {
        final Environment environment = environment();
        bundle = createBundle();

        bundle.run(new AppConfig("SERVICE"), environment);

        Mockito.verify(environment.jersey()).register(Mockito.any(IgnisMQResource.class));
        // Without this feature @RolesAllowed is inert and every mutation would be wide open.
        Mockito.verify(environment.jersey()).register(RolesAllowedDynamicFeature.class);
    }

    @Test
    @DisplayName("disabling the console registers neither the resource nor the dashboard")
    void theConsoleCanBeDisabledEntirely() throws Exception {
        final Environment environment = environment();
        bundle = createBundle(true, ConsoleConfiguration.builder().enabled(false).build());

        bundle.run(new AppConfig("SERVICE"), environment);

        Mockito.verify(environment.jersey(), Mockito.never()).register(Mockito.any(IgnisMQResource.class));
        Mockito.verify(environment.servlets(), Mockito.never())
                .addServlet(Mockito.anyString(), Mockito.any(javax.servlet.Servlet.class));
    }

    /**
     * The mapper is the library's, not the console's: an application that turns the console off still
     * calls ignisMQ from its own resources, and its callers are the ones seeing a 500 today.
     */
    @Test
    @DisplayName("the exception mapper is registered even with the console disabled")
    void theExceptionMapperIsRegisteredIndependentlyOfTheConsole() throws Exception {
        final Environment environment = environment();
        bundle = createBundle(true, ConsoleConfiguration.builder().enabled(false).build());

        bundle.run(new AppConfig("SERVICE"), environment);

        Mockito.verify(environment.jersey()).register(Mockito.any(IgnisMQExceptionMapper.class));
    }

    /**
     * The API without the page: a deployment that will not serve static assets still gets the data.
     */
    @Test
    @DisplayName("disabling only the dashboard keeps the read and action endpoints")
    void theDashboardCanBeDisabledWithoutDisablingTheApi() throws Exception {
        final Environment environment = environment();
        bundle = createBundle(true, ConsoleConfiguration.builder().dashboardEnabled(false).build());

        bundle.run(new AppConfig("SERVICE"), environment);

        Mockito.verify(environment.jersey()).register(Mockito.any(IgnisMQResource.class));
        Mockito.verify(environment.servlets(), Mockito.never())
                .addServlet(Mockito.anyString(), Mockito.any(javax.servlet.Servlet.class));
    }

    private void publishThrough(final String queueName) throws Exception {
        final Map<String, Map.Entry<Class, MessageHandler>> handlers = new HashMap<>();
        handlers.put("handler", new AbstractMap.SimpleEntry<>(String.class, new EchoHandler()));
        bundle.getIgnisMQManager().initialiseMessageHandlers(handlers);
        bundle.getIgnisMQManager().createQueue(CreateQueueRequest.builder()
                .name(queueName)
                .messageHandlerType("handler")
                .concurrency(1)
                .shards(1)
                .messageExpiry(TimeToLive.builder().duration(5).timeUnit(TimeUnit.MINUTE).build())
                .queueExpiry(TimeToLive.builder().duration(5).timeUnit(TimeUnit.MINUTE).build())
                .build());
        bundle.getIgnisMQManager().getQueue(queueName).publish("hello");
    }

    private static Set<String> names(final MetricRegistry metrics) {
        return metrics.getNames();
    }

    private static Environment environment() {
        final Environment environment = Mockito.mock(Environment.class);
        final ObjectMapper mapper = Jackson.newObjectMapper();
        Mockito.when(environment.metrics()).thenReturn(new MetricRegistry());
        Mockito.when(environment.getObjectMapper()).thenReturn(mapper);
        Mockito.when(environment.lifecycle())
                .thenReturn(new LifecycleEnvironment(new MetricRegistry()));
        Mockito.when(environment.jersey()).thenReturn(Mockito.mock(JerseyEnvironment.class));
        final ServletEnvironment servlets = Mockito.mock(ServletEnvironment.class);
        Mockito.when(servlets.addServlet(Mockito.anyString(), Mockito.any(javax.servlet.Servlet.class)))
                .thenReturn(Mockito.mock(ServletRegistration.Dynamic.class));
        Mockito.when(environment.servlets()).thenReturn(servlets);
        return environment;
    }

    private IgnisMQBundle<AppConfig> createBundle() {
        return createBundle(true, ConsoleConfiguration.defaults());
    }

    private IgnisMQBundle<AppConfig> createBundle(final boolean metricsEnabled) {
        return createBundle(metricsEnabled, ConsoleConfiguration.defaults());
    }

    private IgnisMQBundle<AppConfig> createBundle(final boolean metricsEnabled,
                                                  final ConsoleConfiguration console) {
        return new IgnisMQBundle<>() {
            @Override
            protected IgnisMQContext context(final AppConfig config) {
                return IgnisMQContext.builder()
                        .clientId(config.getServiceName())
                        .farmId("NB6")
                        .storage(new AerospikeStorage(AerospikeConfiguration.builder()
                                .hosts(List.of(AerospikeHost.builder()
                                        .host(CONTAINER.getHost())
                                        .port(CONTAINER.getConnectionPort())
                                        .build()))
                                .retries(3).sleepBetweenRetries(100).socketTimeout(3000).totalTimeout(5000)
                                .maxConnectionsPerNode(100).threadPoolSize(4)
                                .scanMaxConcurrentNodes(5).batchMaxConcurrentThreads(5)
                                .build(), NAMESPACE))
                        .curatorFramework(Mockito.mock(CuratorFramework.class))
                        .settings(IgnisMQSettings.builder().metricsEnabled(metricsEnabled).build())
                        .console(console)
                        .build();
            }
        };
    }

    private static final class EchoHandler implements MessageHandler<String> {
        @Override
        public Set<Class<?>> getIgnorableExceptions() {
            return Set.of();
        }

        @Override
        public boolean handle(final String message) {
            return true;
        }

        @Override
        public boolean handle(final List<String> messages) {
            return true;
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class AppConfig extends Configuration {
        private String serviceName;
    }
}
