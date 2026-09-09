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

package com.phonepe.ignis.metric;

import com.codahale.metrics.MetricRegistry;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.config.NamingConvention;
import io.micrometer.core.instrument.dropwizard.DropwizardConfig;
import io.micrometer.core.instrument.dropwizard.DropwizardMeterRegistry;
import io.micrometer.core.instrument.util.HierarchicalNameMapper;

/**
 * Bridges the Micrometer meters recorded by ignismq-core and Magazine into a Dropwizard
 * {@link MetricRegistry}.
 * <p>
 * Dropwizard metrics are hierarchical and have no notion of tags, so {@link HierarchicalNameMapper}
 * flattens each meter's tags into its name: {@code magazine.fire.outcomes} tagged
 * {@code magazine=orders, outcome=delivered} is published as
 * {@code magazine.fire.outcomes.magazine.orders.outcome.delivered}. Magazine's tag values are a
 * queue name crossed with a closed enum, so the resulting series count stays bounded — which
 * matters, because a Dropwizard registry never evicts a name once it has been created.
 * <p>
 * Untagged meters, including ignisMQ's own {@code commands.*} timers, keep their names verbatim,
 * so existing dashboards built against them continue to resolve.
 */
public final class DropwizardMagazineMetrics {
    private DropwizardMagazineMetrics() {
    }

    public static MeterRegistry bridgedTo(final MetricRegistry metrics) {
        final DropwizardConfig config = new DropwizardConfig() {
            @Override
            public String prefix() {
                // Namespaces the property keys handed to get(), not the emitted metric names.
                return "ignismq";
            }

            @Override
            public String get(final String key) {
                // No external configuration source; every setting stays at its default.
                return null;
            }
        };
        final MeterRegistry registry = new DropwizardMeterRegistry(
                config, metrics, HierarchicalNameMapper.DEFAULT, Clock.SYSTEM) {
            @Override
            protected Double nullGaugeValue() {
                // Dropwizard's JSON reporters render a null gauge rather than inventing a zero,
                // which would otherwise be indistinguishable from a real measurement of zero.
                return null;
            }
        };
        registry.config().namingConvention(NamingConvention.dot);
        return registry;
    }
}
