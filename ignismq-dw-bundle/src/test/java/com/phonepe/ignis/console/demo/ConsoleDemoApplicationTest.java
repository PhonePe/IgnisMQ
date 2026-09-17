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

package com.phonepe.ignis.console.demo;

import com.phonepe.ignis.console.response.ActionResult;
import com.phonepe.ignis.console.response.QueueDetail;
import com.phonepe.ignis.console.response.QueueSummary;
import io.dropwizard.testing.ConfigOverride;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.ws.rs.client.Client;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Boots the demo application for real, which is the only thing that exercises the parts a mocked
 * {@code Environment} cannot: that the dashboard is actually served from the jar, and that the role
 * check runs inside a real Jersey stack rather than a {@code ResourceExtension}.
 */
@ExtendWith(DropwizardExtensionsSupport.class)
class ConsoleDemoApplicationTest {

    private static final DropwizardAppExtension<ConsoleDemoConfiguration> APP = new DropwizardAppExtension<>(
            ConsoleDemoApplication.class, (String) null,
            ConfigOverride.config("server.applicationConnectors[0].port", "0"),
            ConfigOverride.config("server.adminConnectors[0].port", "0"));

    private String url(final String path) {
        return "http://localhost:" + APP.getLocalPort() + path;
    }

    private Client client() {
        return APP.client();
    }

    /**
     * The asset wiring, which every other test could only assert as "addServlet was called". If the
     * resource path, the mount path or the index file name are wrong, this is what says so.
     */
    @Test
    void theDashboardIsServedFromTheJar() {
        final Response response = client().target(url("/ignisConsole/")).request().get();

        assertEquals(200, response.getStatus());
        final String page = response.readEntity(String.class);
        assertTrue(page.contains("ignisMQ Console"), "the index page must be the console's own");
        assertTrue(page.contains("/ignismq/v1"), "and it must point at the API it reads");
    }

    @Test
    void theStylesheetTheDashboardAsksForIsServedToo() {
        assertEquals(200, client().target(url("/ignisConsole/style.css")).request().get().getStatus());
    }

    @Test
    void theInventoryShowsActiveAndDeactivatedQueues() {
        final List<QueueSummary> queues = client().target(url("/ignismq/v1/queues"))
                .request().get(new GenericType<>() {
                });

        assertEquals(3, queues.size());
        assertTrue(summary(queues, "order-events").active());
        assertTrue(summary(queues, "order-events").heldHere());
        assertTrue(!summary(queues, "legacy-imports").active());
        // Active but not yet adopted here: a real instance reaches this state only between a queue
        // being created elsewhere and this instance's next refresh, or when adoption failed.
        assertTrue(summary(queues, "payment-retries").active());
        assertTrue(!summary(queues, "payment-retries").heldHere());
    }

    /**
     * Publishing picks a shard at random, so an even spread is the expected shape and the demo data
     * has to look like it. The one shard drifting behind is what the view is actually for.
     */
    @Test
    void theShardViewShowsAnEvenSpreadWithOneShardDrifting() {
        final QueueDetail detail = client().target(url("/ignismq/v1/queues/order-events"))
                .request().get(QueueDetail.class);

        assertEquals(8, detail.shards().size());
        final long total = detail.shards().stream().mapToLong(shard -> shard.getPending()).sum();
        final long busiest = detail.shards().stream().mapToLong(shard -> shard.getPending()).max().orElseThrow();
        assertTrue(busiest * 2 < total, "random assignment means no shard should hold half the backlog");
        assertTrue(busiest > total / 8, "and the demo must still show one shard visibly behind the rest");
    }

    @Test
    void aQueueThisInstanceHasNotAdoptedCarriesNoDepth() {
        final QueueDetail detail = client().target(url("/ignismq/v1/queues/payment-retries"))
                .request().get(QueueDetail.class);

        assertNotNull(detail.queue());
        assertNull(detail.depth());
        assertNull(detail.shards());
    }

    /**
     * The role check, end to end. The demo grants the role, so this is the positive half; the
     * negative half is in {@code ConsoleResourceTest}, where no role is granted at all.
     */
    @Test
    void anActionRunsWhenTheRoleIsGrantedAndChangesWhatTheConsoleThenShows() {
        final ActionResult result = client().target(url("/ignismq/v1/queues/order-events/consumers"))
                .queryParam("delta", 3).request().post(null, ActionResult.class);

        assertEquals("instance", result.scope());
        final QueueDetail detail = client().target(url("/ignismq/v1/queues/order-events"))
                .request().get(QueueDetail.class);
        assertEquals(7, detail.instance().consumers(), "the demo starts at four consumers");
    }

    private static QueueSummary summary(final List<QueueSummary> queues, final String name) {
        return queues.stream().filter(summary -> summary.name().equals(name)).findFirst().orElseThrow();
    }
}
