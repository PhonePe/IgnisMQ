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

package com.phonepe.ignis.demo;

import com.phonepe.ignis.metric.IgnisMetrics;
import com.phonepe.ignis.response.ActionResult;
import com.phonepe.ignis.response.QueueDetail;
import com.phonepe.ignis.response.QueueSummary;
import io.dropwizard.testing.ConfigOverride;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.ws.rs.client.Client;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
    void theDashboardCarriesItsOwnStylingRatherThanLinkingToIt() {
        final String page = client().target(url("/ignisConsole/")).request()
                .get().readEntity(String.class);

        assertTrue(page.contains("<style>"), "the console must carry its styling inline");
        assertFalse(page.contains("href=\"style.css\""),
                "a relative stylesheet href is what broke the un-slashed URL; it must not come back");
    }

    @Test
    void theQueueFilterCanActuallyHideAQueue() {
        final String page = client().target(url("/ignisConsole/")).request()
                .get().readEntity(String.class);

        assertTrue(page.contains("[hidden] { display:none !important; }"),
                "hiding by property needs a rule that outranks 'nav button { display:flex }'");
        assertTrue(page.contains("No queue matches that filter."),
                "and filtering everything out must say so rather than showing a blank panel");
    }

    @Test
    void theMetricsViewFormatsTimersInTheUnitTheyArriveIn() {
        final String page = client().target(url("/ignisConsole/")).request()
                .get().readEntity(String.class);

        assertFalse(page.contains("Math.round(seconds * 100000) / 100"),
                "the seconds-to-millis conversion inflated every timer by 1000x and must not return");
        assertTrue(page.contains("Mean (since start)"), "the mean must say it never decays");
        assertTrue(page.contains("Max (2 min)"), "and the max must say it does");
    }

    @Test
    void hintMarkersUseTheConsolesOwnTooltipAndNotTheBrowsers() {
        final String page = client().target(url("/ignisConsole/")).request()
                .get().readEntity(String.class);

        // Every tag, then filter - not "<button...class=hint...>", which would stop seeing a marker
        // the moment one regressed to a span and leave the loop asserting nothing about it.
        final Matcher tags = Pattern.compile("<[^<>]+>").matcher(page);
        int markers = 0;
        while (tags.find()) {
            final String tag = tags.group();
            if (!tag.contains("class=\"hint\"")) {
                continue;
            }
            markers++;
            assertTrue(tag.startsWith("<button"),
                    "a marker must be a button, not a span made focusable with tabindex: " + tag);
            // A title attribute reinstates the browser's own tooltip and its ~1s delay, which is
            // what made these useless and cannot be shortened from a page.
            assertFalse(tag.contains("title="), "the description must not ride on title: " + tag);
            assertTrue(tag.contains("data-hint="), "the description must be on data-hint: " + tag);
            assertTrue(tag.contains("aria-label="), "the marker needs an accessible name: " + tag);
            assertFalse(tag.contains("tabindex"), "a button does not need tabindex: " + tag);
        }
        assertTrue(markers > 0, "the page must render hint markers for this to mean anything");
        assertTrue(page.contains("id=\"tip\""), "and data-hint needs the tooltip element to be painted into");
    }

    @Test
    void noTwoStyleRulesDefineTheSameBareClass() {
        final String page = client().target(url("/ignisConsole/")).request()
                .get().readEntity(String.class);

        final Matcher style = Pattern.compile("<style>(.*?)</style>", Pattern.DOTALL).matcher(page);
        assertTrue(style.find(), "the console must carry its styling inline");
        final String css = style.group(1).replaceAll("(?s)/\\*.*?\\*/", "");

        final Map<String, Integer> definitions = new LinkedHashMap<>();
        // [ \t] rather than \s: \s matches newlines, which combined with a multiline ^ lets the
        // engine retry the same span from every line and backtrack quadratically.
        final Matcher rule = Pattern.compile("(?m)^[ \\t]*([^{}@\\n][^{}\\n]*)\\{").matcher(css);
        while (rule.find()) {
            for (final String selector : rule.group(1).split(",")) {
                final String bare = selector.trim();
                if (bare.matches("\\.[a-z][a-z0-9-]*")) {
                    definitions.merge(bare, 1, Integer::sum);
                }
            }
        }

        assertFalse(definitions.isEmpty(), "the stylesheet must have been parsed");
        assertEquals(List.of(), definitions.entrySet().stream()
                        .filter(entry -> entry.getValue() > 1).map(Map.Entry::getKey).sorted().toList(),
                "a class defined twice restyles whatever already used that name");
    }

    @Test
    void everyMeterCarriesAnExplanationAndTheViewStatesItsUnits() {
        final String page = client().target(url("/ignisConsole/")).request()
                .get().readEntity(String.class);

        assertTrue(page.contains("const METER_INFO"), "meters must carry explanations");
        // Read off IgnisMetrics rather than listed here, so a meter added there without a
        // description fails this. A hand-written list would simply not mention the new meter.
        for (final String meter : meterNames()) {
            assertTrue(page.contains("'" + meter + "':"), meter + " has no description");
        }
        assertTrue(page.contains("magazine.load.latency':"), "Magazine's meters need them too");
        assertTrue(page.contains("milliseconds"), "the view must say what unit timers are in");
    }

    /** Every meter name IgnisMetrics declares. The tag keys and values share the class but not the prefix. */
    private static List<String> meterNames() {
        final List<String> names = Arrays.stream(IgnisMetrics.class.getDeclaredFields())
                .filter(field -> Modifier.isStatic(field.getModifiers()) && field.getType() == String.class)
                .map(field -> {
                    try {
                        return (String) field.get(null);
                    } catch (IllegalAccessException e) {
                        throw new IllegalStateException(e);
                    }
                })
                .filter(value -> value.startsWith("ignismq."))
                .toList();
        assertFalse(names.isEmpty(), "the meter names must have been read off IgnisMetrics");
        return names;
    }

    @Test
    void theTokenPanelLetsTheOperatorSetTheFullAuthorizationHeader() {
        final String page = client().target(url("/ignisConsole/")).request()
                .get().readEntity(String.class);

        assertTrue(page.contains("id=\"token-input\""), "there must be a field for the Authorization header value");
        assertTrue(page.contains("ignismq-console-authorization"), "and it must be remembered verbatim");
        assertFalse(page.contains("id=\"token-scheme\""),
                "there must be no separate scheme field - the operator supplies the full header value");
        assertFalse(page.contains("`Bearer ${bearer}`"),
                "the scheme must no longer be hardcoded into the Authorization header");
    }

    @Test
    void theDashboardIsStyledEvenWithoutTheTrailingSlash() {
        final Response response = client().target(url("/ignisConsole")).request().get();

        assertEquals(200, response.getStatus());
        final String page = response.readEntity(String.class);
        assertTrue(page.contains("ignisMQ Console"), "the un-slashed URL must still serve the console");
        assertTrue(page.contains("<style>"),
                "and it must arrive styled, with nothing left to fetch by a relative URL");
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
     * negative half is in {@code IgnisMQResourceTest}, where no role is granted at all.
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
