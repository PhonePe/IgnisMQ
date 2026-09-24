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

package com.phonepe.ignis.resource;

import com.phonepe.ignis.IQueue;
import com.phonepe.ignis.IgnisMQManager;
import com.phonepe.ignis.IgnisMQSettings;
import com.phonepe.ignis.MagazineQueue;
import com.phonepe.ignis.common.QueueMetaData;
import com.phonepe.ignis.common.ShardDepth;
import com.phonepe.ignis.entity.QueueEntity;
import com.phonepe.ignis.exception.ErrorCode;
import com.phonepe.ignis.exception.IgnisMQException;
import com.phonepe.ignis.exception.IgnisMQExceptionMapper;
import com.phonepe.ignis.request.ShovelConfig;
import com.phonepe.ignis.response.*;
import com.phonepe.ignis.service.IgnisMQService;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * The console's contract is that the two views never borrow each other's authority: a queue this
 * process does not serve reports its stored configuration and nothing else, and every mutation is
 * closed without the operator role.
 */
@ExtendWith(DropwizardExtensionsSupport.class)
class IgnisMQResourceTest {

    private static final String ORDERS = "orders";
    private static final String ELSEWHERE = "payments";

    private final IgnisMQManager manager = mock(IgnisMQManager.class);
    private final MagazineQueue<String> queue = mock(MagazineQueue.class);
    private final Map<String, IQueue<?>> localQueues = new ConcurrentHashMap<>();

    private final MeterRegistry meterRegistry = new SimpleMeterRegistry();

    private final ResourceExtension readOnly = resources();
    private final ResourceExtension asOperator = resources(IgnisMQResource.OPERATE_ROLE);
    private final ResourceExtension asDeactivator = resources(IgnisMQResource.DEACTIVATE_ROLE);
    private final ResourceExtension asRolelessButAuthenticated = resources(new String[0], true);

    private ResourceExtension resources(final String... roles) {
        return resources(roles, roles.length > 0);
    }

    /**
     * {@code authenticated} is separate from the roles so a caller who authenticated successfully but
     * holds nothing can be expressed - the case the console most needs to tell apart.
     */
    private ResourceExtension resources(final String[] roles, final boolean authenticated) {
        final ResourceExtension.Builder builder = ResourceExtension.builder()
                .addResource(() -> new IgnisMQResource(new IgnisMQService(manager, "billing", "farm-1",
                        IgnisMQSettings.defaults(), meterRegistry, 0)))
                .addProvider(RolesAllowedDynamicFeature.class)
                .addProvider(new IgnisMQExceptionMapper());
        if (authenticated) {
            builder.addProvider(new GrantRoleFilter(roles));
        }
        return builder.build();
    }

    @BeforeEach
    void wireManager() {
        reset(manager, queue);
        localQueues.clear();
        localQueues.put(ORDERS, queue);
        when(manager.getAllQueues()).thenReturn(localQueues);
        when(manager.getStoredQueues(true)).thenReturn(Map.of(ORDERS, entity(true, 8), ELSEWHERE, entity(true, 4)));
        when(manager.getStoredQueues(false)).thenReturn(Map.of("retired", entity(false, 2)));
        when(manager.getStoredQueue(anyString())).thenAnswer(invocation -> {
            final String name = invocation.getArgument(0);
            return switch (name) {
                case ORDERS -> Optional.of(entity(true, 8));
                case ELSEWHERE -> Optional.of(entity(true, 4));
                case "retired" -> Optional.of(entity(false, 2));
                default -> Optional.empty();
            };
        });
        when(queue.getMetaData()).thenReturn(QueueMetaData.builder()
                .published(120).consumed(100).sidelined(5).shovelled(2).build());
        when(queue.getShardDepths()).thenReturn(List.of(
                depth("SHARD_0", 100, 90), depth("SHARD_1", 20, 10)));
        when(queue.getNoOfConsumers()).thenReturn(4);
        when(queue.getNoOfShovelConsumers()).thenReturn(1);
        when(queue.getShovelConfig()).thenReturn(ShovelConfig.builder()
                .concurrency(2).timeIntervalInSecs(600).build());
    }

    @Test
    void theInventoryListsEveryQueueInStorageWhetherActiveOrNot() {
        final List<QueueSummary> queues = readOnly.target("/ignismq/v1/queues")
                .request().get(new GenericType<>() {
                });

        assertEquals(List.of(ORDERS, ELSEWHERE, "retired"), queues.stream().map(QueueSummary::name).toList());
        assertTrue(queues.stream().filter(summary -> summary.name().equals("retired")).noneMatch(QueueSummary::active));
    }

    /**
     * The one piece of instance truth in the cluster view, and it is there because it decides which
     * of the other views can answer at all.
     */
    @Test
    void theInventoryMarksOnlyTheQueuesThisInstanceServes() {
        final List<QueueSummary> queues = readOnly.target("/ignismq/v1/queues")
                .request().get(new GenericType<>() {
                });

        assertTrue(summary(queues, ORDERS).heldHere());
        assertFalse(summary(queues, ELSEWHERE).heldHere());
    }

    @Test
    void theInventoryCarriesTheStoredConfigurationRatherThanDefaults() {
        final QueueSummary summary = summary(readOnly.target("/ignismq/v1/queues")
                .request().get(new GenericType<>() {
                }), ORDERS);

        assertEquals(8, summary.shards());
        assertEquals(6, summary.concurrency());
        assertEquals(1_200_000L, summary.sweepDurationMillis());
        assertEquals(600_000L, summary.handlerTimeoutMillis());
        assertEquals("orderHandler", summary.messageHandlerType());
    }

    /**
     * {@code -1} is how the absence of a shovel is stored, and reporting it verbatim would be read
     * as a concurrency of minus one.
     */
    @Test
    void anAbsentShovelConfigurationIsReportedAsAbsentRatherThanAsMinusOne() {
        final QueueSummary summary = summary(readOnly.target("/ignismq/v1/queues")
                .request().get(new GenericType<>() {
                }), ORDERS);

        assertNull(summary.shovelConcurrency());
        assertNull(summary.shovelIntervalSeconds());
    }

    @Test
    void aQueueServedHereReportsItsDepthItsShardsAndItsConsumers() {
        final QueueDetail detail = readOnly.target("/ignismq/v1/queues/" + ORDERS)
                .request().get(QueueDetail.class);

        assertEquals(120, detail.depth().published());
        assertEquals(20, detail.depth().unconsumed());
        assertEquals(List.of("SHARD_0", "SHARD_1"), detail.shards().stream().map(ShardDepth::getShard).toList());
        assertEquals(4, detail.instance().consumers());
        assertEquals(1, detail.instance().shovels());
    }

    /**
     * The conflation this console exists to avoid. Depth is read through live magazines, which only
     * an instance serving the queue has - so a queue served elsewhere reports its cluster-truthful
     * configuration and explicitly nothing else, rather than zeros that would read as an idle queue.
     */
    @Test
    void aQueueServedElsewhereReportsItsConfigurationAndNoDepthAtAll() {
        final QueueDetail detail = readOnly.target("/ignismq/v1/queues/" + ELSEWHERE)
                .request().get(QueueDetail.class);

        assertEquals(4, detail.queue().shards());
        assertFalse(detail.queue().heldHere());
        assertNull(detail.depth());
        assertNull(detail.shards());
        assertNull(detail.instance());
    }

    @Test
    void anUnknownQueueIsNotFound() {
        assertEquals(Response.Status.NOT_FOUND.getStatusCode(),
                readOnly.target("/ignismq/v1/queues/ghost").request().get().getStatus());
    }

    @Test
    void thePerShardViewReportsEveryShardSeparately() {
        final List<ShardDepth> shards = readOnly.target("/ignismq/v1/queues/" + ORDERS + "/shards")
                .request().get(new GenericType<>() {
                });

        assertEquals(2, shards.size());
        assertEquals(10, shards.get(0).getPending());
        assertEquals(10, shards.get(1).getPending());
    }

    /**
     * "Exists but not here" is a different answer from "does not exist", and an operator chasing a
     * backlog needs to be able to tell them apart.
     */
    @Test
    void thePerShardViewOfAQueueServedElsewhereConflictsRatherThanReportingNothing() {
        assertEquals(Response.Status.CONFLICT.getStatusCode(),
                readOnly.target("/ignismq/v1/queues/" + ELSEWHERE + "/shards").request().get().getStatus());
    }

    @Test
    void theInstanceViewReportsThisProcessAndTheQueuesItServes() {
        final InstanceView view = readOnly.target("/ignismq/v1/instance").request().get(InstanceView.class);

        assertEquals("billing", view.clientId());
        assertEquals("farm-1", view.farmId());
        assertEquals(IgnisMQSettings.defaults().getWorkerThreads(), view.workerThreads());
        assertEquals(List.of(ORDERS), view.queues().stream().map(InstanceQueue::name).toList());
        assertEquals(4, view.queues().get(0).consumers());
    }

    /**
     * The console asks this so it can disable what the caller cannot do, rather than offering every
     * action and letting it fail. It reports capability, never identity: no principal, no role list,
     * nothing an unauthenticated caller could not already learn by attempting one POST.
     */
    @Test
    void whoamiReportsNoPermissionsWithoutARole() {
        final Permissions permissions = readOnly.target("/ignismq/v1/whoami")
                .request().get(Permissions.class);

        assertFalse(permissions.authenticated(), "no SecurityContext was installed at all");
        assertFalse(permissions.operate());
        assertFalse(permissions.deactivate());
    }

    @Test
    void whoamiReportsTheOperatorRoleWithoutGrantingDeactivation() {
        final Permissions permissions = asOperator.target("/ignismq/v1/whoami")
                .request().get(Permissions.class);

        assertTrue(permissions.authenticated());
        assertTrue(permissions.operate());
        assertFalse(permissions.deactivate(),
                "deactivation is a separate grant; conflating them would offer an irreversible "
                        + "action to a caller who cannot perform it");
    }

    @Test
    void whoamiReportsDeactivationWithoutImplyingTheOperatorRole() {
        final Permissions permissions = asDeactivator.target("/ignismq/v1/whoami")
                .request().get(Permissions.class);

        assertTrue(permissions.deactivate());
        assertFalse(permissions.operate());
    }

    /**
     * Distinguishing "you sent nothing" from "what you sent lacks the role" is the whole point: the
     * first is fixed by supplying a token, the second by being granted the role, and the console
     * says so.
     */
    @Test
    void whoamiDistinguishesAnAuthenticatedCallerWithNoRolesFromAnUnauthenticatedOne() {
        final Permissions permissions = asRolelessButAuthenticated.target("/ignismq/v1/whoami")
                .request().get(Permissions.class);

        assertTrue(permissions.authenticated());
        assertFalse(permissions.operate());
        assertFalse(permissions.deactivate());
    }

    /**
     * Asking for more consumers than the cap allows is a request that can never succeed, however many
     * times it is retried. It answered 500, which tells the caller the opposite.
     */
    @Test
    void exceedingTheConsumerCapIsReportedAsAConflictRatherThanAServerError() {
        doThrow(IgnisMQException.builder()
                .errorCode(ErrorCode.MAX_ALLOWED_CONSUMERS_EXCEEDED)
                .message("100 consumers already").build())
                .when(manager).increaseConsumers(anyString(), anyInt());

        final Response response = asOperator.target("/ignismq/v1/queues/" + ORDERS + "/consumers")
                .queryParam("delta", 99).request().post(null);

        assertEquals(409, response.getStatus());
        assertEquals("MAX_ALLOWED_CONSUMERS_EXCEEDED",
                response.readEntity(IgnisMQExceptionMapper.ErrorBody.class).errorCode(),
                "the code is what lets the console say which limit was hit");
    }

    /**
     * The counterpart: storage being down is not something the caller can fix by changing anything.
     */
    @Test
    void aStorageFailureIsStillReportedAsAServerError() {
        doThrow(IgnisMQException.builder()
                .errorCode(ErrorCode.AEROSPIKE_ERROR).message("node unreachable").build())
                .when(manager).sweepQueue(anyString());

        assertEquals(503, asOperator.target("/ignismq/v1/queues/" + ORDERS + "/sweep")
                .request().post(null).getStatus());
    }

    @Test
    void everyMutationIsClosedWithoutTheOperatorRole() {
        assertEquals(403, readOnly.target("/ignismq/v1/queues/" + ORDERS + "/shovel")
                .request().post(null).getStatus());
        assertEquals(403, readOnly.target("/ignismq/v1/queues/" + ORDERS + "/sweep")
                .request().post(null).getStatus());
        assertEquals(403, readOnly.target("/ignismq/v1/queues/" + ORDERS + "/consumers")
                .queryParam("delta", 1).request().post(null).getStatus());
        assertEquals(403, readOnly.target("/ignismq/v1/queues/" + ORDERS + "/deactivate")
                .queryParam("confirm", ORDERS).request().post(null).getStatus());

        verify(queue, never()).shovel(anyInt());
        verify(manager, never()).sweepQueue(anyString());
        verify(manager, never()).increaseConsumers(anyString(), anyInt());
        verify(manager, never()).deactivateQueue(anyString());
    }

    /**
     * The read side must not have been closed along with the write side.
     */
    @Test
    void theReadViewsStayOpenWithoutAnyRole() {
        assertEquals(200, readOnly.target("/ignismq/v1/queues").request().get().getStatus());
        assertEquals(200, readOnly.target("/ignismq/v1/instance").request().get().getStatus());
    }

    @Test
    void aShovelWithoutAnIntervalDrainsOnThisInstanceOnly() {
        final ActionResult result = asOperator.target("/ignismq/v1/queues/" + ORDERS + "/shovel")
                .queryParam("concurrency", 3).request().post(null, ActionResult.class);

        verify(queue, times(1)).shovel(3);
        verify(manager, never()).scheduleShoveling(anyString(), any());
        assertEquals("instance", result.scope());
    }

    /**
     * An interval stores the shovel instead, which every instance adopts on its next refresh. The
     * two differ in blast radius, so they must not be the same button.
     */
    @Test
    void aShovelWithAnIntervalIsStoredForEveryInstance() {
        final ActionResult result = asOperator.target("/ignismq/v1/queues/" + ORDERS + "/shovel")
                .queryParam("concurrency", 2).queryParam("intervalSeconds", 900)
                .request().post(null, ActionResult.class);

        verify(manager, times(1)).scheduleShoveling(ORDERS, ShovelConfig.builder()
                .concurrency(2).timeIntervalInSecs(900).build());
        verify(queue, never()).shovel(anyInt());
        assertEquals("cluster", result.scope());
    }

    @Test
    void aPositiveConsumerDeltaAddsConsumersAndANegativeOneRemovesThem() {
        asOperator.target("/ignismq/v1/queues/" + ORDERS + "/consumers")
                .queryParam("delta", 2).request().post(null, ActionResult.class);
        asOperator.target("/ignismq/v1/queues/" + ORDERS + "/consumers")
                .queryParam("delta", -3).request().post(null, ActionResult.class);

        verify(manager, times(1)).increaseConsumers(ORDERS, 2);
        verify(manager, times(1)).decreaseConsumers(ORDERS, 3);
    }

    @Test
    void aZeroConsumerDeltaIsRejectedRatherThanSilentlyDoingNothing() {
        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(),
                asOperator.target("/ignismq/v1/queues/" + ORDERS + "/consumers")
                        .queryParam("delta", 0).request().post(null).getStatus());

        verify(manager, never()).increaseConsumers(anyString(), anyInt());
        verify(manager, never()).decreaseConsumers(anyString(), anyInt());
    }

    @Test
    void aSweepReachesTheManagerAndSaysItChangedTheCluster() {
        final ActionResult swept = asOperator.target("/ignismq/v1/queues/" + ORDERS + "/sweep")
                .request().post(null, ActionResult.class);

        verify(manager, times(1)).sweepQueue(ORDERS);
        assertEquals("cluster", swept.scope());
    }

    /**
     * Deactivation cannot be undone by any API, so the grant that allows draining a sideline must
     * not also allow destroying a queue. The operator role is explicitly not enough.
     */
    @Test
    void deactivationNeedsItsOwnRoleAndNotMerelyTheOperatorRole() {
        assertEquals(403, asOperator.target("/ignismq/v1/queues/" + ORDERS + "/deactivate")
                .queryParam("confirm", ORDERS).request().post(null).getStatus());

        verify(manager, never()).deactivateQueue(anyString());
    }

    /**
     * A UI-only confirmation protects nobody driving this over HTTP, so the check is server-side.
     */
    @Test
    void deactivationRequiresTheQueueNameAsConfirmation() {
        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(),
                asDeactivator.target("/ignismq/v1/queues/" + ORDERS + "/deactivate")
                        .request().post(null).getStatus());
        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(),
                asDeactivator.target("/ignismq/v1/queues/" + ORDERS + "/deactivate")
                        .queryParam("confirm", ELSEWHERE).request().post(null).getStatus());

        verify(manager, never()).deactivateQueue(anyString());
    }

    @Test
    void aConfirmedDeactivationWithTheRightRoleReachesTheManager() {
        final ActionResult result = asDeactivator.target("/ignismq/v1/queues/" + ORDERS + "/deactivate")
                .queryParam("confirm", ORDERS).request().post(null, ActionResult.class);

        verify(manager, times(1)).deactivateQueue(ORDERS);
        assertEquals("cluster", result.scope());
    }

    /**
     * The instance tab reads the registry directly, so it cannot drift from what a scrape reports.
     * Anything outside ignisMQ's and Magazine's namespaces belongs to the host application and is
     * none of the console's business.
     */
    @Test
    void theMetricsViewReportsThisProcessesIgnisAndMagazineMetersAndNothingElse() {
        meterRegistry.counter("ignismq.messages", "queue", ORDERS, "outcome", "acked").increment(7);
        meterRegistry.counter("magazine.aerospike.calls", "magazine", ORDERS).increment(3);
        meterRegistry.counter("jvm.gc.pause").increment(11);

        final InstanceMetrics metrics = readOnly.target("/ignismq/v1/instance/metrics")
                .request().get(InstanceMetrics.class);

        assertEquals(List.of("ignismq.messages", "magazine.aerospike.calls"),
                metrics.meters().stream().map(MeterSample::name).toList());
        assertEquals(2, metrics.meterCount());
        assertEquals("billing", metrics.clientId());
        assertEquals(Map.of("queue", ORDERS, "outcome", "acked"), metrics.meters().get(0).tags());
        assertEquals(7.0, metrics.meters().get(0).measurements().get("COUNT"));
    }

    /**
     * A shovel needs live magazines, so it is refused on an instance that does not serve the queue
     * rather than being accepted and silently doing nothing.
     */
    @Test
    void aShovelOnAQueueServedElsewhereIsRefused() {
        assertEquals(Response.Status.CONFLICT.getStatusCode(),
                asOperator.target("/ignismq/v1/queues/" + ELSEWHERE + "/shovel")
                        .request().post(null).getStatus());
    }

    /**
     * Rendering the console is a storage read per queue fanned out over every shard, so an open tab
     * must not be a load source. The window is configurable and zero disables it, which is what the
     * rest of this class runs with.
     */
    @Test
    void aCachedInventoryIsReadFromStorageOnceWithinItsWindow() {
        final IgnisMQService cached = new IgnisMQService(manager, "billing", "farm-1",
                IgnisMQSettings.defaults(), meterRegistry, 30);

        cached.queues();
        cached.queues();

        verify(manager, times(1)).getStoredQueues(true);
        verify(manager, times(1)).getStoredQueues(false);
    }

    @Test
    void anUncachedInventoryReadsStorageEveryTime() {
        final IgnisMQService uncached = new IgnisMQService(manager, "billing", "farm-1",
                IgnisMQSettings.defaults(), meterRegistry, 0);

        uncached.queues();
        uncached.queues();

        verify(manager, times(2)).getStoredQueues(true);
        verify(manager, times(2)).getStoredQueues(false);
    }

    private static QueueSummary summary(final List<QueueSummary> queues, final String name) {
        return queues.stream().filter(summary -> summary.name().equals(name)).findFirst().orElseThrow();
    }

    private static ShardDepth depth(final String shard, final long published, final long consumed) {
        return ShardDepth.builder()
                .shard(shard)
                .published(published)
                .consumed(consumed)
                .pending(published - consumed)
                .build();
    }

    private static QueueEntity entity(final boolean active, final int shards) {
        return QueueEntity.builder()
                .active(active)
                .shards(shards)
                .concurrency(6)
                .messageHandlerType("orderHandler")
                .createdAt(1_700_000_000_000L)
                .messageExpiry(172_800)
                .queueExpiry(172_800)
                .sweepDuration(1_200_000L)
                .handlerTimeout(600_000L)
                .shovelConcurrency(-1)
                .shovelTimeIntervalInSecs(-1)
                .build();
    }
}
