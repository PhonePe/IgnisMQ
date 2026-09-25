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

import com.phonepe.ignis.common.ShardDepth;
import com.phonepe.ignis.response.ActionResult;
import com.phonepe.ignis.response.InstanceMetrics;
import com.phonepe.ignis.response.InstanceView;
import com.phonepe.ignis.response.Permissions;
import com.phonepe.ignis.response.QueueDetail;
import com.phonepe.ignis.response.QueueSummary;
import com.phonepe.ignis.service.IgnisMQService;
import lombok.RequiredArgsConstructor;

import javax.annotation.security.RolesAllowed;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.SecurityContext;
import java.util.List;

@Path("/ignismq/v1")
@Produces(MediaType.APPLICATION_JSON)
@RequiredArgsConstructor
public final class IgnisMQResource {

    public static final String OPERATE_ROLE = "ignismq_operate";
    public static final String DEACTIVATE_ROLE = "ignismq_deactivate";

    private final IgnisMQService service;

    @GET
    @Path("/queues")
    public List<QueueSummary> queues() {
        return service.queues();
    }

    @GET
    @Path("/queues/{name}")
    public QueueDetail queue(@PathParam("name") final String name) {
        return service.queue(name);
    }

    @GET
    @Path("/queues/{name}/shards")
    public List<ShardDepth> shards(@PathParam("name") final String name) {
        return service.shards(name);
    }

    @GET
    @Path("/instance")
    public InstanceView instance() {
        return service.instance();
    }

    @GET
    @Path("/instance/metrics")
    public InstanceMetrics metrics() {
        return service.metrics();
    }

    @GET
    @Path("/whoami")
    public Permissions whoami(@Context final SecurityContext security) {
        return new Permissions(security.getUserPrincipal() != null,
                security.isUserInRole(OPERATE_ROLE), security.isUserInRole(DEACTIVATE_ROLE));
    }

    @POST
    @Path("/queues/{name}/shovel")
    @RolesAllowed(OPERATE_ROLE)
    public ActionResult shovel(@PathParam("name") final String name,
                               @QueryParam("concurrency") @DefaultValue("1") final int concurrency,
                               @QueryParam("intervalSeconds") final Integer intervalSeconds) {
        // An interval turns a one-shot drain on this instance into stored configuration every
        // instance adopts, which is a different blast radius and so a different answer.
        return intervalSeconds == null
                ? service.shovel(name, concurrency)
                : service.scheduleShovel(name, concurrency, intervalSeconds);
    }

    @POST
    @Path("/queues/{name}/sweep")
    @RolesAllowed(OPERATE_ROLE)
    public ActionResult sweep(@PathParam("name") final String name) {
        return service.sweep(name);
    }

    @POST
    @Path("/queues/{name}/consumers")
    @RolesAllowed(OPERATE_ROLE)
    public ActionResult consumers(@PathParam("name") final String name,
                                  @QueryParam("delta") final int delta) {
        return service.consumers(name, delta);
    }

    @POST
    @Path("/queues/{name}/deactivate")
    @RolesAllowed(DEACTIVATE_ROLE)
    public ActionResult deactivate(@PathParam("name") final String name,
                                   @QueryParam("confirm") final String confirmation) {
        return service.deactivate(name, confirmation);
    }
}
