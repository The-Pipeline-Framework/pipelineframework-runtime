package org.pipelineframework.orchestrator.release;

import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

/** Privileged read-only observation of the actual current activation-event identity. */
@ApplicationScoped
@Path("/tpf/admin/tenants/{tenantId}/pipelines/{pipelineId}/current-activation")
@Produces(MediaType.APPLICATION_JSON)
public class HostedCurrentActivationResource {
    @Inject HostedReleaseAdminResource admin;

    @GET
    @Blocking
    public Uni<Response> get(@PathParam("tenantId") String tenant, @PathParam("pipelineId") String pipeline,
        @HeaderParam("Authorization") String authorization) {
        var guard = admin.guard(tenant, pipeline, authorization);
        if (guard.isPresent()) return Uni.createFrom().item(guard.get());
        if (!admin.registry().supportsCurrentActivationObservation()) return Uni.createFrom().item(Response.status(501).build());
        return admin.registry().currentActivationObservation(tenant, pipeline).map(value -> Response.ok(value).build())
            .onFailure(UnsupportedOperationException.class).recoverWithItem(error -> Response.status(501).build())
            .onFailure(IllegalStateException.class).recoverWithItem(error -> Response.status(409).build());
    }
}
