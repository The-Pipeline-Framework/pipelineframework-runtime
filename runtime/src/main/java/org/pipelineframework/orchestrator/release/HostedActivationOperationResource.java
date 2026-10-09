package org.pipelineframework.orchestrator.release;

import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

/** Additive privileged activation authority, with genuinely read-only historical inquiry. */
@ApplicationScoped
@Path("/tpf/admin/tenants/{tenantId}/pipelines/{pipelineId}/activation-operations")
@Produces(MediaType.APPLICATION_JSON)
public class HostedActivationOperationResource {
    @Inject
    HostedReleaseAdminResource admin;

    public record Request(String operationKey, String contractVersion, String releaseVersion) { }

    @GET
    @Path("/{operationKey}")
    @Blocking
    public Uni<Response> get(@PathParam("tenantId") String tenant, @PathParam("pipelineId") String pipeline,
        @PathParam("operationKey") String key, @HeaderParam("Authorization") String authorization) {
        var guard = admin.guard(tenant, pipeline, authorization);
        if (guard.isPresent()) {
            return Uni.createFrom().item(guard.get());
        }
        if (key == null || key.isBlank()) {
            return Uni.createFrom().item(Response.status(400).build());
        }
        if (!admin.registry().supportsActivationOperations()) {
            return Uni.createFrom().item(Response.status(501).build());
        }
        return admin.registry().getActivationOperation(tenant, pipeline, key)
            .map(receipt -> receipt.map(value -> Response.ok(value).build()).orElseGet(() -> Response.status(404).build()));
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Blocking
    public Uni<Response> activate(@PathParam("tenantId") String tenant, @PathParam("pipelineId") String pipeline,
        @HeaderParam("Authorization") String authorization, Request request) {
        var guard = admin.guard(tenant, pipeline, authorization);
        if (guard.isPresent()) {
            return Uni.createFrom().item(guard.get());
        }
        if (request == null || blank(request.operationKey()) || blank(request.contractVersion()) || blank(request.releaseVersion())) {
            return Uni.createFrom().item(Response.status(400).build());
        }
        if (!admin.registry().supportsActivationOperations()) {
            return Uni.createFrom().item(Response.status(501).build());
        }
        return admin.registry().getActivationOperation(tenant, pipeline, request.operationKey())
            .flatMap(receipt -> {
                if (receipt.isPresent()) {
                    var original = receipt.get();
                    if (!original.contractVersion().equals(request.contractVersion()) || !original.releaseVersion().equals(request.releaseVersion())) {
                        return Uni.createFrom().item(Response.status(409).build());
                    }
                    // Historical replay requires no current artifact/worker availability and never reactivates.
                    return Uni.createFrom().item(Response.ok(original).build());
                }
                return admin.registry().get(tenant, pipeline, request.releaseVersion()).flatMap(release -> {
                    if (release.isEmpty()) {
                        return Uni.createFrom().item(Response.status(404).build());
                    }
                    if (!release.get().contractVersion().equals(request.contractVersion())) {
                        return Uni.createFrom().item(Response.status(409).build());
                    }
                    try {
                        admin.registrar().verify(release.get());
                    } catch (IllegalArgumentException | IllegalStateException failure) {
                        return Uni.createFrom().item(Response.status(409).build());
                    }
                    return admin.registry().activateOnce(new ActivationOperationCommand(request.operationKey(), release.get(), System.currentTimeMillis()))
                        .map(value -> Response.ok(value).build());
                });
            }).onFailure(IllegalArgumentException.class).recoverWithItem(failure -> Response.status(400).build())
            .onFailure(IllegalStateException.class).recoverWithItem(failure -> Response.status(409).build());
    }

    private static boolean blank(String value) {
        return value == null || value.isBlank();
    }
}
