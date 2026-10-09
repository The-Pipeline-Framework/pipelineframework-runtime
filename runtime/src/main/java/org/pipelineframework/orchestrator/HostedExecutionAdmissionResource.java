package org.pipelineframework.orchestrator;

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
import org.pipelineframework.LocalPipelineControlPlane;
import org.pipelineframework.orchestrator.dto.HostedExecutionSubmitRequest;
import org.pipelineframework.orchestrator.release.ExecutionAdmissionReleaseEvidence;
import org.pipelineframework.orchestrator.worker.PipelineWorkerAvailabilityRequest;

/** Strict expected-pin root admission and read-only original admission inquiry. */
@ApplicationScoped
@Path("/tpf/control-plane/tenants/{tenantId}/pipelines/{pipelineId}/execution-admissions")
@Produces(MediaType.APPLICATION_JSON)
public class HostedExecutionAdmissionResource {
    @Inject
    HostedPipelineControlPlaneResource hosted;
    @Inject
    LocalPipelineControlPlane nativeControlPlane;

    public record Request(String clientKey, String contractVersion, String releaseVersion,
        ExecutionInputShape inputShape, SerializedTransitionPayload inputPayload, boolean outputStreaming) {
        ExecutionAdmissionIntent intent(String tenant, String pipeline) {
            if (inputShape == null || inputPayload == null) {
                throw new IllegalArgumentException("inputShape and inputPayload are required");
            }
            return new ExecutionAdmissionIntent(1, tenant, pipeline, clientKey, contractVersion, releaseVersion,
                inputShape.name(), inputPayload.payloadTypeId(), inputPayload.payloadEncoding(),
                NativeExecutionAdmission.utf8(inputPayload.payload()), outputStreaming);
        }
    }

    @GET
    @Path("/{clientKey}")
    @Blocking
    public Uni<Response> get(@PathParam("tenantId") String tenant, @PathParam("pipelineId") String pipeline,
        @PathParam("clientKey") String key, @HeaderParam("Authorization") String authorization) {
        return Uni.createFrom().deferred(() -> {
            Response guard = hosted.guard(tenant, authorization);
            if (guard != null) {
                return Uni.createFrom().item(guard);
            }
            if (blank(pipeline) || blank(key)) {
                return Uni.createFrom().item(Response.status(400).build());
            }
            return nativeControlPlane.nativeAdmissionStore().lookupAdmission(tenant, pipeline, key)
                .map(receipt -> receipt.map(value -> Response.ok(value).build()).orElseGet(() -> Response.status(404).build()));
        }).onFailure(UnsupportedOperationException.class).recoverWithItem(error -> Response.status(501).build());
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Blocking
    public Uni<Response> admit(@PathParam("tenantId") String tenant, @PathParam("pipelineId") String pipeline,
        @HeaderParam("Authorization") String authorization, Request request) {
        return Uni.createFrom().deferred(() -> {
            Response guard = hosted.guard(tenant, authorization);
            if (guard != null) {
                return Uni.createFrom().item(guard);
            }
            if (request == null) {
                return Uni.createFrom().item(Response.status(400).build());
            }
            ExecutionAdmissionIntent intent = request.intent(tenant, pipeline);
            var store = nativeControlPlane.nativeAdmissionStore();
            return store.inspectExistingAdmission(intent).flatMap(existing -> {
                if (existing.isPresent()) {
                    // Full retained intent has been compared, without current artifact/type/worker lookup.
                    return Uni.createFrom().item(Response.ok(existing.get().receipt()).build());
                }
                return hosted.registry().get(tenant, pipeline, request.releaseVersion()).flatMap(registered -> {
                    if (registered.isEmpty()) {
                        return Uni.createFrom().item(Response.status(404).build());
                    }
                    var release = registered.get();
                    if (!release.contractVersion().equals(request.contractVersion())) {
                        return Uni.createFrom().item(Response.status(409).build());
                    }
                    hosted.registrar().verify(release);
                    Object input;
                    try {
                        input = hosted.executionInput(new HostedExecutionSubmitRequest(pipeline, request.inputShape(),
                            request.inputPayload(), request.clientKey(), request.outputStreaming()), release);
                    } catch (HostedPipelineControlPlaneResource.IngressPayloadTypeResolutionException unavailable) {
                        return Uni.createFrom().item(Response.status(503).build());
                    } catch (RuntimeException invalid) {
                        return Uni.createFrom().item(Response.status(400).build());
                    }
                    var evidence = ExecutionAdmissionReleaseEvidence.snapshot(release);
                    return hosted.availability().check(new PipelineWorkerAvailabilityRequest(tenant, pipeline,
                        release.contractVersion(), release.releaseVersion(), release.primaryArtifactId(), release.primaryArtifactDigest()))
                        .flatMap(availability -> availability.available()
                            ? nativeControlPlane.admitExecution(input, intent, evidence).map(result -> Response.ok(result.receipt()).build())
                            : Uni.createFrom().item(Response.status(503).build()));
                });
            });
        }).onFailure(ExecutionAdmissionTooLargeException.class).recoverWithItem(error -> Response.status(413).build())
            .onFailure(UnsupportedOperationException.class).recoverWithItem(error -> Response.status(501).build())
            .onFailure(IllegalArgumentException.class).recoverWithItem(error -> Response.status(400).build())
            .onFailure(IllegalStateException.class).recoverWithItem(error -> Response.status(409).build());
    }

    private static boolean blank(String value) {
        return value == null || value.isBlank();
    }
}
