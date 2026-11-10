package org.pipelineframework.aws.durable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.pipelineframework.PipelineExecutionService;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.CoordinationHost;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineControlPlane;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.PipelineRuntimeIdentityObservation;

/** Observes the actual runtime through the deployment's private, numbered HTTP Lambda invocation. */
@Path("/_tpf/aws-durable/runtime-identity")
@Produces(MediaType.APPLICATION_JSON)
public class AwsDurableHttpRuntimeIdentityResource {
    private static final ObjectWriter IDENTITY_JSON = PipelineJson.mapper().writerFor(PipelineRuntimeIdentityObservation.class);

    @Inject PipelineControlPlane controlPlane;
    @Inject PipelineExecutionService executionService;
    @Inject PipelineOrchestratorConfig config;
    @Inject @ConfigProperty(name = "tpf.aws.durable.http.enabled", defaultValue = "false")
    boolean enabled;

    @GET
    @Blocking
    public Uni<Response> identity(@QueryParam("tenant") String tenant) {
        if (!enabled || config.mode() != OrchestratorMode.QUEUE_ASYNC
            || config.coordinationHost() != CoordinationHost.AWS_DURABLE
            || !config.strictStartup() || !config.processLoopsDisabled()) {
            return Uni.createFrom().item(Response.status(Response.Status.NOT_FOUND).build());
        }
        if (tenant == null || tenant.isBlank() || !tenant.equals(tenant.trim())) {
            return Uni.createFrom().item(Response.status(Response.Status.BAD_REQUEST).build());
        }
        return Uni.createFrom().deferred(() -> {
            if (executionService.getStartupHealthState() != PipelineExecutionService.StartupHealthState.HEALTHY) {
                return Uni.createFrom().item(unavailable());
            }
            return controlPlane.runtimeIdentity(tenant)
                .map(AwsDurableHttpRuntimeIdentityResource::response);
        }).onFailure().recoverWithItem(AwsDurableHttpRuntimeIdentityResource::unavailable);
    }

    private static Response response(PipelineRuntimeIdentityObservation observed) {
        if (!observed.ready()) {
            return unavailable();
        }
        try {
            return Response.ok(IDENTITY_JSON.writeValueAsBytes(observed), MediaType.APPLICATION_JSON).build();
        } catch (JsonProcessingException invalid) {
            return unavailable();
        }
    }

    private static Response unavailable() {
        return Response.status(Response.Status.SERVICE_UNAVAILABLE).build();
    }
}
