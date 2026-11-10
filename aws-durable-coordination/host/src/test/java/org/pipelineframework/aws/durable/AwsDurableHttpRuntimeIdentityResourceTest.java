package org.pipelineframework.aws.durable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.pipelineframework.PipelineExecutionService;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.CoordinationHost;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineControlPlane;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.PipelineRuntimeIdentityObservation;

class AwsDurableHttpRuntimeIdentityResourceTest {
    @Test
    void privateHttpIdentityReportsTheActualReadyRuntime() throws Exception {
        var resource = enabledResource();
        var contract = AwsDurableRetainedReleaseRoutingIT.contract("release");
        var observed = new PipelineRuntimeIdentityObservation("pipeline", contract.contractVersion(),
            "release", true, contract.steps().size(), contract, "dynamo", true, "command-effects",
            "dynamo", "query-captures");
        when(resource.controlPlane.runtimeIdentity("tenant")).thenReturn(Uni.createFrom().item(observed));

        var response = resource.identity("tenant").await().indefinitely();

        assertThat(response.getStatus()).isEqualTo(200);
        assertThat((byte[]) response.getEntity()).isEqualTo(PipelineJson.mapper().writeValueAsBytes(observed));
    }

    @Test
    void defaultAndWorkerHostingDoNotExposeHttpRuntimeIdentity() {
        var resource = new AwsDurableHttpRuntimeIdentityResource();
        resource.controlPlane = mock(PipelineControlPlane.class);
        assertThat(resource.identity("tenant").await().indefinitely().getStatus()).isEqualTo(404);
        verifyNoInteractions(resource.controlPlane);
    }

    @Test
    void unavailableRuntimeObservationCannotReturnReady() {
        var resource = enabledResource();
        when(resource.controlPlane.runtimeIdentity("tenant"))
            .thenReturn(Uni.createFrom().failure(new IllegalStateException("provider detail")));

        var response = resource.identity("tenant").await().indefinitely();

        assertThat(response.getStatus()).isEqualTo(503);
        assertThat(response.hasEntity()).isFalse();
    }

    @Test
    void startupHealthMustPassBeforeObservingRuntimeIdentity() {
        var resource = enabledResource();
        when(resource.executionService.getStartupHealthState())
            .thenReturn(PipelineExecutionService.StartupHealthState.PENDING);

        assertThat(resource.identity("tenant").await().indefinitely().getStatus()).isEqualTo(503);
        verifyNoInteractions(resource.controlPlane);
    }

    private static AwsDurableHttpRuntimeIdentityResource enabledResource() {
        var resource = new AwsDurableHttpRuntimeIdentityResource();
        resource.enabled = true;
        resource.controlPlane = mock(PipelineControlPlane.class);
        resource.executionService = mock(PipelineExecutionService.class);
        resource.config = mock(PipelineOrchestratorConfig.class);
        when(resource.config.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        when(resource.config.coordinationHost()).thenReturn(CoordinationHost.AWS_DURABLE);
        when(resource.config.strictStartup()).thenReturn(true);
        when(resource.config.processLoopsDisabled()).thenReturn(true);
        when(resource.executionService.getStartupHealthState()).thenReturn(PipelineExecutionService.StartupHealthState.HEALTHY);
        return resource;
    }
}
