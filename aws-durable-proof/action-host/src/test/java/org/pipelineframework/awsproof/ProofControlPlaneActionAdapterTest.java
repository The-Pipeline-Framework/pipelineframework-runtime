package org.pipelineframework.awsproof;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.pipelineframework.aws.durable.model.AwsDurableActionRequest;
import org.pipelineframework.aws.durable.model.AwsDurableAwaitIdentity;
import org.pipelineframework.aws.durable.model.AwsDurableDriverCheckpoint;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionCheckpoint;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.orchestrator.AwaitSemanticCheckpoint;
import org.pipelineframework.orchestrator.PipelineControlPlane;

class ProofControlPlaneActionAdapterTest {

    @Test
    void pendingAwaitUsesExecutionScopedSemanticCheckpoints() {
        PipelineControlPlane controlPlane = mock(PipelineControlPlane.class);
        when(controlPlane.getAwaitSemanticCheckpoints("tenant-1", "execution-1", 100))
            .thenReturn(Uni.createFrom().item(List.of(new AwaitSemanticCheckpoint(
                "tenant-1", "execution-1", "interaction-1", "correlation-1", "unit-1", "step-1",
                AwaitInteractionStatus.WAITING, "pipeline-1", "contract-1", "release-1"))));
        ProofControlPlaneActionAdapter adapter = new ProofControlPlaneActionAdapter();
        adapter.controlPlane = controlPlane;

        var response = adapter.handle(AwsDurableActionRequest.pendingAwait(new AwsDurableDriverCheckpoint(
            new AwsDurableExecutionCheckpoint(
                "tenant-1", "execution-1", "pipeline-1", "contract-1", "release-1"), 1)));

        assertThat(response.awaitIdentity())
            .hasValueSatisfying(identity -> assertThat(identity.interactionId())
                .isEqualTo("interaction-1"));
    }

    @Test
    void missingAwaitCheckpointMatchesProductionActionContract() {
        PipelineControlPlane controlPlane = mock(PipelineControlPlane.class);
        when(controlPlane.getAwaitSemanticCheckpoint("tenant-1", "interaction-1"))
            .thenReturn(Uni.createFrom().item(Optional.empty()));
        ProofControlPlaneActionAdapter adapter = new ProofControlPlaneActionAdapter();
        adapter.controlPlane = controlPlane;

        assertThatThrownBy(() -> adapter.handle(AwsDurableActionRequest.awaitCheckpoint(
            new AwsDurableAwaitIdentity(
                "tenant-1", "execution-1", "interaction-1", "correlation-1", 1))))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Await semantic checkpoint is not available");
    }
}
