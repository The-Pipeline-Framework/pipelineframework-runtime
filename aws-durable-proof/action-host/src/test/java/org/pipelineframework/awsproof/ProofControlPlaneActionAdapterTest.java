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
        ProofControlPlaneActionAdapter adapter = adapter(controlPlane);

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
        ProofControlPlaneActionAdapter adapter = adapter(controlPlane);

        assertThatThrownBy(() -> adapter.handle(AwsDurableActionRequest.awaitCheckpoint(
            new AwsDurableAwaitIdentity(
                "tenant-1", "execution-1", "interaction-1", "correlation-1", 1))))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Await semantic checkpoint is not available");
    }
    @Test
    void faultAfterCommitWrapsActualProductionInvocation() {
        var actions = mock(org.pipelineframework.aws.durable.AwsDurableActionInvoker.class);
        var faults = mock(ProofFaultInjector.class);
        var catalogue = mock(ProofReleaseCatalog.class);
        var request = AwsDurableActionRequest.submit(
            new org.pipelineframework.aws.durable.model.AwsDurableExecutionInput(
                "tenant-1", "key-1", "pipeline-1", "contract-1", "release-1", "{}", Optional.empty(), 1));
        when(catalogue.ensureRegistered("tenant-1", "pipeline-1", "contract-1", "release-1"))
            .thenReturn(Uni.createFrom().voidItem());
        var committed = org.pipelineframework.aws.durable.model.AwsDurableActionResponse.submitted(
            new AwsDurableExecutionCheckpoint("tenant-1", "execution-1", "pipeline-1", "contract-1", "release-1"), false);
        when(actions.invoke(request)).thenReturn(committed);
        org.mockito.Mockito.doThrow(new ProofInjectedFaultException("submit-after-tpf-commit"))
            .when(faults).failIfArmed("submit-after-tpf-commit", "execution-1");

        assertThatThrownBy(() -> new ProofControlPlaneActionAdapter(actions, faults, catalogue).handle(request))
            .isInstanceOf(ProofInjectedFaultException.class);
        var order = org.mockito.Mockito.inOrder(faults, catalogue, actions);
        order.verify(faults).failIfArmed("submit-before-tpf-commit", "key-1");
        order.verify(catalogue).ensureRegistered("tenant-1", "pipeline-1", "contract-1", "release-1");
        order.verify(actions).invoke(request);
        order.verify(faults).failIfArmed("submit-after-tpf-commit", "execution-1");
    }

    private static ProofControlPlaneActionAdapter adapter(PipelineControlPlane controlPlane) {
        return new ProofControlPlaneActionAdapter(
            new org.pipelineframework.aws.durable.AwsDurableControlPlaneActions(
                controlPlane, mock(org.pipelineframework.aws.durable.AwsDurableCallbackBindingRepository.class),
                input -> input, new com.fasterxml.jackson.databind.ObjectMapper(),
                java.time.Duration.ofSeconds(5), java.time.Duration.ofDays(1)),
            mock(ProofFaultInjector.class), mock(ProofReleaseCatalog.class));
    }

}
