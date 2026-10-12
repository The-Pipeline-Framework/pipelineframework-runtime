package org.pipelineframework.aws.durable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.aws.durable.model.AwsDurableActionRequest;
import org.pipelineframework.aws.durable.model.AwsDurableDriverCheckpoint;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionCheckpoint;
import org.pipelineframework.orchestrator.AwaitSemanticCheckpoint;
import org.pipelineframework.orchestrator.PipelineControlPlane;

class AwsDurableControlPlaneActionsTest {

    @Test
    void recoveryRegistrationRequiresTheAuthoritativeSemanticRelease() {
        var controlPlane = mock(PipelineControlPlane.class);
        var bindings = mock(AwsDurableCallbackBindingRepository.class);
        var locator = new org.pipelineframework.aws.durable.model.AwsDurableRecoveryLocator(
            driverCheckpoint(), "provider-name", "provider-arn");
        when(controlPlane.getExecutionSemanticCheckpoint("tenant-1", "execution-1"))
            .thenReturn(Uni.createFrom().item(new org.pipelineframework.orchestrator.ExecutionSemanticCheckpoint(
                "tenant-1", "execution-1", "pipeline-1", "contract-1", "release-1",
                org.pipelineframework.orchestrator.ExecutionStatus.RUNNING)));
        when(bindings.registerRecovery(locator)).thenReturn(true);
        var actions = new AwsDurableControlPlaneActions(controlPlane, bindings, input -> input,
            new ObjectMapper(), Duration.ofSeconds(5), Duration.ofDays(1));
        actions.invoke(AwsDurableActionRequest.registerExecution(driverCheckpoint(), "provider-name", "provider-arn"));
        verify(bindings).registerRecovery(locator);

        when(controlPlane.getExecutionSemanticCheckpoint("tenant-1", "execution-1"))
            .thenReturn(Uni.createFrom().item(new org.pipelineframework.orchestrator.ExecutionSemanticCheckpoint(
                "tenant-1", "execution-1", "pipeline-1", "contract-1", "different-release",
                org.pipelineframework.orchestrator.ExecutionStatus.RUNNING)));
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> actions.invoke(
            AwsDurableActionRequest.registerExecution(driverCheckpoint(), "provider-name", "provider-arn")))
            .isInstanceOf(IllegalStateException.class).hasMessageContaining("semantic execution");
        org.mockito.Mockito.verifyNoMoreInteractions(bindings);
    }

    @Test
    void currentAwaitUsesSemanticLocatorInsteadOfHistoricalOrdering() {
        PipelineControlPlane controlPlane = mock(PipelineControlPlane.class);
        when(controlPlane.getCurrentAwaitSemanticCheckpoint("tenant-1", "execution-1"))
            .thenReturn(Uni.createFrom().item(java.util.Optional.of(checkpoint("waiting", AwaitInteractionStatus.WAITING))));

        var response = actions(controlPlane).invoke(AwsDurableActionRequest.pendingAwait(driverCheckpoint()));

        assertThat(response.awaitIdentity()).hasValueSatisfying(identity -> {
            assertThat(identity.executionId()).isEqualTo("execution-1");
            assertThat(identity.interactionId()).isEqualTo("waiting");
        });
        verify(controlPlane).getCurrentAwaitSemanticCheckpoint("tenant-1", "execution-1");
        verify(controlPlane, never()).getAwaitSemanticCheckpoints("tenant-1", "execution-1", 100);
        verify(controlPlane, never()).queryPendingAwaitInteractions("tenant-1", "", "", "", 100);
    }

    @Test
    void pendingAwaitReturnsEmptyWhenExecutionHasNoPendingInteraction() {
        PipelineControlPlane controlPlane = mock(PipelineControlPlane.class);
        when(controlPlane.getCurrentAwaitSemanticCheckpoint("tenant-1", "execution-1"))
            .thenReturn(Uni.createFrom().item(java.util.Optional.empty()));

        assertThat(actions(controlPlane).invoke(AwsDurableActionRequest.pendingAwait(driverCheckpoint()))
            .awaitIdentity()).isEmpty();
    }

    @Test
    void completionBeforeRegistrationStillReturnsCurrentAwaitIdentity() {
        PipelineControlPlane controlPlane = mock(PipelineControlPlane.class);
        when(controlPlane.getCurrentAwaitSemanticCheckpoint("tenant-1", "execution-1"))
            .thenReturn(Uni.createFrom().item(java.util.Optional.of(checkpoint("completed", AwaitInteractionStatus.COMPLETED))));
        assertThat(actions(controlPlane).invoke(AwsDurableActionRequest.pendingAwait(driverCheckpoint()))
            .awaitIdentity()).hasValueSatisfying(identity -> assertThat(identity.interactionId()).isEqualTo("completed"));
    }

    private static AwsDurableControlPlaneActions actions(PipelineControlPlane controlPlane) {
        return new AwsDurableControlPlaneActions(
            controlPlane,
            mock(AwsDurableCallbackBindingRepository.class),
            input -> input,
            new ObjectMapper(),
            Duration.ofSeconds(5),
            Duration.ofDays(1));
    }

    private static AwsDurableDriverCheckpoint driverCheckpoint() {
        return new AwsDurableDriverCheckpoint(new AwsDurableExecutionCheckpoint(
            "tenant-1", "execution-1", "pipeline-1", "contract-1", "release-1"), 7);
    }

    private static AwaitSemanticCheckpoint checkpoint(
        String interactionId,
        AwaitInteractionStatus status
    ) {
        return new AwaitSemanticCheckpoint(
            "tenant-1", "execution-1", interactionId, "correlation-" + interactionId,
            "unit-1", "step-1", status, "pipeline-1", "contract-1", "release-1");
    }
}
