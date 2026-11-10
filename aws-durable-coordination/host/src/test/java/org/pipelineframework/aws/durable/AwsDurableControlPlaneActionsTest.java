package org.pipelineframework.aws.durable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.pipelineframework.orchestrator.AwaitSemanticSelection;
import org.pipelineframework.awaitable.AwaitInteractionCursor;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.aws.durable.model.AwsDurableActionRequest;
import org.pipelineframework.aws.durable.model.AwsDurableDriverCheckpoint;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionCheckpoint;
import org.pipelineframework.aws.durable.model.AwsDurableOperation;
import org.pipelineframework.orchestrator.AwaitSemanticCheckpoint;
import org.pipelineframework.orchestrator.PipelineControlPlane;
import org.pipelineframework.orchestrator.PipelineRuntimeIdentityObservation;

class AwsDurableControlPlaneActionsTest {

    @Test
    void identityReportsActualRuntimeOnlyForTheExactRequestedRelease() throws Exception {
        var controlPlane = mock(PipelineControlPlane.class);
        var actual = new PipelineRuntimeIdentityObservation("pipeline-1", "contract-1", "release-1", true, 3, null,
            "dynamo", true, "command-effects", "dynamo", "query-captures");
        when(controlPlane.runtimeIdentity("tenant-1")).thenReturn(Uni.createFrom().item(actual));
        var actions = actions(controlPlane);
        var response = actions.invoke(identityRequest("release-1"));
        var parsed = new ObjectMapper().readValue(response.resultJson().orElseThrow(), PipelineRuntimeIdentityObservation.class);
        assertThat(parsed).isEqualTo(actual);
        assertThatThrownBy(() -> actions.invoke(identityRequest("foreign-release")))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("requested Release")
            .hasMessageContaining("pipelineId=pipeline-1, contractVersion=contract-1, releaseVersion=release-1");
        assertThatThrownBy(() -> actions.invoke(identityRequest(null)))
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("releaseVersion");
    }

    private static AwsDurableActionRequest identityRequest(String release) {
        return new AwsDurableActionRequest(AwsDurableOperation.READ_RUNTIME_IDENTITY, "tenant-1",
            Optional.empty(), Optional.empty(), Optional.of("pipeline-1"), Optional.of("contract-1"),
            Optional.ofNullable(release), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
            Optional.empty(), Optional.empty(), Optional.empty(), 1);
    }

    @Test
    void currentAwaitUsesSemanticLocatorInsteadOfHistoricalOrdering() {
        PipelineControlPlane controlPlane = mock(PipelineControlPlane.class);
        when(controlPlane.getCurrentAwaitSemanticCheckpoint("tenant-1", "execution-1", Optional.empty()))
            .thenReturn(Uni.createFrom().item(new AwaitSemanticSelection(Optional.of(checkpoint("waiting", AwaitInteractionStatus.WAITING)), Optional.empty())));

        var response = actions(controlPlane).invoke(AwsDurableActionRequest.pendingAwait(driverCheckpoint()));

        assertThat(response.awaitIdentity()).hasValueSatisfying(identity -> {
            assertThat(identity.executionId()).isEqualTo("execution-1");
            assertThat(identity.interactionId()).isEqualTo("waiting");
        });
        verify(controlPlane).getCurrentAwaitSemanticCheckpoint("tenant-1", "execution-1", Optional.empty());
        verify(controlPlane, never()).getAwaitSemanticCheckpoints("tenant-1", "execution-1", 100);
        verify(controlPlane, never()).queryPendingAwaitInteractions("tenant-1", "", "", "", 100);
    }

    @Test
    void pendingAwaitReturnsEmptyWhenExecutionHasNoPendingInteraction() {
        PipelineControlPlane controlPlane = mock(PipelineControlPlane.class);
        when(controlPlane.getCurrentAwaitSemanticCheckpoint("tenant-1", "execution-1", Optional.empty()))
            .thenReturn(Uni.createFrom().item(new AwaitSemanticSelection(Optional.empty(), Optional.empty())));

        assertThat(actions(controlPlane).invoke(AwsDurableActionRequest.pendingAwait(driverCheckpoint()))
            .awaitIdentity()).isEmpty();
    }

    @Test
    void completionBeforeRegistrationStillReturnsCurrentAwaitIdentity() {
        PipelineControlPlane controlPlane = mock(PipelineControlPlane.class);
        when(controlPlane.getCurrentAwaitSemanticCheckpoint("tenant-1", "execution-1", Optional.empty()))
            .thenReturn(Uni.createFrom().item(new AwaitSemanticSelection(Optional.of(checkpoint("completed", AwaitInteractionStatus.COMPLETED)), Optional.empty())));
        assertThat(actions(controlPlane).invoke(AwsDurableActionRequest.pendingAwait(driverCheckpoint()))
            .awaitIdentity()).hasValueSatisfying(identity -> assertThat(identity.interactionId()).isEqualTo("completed"));
    }

    @Test
    void emptyPageContinuationIsPassedWithoutDecodingOrHistoryLookup() {
        PipelineControlPlane controlPlane = mock(PipelineControlPlane.class);
        var cursor = new AwaitInteractionCursor("tenant-1", "unit-1", "opaque-position");
        when(controlPlane.getCurrentAwaitSemanticCheckpoint("tenant-1", "execution-1", Optional.empty()))
            .thenReturn(Uni.createFrom().item(new AwaitSemanticSelection(Optional.empty(), Optional.of(cursor))));
        when(controlPlane.getCurrentAwaitSemanticCheckpoint("tenant-1", "execution-1", Optional.of(cursor)))
            .thenReturn(Uni.createFrom().item(new AwaitSemanticSelection(
                Optional.of(checkpoint("beyond-page-one", AwaitInteractionStatus.DISPATCHED)), Optional.empty())));
        var first = actions(controlPlane).invoke(AwsDurableActionRequest.pendingAwait(driverCheckpoint()));
        assertThat(first.awaitIdentity()).isEmpty();
        assertThat(first.awaitSelectionCursor()).contains(cursor);
        var second = actions(controlPlane).invoke(AwsDurableActionRequest.pendingAwait(driverCheckpoint())
            .withAwaitSelectionCursor(first.awaitSelectionCursor()));
        assertThat(second.awaitIdentity()).hasValueSatisfying(identity ->
            assertThat(identity.interactionId()).isEqualTo("beyond-page-one"));
        assertThat(second.awaitSelectionCursor()).isEmpty();
        verify(controlPlane, never()).getAwaitSemanticCheckpoints("tenant-1", "execution-1", 100);
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
