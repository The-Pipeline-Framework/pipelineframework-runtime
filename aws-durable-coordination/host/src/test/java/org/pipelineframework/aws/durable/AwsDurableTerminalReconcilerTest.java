package org.pipelineframework.aws.durable;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.time.Duration;
import java.util.Optional;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.pipelineframework.aws.durable.model.*;
import org.pipelineframework.orchestrator.*;

class AwsDurableTerminalReconcilerTest {
    private static final String ARN = "arn:aws:lambda:eu-west-1:123456789012:function:driver:7/durable-execution/original/id";
    private final AwsDurableCallbackBindingRepository bindings = mock(AwsDurableCallbackBindingRepository.class);
    private final PipelineControlPlane controlPlane = mock(PipelineControlPlane.class);
    private final AwsDurableExecutionStarter starter = mock(AwsDurableExecutionStarter.class);
    private final AwsDurableExecutionStarter pinned = mock(AwsDurableExecutionStarter.class);
    private final AwsDurableProviderExecutionInspector provider = mock(AwsDurableProviderExecutionInspector.class);
    private final AwsDurableExecutionCheckpoint checkpoint = new AwsDurableExecutionCheckpoint("tenant", "execution", "pipeline", "contract", "release");
    private final AwsDurableTerminalReconciler reconciler = new AwsDurableTerminalReconciler(bindings, controlPlane,
        starter, provider, Duration.ofSeconds(1));

    private void seed(ExecutionStatus status) {
        when(provider.inspect(ARN)).thenReturn(AwsDurableProviderExecutionState.MISSING);
        when(bindings.findRecoveryByProviderExecutionArn(ARN)).thenReturn(Optional.of(
            new AwsDurableRecoveryLocator(new AwsDurableDriverCheckpoint(checkpoint, 1), "original", ARN)));
        when(controlPlane.getExecutionSemanticCheckpoint("tenant", "execution")).thenReturn(Uni.createFrom().item(
            new ExecutionSemanticCheckpoint("tenant", "execution", "pipeline", "contract", "release", status)));
        when(starter.pinnedToExecution(ARN)).thenReturn(pinned);
        when(bindings.claimGeneration(new AwsDurableDriverCheckpoint(checkpoint, 2))).thenReturn(AwsDurableGenerationAdmission.CREATED);
        when(pinned.start(any())).thenReturn(new AwsDurableStartResponse("replacement", 202));
    }

    @ParameterizedTest
    @EnumSource(value = ExecutionStatus.class, names = {"QUEUED", "RUNNING", "WAIT_RETRY", "WAITING_EXTERNAL"})
    void reconstructsBeforeFirstRegistrationOrAfterDisposableCallbackRowsAreLost(ExecutionStatus status) {
        seed(status);
        assertThat(reconciler.reconcile(ARN)).isTrue();
        var input = org.mockito.ArgumentCaptor.forClass(AwsDurableExecutionInput.class);
        verify(pinned).start(input.capture());
        assertThat(input.getValue().resumeExecutionId()).contains("execution");
        assertThat(input.getValue().generation()).isEqualTo(2);
        assertThat(input.getValue().releaseVersion()).isEqualTo("release");
        verify(controlPlane, never()).executePipelineAsync(any(), anyString(), anyString(), anyBoolean());
        verify(bindings, never()).findRegistrationByProviderExecutionArn(anyString());
        verify(bindings, never()).bind(any());
        verify(starter, never()).start(any());
    }

    @Test
    void lostStartAcknowledgementReplaysFencedStartInsteadOfRetiringClaimOnly() {
        seed(ExecutionStatus.WAITING_EXTERNAL);
        when(bindings.claimGeneration(any())).thenReturn(AwsDurableGenerationAdmission.CREATED, AwsDurableGenerationAdmission.REPLAYED);
        when(pinned.start(any())).thenThrow(new IllegalStateException("lost acknowledgement"))
            .thenReturn(new AwsDurableStartResponse("replacement", 202));
        assertThatThrownBy(() -> reconciler.reconcile(ARN)).hasMessageContaining("lost acknowledgement");
        assertThat(reconciler.reconcile(ARN)).isTrue();
        verify(pinned, times(2)).start(any());
    }

    @Test
    void laggingRecoveryIndexAndSemanticReadFailuresRemainRetryable() {
        when(provider.inspect(ARN)).thenReturn(AwsDurableProviderExecutionState.REPLACEABLE);
        when(bindings.findRecoveryByProviderExecutionArn(ARN)).thenReturn(Optional.empty());
        assertThatThrownBy(() -> reconciler.reconcile(ARN)).hasMessageContaining("locator is not visible");
        seed(ExecutionStatus.WAITING_EXTERNAL);
        when(controlPlane.getExecutionSemanticCheckpoint("tenant", "execution"))
            .thenReturn(Uni.createFrom().failure(new IllegalStateException("semantic unavailable")));
        assertThatThrownBy(() -> reconciler.reconcile(ARN)).hasMessageContaining("semantic unavailable");
        verifyNoInteractions(pinned);
    }

    @ParameterizedTest
    @EnumSource(value = ExecutionStatus.class, names = {"SUCCEEDED", "FAILED", "DLQ", "REMOTE_OUTCOME_UNKNOWN"})
    void semanticTerminalStateNeverStartsReplacementBusinessWork(ExecutionStatus status) {
        seed(status);
        assertThat(reconciler.reconcile(ARN)).isFalse();
        verify(bindings, never()).claimGeneration(any());
        verifyNoInteractions(pinned);
    }

    @ParameterizedTest
    @EnumSource(value = AwsDurableProviderExecutionState.class, names = {"ACTIVE", "SUCCEEDED"})
    void liveOrSuccessfulProviderExecutionDoesNotNeedRepair(AwsDurableProviderExecutionState state) {
        when(provider.inspect(ARN)).thenReturn(state);
        assertThat(reconciler.reconcile(ARN)).isFalse();
        verifyNoInteractions(bindings, controlPlane, starter);
    }

    @Test
    void laterGenerationFencesOldRepairAndPinMismatchFailsClosed() {
        seed(ExecutionStatus.WAITING_EXTERNAL);
        when(bindings.claimGeneration(any())).thenReturn(AwsDurableGenerationAdmission.STALE);
        assertThat(reconciler.reconcile(ARN)).isFalse();
        verifyNoInteractions(pinned);
        when(controlPlane.getExecutionSemanticCheckpoint("tenant", "execution")).thenReturn(Uni.createFrom().item(
            new ExecutionSemanticCheckpoint("tenant", "execution", "pipeline", "contract", "other-release", ExecutionStatus.RUNNING)));
        assertThatThrownBy(() -> reconciler.reconcile(ARN)).hasMessageContaining("identity and release");
    }
}
