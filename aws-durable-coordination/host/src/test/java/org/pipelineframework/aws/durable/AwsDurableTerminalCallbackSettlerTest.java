package org.pipelineframework.aws.durable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import java.time.Duration;
import java.util.Optional;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.pipelineframework.aws.durable.model.*;
import org.pipelineframework.orchestrator.*;
import software.amazon.awssdk.services.lambda.model.CallbackTimeoutException;

class AwsDurableTerminalCallbackSettlerTest {
    private final PipelineControlPlane controlPlane = mock(PipelineControlPlane.class);
    private final AwsDurableCallbackBindingRepository bindings = mock(AwsDurableCallbackBindingRepository.class);
    private final AwsDurableCallbackClient callbacks = mock(AwsDurableCallbackClient.class);
    private final AwsDurableAwaitIdentity identity = new AwsDurableAwaitIdentity("tenant", "execution", "interaction", "correlation", 2, 3);
    private final AwsDurableCallbackRegistration registration = new AwsDurableCallbackRegistration(
        new AwsDurableDriverCheckpoint(new AwsDurableExecutionCheckpoint("tenant", "execution", "pipeline", "contract", "release"), 2),
        "provider", "arn:provider", "callback", 10, 100, Optional.of(identity));
    private final AwsDurableTerminalCallbackSettler settler = new AwsDurableTerminalCallbackSettler(
        controlPlane, bindings, callbacks, Duration.ofSeconds(1));

    private void terminal(ExecutionStatus status) {
        when(controlPlane.getExecutionSemanticCheckpoint("tenant", "execution")).thenReturn(Uni.createFrom().item(
            new ExecutionSemanticCheckpoint("tenant", "execution", "pipeline", "contract", "release", status)));
        when(bindings.isCurrentGeneration("tenant", "execution", 2)).thenReturn(true);
    }

    @ParameterizedTest
    @EnumSource(value = ExecutionStatus.class, names = {"FAILED", "DLQ", "REMOTE_OUTCOME_UNKNOWN"})
    void terminalSemanticStateFailsCallbackWithoutCompletionOrReplacement(ExecutionStatus status) {
        terminal(status);
        assertThat(settler.settle(registration)).contains(AwsDurableWakeupDisposition.ACKNOWLEDGE);
        var binding = registration.bind(identity);
        verify(callbacks).sendFailure(binding, status.name());
        verify(callbacks, never()).sendSuccess(any(), any());
        verify(bindings).recordDelivered(binding, "TERMINAL_CALLBACK_ACCEPTED");
        verify(controlPlane, never()).completeAwait(any());
    }

    @Test
    void executionEventAfterRegistrationAndRegistrationAfterFailureConverge() {
        terminal(ExecutionStatus.FAILED);
        when(bindings.findLatestRegistration("tenant", "execution")).thenReturn(Optional.of(registration));
        assertThat(settler.settleExecution("tenant", "execution")).isEqualTo(AwsDurableWakeupDisposition.ACKNOWLEDGE);
        when(bindings.delivered(registration.bind(identity))).thenReturn(true);
        assertThat(settler.settle(registration)).contains(AwsDurableWakeupDisposition.ACKNOWLEDGE);
        verify(callbacks, times(1)).sendFailure(any(), anyString());
    }

    @Test
    void staleTerminalNotificationCannotFailAStillRunningExecution() {
        terminal(ExecutionStatus.RUNNING);
        assertThat(settler.settle(registration)).isEmpty();
        verifyNoInteractions(bindings, callbacks);
    }

    @Test
    void supersededGenerationCannotFailCallbackAndMissingClaimRemainsRetryable() {
        terminal(ExecutionStatus.FAILED);
        when(bindings.isSuperseded("tenant", "execution", 2)).thenReturn(true);
        assertThat(settler.settle(registration)).contains(AwsDurableWakeupDisposition.ACKNOWLEDGE);
        when(bindings.isSuperseded("tenant", "execution", 2)).thenReturn(false);
        when(bindings.isCurrentGeneration("tenant", "execution", 2)).thenReturn(false);
        assertThat(settler.settle(registration)).contains(AwsDurableWakeupDisposition.RETRY);
        verifyNoInteractions(callbacks);
    }

    @ParameterizedTest
    @EnumSource(AwsDurableProviderCallbackState.class)
    void uncertainDeliveryUsesProviderStateWithoutReconstructingTerminalBusinessWork(AwsDurableProviderCallbackState state) {
        terminal(ExecutionStatus.FAILED);
        doThrow(CallbackTimeoutException.builder().message("unknown delivery").build()).when(callbacks).sendFailure(any(), anyString());
        when(callbacks.state(any())).thenReturn(state);
        var expected = state == AwsDurableProviderCallbackState.OPEN || state == AwsDurableProviderCallbackState.UNKNOWN
            ? AwsDurableWakeupDisposition.RETRY : AwsDurableWakeupDisposition.ACKNOWLEDGE;
        assertThat(settler.settle(registration)).contains(expected);
        if (expected == AwsDurableWakeupDisposition.RETRY) verify(bindings, never()).recordDelivered(any(), anyString());
        verify(controlPlane, never()).redriveExecution(anyString(), anyString(), anyLong(), anyBoolean(), anyString());
    }

    @Test
    void transientProviderFailureRemainsRetryable() {
        terminal(ExecutionStatus.FAILED);
        doThrow(new IllegalStateException("transient")).when(callbacks).sendFailure(any(), anyString());
        assertThat(settler.settle(registration)).contains(AwsDurableWakeupDisposition.RETRY);
        verify(bindings, never()).recordDelivered(any(), anyString());
    }
}
