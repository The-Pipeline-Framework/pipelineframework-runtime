package org.pipelineframework.awsproof;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awsproof.model.ProofAwaitIdentity;
import org.pipelineframework.awsproof.model.ProofBindingStatus;
import org.pipelineframework.awsproof.model.ProofCallbackBinding;
import org.pipelineframework.awsproof.model.ProofStartResponse;
import org.pipelineframework.orchestrator.CoordinatorSweepResult;
import org.pipelineframework.orchestrator.PipelineControlPlane;

class ProofReconciliationIsolationTest {

    @Test
    void reconcilerContinuesAfterOneWakeupFails() {
        ProofCallbackBinding first = binding("execution-1", 1);
        ProofCallbackBinding second = binding("execution-2", 1);
        ProofAwaitReconcilerHandler handler = new ProofAwaitReconcilerHandler();
        handler.controlPlane = mock(PipelineControlPlane.class);
        handler.bindings = mock(ProofCallbackBindingRepository.class);
        handler.wakeups = mock(ProofWakeupService.class);
        handler.resolver = mock(ProofDurableBindingResolver.class);
        handler.recovery = mock(ProofDriverRecoveryService.class);

        when(handler.controlPlane.sweepOnce(anyLong()))
            .thenReturn(Uni.createFrom().item(new CoordinatorSweepResult(1, 1, 0, 0)));
        when(handler.bindings.scanOpen(100)).thenReturn(List.of(first, second));
        when(handler.resolver.reconstructOpenBindings()).thenReturn(List.of());
        when(handler.wakeups.wake(first.awaitIdentity())).thenThrow(new IllegalStateException("first failed"));
        when(handler.wakeups.wake(second.awaitIdentity())).thenReturn(ProofWakeupDisposition.ACKNOWLEDGE);

        assertThat(handler.handleRequest(Map.of(), mock(com.amazonaws.services.lambda.runtime.Context.class)))
            .isEqualTo(3);
        verify(handler.wakeups).wake(first.awaitIdentity());
        verify(handler.wakeups).wake(second.awaitIdentity());
    }

    @Test
    void closedBindingRecoveryContinuesAfterOneProviderLookupFails() {
        ProofCallbackBinding first = binding("execution-1", 1);
        ProofCallbackBinding second = binding("execution-2", 1);
        ProofDriverRecoveryService recovery = new ProofDriverRecoveryService();
        recovery.bindings = mock(ProofCallbackBindingRepository.class);
        recovery.callbacks = mock(ProofDurableCallbackClient.class);
        recovery.starter = mock(ProofDurableExecutionStarter.class);

        when(recovery.bindings.scanOpen(100)).thenReturn(List.of(first, second));
        when(recovery.callbacks.state(first)).thenThrow(new IllegalStateException("history unavailable"));
        when(recovery.callbacks.state(second)).thenReturn(ProofProviderCallbackState.CLOSED);
        when(recovery.starter.start(org.mockito.ArgumentMatchers.any()))
            .thenReturn(new ProofStartResponse("replacement", 202));

        assertThat(recovery.recoverClosedBindings()).isEqualTo(1);
        verify(recovery.callbacks).state(first);
        verify(recovery.callbacks).state(second);
        verify(recovery.bindings).recordDelivered(second, "REPLACEMENT_GENERATION_STARTED");
    }

    private static ProofCallbackBinding binding(String executionId, long generation) {
        return new ProofCallbackBinding(
            new ProofAwaitIdentity("tenant", executionId, "interaction-" + executionId,
                "correlation-" + executionId, generation),
            "provider-name-" + executionId,
            "arn:aws:lambda:us-east-2:123456789012:function:proof:" + executionId,
            "callback-" + executionId,
            ProofBindingStatus.OPEN,
            1,
            2);
    }
}
