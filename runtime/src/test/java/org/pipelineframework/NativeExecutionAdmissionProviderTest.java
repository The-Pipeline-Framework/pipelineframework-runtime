package org.pipelineframework;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.*;

class NativeExecutionAdmissionProviderTest {
    @Test
    void customPortableCreationAdvertisementCannotAuthorizeStrictNativeAdmission() {
        var store = mock(ExecutionStateStore.class);
        when(store.providerName()).thenReturn("custom");
        when(store.supportsExecutionAdmission()).thenReturn(true);
        when(store.supportsLeaseRenewal()).thenReturn(true);
        when(store.startupValidationError()).thenReturn(Optional.empty());
        var config = mock(PipelineOrchestratorConfig.class);
        when(config.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        var dispatcher = mock(WorkDispatcher.class);
        when(dispatcher.startupValidationError()).thenReturn(Optional.empty());
        var dlq = mock(DeadLetterPublisher.class);
        when(dlq.startupValidationError()).thenReturn(Optional.empty());
        var coordinator = new QueueAsyncCoordinator();
        coordinator.orchestratorConfig = config;
        coordinator.executionStateStore = store;
        coordinator.workDispatcher = dispatcher;
        coordinator.deadLetterPublisher = dlq;
        coordinator.releaseIdentityResolver = new PipelineReleaseIdentityResolver();
        assertThrows(UnsupportedOperationException.class, coordinator::nativeAdmissionStore);
        verify(store, never()).createOrGetExecution(any());
        verify(store, never()).createOrGetAdmittedExecution(any());
        verify(store, never()).getExecutionAdmission(any(), any(), any());
        verify(dispatcher, never()).enqueueNow(any());
    }
}
