package org.pipelineframework.orchestrator;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.core.Response;
import java.lang.reflect.InvocationTargetException;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.pipelineframework.LocalPipelineControlPlane;
import org.pipelineframework.orchestrator.dto.HostedExecutionSubmitRequest;

class HostedExecutionAdmissionBoundaryTest {
    @Test
    void disabledRoutesReturn404BeforeAnyNativeProviderEffect() {
        var hosted = hosted(false);
        var local = mock(LocalPipelineControlPlane.class);
        var resource = resource(hosted, local);
        assertEquals(404, resource.get("tenant", "pipeline", "key", "Bearer token").await().indefinitely().getStatus());
        assertEquals(404, resource.admit("tenant", "pipeline", "Bearer token", request()).await().indefinitely().getStatus());
        verifyNoInteractions(local);
    }

    @Test
    void fileAndCustomProvidersFailClosedAtActualNativeSelectorBeforeRouteEffects() throws Exception {
        for (String provider : new String[] {"file", "custom"}) {
            var state = mock(ExecutionStateStore.class);
            when(state.providerName()).thenReturn(provider);
            when(state.supportsExecutionAdmission()).thenReturn(true);
            when(state.supportsLeaseRenewal()).thenReturn(true);
            when(state.startupValidationError()).thenReturn(Optional.empty());
            var config = mock(PipelineOrchestratorConfig.class);
            when(config.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
            var dispatcher = mock(WorkDispatcher.class);
            when(dispatcher.startupValidationError()).thenReturn(Optional.empty());
            var dlq = mock(DeadLetterPublisher.class);
            when(dlq.startupValidationError()).thenReturn(Optional.empty());
            var coordinatorType = Class.forName("org.pipelineframework.QueueAsyncCoordinator");
            var constructor = coordinatorType.getDeclaredConstructor();
            constructor.setAccessible(true);
            var coordinator = constructor.newInstance();
            field(coordinator, "orchestratorConfig", config);
            field(coordinator, "executionStateStore", state);
            field(coordinator, "workDispatcher", dispatcher);
            field(coordinator, "deadLetterPublisher", dlq);
            field(coordinator, "releaseIdentityResolver", new PipelineReleaseIdentityResolver());
            var selector = coordinatorType.getDeclaredMethod("nativeAdmissionStore");
            selector.setAccessible(true);
            var local = mock(LocalPipelineControlPlane.class);
            when(local.nativeAdmissionStore()).thenAnswer(invocation -> {
                try { return selector.invoke(coordinator); }
                catch (InvocationTargetException failure) { throw failure.getCause(); }
            });
            var resource = resource(hosted(true), local);
            assertEquals(501, resource.get("tenant", "pipeline", "key", "Bearer token").await().indefinitely().getStatus());
            assertEquals(501, resource.admit("tenant", "pipeline", "Bearer token", request()).await().indefinitely().getStatus());
            verify(state, never()).createOrGetExecution(any());
            verify(state, never()).createOrGetAdmittedExecution(any());
            verify(state, never()).getExecutionAdmission(any(), any(), any());
            verify(dispatcher, never()).enqueueNow(any());
            verify(local, never()).admitExecution(any(), any(), any());
        }
    }

    @Test
    void payloadResolutionAndMalformedPayloadHaveDistinctPreEffectStatuses() {
        for (boolean unavailable : new boolean[] {true, false}) {
            var hosted = spy(hosted(true));
            hosted.releaseRegistry = mock(org.pipelineframework.orchestrator.release.PipelineReleaseRegistry.class);
            hosted.releaseRegistrar = mock(org.pipelineframework.orchestrator.release.PipelineReleaseRegistrar.class);
            hosted.workerAvailability = mock(org.pipelineframework.orchestrator.worker.PipelineWorkerAvailability.class);
            var release = mock(org.pipelineframework.orchestrator.release.PipelineReleaseRecord.class);
            when(release.contractVersion()).thenReturn("contract");
            when(hosted.releaseRegistry.get("tenant", "pipeline", "release"))
                .thenReturn(Uni.createFrom().item(Optional.of(release)));
            RuntimeException failure = unavailable
                ? new HostedPipelineControlPlaneResource.IngressPayloadTypeResolutionException(new ClassNotFoundException())
                : new IllegalArgumentException("malformed payload");
            doThrow(failure).when(hosted).executionInput(any(HostedExecutionSubmitRequest.class), eq(release));
            var store = mock(NativeExecutionAdmissionStore.class);
            when(store.inspectExistingAdmission(any())).thenReturn(Uni.createFrom().item(Optional.empty()));
            var local = mock(LocalPipelineControlPlane.class);
            when(local.nativeAdmissionStore()).thenReturn(store);
            assertEquals(unavailable ? 503 : 400, resource(hosted, local)
                .admit("tenant", "pipeline", "Bearer token", request()).await().indefinitely().getStatus());
            verifyNoInteractions(hosted.workerAvailability);
            verify(local, never()).admitExecution(any(), any(), any());
        }
    }

    @Test
    void genericDecodeFailureReturns400BeforeAvailabilityOrAdmission() {
        var hosted = spy(hosted(true));
        hosted.releaseRegistry = mock(org.pipelineframework.orchestrator.release.PipelineReleaseRegistry.class);
        hosted.releaseRegistrar = mock(org.pipelineframework.orchestrator.release.PipelineReleaseRegistrar.class);
        hosted.workerAvailability = mock(org.pipelineframework.orchestrator.worker.PipelineWorkerAvailability.class);
        var release = mock(org.pipelineframework.orchestrator.release.PipelineReleaseRecord.class);
        when(release.contractVersion()).thenReturn("contract");
        when(hosted.releaseRegistry.get("tenant", "pipeline", "release"))
            .thenReturn(Uni.createFrom().item(Optional.of(release)));
        doThrow(new RuntimeException("decoder failure")).when(hosted)
            .executionInput(any(HostedExecutionSubmitRequest.class), eq(release));
        var store = mock(NativeExecutionAdmissionStore.class);
        when(store.inspectExistingAdmission(any())).thenReturn(Uni.createFrom().item(Optional.empty()));
        var local = mock(LocalPipelineControlPlane.class);
        when(local.nativeAdmissionStore()).thenReturn(store);
        assertEquals(400, resource(hosted, local).admit("tenant", "pipeline", "Bearer token", request())
            .await().indefinitely().getStatus());
        verifyNoInteractions(hosted.workerAvailability);
        verify(local, never()).admitExecution(any(), any(), any());
    }

    @Test
    void actualTypedSizeFailureMapsTo413WithoutArtifactOrEnqueueEffects() {
        var hosted = hosted(true);
        hosted.releaseRegistry = mock(org.pipelineframework.orchestrator.release.PipelineReleaseRegistry.class);
        hosted.releaseRegistrar = mock(org.pipelineframework.orchestrator.release.PipelineReleaseRegistrar.class);
        hosted.workerAvailability = mock(org.pipelineframework.orchestrator.worker.PipelineWorkerAvailability.class);
        var store = mock(NativeExecutionAdmissionStore.class);
        when(store.inspectExistingAdmission(any())).thenReturn(Uni.createFrom().failure(new ExecutionAdmissionTooLargeException()));
        var local = mock(LocalPipelineControlPlane.class);
        when(local.nativeAdmissionStore()).thenReturn(store);
        assertEquals(413, resource(hosted, local).admit("tenant", "pipeline", "Bearer token", request())
            .await().indefinitely().getStatus());
        verifyNoInteractions(hosted.releaseRegistry, hosted.releaseRegistrar, hosted.workerAvailability);
        verify(store, never()).createOrGetNativeAdmittedExecution(any(), any());
        verify(local, never()).admitExecution(any(), any(), any());
    }

    private static HostedPipelineControlPlaneResource hosted(boolean enabled) {
        var resource = new HostedPipelineControlPlaneResource();
        var config = mock(PipelineOrchestratorConfig.class);
        var control = mock(PipelineOrchestratorConfig.ControlPlaneConfig.class);
        when(config.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        when(config.controlPlane()).thenReturn(control);
        when(control.enabled()).thenReturn(enabled);
        when(control.adminToken()).thenReturn(Optional.of("token"));
        when(control.adminTokenRef()).thenReturn(Optional.empty());
        resource.orchestratorConfig = config;
        resource.secretResolver = new LocalControlPlaneSecretResolver();
        return resource;
    }

    private static HostedExecutionAdmissionResource resource(HostedPipelineControlPlaneResource hosted, LocalPipelineControlPlane local) {
        var resource = new HostedExecutionAdmissionResource();
        resource.hosted = hosted;
        resource.nativeControlPlane = local;
        return resource;
    }

    private static HostedExecutionAdmissionResource.Request request() {
        return new HostedExecutionAdmissionResource.Request("key", "contract", "release", ExecutionInputShape.UNI,
            new SerializedTransitionPayload("java.lang.String", "application/tpf-transition+json", "\"value\""), false);
    }

    private static void field(Object target, String name, Object value) throws Exception {
        var field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
