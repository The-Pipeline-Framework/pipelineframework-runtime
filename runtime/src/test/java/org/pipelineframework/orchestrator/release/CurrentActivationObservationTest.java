package org.pipelineframework.orchestrator.release;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;

class CurrentActivationObservationTest {
    @Test
    void memoryIdentitySupersedesSameReleaseTieAndRegressingClockWithoutChangingOldReceipt() {
        var registry = new InMemoryPipelineReleaseRegistry();
        assertEquals(CurrentActivationObservation.State.NONE, registry.currentActivationObservation("tenant", "pipeline").await().indefinitely().state());
        var release = release(PipelineReleaseStatus.REGISTERED);
        registry.register(release).await().indefinitely();
        var first = registry.activateOnce(new ActivationOperationCommand("first", release, 3000)).await().indefinitely();
        var second = registry.activateOnce(new ActivationOperationCommand("second", release, 3000)).await().indefinitely();
        assertNotEquals(first.activationId(), second.activationId());
        var current = registry.currentActivationObservation("tenant", "pipeline").await().indefinitely();
        assertEquals(second.activationId(), current.identifiedEvent().orElseThrow().activationId());
        assertEquals(second.immutableReleaseIdentity(), current.identifiedEvent().orElseThrow().immutableReleaseIdentity());
        registry.activateOnce(new ActivationOperationCommand("first", release, 9000)).await().indefinitely();
        assertEquals(current, registry.currentActivationObservation("tenant", "pipeline").await().indefinitely());
        assertEquals(first, registry.getActivationOperation("tenant", "pipeline", "first").await().indefinitely().orElseThrow());
        registry.activate("tenant", "pipeline", "release", 2000).await().indefinitely();
        var legacyApiEvent = registry.currentActivationObservation("tenant", "pipeline").await().indefinitely().identifiedEvent().orElseThrow();
        assertNotEquals(second.activationId(), legacyApiEvent.activationId());
        assertEquals(2000, legacyApiEvent.activatedAtEpochMs());
        assertEquals(CurrentActivationObservation.State.NONE, registry.currentActivationObservation("other", "pipeline").await().indefinitely().state());
    }

    @Test
    void importedMemoryActiveWithoutEventIsExplicitlyUnknownNotFabricated() {
        var registry = new InMemoryPipelineReleaseRegistry();
        registry.register(release(PipelineReleaseStatus.ACTIVE)).await().indefinitely();
        var current = registry.currentActivationObservation("tenant", "pipeline").await().indefinitely();
        assertEquals(CurrentActivationObservation.State.LEGACY_UNKNOWN, current.state());
        assertTrue(current.identifiedEvent().isEmpty());
        assertTrue(registry.getActivationOperation("tenant", "pipeline", "unknown").await().indefinitely().isEmpty());
    }

    @Test
    void guardedReadOnlyRoutePreservesAuthAndUnsupportedDefaults() {
        var admin = new HostedReleaseAdminResource();
        admin.orchestratorConfig = mock(PipelineOrchestratorConfig.class);
        var config = mock(PipelineOrchestratorConfig.AdminConfig.class);
        when(admin.orchestratorConfig.admin()).thenReturn(config);
        when(config.adminToken()).thenReturn(Optional.of("token"));
        when(config.adminTokenRef()).thenReturn(Optional.empty());
        admin.releaseRegistrar = mock(PipelineReleaseRegistrar.class);
        admin.releaseRegistry = new InMemoryPipelineReleaseRegistry();
        var resource = new HostedCurrentActivationResource();
        resource.admin = admin;
        assertEquals(404, resource.get("tenant", "pipeline", "Bearer token").await().indefinitely().getStatus());
        when(config.enabled()).thenReturn(true);
        assertEquals(401, resource.get("tenant", "pipeline", null).await().indefinitely().getStatus());
        assertEquals(400, resource.get("tenant", "", "Bearer token").await().indefinitely().getStatus());
        assertEquals(200, resource.get("tenant", "pipeline", "Bearer token").await().indefinitely().getStatus());
        var unsupported = mock(PipelineReleaseRegistry.class, CALLS_REAL_METHODS);
        admin.releaseRegistry = unsupported;
        assertFalse(unsupported.supportsCurrentActivationObservation());
        assertEquals(501, resource.get("tenant", "pipeline", "Bearer token").await().indefinitely().getStatus());
        verify(unsupported, never()).activate(any(), any(), any(), anyLong());
        verify(unsupported, never()).activateOnce(any());
        verifyNoInteractions(admin.releaseRegistrar);
    }

    static PipelineReleaseRecord release(PipelineReleaseStatus status) {
        return new PipelineReleaseRecord("tenant", "pipeline", "contract", "release", status,
            new PipelineReleaseDescriptor(1, "pipeline", "contract", "release", "artifact", List.of()),
            "artifact", "sha256:artifact", "file:/artifact.jar", 1, "artifact", null, 1, 1, 0);
    }
}
