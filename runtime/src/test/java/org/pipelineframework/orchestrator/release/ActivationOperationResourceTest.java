package org.pipelineframework.orchestrator.release;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.List;
import java.util.Optional;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;

class ActivationOperationResourceTest {
    @TempDir Path directory;
    private HostedActivationOperationResource resource;
    private HostedReleaseAdminResource admin;
    private PipelineOrchestratorConfig.AdminConfig adminConfig;

    @BeforeEach
    void setUp() {
        admin = new HostedReleaseAdminResource();
        admin.orchestratorConfig = mock(PipelineOrchestratorConfig.class);
        adminConfig = mock(PipelineOrchestratorConfig.AdminConfig.class);
        when(admin.orchestratorConfig.admin()).thenReturn(adminConfig);
        when(adminConfig.enabled()).thenReturn(true);
        when(adminConfig.adminToken()).thenReturn(Optional.of("token"));
        when(adminConfig.adminTokenRef()).thenReturn(Optional.empty());
        admin.releaseRegistry = new InMemoryPipelineReleaseRegistry();
        admin.releaseRegistrar = mock(PipelineReleaseRegistrar.class);
        resource = new HostedActivationOperationResource();
        resource.admin = admin;
    }

    @Test
    void defaultDisabledAndAuthenticationGuardsPrecedeEffects() {
        when(adminConfig.enabled()).thenReturn(false);
        assertEquals(404, resource.activate("tenant", "pipeline", "Bearer token", request("key", "a")).await().indefinitely().getStatus());
        when(adminConfig.enabled()).thenReturn(true);
        assertEquals(401, resource.activate("tenant", "pipeline", null, request("key", "a")).await().indefinitely().getStatus());
        assertEquals(401, resource.get("tenant", "pipeline", "key", "Bearer wrong").await().indefinitely().getStatus());
        assertTrue(admin.registry().active("tenant", "pipeline").await().indefinitely().isEmpty());
        verifyNoInteractions(admin.releaseRegistrar);
    }

    @Test
    void missingKeyPinAndWrongScopeDoNotActivate() {
        assertEquals(400, resource.activate("tenant", "pipeline", "Bearer token", request("", "a")).await().indefinitely().getStatus());
        assertEquals(400, resource.activate("tenant", "pipeline", "Bearer token", new HostedActivationOperationResource.Request("key", "", "a")).await().indefinitely().getStatus());
        admin.registry().register(release("a")).await().indefinitely();
        assertEquals(404, resource.activate("other", "pipeline", "Bearer token", request("key", "a")).await().indefinitely().getStatus());
        assertEquals(404, resource.activate("tenant", "other", "Bearer token", request("key", "a")).await().indefinitely().getStatus());
        assertEquals(409, resource.activate("tenant", "pipeline", "Bearer token", new HostedActivationOperationResource.Request("key", "wrong", "a")).await().indefinitely().getStatus());
        assertTrue(admin.registry().active("tenant", "pipeline").await().indefinitely().isEmpty());
        verifyNoInteractions(admin.releaseRegistrar);
    }

    @Test
    void memoryReplayAfterBReturnsOriginalReceiptWithoutReverificationOrActivation() {
        admin.registry().register(release("a")).await().indefinitely();
        admin.registry().register(release("b")).await().indefinitely();
        var first = resource.activate("tenant", "pipeline", "Bearer token", request("key", "a")).await().indefinitely();
        assertEquals(200, first.getStatus());
        ActivationOperationReceipt original = assertInstanceOf(ActivationOperationReceipt.class, first.getEntity());
        admin.registry().activate("tenant", "pipeline", "b", 2000).await().indefinitely();
        doThrow(new IllegalStateException("artifact unavailable")).when(admin.releaseRegistrar).verify(any());
        var replay = resource.activate("tenant", "pipeline", "Bearer token", request("key", "a")).await().indefinitely();
        assertEquals(200, replay.getStatus());
        assertEquals(original, replay.getEntity());
        assertEquals(original, resource.get("tenant", "pipeline", "key", "Bearer token").await().indefinitely().getEntity());
        assertEquals(409, resource.activate("tenant", "pipeline", "Bearer token", request("key", "b")).await().indefinitely().getStatus());
        assertEquals("b", admin.registry().active("tenant", "pipeline").await().indefinitely().orElseThrow().releaseVersion());
        verify(admin.releaseRegistrar, times(1)).verify(any());
    }

    @Test
    void unknownInquiryAndUnavailableArtifactCreateNoReceipt() {
        admin.registry().register(release("a")).await().indefinitely();
        assertEquals(404, resource.get("tenant", "pipeline", "key", "Bearer token").await().indefinitely().getStatus());
        doThrow(new IllegalStateException("artifact unavailable")).when(admin.releaseRegistrar).verify(any());
        assertEquals(409, resource.activate("tenant", "pipeline", "Bearer token", request("key", "a")).await().indefinitely().getStatus());
        assertTrue(admin.registry().getActivationOperation("tenant", "pipeline", "key").await().indefinitely().isEmpty());
        assertTrue(admin.registry().active("tenant", "pipeline").await().indefinitely().isEmpty());
    }

    @Test
    void unsupportedFileAndCustomStoresFailBeforeEffects() {
        for (PipelineReleaseRegistry registry : List.of(new FileBackedPipelineReleaseRegistry(directory),
                mock(PipelineReleaseRegistry.class, CALLS_REAL_METHODS))) {
            admin.releaseRegistry = registry;
            assertFalse(registry.supportsActivationOperations());
            assertEquals(501, resource.activate("tenant", "pipeline", "Bearer token", request("key", "a")).await().indefinitely().getStatus());
            assertEquals(501, resource.get("tenant", "pipeline", "key", "Bearer token").await().indefinitely().getStatus());
            assertThrows(UnsupportedOperationException.class, () -> registry.activateOnce(new ActivationOperationCommand("key", release("a"), 2000))
                .await().indefinitely());
        }
        verifyNoInteractions(admin.releaseRegistrar);
    }

    private static HostedActivationOperationResource.Request request(String key, String version) {
        return new HostedActivationOperationResource.Request(key, "contract", version);
    }

    private static PipelineReleaseRecord release(String version) {
        return new PipelineReleaseRecord("tenant", "pipeline", "contract", version, PipelineReleaseStatus.REGISTERED,
            new PipelineReleaseDescriptor(1, "pipeline", "contract", version, "artifact", List.of()),
            "artifact", "sha256:artifact", "file:/artifact.jar", 1, "artifact", null, 1000, 1000, 0);
    }
}
