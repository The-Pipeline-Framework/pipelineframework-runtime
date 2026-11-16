package org.pipelineframework.orchestrator;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.inject.Instance;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.AwaitTransitionScope;
import org.pipelineframework.orchestrator.release.*;
import org.pipelineframework.orchestrator.worker.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RetainedWorkerDispatchTest {
    @Test
    void authenticatesOnlyTheEntryRetainedOnTheExecutionWithoutProbingWorker() {
        var fixture = fixture();
        assertEquals("env:PINNED", fixture.targets.lifecycleCredentialRef(scope("tenant"), "worker", "entry")
            .await().indefinitely());
        verifyNoInteractions(fixture.targets.rest);
        assertThrows(RuntimeException.class, () -> fixture.targets.lifecycleCredentialRef(scope("other"), "worker", "entry").await().indefinitely());
        assertThrows(RuntimeException.class, () -> fixture.targets.lifecycleCredentialRef(scope("tenant"), "worker", "replacement").await().indefinitely());
    }

    @Test
    void configuredEndpointOrCredentialCannotReplaceRetainedIdentity() {
        var fixture = fixture();
        when(fixture.target.endpoint()).thenReturn("https://replacement.test");
        assertThrows(RuntimeException.class, () -> fixture.targets.lifecycleCredentialRef(scope("tenant"), "worker", "entry").await().indefinitely());
        when(fixture.target.endpoint()).thenReturn("https://worker.test");
        when(fixture.target.sharedSecretRef()).thenReturn("env:REPLACEMENT");
        assertThrows(RuntimeException.class, () -> fixture.targets.lifecycleCredentialRef(scope("tenant"), "worker", "entry").await().indefinitely());
        verifyNoInteractions(fixture.targets.rest);
    }

    @Test
    void changedDeploymentObservationCannotAuthorizeOldExecution() {
        var fixture = fixture();
        when(fixture.execution.dispatchPin()).thenReturn(Optional.of(new ExecutionDispatchPin("entry", "different-operation")));
        assertThrows(RuntimeException.class, () -> fixture.targets.lifecycleCredentialRef(scope("tenant"), "worker", "entry").await().indefinitely());
    }

    @SuppressWarnings("unchecked")
    private static Fixture fixture() {
        var targets = new RegisteredRestWorkerTargets();
        targets.config = mock(PipelineOrchestratorConfig.class, RETURNS_DEEP_STUBS);
        when(targets.config.worker().targetingMode()).thenReturn("registered-rest");
        when(targets.config.workerRest().path()).thenReturn(RestTransitionWorkerProtocol.EXECUTE_PATH);
        when(targets.config.workerRest().capabilitiesPath()).thenReturn(RestTransitionWorkerProtocol.CAPABILITIES_PATH);
        when(targets.config.controlPlane().payloadMaterialization()).thenReturn(PipelineOrchestratorConfig.ControlPlaneConfig.PayloadMaterialization.SERIALIZED);
        when(targets.config.stateProvider()).thenReturn("test");
        when(targets.config.worker().lifecycle().staleAfter()).thenReturn(java.time.Duration.ofMinutes(2));
        var target = mock(PipelineOrchestratorConfig.RegisteredRestTargetConfig.class);
        when(target.tenantId()).thenReturn("tenant"); when(target.pipelineId()).thenReturn("pipeline");
        when(target.contractVersion()).thenReturn("contract"); when(target.releaseVersion()).thenReturn("release");
        when(target.workerId()).thenReturn("worker"); when(target.entryId()).thenReturn(Optional.of("entry"));
        when(target.protocol()).thenReturn("rest"); when(target.endpoint()).thenReturn("https://worker.test");
        when(target.sharedSecretRef()).thenReturn("env:PINNED"); when(target.artifactId()).thenReturn("app");
        when(target.artifactDigest()).thenReturn("sha256:app"); when(targets.config.worker().targets()).thenReturn(Map.of("worker", target));
        var execution = (ExecutionRecord<Object,Object>) mock(ExecutionRecord.class);
        when(execution.executionId()).thenReturn("execution"); when(execution.tenantId()).thenReturn("tenant"); when(execution.pipelineId()).thenReturn("pipeline");
        when(execution.contractVersion()).thenReturn("contract"); when(execution.releaseVersion()).thenReturn("release");
        when(execution.dispatchPin()).thenReturn(Optional.of(new ExecutionDispatchPin("entry", "operation")));
        var store = mock(ExecutionStateStore.class); when(store.providerName()).thenReturn("test");
        when(store.getExecution(anyString(), anyString())).thenReturn(Uni.createFrom().item(Optional.of(execution)));
        targets.executionStores = mock(Instance.class); when(targets.executionStores.stream()).thenAnswer(call -> Stream.of(store));
        var artifact = new PipelineReleaseArtifactDescriptor("app", "application-archive", "file:/app.jar", "sha256:app", List.of(), List.of());
        var deployment = new PipelineReleaseDeployment("process", List.of(new PipelineReleaseDeployment.Entry(
            "entry", "app", "runtime", ".", "example.App", "rest", List.of())), List.of(),
            new PipelineReleaseDeployment.Check("entry", Map.of(), Map.of(), 1000));
        var descriptor = new PipelineReleaseDescriptor(2, "pipeline", "contract", "release", "app", List.of(artifact), deployment);
        var release = new PipelineReleaseRecord("tenant", "pipeline", "contract", "release", PipelineReleaseStatus.REGISTERED,
            descriptor, "app", artifact.digest(), artifact.uri(), 0, "", PipelineContractDescriptor.localFallback(), 1, 1, 0);
        targets.releases = mock(PipelineReleaseRegistry.class);
        when(targets.releases.get("tenant", "pipeline", "release")).thenReturn(Uni.createFrom().item(Optional.of(release)));
        var binding = new PipelineReleaseEntryBinding("tenant", "pipeline", "contract", "release", "entry", "app", "sha256:app",
            "process", "resource", "sha256:code", "operation", Optional.of(new PipelineReleaseDispatchBinding("rest", "https://worker.test", "env:PINNED", "worker")));
        when(targets.releases.getEntryBinding("tenant", "pipeline", "contract", "release", "entry")).thenReturn(Uni.createFrom().item(Optional.of(binding)));
        targets.workers = new InMemoryPipelineWorkerRegistry();
        targets.workers.register(new PipelineWorkerRegistration("tenant", "pipeline", "contract", "release", "worker", "rest", "https://worker.test", "app", "sha256:app"), System.currentTimeMillis()).await().indefinitely();
        targets.rest = mock(RestPipelineTransitionWorker.class);
        return new Fixture(targets, target, execution);
    }

    private static AwaitTransitionScope scope(String tenant) { return new AwaitTransitionScope(tenant, "execution", "pipeline", "contract", "release", 1, "transition"); }
    private record Fixture(RegisteredRestWorkerTargets targets, PipelineOrchestratorConfig.RegisteredRestTargetConfig target, ExecutionRecord<Object,Object> execution) { }
}
