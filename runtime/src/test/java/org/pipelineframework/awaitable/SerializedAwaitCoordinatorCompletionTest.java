package org.pipelineframework.awaitable;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.inject.Instance;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.spi.AwaitInteractionStore;
import org.pipelineframework.awaitable.spi.AwaitUnitStore;
import org.pipelineframework.awaitable.store.InMemoryAwaitInteractionStore;
import org.pipelineframework.awaitable.store.InMemoryAwaitUnitStore;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.*;
import org.pipelineframework.orchestrator.release.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

/** Real authoritative store/fences with a controlled remote pure-operation boundary. No application classes exist. */
class SerializedAwaitCoordinatorCompletionTest {
    private InMemoryAwaitInteractionStore store;
    private InMemoryAwaitUnitStore units;
    private RegisteredRestWorkerTargets workers;
    private AwaitCoordinator coordinator;
    private AwaitInteractionRecord original;
    private PipelineOrchestratorConfig config;
    private AwaitDurablePayloadResolver resolver;
    private long now;
    private final JsonTransitionPayloadCodec wire = new JsonTransitionPayloadCodec();

    @Test void typedConnectorIngressFailsClearlyWithoutDescriptorLookup() {
        var failure = assertThrows(UnsupportedOperationException.class,
            () -> coordinator.callbackDescriptor(original));
        assertTrue(failure.getMessage().contains("authenticated completion boundary"));
        verifyNoInteractions(workers);
    }

    @Test void canonicalCompletionRetainsBinaryEnvelopeAcrossCoordinatorNormalization() throws Exception {
        var canonical = payload("Output", "canonical-reply");
        doAnswer(call -> {
            var request = call.getArgument(0, AwaitValueOperationRequest.class);
            var decoded = assertInstanceOf(TypedDurablePayload.class, wire.decode(request.completionPayload().orElseThrow()));
            assertEquals(canonical.canonicalTypeId(), decoded.canonicalTypeId());
            assertArrayEquals(canonical.payload(), decoded.payload());
            return Uni.createFrom().item(reply(request, "projected"));
        }).when(workers).executeAwaitValueOperation(any());
        var command = new AwaitCompletionCommand("tenant", original.interactionId(), null, "canonical-completion", canonical,
            "actor", now + 1);
        assertEquals(AwaitInteractionStatus.COMPLETED, coordinator.complete(command).await().indefinitely().record().status());
    }

    @BeforeEach void setUp() throws Exception {
        now = System.currentTimeMillis();
        store = spy(new InMemoryAwaitInteractionStore());
        units = spy(new InMemoryAwaitUnitStore());
        workers = mock(RegisteredRestWorkerTargets.class);
        config = mock(PipelineOrchestratorConfig.class);
        var control = mock(PipelineOrchestratorConfig.ControlPlaneConfig.class);
        when(config.controlPlane()).thenReturn(control);
        when(control.payloadMaterialization()).thenReturn(PipelineOrchestratorConfig.ControlPlaneConfig.PayloadMaterialization.SERIALIZED);
        var contract = new PipelineContractDescriptor(2, "pipeline", "contract", "hash", null, null, null, false, null,
            List.of(new PipelineBundleStepDescriptor(0, "Review", "internal", "ONE_TO_ONE", "Request", "Output", null, null,
                Map.of("runtimeDescriptor", descriptor()))), PipelineBundleCapabilities.defaults(),
            Map.of("Request", definition("Request"), "Output", definition("Output")), "catalog");
        var release = mock(PipelineReleaseRecord.class);
        when(release.tenantId()).thenReturn("tenant"); when(release.pipelineId()).thenReturn("pipeline");
        when(release.contractVersion()).thenReturn("contract"); when(release.releaseVersion()).thenReturn("release-original");
        when(release.contract()).thenReturn(contract);
        var releases = mock(PipelineReleaseRegistry.class);
        when(releases.get("tenant", "pipeline", "release-original")).thenReturn(Uni.createFrom().item(Optional.of(release)));
        var executions = mock(ExecutionStateStore.class);
        @SuppressWarnings("unchecked") ExecutionRecord<Object, Object> execution = mock(ExecutionRecord.class);
        when(execution.pipelineId()).thenReturn("pipeline"); when(execution.contractVersion()).thenReturn("contract");
        when(execution.releaseVersion()).thenReturn("release-original");
        when(executions.getExecution("tenant", "execution")).thenReturn(Uni.createFrom().item(Optional.of(execution)));
        resolver = new AwaitDurablePayloadResolver(); resolver.orchestratorConfig = config;
        resolver.executionStateStore = executions; resolver.releaseRegistry = releases;
        original = store.createOrGet(new AwaitCreateCommand("tenant", "execution", "Review", 0,
            "absent.application.Output", "absent.application.Output", "cause", "occurrence", "correlation",
            payload("Request", "retained-request"), "", "", "interaction-api", "unit", null, Map.of("tpf.await.completion.projector", "original-projector"), now,
            now + 60_000, now / 1000 + 3600)).await().indefinitely().record();
        units.createOrGet(new AwaitUnitCreateCommand("tenant", "unit", "execution", "Review", 0, "ONE_TO_ONE", now, now / 1000 + 3600))
            .await().indefinitely();
        units.attachPrimaryInteraction("tenant", "unit", original.interactionId(), now).await().indefinitely().orElseThrow();
        coordinator = fresh();
        when(workers.executeAwaitValueOperation(any())).thenAnswer(call -> {
            var request = call.getArgument(0, AwaitValueOperationRequest.class);
            assertEquals("release-original", request.releaseVersion());
            assertEquals(Optional.of("original-projector"), request.completionProjectorId());
            assertEquals("pipeline", request.pipelineId());
            assertEquals("tenant", request.tenantId());
            return Uni.createFrom().item(reply(request, "projected"));
        });
    }

    @Test void committedCompletionReplaysAfterCoordinatorReplacementWithoutWorkerOrApplicationTypes() {
        var result = coordinator.complete(command()).await().indefinitely();
        assertFalse(result.duplicate()); assertEquals(AwaitInteractionStatus.COMPLETED, result.record().status());
        assertEquals("projected", value(result.record()));
        clearInvocations(workers);
        doReturn(Uni.createFrom().failure(new IllegalStateException("worker unavailable"))).when(workers).executeAwaitValueOperation(any());
        var restarted = fresh();
        var replay = restarted.complete(command()).await().indefinitely();
        assertTrue(replay.duplicate()); assertEquals(result.record(), replay.record());
        assertEquals(result.record().responsePayload(), restarted.resumePayload(replay.record()));
        verifyNoInteractions(workers);
    }

    @Test void changedAuthoritativeVersionRequiresFreshProjectionBeforeCommit() {
        AtomicInteger projections = new AtomicInteger();
        doAnswer(call -> {
            var request = call.getArgument(0, AwaitValueOperationRequest.class);
            if (projections.incrementAndGet() == 1) {
                store.markDispatching("tenant", original.interactionId(), original.version(), now + 1).await().indefinitely().orElseThrow();
                assertEquals(original.version(), request.expectedInteractionVersion());
                return Uni.createFrom().item(reply(request, "stale"));
            }
            assertEquals(original.version() + 1, request.expectedInteractionVersion());
            return Uni.createFrom().item(reply(request, "fresh"));
        }).when(workers).executeAwaitValueOperation(any());
        var result = coordinator.complete(command()).await().indefinitely();
        assertEquals(2, projections.get()); assertEquals("fresh", value(result.record()));
        assertEquals(original.version() + 2, result.record().version());
    }

    @Test void timeoutWinningWhileProjectionRunsCannotBeOverwritten() {
        doAnswer(call -> {
            store.markTimedOut("tenant", original.interactionId(), original.version(), original.deadlineEpochMs())
                .await().indefinitely().orElseThrow();
            return Uni.createFrom().item(reply(call.getArgument(0), "late"));
        }).when(workers).executeAwaitValueOperation(any());
        assertThrows(AwaitInteractionTerminalException.class, () -> coordinator.complete(command()).await().indefinitely());
        assertEquals(AwaitInteractionStatus.TIMED_OUT, retained().status()); assertNull(retained().responsePayload());
        verify(workers, times(1)).executeAwaitValueOperation(any());
    }

    @Test void unavailableOrForeignOperationResponseLeavesAuthoritativeRequestUntouched() {
        doReturn(Uni.createFrom().failure(new IllegalStateException("unavailable"))).when(workers).executeAwaitValueOperation(any());
        assertThrows(IllegalStateException.class, () -> coordinator.complete(command()).await().indefinitely());
        assertEquals(original, retained());
        doAnswer(call -> {
            var request = call.getArgument(0, AwaitValueOperationRequest.class);
            return Uni.createFrom().item(new AwaitValueOperationResult(request.operation(), request.operationId(), "foreign-digest",
                wire.encode(payload("Output", "foreign"))));
        }).when(workers).executeAwaitValueOperation(any());
        assertThrows(IllegalArgumentException.class, () -> coordinator.complete(command()).await().indefinitely());
        assertEquals(original, retained());
    }

    @Test void replyReceivedButCommitNotAttemptedCanReprojectAfterRestart() {
        doReturn(Uni.createFrom().failure(new IllegalStateException("lost before commit"))).doCallRealMethod()
            .when(store).complete(any(AwaitInteractionRecord.class), any(AwaitCompletionCommand.class));
        assertThrows(IllegalStateException.class, () -> coordinator.complete(command()).await().indefinitely());
        assertEquals(original, retained());
        assertFalse(fresh().complete(command()).await().indefinitely().duplicate());
        verify(workers, times(2)).executeAwaitValueOperation(any());
        assertEquals(original.version() + 1, retained().version());
    }

    @Test void lostCommitAcknowledgementReplaysStoredCompletionWithoutReprojection() {
        doAnswer(call -> {
            @SuppressWarnings("unchecked") Uni<Optional<AwaitCompletionResult>> committed = (Uni<Optional<AwaitCompletionResult>>) call.callRealMethod();
            return committed.invoke(ignored -> { throw new IllegalStateException("commit reply lost"); });
        }).doCallRealMethod().when(store).complete(any(AwaitInteractionRecord.class), any(AwaitCompletionCommand.class));
        assertThrows(IllegalStateException.class, () -> coordinator.complete(command()).await().indefinitely());
        assertEquals(AwaitInteractionStatus.COMPLETED, retained().status());
        assertTrue(fresh().complete(command()).await().indefinitely().duplicate());
        verify(workers, times(1)).executeAwaitValueOperation(any());
        assertEquals(original.version() + 1, retained().version());
    }

    @Test void foreignTenantAndInvalidTokenFailBeforePureOperationOrMutation() {
        assertThrows(AwaitInteractionNotFoundException.class, () -> coordinator.complete(new AwaitCompletionCommand(
            "other", original.interactionId(), null, "completion", Map.of("value", "answer"), "actor", now + 1)).await().indefinitely());
        assertThrows(AwaitResumeTokenRejectedException.class, () -> coordinator.complete(new AwaitCompletionCommand(
            "tenant", original.interactionId(), null, "invalid-token", "completion", Map.of("value", "answer"), "actor", now + 1)).await().indefinitely());
        verifyNoInteractions(workers); assertEquals(original, retained());
    }

    @Test void workerProposedCompletionCannotOverwriteAuthoritativeSuspensionState() throws Exception {
        var actualUnit = units.get("tenant", "unit").await().indefinitely().orElseThrow();
        var provisionalUnits = new InMemoryAwaitUnitStore();
        provisionalUnits.importRecord(actualUnit).await().indefinitely();
        var forgedUnit = provisionalUnits.markTerminal("tenant", "unit", AwaitUnitStatus.COMPLETED, now + 1)
            .await().indefinitely().orElseThrow();
        var provisionalInteractions = new InMemoryAwaitInteractionStore();
        provisionalInteractions.importRecord(original).await().indefinitely();
        var forged = provisionalInteractions.complete(new AwaitCompletionCommand("tenant", original.interactionId(), null,
            "forged", payload("Output", "uncommitted-worker-value"), "worker", now + 1)).await().indefinitely().record();
        var proposal = new TransitionAwaitSuspension("tenant", "execution", "unit", 0, forgedUnit, List.of(forged));

        assertTrue(coordinator.requiresAuthoritativeSuspension());
        var observed = coordinator.authoritativeSuspension(proposal).await().indefinitely();
        assertEquals(actualUnit, observed.unit());
        assertEquals(PipelineJson.mapper().valueToTree(List.of(original)),
            PipelineJson.mapper().valueToTree(observed.interactions()),
            "authoritative snapshot must preserve every field and payload byte");
        coordinator.importSuspension(proposal).await().indefinitely();
        assertEquals(original, retained()); assertEquals(actualUnit, units.get("tenant", "unit").await().indefinitely().orElseThrow());
        verify(store, never()).importRecord(any()); verify(units, never()).importRecord(any());
        verifyNoInteractions(workers);
    }

    @Test void unregisteredOrForeignWorkerSuspensionCannotCreateCoordinatorState() {
        assertThrows(AwaitInteractionNotFoundException.class, () -> coordinator.importSuspension(
            new TransitionAwaitSuspension("tenant", "execution", "absent-unit", 0)).await().indefinitely());
        assertThrows(IllegalStateException.class, () -> coordinator.authoritativeSuspension(
            new TransitionAwaitSuspension("tenant", "foreign-execution", "unit", 0)).await().indefinitely());
        assertThrows(AwaitInteractionNotFoundException.class, () -> coordinator.authoritativeSuspension(
            new TransitionAwaitSuspension("foreign-tenant", "execution", "unit", 0)).await().indefinitely());
        assertTrue(units.get("tenant", "absent-unit").await().indefinitely().isEmpty());
        assertEquals(original, retained());
        verify(store, never()).importRecord(any()); verify(units, never()).importRecord(any());
        verifyNoInteractions(workers);
    }

    private AwaitCoordinator fresh() {
        var result = new AwaitCoordinator(); result.interactionStores = instances(store); result.unitStores = instances(units);
        result.orchestratorConfig = config; result.durablePayloadResolver = resolver; result.registeredWorkers = workers;
        result.transitionPayloadCodec = wire; result.resumeTokenService = new AwaitResumeTokenService("test-authority-secret");
        // No descriptor registry, application adapter or typed runtime is available in this Coordinator.
        return result;
    }
    @SuppressWarnings("unchecked") private static <T> Instance<T> instances(T item) {
        Instance<T> result = mock(Instance.class); when(result.stream()).thenAnswer(ignored -> java.util.stream.Stream.of(item)); return result;
    }
    private AwaitCompletionCommand command() { return new AwaitCompletionCommand("tenant", original.interactionId(), null,
        "completion", Map.of("value", "answer"), "actor", now + 1); }
    private AwaitInteractionRecord retained() { return store.get("tenant", original.interactionId()).await().indefinitely().orElseThrow(); }
    private AwaitValueOperationResult reply(AwaitValueOperationRequest request, String value) throws Exception {
        return new AwaitValueOperationResult(request.operation(), request.operationId(), request.requestDigest(), wire.encode(payload("Output", value)));
    }
    private static String value(AwaitInteractionRecord record) {
        try { return PipelineJson.mapper().readTree(TypedDurablePayload.fromDurableValue(record.responsePayload()).orElseThrow().payload()).path("value").asText(); }
        catch (Exception failure) { throw new AssertionError(failure); }
    }
    private static TypedDurablePayload payload(String type, String value) throws Exception {
        return new TypedDurablePayload(type, type + "-fingerprint", "catalog", JsonDurablePayloadCodec.ENCODING, 1,
            PipelineJson.mapper().writeValueAsBytes(Map.of("value", value)));
    }
    private static Map<String, Object> definition(String type) {
        return Map.of("runtimeClass", "absent.application." + type, "definitionFingerprint", type + "-fingerprint", "definition",
            Map.of("kind", "record", "id", type, "fields", List.of(Map.of("name", "value", "presence", "REQUIRED", "nullability", "NON_NULL",
                "type", Map.of("kind", "scalar", "id", "string")))));
    }
    private static Map<String, Object> descriptor() {
        var metadata = new LinkedHashMap<String, Object>(Map.of("schemaVersion", 1, "stepId", "Review", "inputTypeId", "absent.application.Request", "outputTypeId", "absent.application.Output",
            "transportInputTypeId", "absent.application.Request", "transportOutputTypeId", "absent.application.Output", "canonicalInputTypeId", "Request",
            "canonicalOutputTypeId", "Output", "completionPayloadTypeId", "Output", "requestAwareCompletion", true));
        metadata.put("completionProjectorId", "original-projector");
        return Map.copyOf(metadata);
    }
}
