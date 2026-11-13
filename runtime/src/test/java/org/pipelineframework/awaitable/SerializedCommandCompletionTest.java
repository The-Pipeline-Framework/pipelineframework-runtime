package org.pipelineframework.awaitable;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.inject.Instance;
import java.util.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.store.InMemoryAwaitInteractionStore;
import org.pipelineframework.awaitable.store.InMemoryAwaitUnitStore;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.*;
import org.pipelineframework.orchestrator.release.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

/** Authoritative Command state with canonical payloads and no application classes or descriptor registry. */
class SerializedCommandCompletionTest {
    private final JsonTransitionPayloadCodec wire = new JsonTransitionPayloadCodec();
    private final Map<String, Object> callbackContract = Map.of("binding", "operation", "callback", "completed");
    private final List<AwaitValueOperationRequest> operations = new ArrayList<>();
    private InMemoryAwaitInteractionStore interactions;
    private InMemoryAwaitUnitStore units;
    private RegisteredRestWorkerTargets workers;
    private PipelineOrchestratorConfig config;
    private AwaitDurablePayloadResolver resolver;
    private AwaitCoordinator coordinator;
    private AwaitInteractionRecord dispatching;
    private AwaitResumeTokenService tokens;

    @BeforeEach void setUp() throws Exception {
        long now = System.currentTimeMillis();
        interactions = new InMemoryAwaitInteractionStore();
        units = new InMemoryAwaitUnitStore();
        workers = mock(RegisteredRestWorkerTargets.class);
        config = mock(PipelineOrchestratorConfig.class);
        var control = mock(PipelineOrchestratorConfig.ControlPlaneConfig.class);
        when(config.controlPlane()).thenReturn(control);
        when(control.payloadMaterialization()).thenReturn(PipelineOrchestratorConfig.ControlPlaneConfig.PayloadMaterialization.SERIALIZED);
        var contract = new PipelineContractDescriptor(2, "pipeline", "contract", "hash", null, null, null, false, null,
            List.of(new PipelineBundleStepDescriptor(0, "Submit", "command", "ONE_TO_ONE", "Request", "Output", null, null,
                Map.of("runtimeDescriptor", descriptor()))), PipelineBundleCapabilities.defaults(),
            Map.of("Request", definition("Request"), "Output", definition("Output"),
                "Acknowledgement", definition("Acknowledgement"), "Callback", definition("Callback")), "catalog");
        var releases = new InMemoryPipelineReleaseRegistry();
        var releaseDescriptor = new PipelineReleaseDescriptor(1, "pipeline", "contract", "release-original", "artifact", List.of());
        releases.register(new PipelineReleaseRecord("tenant", "pipeline", "contract", "release-original",
            PipelineReleaseStatus.ACTIVE, releaseDescriptor, "artifact", "digest", "", 0, "", contract, now, now, now))
            .await().indefinitely();
        var executions = new InMemoryExecutionStateStore();
        var execution = executions.createOrGetExecution(new ExecutionCreateCommand("tenant", "key", "pipeline", "contract",
            "release-original", payload("Request", "request"), ExecutionResultShape.SINGLE, now, now / 1000 + 3600))
            .await().indefinitely().record();
        resolver = new AwaitDurablePayloadResolver();
        resolver.orchestratorConfig = config; resolver.executionStateStore = executions; resolver.releaseRegistry = releases;
        var original = interactions.createOrGet(new AwaitCreateCommand("tenant", execution.executionId(), "Submit", 0,
            "absent.application.Output", "absent.application.Callback", "cause", "occurrence", "correlation",
            payload("Request", "request"), "", "", "connector-callback", "unit", null,
            Map.of("tpf.await.completion.projector", "original-projector", "completionMode", "CONNECTOR_CALLBACK",
                "commandCompletionMode", "ACKNOWLEDGEMENT_REQUIRED", "commandAcknowledgementType", "absent.application.Acknowledgement",
                "callbackSelection", callbackContract), now, now + 900_000, now / 1000 + 3600))
            .await().indefinitely().record();
        dispatching = interactions.markDispatching("tenant", original.interactionId(), original.version(), now)
            .await().indefinitely().orElseThrow();
        tokens = new AwaitResumeTokenService("test-authority-secret");
        coordinator = fresh();
        when(workers.executeAwaitValueOperation(any())).thenAnswer(call -> successful(call.getArgument(0)));
    }

    @Test void earlyCallbackStagesCanonicalEvidenceUntilAcknowledgementThenProjectsOriginalPinnedValues() throws Exception {
        var staged = coordinator.complete(callback("callback")).await().indefinitely();
        assertFalse(staged.duplicate());
        assertEquals(AwaitInteractionStatus.COMPLETION_OBSERVED, staged.record().status());
        assertTrue(staged.record().commandAcknowledgement().isEmpty());
        assertNull(staged.record().responsePayload());
        assertEquals("callback", value(staged.record().stagedCompletionPayload().orElseThrow()));
        assertEquals(List.of(AwaitValueOperation.NORMALIZE_COMPLETION), kinds());

        var settled = coordinator.settleCommandDispatch(dispatching, CommandDispatchSettlement.SUCCEEDED,
            Optional.of(Map.of("value", "ack"))).await().indefinitely();
        assertTrue(settled.completedByDispatch());
        assertEquals(AwaitInteractionStatus.COMPLETED, settled.record().status());
        assertEquals("request/ack/callback", value(settled.record().responsePayload()));
        assertEquals("ack", value(settled.record().commandAcknowledgement().orElseThrow()));
        assertEquals("callback", value(settled.record().stagedCompletionPayload().orElseThrow()));
        assertEquals(List.of(AwaitValueOperation.NORMALIZE_COMPLETION, AwaitValueOperation.NORMALIZE_ACKNOWLEDGEMENT,
            AwaitValueOperation.PROJECT_COMPLETION), kinds());
    }

    @Test void unavailableProjectionRetainsCallbackAndAcknowledgementForFreshCoordinatorReplay() throws Exception {
        coordinator.complete(callback("callback")).await().indefinitely();
        doAnswer(call -> {
            AwaitValueOperationRequest request = call.getArgument(0);
            if (request.operation() == AwaitValueOperation.PROJECT_COMPLETION) {
                operations.add(request);
                return Uni.createFrom().failure(new IllegalStateException("worker unavailable"));
            }
            return successful(request);
        }).when(workers).executeAwaitValueOperation(any());
        var pending = coordinator.settleCommandDispatch(dispatching, CommandDispatchSettlement.SUCCEEDED,
            Optional.of(Map.of("value", "ack"))).await().indefinitely().record();
        assertEquals(AwaitInteractionStatus.COMPLETION_OBSERVED, pending.status());
        assertEquals(true, pending.transportMetadata().get("completionProjectionPending"));
        assertEquals("ack", value(pending.commandAcknowledgement().orElseThrow()));
        assertEquals("callback", value(pending.stagedCompletionPayload().orElseThrow()));
        assertNull(pending.responsePayload());

        operations.clear();
        doAnswer(call -> successful(call.getArgument(0))).when(workers).executeAwaitValueOperation(any());
        var completed = fresh().settleCommandDispatch(pending, CommandDispatchSettlement.AMBIGUOUS,
            Optional.empty()).await().indefinitely();
        assertTrue(completed.completedByDispatch());
        assertEquals("request/ack/callback", value(completed.record().responsePayload()));
        assertEquals(pending.commandAcknowledgement(), completed.record().commandAcknowledgement());
        assertEquals(pending.stagedCompletionPayload(), completed.record().stagedCompletionPayload());
        assertEquals(List.of(AwaitValueOperation.PROJECT_COMPLETION), kinds());
        assertEquals(pending.version(), operations.getFirst().expectedInteractionVersion());
    }

    @Test void duplicateCallbackRetriesPendingProjectionAfterCoordinatorRestart() throws Exception {
        coordinator.complete(callback("callback")).await().indefinitely();
        doAnswer(call -> {
            AwaitValueOperationRequest request = call.getArgument(0);
            if (request.operation() == AwaitValueOperation.PROJECT_COMPLETION) {
                operations.add(request);
                return Uni.createFrom().failure(new IllegalStateException("worker unavailable"));
            }
            return successful(request);
        }).when(workers).executeAwaitValueOperation(any());
        var pending = coordinator.settleCommandDispatch(dispatching, CommandDispatchSettlement.SUCCEEDED,
            Optional.of(Map.of("value", "ack"))).await().indefinitely().record();
        assertEquals(AwaitInteractionStatus.COMPLETION_OBSERVED, pending.status());
        assertEquals(true, pending.transportMetadata().get("completionProjectionPending"));
        assertEquals("ack", value(pending.commandAcknowledgement().orElseThrow()));
        assertEquals("callback", value(pending.stagedCompletionPayload().orElseThrow()));
        assertNull(pending.responsePayload());

        operations.clear();
        doAnswer(call -> successful(call.getArgument(0))).when(workers).executeAwaitValueOperation(any());
        var completed = fresh().complete(callback("callback")).await().indefinitely();
        assertFalse(completed.duplicate());
        assertEquals(AwaitInteractionStatus.COMPLETED, completed.record().status());
        assertFalse(completed.record().transportMetadata().containsKey("completionProjectionPending"));
        assertEquals("request/ack/callback", value(completed.record().responsePayload()));
        assertEquals(pending.commandAcknowledgement(), completed.record().commandAcknowledgement());
        assertEquals(pending.stagedCompletionPayload(), completed.record().stagedCompletionPayload());
        assertEquals(List.of(AwaitValueOperation.NORMALIZE_COMPLETION, AwaitValueOperation.PROJECT_COMPLETION), kinds());
        assertEquals(pending.version(), operations.getLast().expectedInteractionVersion());
    }

    @Test void authenticatedIdentityRejectionClearsOnlyRejectedCallbackAndAcceptsCorrectedEvidence() throws Exception {
        coordinator.complete(callback("wrong-callback")).await().indefinitely();
        doAnswer(call -> {
            AwaitValueOperationRequest request = call.getArgument(0);
            if (request.operation() == AwaitValueOperation.PROJECT_COMPLETION) {
                operations.add(request);
                return Uni.createFrom().failure(new CommandCompletionRejectedException("request-bound identity rejection"));
            }
            return successful(request);
        }).when(workers).executeAwaitValueOperation(any());
        var rejected = coordinator.settleCommandDispatch(dispatching, CommandDispatchSettlement.SUCCEEDED,
            Optional.of(Map.of("value", "ack"))).await().indefinitely().record();
        assertEquals(AwaitInteractionStatus.DISPATCHED, rejected.status());
        assertEquals(true, rejected.transportMetadata().get("rejectedEarlyCallback"));
        assertTrue(rejected.stagedCompletionPayload().isEmpty());
        assertEquals("ack", value(rejected.commandAcknowledgement().orElseThrow()));
        assertNull(rejected.responsePayload());

        doAnswer(call -> successful(call.getArgument(0))).when(workers).executeAwaitValueOperation(any());
        var corrected = fresh().complete(callback("correct-callback")).await().indefinitely();
        assertFalse(corrected.duplicate());
        assertEquals(AwaitInteractionStatus.COMPLETED, corrected.record().status());
        assertEquals("request/ack/correct-callback", value(corrected.record().responsePayload()));
    }

    @Test void identicalCallbackReplaysWithoutProjectionWhileContradictoryEvidenceCannotMutateAdmission() {
        var staged = coordinator.complete(callback("callback")).await().indefinitely().record();
        assertTrue(fresh().complete(callback("callback")).await().indefinitely().duplicate());
        assertEquals(staged, retained());
        assertThrows(CommandCompletionRejectedException.class,
            () -> coordinator.complete(callback("other")).await().indefinitely());
        assertEquals(staged, retained());
        assertTrue(kinds().stream().allMatch(kind -> kind == AwaitValueOperation.NORMALIZE_COMPLETION));
        var completed = coordinator.settleCommandDispatch(staged, CommandDispatchSettlement.SUCCEEDED,
            Optional.of(Map.of("value", "ack"))).await().indefinitely().record();
        operations.clear();
        var duplicate = fresh().complete(callback("callback")).await().indefinitely();
        assertTrue(duplicate.duplicate());
        assertEquals(completed, duplicate.record());
        assertEquals(List.of(AwaitValueOperation.NORMALIZE_COMPLETION), kinds());
    }

    @Test void timeoutWinningDuringProjectionCannotBeOverwrittenByLateAcknowledgementSettlement() {
        var staged = coordinator.complete(callback("callback")).await().indefinitely().record();
        doAnswer(call -> {
            AwaitValueOperationRequest request = call.getArgument(0);
            if (request.operation() == AwaitValueOperation.PROJECT_COMPLETION) {
                interactions.markTimedOut("tenant", staged.interactionId(), staged.version(), staged.deadlineEpochMs())
                    .await().indefinitely().orElseThrow();
            }
            return successful(request);
        }).when(workers).executeAwaitValueOperation(any());
        var settled = coordinator.settleCommandDispatch(staged, CommandDispatchSettlement.SUCCEEDED,
            Optional.of(Map.of("value", "ack"))).await().indefinitely();
        assertFalse(settled.completedByDispatch());
        assertEquals(AwaitInteractionStatus.TIMED_OUT, settled.record().status());
        assertEquals(staged.version() + 1, settled.record().version());
        assertNull(settled.record().responsePayload());
        assertEquals(staged.stagedCompletionPayload(), settled.record().stagedCompletionPayload());
    }

    @Test void commandCallbackRequiresSignedAuthorityBeforeAnyPureOperationOrMutation() {
        var command = callback("callback");
        assertThrows(AwaitResumeTokenRejectedException.class, () -> coordinator.complete(new AwaitCompletionCommand(
            command.tenantId(), command.interactionId(), null, null, command.idempotencyKey(), command.responsePayload(),
            command.actor(), command.nowEpochMs())).await().indefinitely());
        verifyNoInteractions(workers);
        assertEquals(dispatching, retained());
    }

    private Uni<AwaitValueOperationResult> successful(AwaitValueOperationRequest request) throws Exception {
        operations.add(request);
        assertEquals("tenant", request.tenantId());
        assertEquals(dispatching.executionId(), request.executionId());
        assertEquals("pipeline", request.pipelineId());
        assertEquals("contract", request.contractVersion());
        assertEquals("release-original", request.releaseVersion());
        assertEquals(Optional.of("original-projector"), request.completionProjectorId());
        assertEquals(Optional.of(AwaitValueOperationExecutor.callbackFingerprint(callbackContract)), request.callbackContractFingerprint());
        String type;
        String value;
        switch (request.operation()) {
            case NORMALIZE_COMPLETION -> { type = "Callback"; value = rawValue(wire.decode(request.completionPayload().orElseThrow())); }
            case NORMALIZE_ACKNOWLEDGEMENT -> { type = "Acknowledgement"; value = rawValue(wire.decode(request.acknowledgementPayload().orElseThrow())); }
            case PROJECT_COMPLETION -> {
                type = "Output";
                value = value(wire.decode(request.requestPayload().orElseThrow())) + "/"
                    + value(wire.decode(request.acknowledgementPayload().orElseThrow())) + "/"
                    + value(wire.decode(request.completionPayload().orElseThrow()));
            }
            default -> throw new AssertionError("Unexpected pure operation " + request.operation());
        }
        return Uni.createFrom().item(new AwaitValueOperationResult(request.operation(), request.operationId(), request.requestDigest(),
            wire.encode(payload(type, value))));
    }
    private static String rawValue(Object raw) { return PipelineJson.mapper().valueToTree(raw).path("value").asText(); }
    private static String value(Object canonical) throws Exception {
        return PipelineJson.mapper().readTree(TypedDurablePayload.fromDurableValue(canonical).orElseThrow().payload()).path("value").asText();
    }
    private List<AwaitValueOperation> kinds() { return operations.stream().map(AwaitValueOperationRequest::operation).toList(); }
    private AwaitCompletionCommand callback(String value) {
        return new AwaitCompletionCommand("tenant", dispatching.interactionId(), null, tokens.sign(dispatching, dispatching.createdAtEpochMs()),
            "completion", Map.of("value", value), "provider", System.currentTimeMillis());
    }
    private AwaitInteractionRecord retained() { return interactions.get("tenant", dispatching.interactionId()).await().indefinitely().orElseThrow(); }
    private AwaitCoordinator fresh() {
        var result = new AwaitCoordinator(); result.interactionStores = instances(interactions); result.unitStores = instances(units);
        result.orchestratorConfig = config; result.durablePayloadResolver = resolver; result.registeredWorkers = workers;
        result.transitionPayloadCodec = wire; result.resumeTokenService = tokens;
        return result;
    }
    @SuppressWarnings("unchecked") private static <T> Instance<T> instances(T item) {
        Instance<T> result = mock(Instance.class); when(result.stream()).thenAnswer(ignored -> java.util.stream.Stream.of(item)); return result;
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
    private Map<String, Object> descriptor() {
        var value = new LinkedHashMap<String, Object>();
        value.put("schemaVersion", 1); value.put("stepId", "Submit");
        value.put("inputTypeId", "absent.application.Request"); value.put("outputTypeId", "absent.application.Output");
        value.put("transportInputTypeId", "absent.application.Request"); value.put("transportOutputTypeId", "absent.application.Callback");
        value.put("canonicalInputTypeId", "Request"); value.put("canonicalOutputTypeId", "Output");
        value.put("completionPayloadTypeId", "Callback"); value.put("requestAwareCompletion", true);
        value.put("completionProjectorId", "original-projector"); value.put("acknowledgementAwareCommandCompletion", true);
        value.put("commandAcknowledgementTypeId", "absent.application.Acknowledgement");
        value.put("canonicalCommandAcknowledgementTypeId", "Acknowledgement"); value.put("callbackContract", callbackContract);
        return Map.copyOf(value);
    }
}
