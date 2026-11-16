package org.pipelineframework.awaitable;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.*;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;

class AwaitValueOperationExecutorTest {
    @Test void pureProjectionUsesPinnedCompiledTypesAndRoundTripsThroughWire() throws Exception {
        Fixture f = fixture();
        assertTrue(f.executor.supported());
        AwaitValueOperationRequest request = request(f, "release-a1");
        var mapper = PipelineJson.mapper();
        request = mapper.readValue(mapper.writeValueAsBytes(request), AwaitValueOperationRequest.class);
        AwaitValueOperationResult result = f.executor.execute(request).await().indefinitely();
        result = mapper.readValue(mapper.writeValueAsBytes(result), AwaitValueOperationResult.class);
        assertTrue(result.matches(request));
        assertEquals(new Output("requested:approved"), codec(f, "Output").decode(result.value()));
        assertEquals(1, f.projected.get());
    }

    @Test void canonicalCompletionPayloadProjectsThroughActualExecutorAndWire() throws Exception {
        Fixture f = fixture();
        var original = request(f, "release-a1");
        var request = new AwaitValueOperationRequest(original.operation(), original.operationId(), original.tenantId(),
            original.executionId(), original.interactionId(), original.unitId(), original.stepId(), original.expectedInteractionVersion(),
            original.pipelineId(), original.contractVersion(), original.releaseVersion(), original.completionProjectorId(),
            original.callbackContractFingerprint(), original.requestPayload(), original.acknowledgementPayload(),
            Optional.of(codec(f, "Completion").encode(new Completion("approved"))), original.actor(), original.admittedAtEpochMs());
        request = PipelineJson.mapper().readValue(PipelineJson.mapper().writeValueAsBytes(request), AwaitValueOperationRequest.class);
        var result = f.executor.execute(request).await().indefinitely();
        assertEquals(new Output("requested:approved"), codec(f, "Output").decode(result.value()));
        assertEquals(1, f.projected.get());
    }

    @Test void wrongReleaseAndProjectorAreRejectedBeforeApplicationCode() {
        Fixture f = fixture();
        assertThrows(IllegalArgumentException.class,
            () -> f.executor.execute(request(f, "release-a2")).await().indefinitely());
        AwaitValueOperationRequest original = request(f, "release-a1");
        var wrong = new AwaitValueOperationRequest(original.operation(), original.operationId(), original.tenantId(),
            original.executionId(), original.interactionId(), original.unitId(), original.stepId(),
            original.expectedInteractionVersion(), original.pipelineId(), original.contractVersion(), original.releaseVersion(),
            Optional.of("other-projector"), original.callbackContractFingerprint(), original.requestPayload(),
            original.acknowledgementPayload(), original.completionPayload(), original.actor(), original.admittedAtEpochMs());
        assertThrows(IllegalArgumentException.class, () -> f.executor.execute(wrong).await().indefinitely());
        assertEquals(0, f.projected.get());
    }

    private static AwaitValueOperationRequest request(Fixture f, String release) {
        return new AwaitValueOperationRequest(AwaitValueOperation.NORMALIZE_AND_PROJECT_COMPLETION,
            "operation", "tenant", "execution", "interaction", "unit", "same-step", 2,
            "application-a", "contract", release, Optional.of("projector-a1"), Optional.empty(),
            Optional.of(codec(f, "Request").encode(new Request("requested"))), Optional.empty(),
            Optional.of(f.executor.framing.encode(Map.of("decision", "approved"))), "admitted-actor", 1000);
    }

    private static CompiledCanonicalTransitionPayloadCodec codec(Fixture f, String type) {
        return new CompiledCanonicalTransitionPayloadCodec(f.executor.framing, f.contract, type, type);
    }

    private static Fixture fixture() {
        AtomicInteger projected = new AtomicInteger();
        var descriptor = new AwaitCompletionDescriptor("same-step", Request.class.getName(), Output.class.getName(),
            "ONE_TO_ONE", Duration.ofSeconds(30), "interactionId", "interaction-api", Map.of(), List.of(),
            Request.class.getName(), Completion.class.getName(), Function.identity(), Function.identity(),
            "projector-a1", (request, completion, metadata) -> {
                projected.incrementAndGet();
                return new Output(((Request) request).value() + ":" + ((Completion) completion).decision());
            }, true);
        Map<String, Object> facts = Map.ofEntries(Map.entry("schemaVersion", 1), Map.entry("stepId", "same-step"),
            Map.entry("inputTypeId", Request.class.getName()), Map.entry("outputTypeId", Output.class.getName()),
            Map.entry("transportInputTypeId", Request.class.getName()), Map.entry("transportOutputTypeId", Completion.class.getName()),
            Map.entry("canonicalInputTypeId", "Request"), Map.entry("canonicalOutputTypeId", "Output"),
            Map.entry("completionPayloadTypeId", "Completion"), Map.entry("requestAwareCompletion", true),
            Map.entry("completionProjectorId", "projector-a1"));
        var contract = new PipelineContractDescriptor(2, "application-a", "contract", "hash", null, null, null, false, null,
            List.of(new PipelineBundleStepDescriptor(0, "same-step", "internal", "ONE_TO_ONE", "Request", "Output",
                null, null, Map.of("runtimeDescriptor", facts))), PipelineBundleCapabilities.defaults(),
            Map.of("Request", binding(Request.class), "Completion", binding(Completion.class), "Output", binding(Output.class)), "catalog");
        var executor = new AwaitValueOperationExecutor();
        executor.config = mock(PipelineOrchestratorConfig.class);
        executor.identities = mock(PipelineReleaseIdentityResolver.class);
        when(executor.identities.pipelineId(executor.config)).thenReturn("application-a");
        when(executor.identities.contractVersion()).thenReturn("contract");
        when(executor.identities.releaseVersion(executor.config)).thenReturn("release-a1");
        when(executor.identities.compiledContract()).thenReturn(Optional.of(contract));
        executor.framing = new JsonTransitionPayloadCodec();
        executor.descriptors = new AwaitCompletionDescriptorRegistry();
        executor.descriptors.register(descriptor);
        return new Fixture(executor, contract, projected);
    }

    private static Map<String, Object> binding(Class<?> type) {
        return Map.of("runtimeClass", type.getName(), "definitionFingerprint", type.getSimpleName() + "-fingerprint",
            "definition", Map.of("kind", "record", "id", type.getSimpleName(), "fields",
                java.util.Arrays.stream(type.getRecordComponents()).map(field -> Map.of("name", field.getName(),
                    "presence", "REQUIRED", "nullability", "NON_NULL", "type", Map.of("kind", "scalar", "id", "string"))).toList()));
    }
    private record Fixture(AwaitValueOperationExecutor executor, PipelineContractDescriptor contract, AtomicInteger projected) { }
    private record Request(String value) { }
    private record Completion(String decision) { }
    private record Output(String value) { }
}
