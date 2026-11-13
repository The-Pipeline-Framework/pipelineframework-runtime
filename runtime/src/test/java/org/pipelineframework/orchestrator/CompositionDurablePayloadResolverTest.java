package org.pipelineframework.orchestrator;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.pipelineframework.invocation.PipelineInvocationPath;
import org.pipelineframework.orchestrator.composition.PipelineCompositionContinuation;
import org.pipelineframework.orchestrator.composition.PipelineCompositionContinuationKind;
import org.pipelineframework.orchestrator.composition.PipelineCompositionDefinition;
import org.pipelineframework.orchestrator.composition.PipelineCompositionDescriptor;
import org.pipelineframework.orchestrator.composition.PipelineCompositionNode;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseRecord;
import org.pipelineframework.orchestrator.release.PipelineReleaseRegistry;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CompositionDurablePayloadResolverTest {
    @Test
    void rootEntryUsesItsNamedBoundaryInsteadOfTheFirstLeafAtEveryCursor() {
        var resolver = resolver(false, false);
        var input = new Entry("admission");
        for (int cursor : List.of(0, 1, 3, 8)) {
            var execution = execution(cursor);
            String stored = resolver.encode(execution, ExecutionDurablePayloadResolver.Slot.INPUT, input);
            assertEquals("Admission", payload(stored).canonicalTypeId());
            assertEquals(input, resolver.decode(execution, ExecutionDurablePayloadResolver.Slot.INPUT, stored));
        }
    }

    @Test
    void completedRootResultsUseRootOutputRatherThanTheLastChildLeafAtEveryCursor() {
        var resolver = resolver(false, false);
        var result = new Receipt("published");
        for (int cursor : List.of(0, 1, 3, 8)) {
            var execution = execution(cursor);
            String scalar = resolver.encode(execution, ExecutionDurablePayloadResolver.Slot.RESULT, result);
            String collection = resolver.encode(execution, ExecutionDurablePayloadResolver.Slot.RESULT, List.of(result));
            assertEquals("RootResult", payload(scalar).canonicalTypeId());
            assertEquals("List<RootResult>", payload(collection).canonicalTypeId());
            assertEquals(result, resolver.decode(execution, ExecutionDurablePayloadResolver.Slot.RESULT, scalar));
            assertEquals(List.of(result), resolver.decode(execution, ExecutionDurablePayloadResolver.Slot.RESULT, collection));
        }
    }

    @Test
    void storedChildEntryKeepsItsExactAliasAcrossNestedSuspensionAndRootCursorMoves() {
        var resolver = resolver(false, false);
        var input = new Entry("child-entry");
        // An existing explicit boundary pin is authoritative even when it differs from root input.
        String stored = resolver.encode(execution(0), "ChildEntry", input);
        assertEquals("ChildEntry", payload(stored).canonicalTypeId());
        var resumed = execution(3);
        var path = new PipelineInvocationPath(List.of(
            new PipelineInvocationPath.Frame("prepare", "Prepare child", 1, 0, false))).atStep(1);
        when(resumed.inputPayload()).thenReturn(new ExecutionInputSnapshot(
            ExecutionInputShape.UNI, input, Optional.empty(), Optional.of(path)));
        assertEquals(input, resolver.decode(resumed, ExecutionDurablePayloadResolver.Slot.INPUT, stored));
        assertEquals(input, resolver.decode(resumed, ExecutionDurablePayloadResolver.Slot.CONTINUATION_INPUT, stored));
    }

    @Test
    void rootResultRetainsTheDeclaredUnionInsteadOfSelectingAConcreteCatalogClass() {
        var resolver = resolver(true, false);
        Outcome result = new Outcome.Completed(new Receipt("approved"));
        String stored = resolver.encode(execution(1), ExecutionDurablePayloadResolver.Slot.RESULT, result);
        assertEquals("RootResult", payload(stored).canonicalTypeId());
        assertEquals(result, resolver.decode(execution(3), ExecutionDurablePayloadResolver.Slot.RESULT, stored));
    }

    @Test
    void legacyWrappersUsePinnedRootBoundariesAndIgnoreUntrustedClassHints() {
        var resolver = resolver(false, false);
        assertEquals(new Entry("legacy-entry"), resolver.decodeLegacy(execution(2),
            ExecutionDurablePayloadResolver.Slot.INPUT,
            "{\"_tpf_java_class\":\"untrusted.Other\",\"_tpf_payload\":{\"id\":\"legacy-entry\"}}"));
        assertEquals(new Receipt("legacy-result"), resolver.decodeLegacy(execution(1),
            ExecutionDurablePayloadResolver.Slot.RESULT,
            "{\"_tpf_java_class\":\"untrusted.Other\",\"_tpf_payload\":{\"id\":\"legacy-result\"}}"));
    }

    @Test
    void sameClassAndFingerprintDoNotAuthorizeAnUndeclaredAlias() {
        var resolver = resolver(false, false);
        // Both aliases have the same Java binding and expression fingerprint; only ChildEntry
        // is declared by the composition. Catalog membership alone grants no boundary authority.
        String undeclared = resolver.encode(execution(0), "UndeclaredEntry", new Entry("unrelated"));
        var failure = assertThrows(IllegalStateException.class,
            () -> resolver.decode(execution(2), ExecutionDurablePayloadResolver.Slot.INPUT, undeclared));
        assertTrue(failure.getCause().getMessage().contains("not permitted for INPUT"));
    }

    @Test
    void rejectsMissingNamedRootBindingAndChangedCatalogWithoutLeafFallback() {
        var missing = resolver(false, true);
        var absent = assertThrows(IllegalStateException.class,
            () -> missing.encode(execution(0), ExecutionDurablePayloadResolver.Slot.INPUT, new Entry("missing")));
        assertTrue(absent.getMessage().contains("no canonical binding for Admission"));

        var resolver = resolver(false, false);
        String stored = resolver.encode(execution(0), "ChildEntry", new Entry("catalog"));
        assertThrows(IllegalStateException.class, () -> resolver.decode(execution(3),
            ExecutionDurablePayloadResolver.Slot.INPUT,
            stored.replace("\"catalogFingerprint\":\"catalog\"", "\"catalogFingerprint\":\"changed\"")));
    }

    private static ExecutionDurablePayloadResolver resolver(boolean unionResult, boolean missingAdmission) {
        var types = new LinkedHashMap<String, Map<String, Object>>();
        types.put("UndeclaredEntry", binding(Entry.class, "child-entry"));
        if (!missingAdmission) types.put("Admission", binding(Entry.class, "root-entry"));
        types.put("ChildEntry", binding(Entry.class, "child-entry"));
        types.put("Ready", binding(Ready.class, "ready"));
        types.put("Activation", binding(Activation.class, "activation"));
        types.put("ChildComplete", binding(Complete.class, "complete"));
        types.put("RootResult", binding(unionResult ? Outcome.class : Receipt.class, "root-result"));
        var root = definition("$root", "Admission", "RootResult", List.of(
            invocation(0, "Prepare child", "Admission", "Ready", "prepare"),
            direct(1, "Activate", "Ready", "Activation"),
            invocation(2, "Canary child", "Activation", "ChildComplete", "canary"),
            direct(3, "Record", "ChildComplete", "RootResult")));
        var prepare = definition("prepare", "Admission", "Ready", List.of(
            direct(0, "Admit child", "Admission", "ChildEntry"),
            direct(1, "Prepare", "ChildEntry", "Ready")));
        var canary = definition("canary", "Activation", "ChildComplete", List.of(
            direct(0, "Canary", "Activation", "ChildComplete")));
        // The real compiler catalog lists direct root leaves before child leaves. Its first
        // and last elements deliberately differ from the root entry and terminal contracts.
        var contract = new PipelineContractDescriptor(3, "composed", "3", "contract", null, null, null,
            false, null, List.of(
                leaf(0, "Activate", Ready.class, Activation.class),
                leaf(1, "Record", Complete.class, unionResult ? Outcome.class : Receipt.class),
                leaf(2, "Admit child", Entry.class, Entry.class),
                leaf(3, "Prepare", Entry.class, Ready.class),
                leaf(4, "Canary", Activation.class, Complete.class)),
            PipelineBundleCapabilities.defaults(), types, "catalog",
            new PipelineCompositionDescriptor("$root", List.of(root, prepare, canary)));
        var release = mock(PipelineReleaseRecord.class);
        when(release.contract()).thenReturn(contract);
        when(release.tenantId()).thenReturn("tenant");
        when(release.pipelineId()).thenReturn("composed");
        when(release.contractVersion()).thenReturn("3");
        when(release.releaseVersion()).thenReturn("release");
        var registry = mock(PipelineReleaseRegistry.class);
        when(registry.get("tenant", "composed", "release")).thenReturn(Uni.createFrom().item(Optional.of(release)));
        var resolver = new ExecutionDurablePayloadResolver();
        resolver.releaseRegistry = registry;
        resolver.codec = new JsonDurablePayloadCodec();
        return resolver;
    }

    private static PipelineCompositionDefinition definition(String id, String input, String output,
        List<PipelineCompositionNode> nodes) {
        var continuations = nodes.stream().map(node -> node.index() + 1 < nodes.size()
            ? new PipelineCompositionContinuation(node.nodeId(), PipelineCompositionContinuationKind.NEXT_LOCAL,
                nodes.get(node.index() + 1).nodeId())
            : new PipelineCompositionContinuation(node.nodeId(), "$root".equals(id)
                ? PipelineCompositionContinuationKind.ROOT_TERMINAL : PipelineCompositionContinuationKind.RETURN, ""))
            .toList();
        return new PipelineCompositionDefinition(id, id + "-fingerprint", input, output, nodes, continuations);
    }

    private static PipelineCompositionNode direct(int index, String name, String input, String output) {
        return new PipelineCompositionNode(index, name, PipelineCompositionNode.DIRECT, input, output, "ONE_TO_ONE", "");
    }

    private static PipelineCompositionNode invocation(int index, String name, String input, String output, String target) {
        return new PipelineCompositionNode(index, name, PipelineCompositionNode.INVOCATION, input, output, "ONE_TO_ONE", target);
    }

    private static PipelineBundleStepDescriptor leaf(int index, String name, Class<?> input, Class<?> output) {
        return new PipelineBundleStepDescriptor(index, name, "internal", "ONE_TO_ONE", input.getName(), output.getName(),
            null, null, null);
    }

    @SuppressWarnings("unchecked")
    private static ExecutionRecord<Object, Object> execution(int cursor) {
        var execution = (ExecutionRecord<Object, Object>) mock(ExecutionRecord.class);
        when(execution.tenantId()).thenReturn("tenant");
        when(execution.pipelineId()).thenReturn("composed");
        when(execution.contractVersion()).thenReturn("3");
        when(execution.releaseVersion()).thenReturn("release");
        when(execution.executionId()).thenReturn("execution");
        when(execution.currentStepIndex()).thenReturn(cursor);
        when(execution.resultShape()).thenReturn(ExecutionResultShape.SINGLE);
        return execution;
    }

    private static TypedDurablePayload payload(String stored) {
        return TypedDurablePayload.fromSerializedBytes(stored.getBytes(StandardCharsets.UTF_8)).orElseThrow();
    }

    private static Map<String, Object> binding(Class<?> type, String fingerprint) {
        return Map.of("runtimeClass", type.getName(), "definitionFingerprint", fingerprint);
    }

    private record Entry(String id) { }
    private record Ready(String id) { }
    private record Activation(String id) { }
    private record Complete(String id) { }
    private record Receipt(String id) { }
    sealed interface Outcome {
        record Completed(Receipt value) implements Outcome { }
        record Rejected(String reason) implements Outcome { }
    }
}
