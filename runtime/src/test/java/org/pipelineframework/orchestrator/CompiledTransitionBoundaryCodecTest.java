package org.pipelineframework.orchestrator;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.config.pipeline.PipelineOrderResourceLoader;
import org.pipelineframework.invocation.PipelineInvocationPath;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CompiledTransitionBoundaryCodecTest {
    @TempDir Path directory;
    final JsonTransitionPayloadCodec framing = new JsonTransitionPayloadCodec();

    @Test void publicRootAliasesMayDifferFromPhysicalFirstAndLastAliases() throws Exception {
        withOrder(List.of(edge("java.lang.String", "InputAlias", "OutputAlias")), List.of(), () -> {
            var contract = contract();
            var input = codec(contract, "Root").encode(new Value("initial"));
            var boundary = new CompiledTransitionBoundaryCodec(framing, contract, command(0, -1, input));
            assertEquals(new Value("initial"), boundary.decode(input));
            var output = TypedDurablePayload.fromDurableValue(framing.decode(boundary.encode(new Value("final")))).orElseThrow();
            assertEquals("Root", output.canonicalTypeId());
            assertThrows(IllegalArgumentException.class, () -> boundary.decode(codec(contract, "InputAlias").encode(new Value("wrong public alias"))));
        });
    }

    @Test void intermediatePhysicalBoundaryDoesNotUseSemanticStepIndex() throws Exception {
        withOrder(List.of(edge("java.lang.String", "InputAlias", "Pending"), edge("java.lang.Integer", "Pending", "Approved"),
            edge("java.lang.Long", "Approved", "OutputAlias")), List.of(), () -> {
            var contract = contract(); // No semantic-step indexing is available in this fixture.
            var input = codec(contract, "Pending").encode(new Value("pending"));
            var boundary = new CompiledTransitionBoundaryCodec(framing, contract, command(1, 2, input));
            assertEquals(new Value("pending"), boundary.decode(input));
            assertEquals("Approved", TypedDurablePayload.fromDurableValue(framing.decode(boundary.encode(new Value("approved")))).orElseThrow().canonicalTypeId());
        });
    }

    @Test void terminalNoopCanConvertLastPhysicalAliasToPublicRootAlias() throws Exception {
        withOrder(List.of(edge("java.lang.String", "InputAlias", "OutputAlias")), List.of(), () -> {
            var contract = contract();
            var input = codec(contract, "OutputAlias").encode(new Value("completed"));
            var boundary = new CompiledTransitionBoundaryCodec(framing, contract, command(1, -1, input));
            var restored = boundary.decode(input);
            assertEquals("Root", TypedDurablePayload.fromDurableValue(framing.decode(boundary.encode(restored))).orElseThrow().canonicalTypeId());
        });
    }

    @Test void nestedCursorUsesExactInvocationClassDefinitionAndCallsite() throws Exception {
        var nested = Map.<String,Object>of("invocationClass", "java.lang.String", "definitionId", "child", "callsiteId", "Call",
            "stepBoundaries", List.of(edge("java.lang.Integer", "Pending", "Approved"), edge("java.lang.Long", "Approved", "OutputAlias")));
        withOrder(List.of(edge("java.lang.String", "InputAlias", "OutputAlias")), List.of(nested), () -> {
            var contract = contract();
            var value = framing.decode(codec(contract, "Approved").encode(new Value("resumed")));
            var path = new PipelineInvocationPath(2, List.of(new PipelineInvocationPath.Frame("child", "Call", 1, 0, false)), 1);
            var input = framing.encode(new ExecutionInputSnapshot(ExecutionInputShape.UNI, value, Optional.empty(), Optional.of(path)));
            var boundary = new CompiledTransitionBoundaryCodec(framing, contract, command(0, -1, input));
            assertEquals(new Value("resumed"), ((ExecutionInputSnapshot) boundary.decode(input)).payload());
            var forged = new PipelineInvocationPath(2, List.of(new PipelineInvocationPath.Frame("child", "Other", 1, 0, false)), 1);
            assertThrows(IllegalStateException.class, () -> new CompiledTransitionBoundaryCodec(framing, contract,
                command(0, -1, framing.encode(new ExecutionInputSnapshot(ExecutionInputShape.UNI, value, Optional.empty(), Optional.of(forged))))));
        });
    }

    private CompiledCanonicalTransitionPayloadCodec codec(PipelineContractDescriptor contract, String type) {
        return new CompiledCanonicalTransitionPayloadCodec(framing, contract, type, type);
    }
    private static PipelineContractDescriptor contract() {
        var contract = mock(PipelineContractDescriptor.class);
        var definitions = new java.util.HashMap<String,Map<String,Object>>();
        for (String type : List.of("Root", "InputAlias", "OutputAlias", "Pending", "Approved")) definitions.put(type,
            Map.of("runtimeClass", Value.class.getName(), "definitionFingerprint", type, "definition",
                Map.of("kind", "record", "id", type, "fields", List.of(Map.of("name", "text", "presence", "REQUIRED",
                    "nullability", "NON_NULL", "type", Map.of("kind", "scalar", "id", "string"))))));
        when(contract.canonicalTypes()).thenReturn(definitions); when(contract.canonicalCatalogFingerprint()).thenReturn("catalog");
        when(contract.rootBoundary()).thenReturn(Map.of("schemaVersion", 1, "inputCanonicalTypeId", "Root", "outputCanonicalTypeId", "Root"));
        return contract;
    }
    private static Map<String,Object> edge(String step, String input, String output) {
        return Map.of("stepClass", step, "inputTypeId", input, "outputTypeId", output);
    }
    private static TransitionCommandEnvelope command(int current, int stop, SerializedTransitionPayload payload) {
        return new TransitionCommandEnvelope("tenant", "execution", "pipeline", "contract", "release", current, stop, 0,
            ExecutionResultShape.SINGLE, 1, "transition", "trace", payload.payloadTypeId(), payload.payloadEncoding(), payload.payload());
    }
    private void withOrder(List<Map<String,Object>> edges, List<Map<String,Object>> nested, Runnable assertion) throws Exception {
        Path file = directory.resolve("order.json");
        java.nio.file.Files.write(file, PipelineJson.mapper().writeValueAsBytes(Map.of("order", edges.stream().map(edge -> edge.get("stepClass")).toList(),
            "boundarySchemaVersion", 1, "stepBoundaries", edges, "definitionBoundaries", nested)));
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        var url = file.toUri().toURL();
        ClassLoader loader = new ClassLoader(previous) {
            @Override public java.util.Enumeration<java.net.URL> getResources(String name) throws java.io.IOException {
                return name.equals("META-INF/pipeline/order.json") ? Collections.enumeration(List.of(url)) : super.getResources(name);
            }
        };
        try { Thread.currentThread().setContextClassLoader(loader); assertion.run(); }
        finally { Thread.currentThread().setContextClassLoader(previous); }
    }
    public record Value(String text) { }
}
