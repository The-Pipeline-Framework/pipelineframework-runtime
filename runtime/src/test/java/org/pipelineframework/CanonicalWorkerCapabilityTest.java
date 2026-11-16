package org.pipelineframework;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.PipelineReleaseIdentityResolver;
import org.pipelineframework.orchestrator.composition.*;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Capability publication must cover the complete statically linked graph before accepting remote work. */
class CanonicalWorkerCapabilityTest {
    @TempDir Path directory;
    private static final String CALL = "java.lang.String";
    private static final String CHILD_CALL = "java.lang.Integer";
    private static final String LEAF = "java.lang.Long";

    @Test void rootAliasAbsentFromCatalogDoesNotAdvertiseEvenWhenPhysicalAliasesExist() throws Exception {
        var contract = contract(PipelineCompositionDescriptor.empty());
        when(contract.rootBoundary()).thenReturn(Map.of("schemaVersion", 1, "inputCanonicalTypeId", "MissingRoot", "outputCanonicalTypeId", "RootOutput"));
        withOrder(List.of(edge(CALL, "ChildInput", "ChildOutput")), List.of(), () -> assertFalse(service(contract).supportsCanonicalTransitions()));
    }

    @Test void missingNestedPhysicalMetadataDoesNotAdvertise() throws Exception {
        withOrder(List.of(edge(CALL, "ChildInput", "ChildOutput")), List.of(),
            () -> assertFalse(service(contract(composition())).supportsCanonicalTransitions()));
    }

    @Test void partiallyPresentNestedGraphDoesNotAdvertise() throws Exception {
        withOrder(List.of(edge(CALL, "ChildInput", "ChildOutput")), List.of(childBoundary()),
            () -> assertFalse(service(contract(composition())).supportsCanonicalTransitions()));
    }

    @Test void wrongNestedCallsiteDoesNotAdvertise() throws Exception {
        var forged = new LinkedHashMap<String, Object>(leafBoundary()); forged.put("callsiteId", "DifferentCallsite");
        withOrder(List.of(edge(CALL, "ChildInput", "ChildOutput")), List.of(childBoundary(), forged),
            () -> assertFalse(service(contract(composition())).supportsCanonicalTransitions()));
    }

    @Test void missingNestedCanonicalAliasDoesNotAdvertise() throws Exception {
        var contract = contract(composition()); var catalog = new LinkedHashMap<>(contract.canonicalTypes()); catalog.remove("Pending");
        when(contract.canonicalTypes()).thenReturn(catalog);
        withOrder(List.of(edge(CALL, "ChildInput", "ChildOutput")), List.of(childBoundary(), leafBoundary()),
            () -> assertFalse(service(contract).supportsCanonicalTransitions()));
    }

    @Test void exactCompleteNestedGraphMayAdvertiseDespiteExpandedPhysicalOrder() throws Exception {
        // The leaf has one semantic node but two physical steps; cardinality must not become index inference.
        withOrder(List.of(edge(CALL, "ChildInput", "ChildOutput")), List.of(childBoundary(), leafBoundary()),
            () -> assertTrue(service(contract(composition())).supportsCanonicalTransitions()));
    }

    @Test void completeRootOnlyWorkerMayAdvertise() throws Exception {
        withOrder(List.of(edge(LEAF, "ChildInput", "ChildOutput")), List.of(),
            () -> assertTrue(service(contract(PipelineCompositionDescriptor.empty())).supportsCanonicalTransitions()));
    }

    private static PipelineExecutionService service(PipelineContractDescriptor contract) {
        var service = new PipelineExecutionService();
        service.releaseIdentityResolver = mock(PipelineReleaseIdentityResolver.class);
        when(service.releaseIdentityResolver.compiledContract()).thenReturn(Optional.of(contract));
        return service;
    }
    private static PipelineContractDescriptor contract(PipelineCompositionDescriptor composition) {
        var result = mock(PipelineContractDescriptor.class);
        var catalog = new LinkedHashMap<String, Map<String, Object>>();
        for (String id : List.of("RootInput", "RootOutput", "ChildInput", "ChildOutput", "Pending")) {
            catalog.put(id, Map.of("runtimeClass", Value.class.getName(), "definitionFingerprint", id,
                "definition", Map.of("kind", "record", "id", id, "fields", List.of(Map.of("name", "text",
                    "presence", "REQUIRED", "nullability", "NON_NULL", "type", Map.of("kind", "scalar", "id", "string"))))));
        }
        when(result.canonicalTypes()).thenReturn(catalog); when(result.canonicalCatalogFingerprint()).thenReturn("catalog");
        when(result.rootBoundary()).thenReturn(Map.of("schemaVersion", 1, "inputCanonicalTypeId", "RootInput", "outputCanonicalTypeId", "RootOutput"));
        when(result.composition()).thenReturn(composition);
        return result;
    }
    private static PipelineCompositionDescriptor composition() {
        return new PipelineCompositionDescriptor("root", List.of(
            definition("root", "Call", "child", PipelineCompositionContinuationKind.ROOT_TERMINAL),
            definition("child", "Inner", "leaf", PipelineCompositionContinuationKind.RETURN),
            definition("leaf", "Business", "", PipelineCompositionContinuationKind.RETURN)));
    }
    private static PipelineCompositionDefinition definition(String id, String node, String target, PipelineCompositionContinuationKind end) {
        return new PipelineCompositionDefinition(id, "fingerprint-" + id,
            id.equals("root") ? "RootInput" : "ChildInput", id.equals("root") ? "RootOutput" : "ChildOutput",
            List.of(new PipelineCompositionNode(0, node, target.isEmpty() ? "direct" : "pipeline", "ChildInput", "ChildOutput", "ONE_TO_ONE", target)),
            List.of(new PipelineCompositionContinuation(node, end, "")));
    }
    private static Map<String, Object> childBoundary() {
        return Map.of("invocationClass", CALL, "definitionId", "child", "callsiteId", "Call",
            "stepBoundaries", List.of(edge(CHILD_CALL, "ChildInput", "ChildOutput")));
    }
    private static Map<String, Object> leafBoundary() {
        return Map.of("invocationClass", CHILD_CALL, "definitionId", "leaf", "callsiteId", "Inner",
            "stepBoundaries", List.of(edge(LEAF, "ChildInput", "Pending"), edge("java.lang.Double", "Pending", "ChildOutput")));
    }
    private static Map<String, Object> edge(String type, String input, String output) {
        return Map.of("stepClass", type, "inputTypeId", input, "outputTypeId", output);
    }
    private void withOrder(List<Map<String, Object>> edges, List<Map<String, Object>> nested, Runnable assertion) throws Exception {
        Path file = directory.resolve("order.json");
        Files.write(file, PipelineJson.mapper().writeValueAsBytes(Map.of("order", edges.stream().map(edge -> edge.get("stepClass")).toList(),
            "boundarySchemaVersion", 1, "stepBoundaries", edges, "definitionBoundaries", nested)));
        ClassLoader previous = Thread.currentThread().getContextClassLoader(); var url = file.toUri().toURL();
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
