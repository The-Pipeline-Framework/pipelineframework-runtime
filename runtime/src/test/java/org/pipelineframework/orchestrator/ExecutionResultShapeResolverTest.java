package org.pipelineframework.orchestrator;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptorLoader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ExecutionResultShapeResolverTest {

    @TempDir
    Path tempDir;

    @AfterEach
    void clearPipelineConfigProperty() {
        System.clearProperty("pipeline.config");
    }

    @Test
    void resolveUsesExplicitPipelineConfigFileNotSiblingDefault() throws Exception {
        Files.writeString(tempDir.resolve("pipeline.yaml"), pipelineYaml("ONE_TO_MANY"));
        Path explicit = tempDir.resolve("pipeline.container-sqs.yaml");
        Files.writeString(explicit, pipelineYaml("ONE_TO_ONE"));
        System.setProperty("pipeline.config", explicit.toString());

        ExecutionResultShapeResolver resolver = new ExecutionResultShapeResolver();

        assertEquals(ExecutionResultShape.SINGLE, resolver.resolve());
    }

    @Test
    void resolvePreservesFanOutThroughOneToOneTerminalSteps() throws Exception {
        Path explicit = tempDir.resolve("pipeline.yaml");
        Files.writeString(explicit, pipelineYaml("ONE_TO_MANY", "ONE_TO_ONE"));
        System.setProperty("pipeline.config", explicit.toString());

        ExecutionResultShapeResolver resolver = new ExecutionResultShapeResolver();

        assertEquals(ExecutionResultShape.MATERIALIZED_MULTI, resolver.resolve());
    }

    @Test
    void resolveTreatsFanInAsSingleTerminalShape() throws Exception {
        Path explicit = tempDir.resolve("pipeline.yaml");
        Files.writeString(explicit, pipelineYaml("ONE_TO_MANY", "MANY_TO_ONE"));
        System.setProperty("pipeline.config", explicit.toString());

        ExecutionResultShapeResolver resolver = new ExecutionResultShapeResolver();

        assertEquals(ExecutionResultShape.SINGLE, resolver.resolve());
    }

    @Test
    void resolveUsesGeneratedContractWhenNoExplicitPipelineConfigExists() {
        PipelineContractDescriptorLoader contractLoader = mock(PipelineContractDescriptorLoader.class);
        when(contractLoader.load()).thenReturn(Optional.of(new PipelineContractDescriptor(
            2,
            "search",
            "contract-v1",
            "hash-v1",
            "COMPUTE",
            "REST",
            "orchestrator-svc",
            false,
            "modular",
            List.of(
                step(0, "ONE_TO_MANY"),
                step(1, "ONE_TO_ONE")),
            PipelineBundleCapabilities.defaults())));
        ExecutionResultShapeResolver resolver = new ExecutionResultShapeResolver();
        resolver.contractLoader = contractLoader;

        assertEquals(ExecutionResultShape.MATERIALIZED_MULTI, resolver.resolve());
    }

    @Test
    void pinnedApplicationsAndReleasesDoNotShareLocalShapeCache() {
        var resolver = new ExecutionResultShapeResolver();
        resolver.releases = mock(org.pipelineframework.orchestrator.release.PipelineReleaseRegistry.class);
        resolver.contractLoader = mock(PipelineContractDescriptorLoader.class);
        var a1 = contract("a", "ONE_TO_MANY", "ONE_TO_ONE");
        var a2 = contract("a", "ONE_TO_MANY", "MANY_TO_ONE");
        var b = contract("b", "ONE_TO_ONE");
        when(resolver.releases.get("tenant", "a", "v1")).thenReturn(io.smallrye.mutiny.Uni.createFrom().item(Optional.of(release("tenant", "v1", a1))));
        when(resolver.releases.get("tenant", "a", "v2")).thenReturn(io.smallrye.mutiny.Uni.createFrom().item(Optional.of(release("tenant", "v2", a2))));
        when(resolver.releases.get("tenant", "b", "v1")).thenReturn(io.smallrye.mutiny.Uni.createFrom().item(Optional.of(release("tenant", "v1", b))));
        assertEquals(ExecutionResultShape.MATERIALIZED_MULTI, resolver.resolvePinned("tenant", "a", "contract", "v1").await().indefinitely());
        assertEquals(ExecutionResultShape.SINGLE, resolver.resolvePinned("tenant", "b", "contract", "v1").await().indefinitely());
        assertEquals(ExecutionResultShape.SINGLE, resolver.resolvePinned("tenant", "a", "contract", "v2").await().indefinitely());
        assertEquals(ExecutionResultShape.MATERIALIZED_MULTI, resolver.resolvePinned("tenant", "a", "contract", "v1").await().indefinitely());
        org.mockito.Mockito.verifyNoInteractions(resolver.contractLoader);
    }

    @Test
    void pinnedShapeRejectsMissingAndForeignMetadataWithoutLocalFallback() {
        var resolver = new ExecutionResultShapeResolver();
        resolver.releases = mock(org.pipelineframework.orchestrator.release.PipelineReleaseRegistry.class);
        resolver.contractLoader = mock(PipelineContractDescriptorLoader.class);
        when(resolver.releases.get("tenant", "a", "v1")).thenReturn(io.smallrye.mutiny.Uni.createFrom().item(Optional.empty()));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
            () -> resolver.resolvePinned("tenant", "a", "contract", "v1").await().indefinitely());
        when(resolver.releases.get("tenant", "a", "v1")).thenReturn(io.smallrye.mutiny.Uni.createFrom().item(Optional.of(release("foreign", "v1", contract("a", "ONE_TO_ONE")))));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
            () -> resolver.resolvePinned("tenant", "a", "contract", "v1").await().indefinitely());
        org.mockito.Mockito.verifyNoInteractions(resolver.contractLoader);
    }

    @Test
    void compositionUsesRootCallsiteCardinalityRatherThanFlatSemanticStepInventory() {
        var root = new org.pipelineframework.orchestrator.composition.PipelineCompositionDefinition("root", "root-hash", "Input", "Output",
            List.of(new org.pipelineframework.orchestrator.composition.PipelineCompositionNode(0, "call", "pipeline", "Input", "Output", "ONE_TO_MANY", "nested")),
            List.of(new org.pipelineframework.orchestrator.composition.PipelineCompositionContinuation("call",
                org.pipelineframework.orchestrator.composition.PipelineCompositionContinuationKind.ROOT_TERMINAL, "")));
        var nested = new org.pipelineframework.orchestrator.composition.PipelineCompositionDefinition("nested", "nested-hash", "Input", "Output",
            List.of(new org.pipelineframework.orchestrator.composition.PipelineCompositionNode(0, "inner", "direct", "Input", "Output", "ONE_TO_MANY", "")),
            List.of(new org.pipelineframework.orchestrator.composition.PipelineCompositionContinuation("inner",
                org.pipelineframework.orchestrator.composition.PipelineCompositionContinuationKind.RETURN, "")));
        // The semantic inventory is deliberately not the physical root order.
        var contract = new PipelineContractDescriptor(3, "a", "contract", "hash", "COMPUTE", "LOCAL", "module", false, "monolith",
            List.of(step(0, "ONE_TO_ONE")), PipelineBundleCapabilities.defaults(), java.util.Map.of(), "",
            new org.pipelineframework.orchestrator.composition.PipelineCompositionDescriptor("root", List.of(root, nested)));
        var resolver = new ExecutionResultShapeResolver();
        resolver.releases = mock(org.pipelineframework.orchestrator.release.PipelineReleaseRegistry.class);
        when(resolver.releases.get("tenant", "a", "v1")).thenReturn(io.smallrye.mutiny.Uni.createFrom().item(Optional.of(release("tenant", "v1", contract))));
        assertEquals(ExecutionResultShape.MATERIALIZED_MULTI, resolver.resolvePinned("tenant", "a", "contract", "v1").await().indefinitely());
    }

    private static PipelineContractDescriptor contract(String pipeline, String... cardinalities) {
        return new PipelineContractDescriptor(2, pipeline, "contract", "hash", "COMPUTE", "LOCAL", "module", false, "monolith",
            java.util.stream.IntStream.range(0, cardinalities.length).mapToObj(i -> step(i, cardinalities[i])).toList(), PipelineBundleCapabilities.defaults());
    }

    private static org.pipelineframework.orchestrator.release.PipelineReleaseRecord release(String tenant, String version, PipelineContractDescriptor contract) {
        return new org.pipelineframework.orchestrator.release.PipelineReleaseRecord(tenant, contract.pipelineId(), contract.contractVersion(), version,
            org.pipelineframework.orchestrator.release.PipelineReleaseStatus.ACTIVE, mock(org.pipelineframework.orchestrator.release.PipelineReleaseDescriptor.class),
            "artifact", "digest", "uri", 1, "checksum", contract, 1, 1, 1);
    }

    private static PipelineBundleStepDescriptor step(int index, String cardinality) {
        return new PipelineBundleStepDescriptor(
            index,
            "Step " + index,
            "internal",
            cardinality,
            "org.example.Input" + index,
            "org.example.Output" + index,
            "org.example.Step" + index,
            "org.example.StepClient" + index,
            java.util.Map.of());
    }

    private static String pipelineYaml(String terminalCardinality) {
        return pipelineYaml("ONE_TO_ONE", terminalCardinality);
    }

    private static String pipelineYaml(String firstCardinality, String terminalCardinality) {
        return """
            basePackage: org.example
            transport: GRPC
            steps:
              - name: Process Input
                cardinality: %s
                input: org.example.Input
                output: org.example.Intermediate
              - name: Process Output
                cardinality: %s
                input: org.example.Intermediate
                output: org.example.Output
            """.formatted(firstCardinality, terminalCardinality);
    }
}
