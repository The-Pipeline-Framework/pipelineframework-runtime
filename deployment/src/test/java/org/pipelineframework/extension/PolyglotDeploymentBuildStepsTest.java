package org.pipelineframework.extension;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.PipelineBundleCapabilities;
import org.pipelineframework.orchestrator.PipelineBundleStepDescriptor;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;

class PolyglotDeploymentBuildStepsTest {

    @Test
    void acceptsValidClassFreeCanonicalV3Contract() {
        PipelineContractDescriptor contract = contractWithSteps(List.of(
            new PipelineBundleStepDescriptor(0, "Validate", "internal", "ONE_TO_ONE", "Order", "Order", null, null, Map.of()),
            new PipelineBundleStepDescriptor(1, "Split", "internal", "ONE_TO_MANY", "Order", "Line", null, null, Map.of()),
            new PipelineBundleStepDescriptor(2, "Aggregate", "internal", "MANY_TO_ONE", "Line", "Summary", null, null, Map.of())));
        assertDoesNotThrow(() -> PolyglotDeploymentBuildSteps.validateClassFreeContract(contract));
    }

    @Test
    void rejectsManyToManyCardinalityAtBuildTime() {
        PipelineContractDescriptor contract = contractWithSteps(List.of(
            new PipelineBundleStepDescriptor(0, "Duplex", "internal", "MANY_TO_MANY", "Order", "Summary", null, null, Map.of())));
        IllegalStateException failure = assertThrows(IllegalStateException.class,
            () -> PolyglotDeploymentBuildSteps.validateClassFreeContract(contract));
        assertTrue(failure.getMessage().contains("MANY_TO_MANY"));
    }

    @Test
    void rejectsUnknownCanonicalTypeAtBuildTime() {
        PipelineContractDescriptor contract = contractWithSteps(List.of(
            new PipelineBundleStepDescriptor(0, "BadStep", "internal", "ONE_TO_ONE", "Order", "MissingType", null, null, Map.of())));
        IllegalStateException failure = assertThrows(IllegalStateException.class,
            () -> PolyglotDeploymentBuildSteps.validateClassFreeContract(contract));
        assertTrue(failure.getMessage().contains("MissingType"));
    }

    @Test
    void rejectsMissingDefinitionFingerprintAtBuildTime() {
        Map<String, Map<String, Object>> types = new LinkedHashMap<>();
        types.put("Order", Map.of("definition", Map.of("kind", "record", "id", "Order", "fields", List.of())));
        PipelineContractDescriptor contract = new PipelineContractDescriptor(
            3, "polyglot-build-test", "v1", "hash1", "COMPUTE", "LOCAL", null, false, "MONOLITH",
            List.of(new PipelineBundleStepDescriptor(0, "Validate", "internal", "ONE_TO_ONE", "Order", "Order", null, null, Map.of())),
            PipelineBundleCapabilities.defaults(), types, "catalog-fp-1",
            null, List.of(), List.of(), List.of(),
            Map.of("schemaVersion", 1, "inputCanonicalTypeId", "Order", "outputCanonicalTypeId", "Order"));
        IllegalStateException failure = assertThrows(IllegalStateException.class,
            () -> PolyglotDeploymentBuildSteps.validateClassFreeContract(contract));
        assertTrue(failure.getMessage().contains("definitionFingerprint"));
    }

    private static PipelineContractDescriptor contractWithSteps(List<PipelineBundleStepDescriptor> steps) {
        Map<String, Map<String, Object>> types = new LinkedHashMap<>();
        types.put("Order", Map.of("definitionFingerprint", "fp-Order", "definition", Map.of("kind", "record", "id", "Order", "fields", List.of())));
        types.put("Line", Map.of("definitionFingerprint", "fp-Line", "definition", Map.of("kind", "record", "id", "Line", "fields", List.of())));
        types.put("Summary", Map.of("definitionFingerprint", "fp-Summary", "definition", Map.of("kind", "record", "id", "Summary", "fields", List.of())));
        return new PipelineContractDescriptor(
            3, "polyglot-build-test", "v1", "hash1", "COMPUTE", "LOCAL", null, false, "MONOLITH",
            steps, PipelineBundleCapabilities.defaults(), types, "catalog-fp-1",
            null, List.of(), List.of(), List.of(),
            Map.of("schemaVersion", 1, "inputCanonicalTypeId", "Order", "outputCanonicalTypeId", "Summary"));
    }
}
