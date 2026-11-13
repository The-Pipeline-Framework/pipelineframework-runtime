package org.pipelineframework.orchestrator.release;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.PipelineBundleCapabilities;
import org.pipelineframework.orchestrator.composition.PipelineCompositionDescriptor;
import static org.junit.jupiter.api.Assertions.*;

class PipelineContractRootBoundaryJsonTest {
    @Test void retainedContractWithoutRootBoundaryAndNewAliasedContractRemainReadable() throws Exception {
        var boundary = Map.<String, Object>of("schemaVersion", 1, "inputCanonicalTypeId", "InputAlias", "outputCanonicalTypeId", "OutputAlias");
        var contract = new PipelineContractDescriptor(3, "pipeline", "contract", "hash", "COMPUTE", "LOCAL", "module", false,
            "monolith", List.of(), PipelineBundleCapabilities.defaults(), Map.of(), "", PipelineCompositionDescriptor.empty(),
            List.of(), List.of(), List.of(), boundary);
        byte[] bytes = PipelineJson.mapper().writeValueAsBytes(contract);
        assertEquals(contract, PipelineJson.mapper().readValue(bytes, PipelineContractDescriptor.class));
        String fractionalVersion = new String(bytes, java.nio.charset.StandardCharsets.UTF_8)
            .replace("\"schemaVersion\":1", "\"schemaVersion\":1.0000000000000000001");
        assertThrows(com.fasterxml.jackson.core.JsonProcessingException.class,
            () -> PipelineJson.mapper().readValue(fractionalVersion, PipelineContractDescriptor.class));
        var legacy = (com.fasterxml.jackson.databind.node.ObjectNode) PipelineJson.mapper().readTree(bytes);
        legacy.remove("rootBoundary");
        assertTrue(PipelineJson.mapper().treeToValue(legacy, PipelineContractDescriptor.class).rootBoundary().isEmpty());
    }
}
