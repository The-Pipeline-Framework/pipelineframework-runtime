package org.pipelineframework.awsproof;

import org.junit.jupiter.api.Test;
import org.pipelineframework.config.pipeline.PipelineOrderResourceLoader;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptorLoader;
import org.pipelineframework.orchestrator.release.PipelineReleaseDescriptorValidator;

import static org.assertj.core.api.Assertions.assertThat;

class ProofPipelineMetadataTest {
    @Test
    void packagedMetadataDescribesTheSingleAwaitProofPipeline() {
        var contract = new PipelineContractDescriptorLoader().load().orElseThrow();

        assertThat(contract.pipelineId()).isEqualTo("aws-durable-proof");
        assertThat(contract.contractVersion()).isEqualTo("1");
        assertThat(contract.steps()).hasSize(1);
        assertThat(contract.steps().getFirst())
            .satisfies(step -> {
                assertThat(step.runtimeClass()).isEqualTo(ProofAwaitStep.class.getName());
                assertThat(step.cardinality()).isEqualTo("ONE_TO_ONE");
                assertThat(step.inputTypeId()).isEqualTo(ProofPipelineInput.class.getName());
            });
        assertThat(contract.canonicalTypes())
            .containsKeys("ProofPipelineInput", "ProofPipelineResult");
        assertThat(PipelineOrderResourceLoader.loadOrder())
            .contains(java.util.List.of(ProofAwaitStep.class.getName()));

        var release = ProofReleaseCatalog.releaseDescriptor(
            contract.pipelineId(),
            contract.contractVersion(),
            "proof-release-1",
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        new PipelineReleaseDescriptorValidator().validate(release, contract);
        assertThat(release.artifacts()).singleElement()
            .satisfies(artifact -> assertThat(artifact.uri())
                .isEqualTo("maven:org.pipelineframework:pipelineframework-aws-durable-proof-action-host:26.9.4-SNAPSHOT"));
    }
}
