package org.pipelineframework.awsproof.model;

public record ProofExecutionCheckpoint(
    String tenantId,
    String executionId,
    boolean duplicate,
    String pipelineId,
    String contractVersion,
    String releaseVersion,
    long generation
) {
    public ProofExecutionCheckpoint {
        tenantId = ProofValidation.required(tenantId, "tenantId");
        executionId = ProofValidation.required(executionId, "executionId");
        pipelineId = ProofValidation.required(pipelineId, "pipelineId");
        contractVersion = ProofValidation.required(contractVersion, "contractVersion");
        releaseVersion = ProofValidation.required(releaseVersion, "releaseVersion");
        if (generation < 1) {
            throw new IllegalArgumentException("generation must be positive");
        }
    }
}
