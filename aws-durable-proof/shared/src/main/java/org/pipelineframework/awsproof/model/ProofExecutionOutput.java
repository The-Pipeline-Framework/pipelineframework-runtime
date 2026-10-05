package org.pipelineframework.awsproof.model;

public record ProofExecutionOutput(
    String tenantId,
    String executionId,
    String pipelineId,
    String contractVersion,
    String releaseVersion,
    long generation,
    String resultJson
) {
    public ProofExecutionOutput {
        tenantId = ProofValidation.required(tenantId, "tenantId");
        executionId = ProofValidation.required(executionId, "executionId");
        pipelineId = ProofValidation.required(pipelineId, "pipelineId");
        contractVersion = ProofValidation.required(contractVersion, "contractVersion");
        releaseVersion = ProofValidation.required(releaseVersion, "releaseVersion");
        resultJson = ProofValidation.required(resultJson, "resultJson");
        if (generation < 1) {
            throw new IllegalArgumentException("generation must be positive");
        }
    }
}
