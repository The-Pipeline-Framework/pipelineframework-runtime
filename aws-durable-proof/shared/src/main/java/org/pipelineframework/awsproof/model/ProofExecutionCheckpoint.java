package org.pipelineframework.awsproof.model;

/** Reconstructable TPF identity with no AWS Durable execution, callback, or generation state. */
public record ProofExecutionCheckpoint(
    String tenantId,
    String executionId,
    String pipelineId,
    String contractVersion,
    String releaseVersion
) {
    public ProofExecutionCheckpoint {
        tenantId = ProofValidation.required(tenantId, "tenantId");
        executionId = ProofValidation.required(executionId, "executionId");
        pipelineId = ProofValidation.required(pipelineId, "pipelineId");
        contractVersion = ProofValidation.required(contractVersion, "contractVersion");
        releaseVersion = ProofValidation.required(releaseVersion, "releaseVersion");
    }
}
