package org.pipelineframework.aws.durable.model;

/** Reconstructable TPF identity with no AWS Durable execution, callback, or generation state. */
public record AwsDurableExecutionCheckpoint(
    String tenantId,
    String executionId,
    String pipelineId,
    String contractVersion,
    String releaseVersion
) {
    public AwsDurableExecutionCheckpoint {
        tenantId = AwsDurableValidation.required(tenantId, "tenantId");
        executionId = AwsDurableValidation.required(executionId, "executionId");
        pipelineId = AwsDurableValidation.required(pipelineId, "pipelineId");
        contractVersion = AwsDurableValidation.required(contractVersion, "contractVersion");
        releaseVersion = AwsDurableValidation.required(releaseVersion, "releaseVersion");
    }
}
