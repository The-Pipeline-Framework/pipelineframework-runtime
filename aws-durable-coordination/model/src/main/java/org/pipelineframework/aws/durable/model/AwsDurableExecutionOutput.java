package org.pipelineframework.aws.durable.model;

public record AwsDurableExecutionOutput(
    String tenantId,
    String executionId,
    String pipelineId,
    String contractVersion,
    String releaseVersion,
    long generation,
    String resultJson
) {
    public AwsDurableExecutionOutput {
        tenantId = AwsDurableValidation.required(tenantId, "tenantId");
        executionId = AwsDurableValidation.required(executionId, "executionId");
        pipelineId = AwsDurableValidation.required(pipelineId, "pipelineId");
        contractVersion = AwsDurableValidation.required(contractVersion, "contractVersion");
        releaseVersion = AwsDurableValidation.required(releaseVersion, "releaseVersion");
        resultJson = AwsDurableValidation.required(resultJson, "resultJson");
        if (generation < 1) {
            throw new IllegalArgumentException("generation must be positive");
        }
    }
}
