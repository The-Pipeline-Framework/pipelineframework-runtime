package org.pipelineframework.aws.durable.model;

/** Provider-neutral TPF Await projection used to authorize a mechanical callback wake-up. */
public record AwsDurableAwaitCheckpoint(
    String tenantId,
    String executionId,
    String interactionId,
    String correlationId,
    String unitId,
    String stepId,
    String status,
    String pipelineId,
    String contractVersion,
    String releaseVersion
) {
    public AwsDurableAwaitCheckpoint {
        tenantId = AwsDurableValidation.required(tenantId, "tenantId");
        executionId = AwsDurableValidation.required(executionId, "executionId");
        interactionId = AwsDurableValidation.required(interactionId, "interactionId");
        correlationId = AwsDurableValidation.required(correlationId, "correlationId");
        unitId = AwsDurableValidation.required(unitId, "unitId");
        stepId = AwsDurableValidation.required(stepId, "stepId");
        status = AwsDurableValidation.required(status, "status");
        pipelineId = AwsDurableValidation.required(pipelineId, "pipelineId");
        contractVersion = AwsDurableValidation.required(contractVersion, "contractVersion");
        releaseVersion = AwsDurableValidation.required(releaseVersion, "releaseVersion");
    }
}
