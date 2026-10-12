package org.pipelineframework.aws.durable.model;

public record AwsDurableCallbackSignal(
    String tenantId,
    String executionId,
    String interactionId,
    String correlationId,
    long generation,
    String admittedStatus,
    long occurrence
) {
    public AwsDurableCallbackSignal(String tenantId, String executionId, String interactionId,
                                   String correlationId, long generation, String admittedStatus) {
        this(tenantId, executionId, interactionId, correlationId, generation, admittedStatus, 1);
    }

    public AwsDurableCallbackSignal {
        tenantId = AwsDurableValidation.required(tenantId, "tenantId");
        executionId = AwsDurableValidation.required(executionId, "executionId");
        interactionId = AwsDurableValidation.required(interactionId, "interactionId");
        correlationId = AwsDurableValidation.required(correlationId, "correlationId");
        admittedStatus = AwsDurableValidation.required(admittedStatus, "admittedStatus");
        if (occurrence < 1) throw new IllegalArgumentException("occurrence must be positive");
        if (generation < 1) {
            throw new IllegalArgumentException("generation must be positive");
        }
    }
}
