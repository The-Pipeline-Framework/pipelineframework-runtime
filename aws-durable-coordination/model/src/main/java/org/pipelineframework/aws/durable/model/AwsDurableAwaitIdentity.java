package org.pipelineframework.aws.durable.model;

public record AwsDurableAwaitIdentity(
    String tenantId,
    String executionId,
    String interactionId,
    String correlationId,
    long generation,
    long occurrence
) {
    public AwsDurableAwaitIdentity(String tenantId, String executionId, String interactionId,
                                  String correlationId, long generation) {
        this(tenantId, executionId, interactionId, correlationId, generation, 1);
    }

    public AwsDurableAwaitIdentity {
        tenantId = AwsDurableValidation.required(tenantId, "tenantId");
        executionId = AwsDurableValidation.required(executionId, "executionId");
        interactionId = AwsDurableValidation.required(interactionId, "interactionId");
        correlationId = AwsDurableValidation.required(correlationId, "correlationId");
        if (occurrence < 1) throw new IllegalArgumentException("occurrence must be positive");
        if (generation < 1) {
            throw new IllegalArgumentException("generation must be positive");
        }
    }
}
