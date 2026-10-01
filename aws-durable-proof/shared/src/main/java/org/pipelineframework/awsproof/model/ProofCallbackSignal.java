package org.pipelineframework.awsproof.model;

public record ProofCallbackSignal(
    String tenantId,
    String executionId,
    String interactionId,
    String correlationId,
    long generation,
    String admittedStatus
) {
    public ProofCallbackSignal {
        tenantId = ProofValidation.required(tenantId, "tenantId");
        executionId = ProofValidation.required(executionId, "executionId");
        interactionId = ProofValidation.required(interactionId, "interactionId");
        correlationId = ProofValidation.required(correlationId, "correlationId");
        admittedStatus = ProofValidation.required(admittedStatus, "admittedStatus");
        if (generation < 1) {
            throw new IllegalArgumentException("generation must be positive");
        }
    }
}
