package org.pipelineframework.awsproof.model;

public record ProofAwaitIdentity(
    String tenantId,
    String executionId,
    String interactionId,
    String correlationId,
    long generation
) {
    public ProofAwaitIdentity {
        tenantId = ProofValidation.required(tenantId, "tenantId");
        executionId = ProofValidation.required(executionId, "executionId");
        interactionId = ProofValidation.required(interactionId, "interactionId");
        correlationId = ProofValidation.required(correlationId, "correlationId");
        if (generation < 1) {
            throw new IllegalArgumentException("generation must be positive");
        }
    }
}
