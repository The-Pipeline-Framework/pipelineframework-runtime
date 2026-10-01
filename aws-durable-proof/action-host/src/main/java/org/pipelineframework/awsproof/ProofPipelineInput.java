package org.pipelineframework.awsproof;

/** Typed input owned by the deployed proof pipeline contract. */
public record ProofPipelineInput(String request, boolean deadlineProbe, boolean callbackExpiryProbe) {
    public ProofPipelineInput {
        if (request == null || request.isBlank()) {
            throw new IllegalArgumentException("request is required");
        }
    }
}
