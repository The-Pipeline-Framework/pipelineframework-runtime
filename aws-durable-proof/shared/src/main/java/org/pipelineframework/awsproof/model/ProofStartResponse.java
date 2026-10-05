package org.pipelineframework.awsproof.model;

public record ProofStartResponse(String durableExecutionName, int statusCode) {
    public ProofStartResponse {
        durableExecutionName = ProofValidation.required(durableExecutionName, "durableExecutionName");
        if (statusCode < 100 || statusCode > 599) {
            throw new IllegalArgumentException("statusCode must be an HTTP-compatible status code");
        }
    }
}
