package org.pipelineframework.awsproof.model;

public record ProofExecutionStatusPoll(String status) {
    public ProofExecutionStatusPoll {
        status = ProofValidation.required(status, "status");
    }

    public boolean succeeded() {
        return "SUCCEEDED".equals(status);
    }

    public boolean terminal() {
        return switch (status) {
            case "SUCCEEDED", "FAILED", "DLQ", "REMOTE_OUTCOME_UNKNOWN" -> true;
            default -> false;
        };
    }
}
