package org.pipelineframework.aws.durable.model;

public record AwsDurableExecutionStatusPoll(String status) {
    public AwsDurableExecutionStatusPoll {
        status = AwsDurableValidation.required(status, "status");
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
