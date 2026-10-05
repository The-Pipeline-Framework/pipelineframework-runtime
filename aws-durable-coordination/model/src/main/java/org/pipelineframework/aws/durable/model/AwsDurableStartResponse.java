package org.pipelineframework.aws.durable.model;

public record AwsDurableStartResponse(String durableExecutionName, int statusCode) {
    public AwsDurableStartResponse {
        durableExecutionName = AwsDurableValidation.required(durableExecutionName, "durableExecutionName");
        if (statusCode < 100 || statusCode > 599) {
            throw new IllegalArgumentException("statusCode must be an HTTP-compatible status code");
        }
    }
}
