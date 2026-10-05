package org.pipelineframework.aws.durable.model;

import java.util.Objects;

/** AWS Durable driver state around the provider-neutral TPF execution checkpoint. */
public record AwsDurableDriverCheckpoint(
    AwsDurableExecutionCheckpoint execution,
    long generation
) {
    public AwsDurableDriverCheckpoint {
        execution = Objects.requireNonNull(execution, "execution");
        if (generation < 1) {
            throw new IllegalArgumentException("generation must be positive");
        }
    }

    public String tenantId() {
        return execution.tenantId();
    }

    public String executionId() {
        return execution.executionId();
    }

    public String pipelineId() {
        return execution.pipelineId();
    }

    public String contractVersion() {
        return execution.contractVersion();
    }

    public String releaseVersion() {
        return execution.releaseVersion();
    }
}
