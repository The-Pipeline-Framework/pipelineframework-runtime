package org.pipelineframework.awsproof.model;

import java.util.Objects;

/** AWS Durable driver state around the provider-neutral TPF execution checkpoint. */
public record ProofDriverCheckpoint(
    ProofExecutionCheckpoint execution,
    long generation
) {
    public ProofDriverCheckpoint {
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
