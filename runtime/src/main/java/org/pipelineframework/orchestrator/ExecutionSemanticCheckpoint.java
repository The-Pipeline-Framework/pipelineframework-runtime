package org.pipelineframework.orchestrator;

import java.util.Objects;

/** Current semantic execution identity and release pin, independent of coordination hosting. */
public record ExecutionSemanticCheckpoint(String tenantId, String executionId, String pipelineId,
    String contractVersion, String releaseVersion, ExecutionStatus status) {
    public ExecutionSemanticCheckpoint {
        tenantId = required(tenantId, "tenantId");
        executionId = required(executionId, "executionId");
        pipelineId = required(pipelineId, "pipelineId");
        contractVersion = required(contractVersion, "contractVersion");
        releaseVersion = required(releaseVersion, "releaseVersion");
        status = Objects.requireNonNull(status, "status");
    }
    private static String required(String value, String name) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " must not be blank");
        return value;
    }
}
