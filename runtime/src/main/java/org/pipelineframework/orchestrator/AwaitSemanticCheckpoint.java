package org.pipelineframework.orchestrator;

import java.util.Objects;

import org.pipelineframework.awaitable.AwaitInteractionStatus;

/**
 * Provider-neutral semantic projection of one durable Await interaction and its pinned execution.
 * Provider execution, callback, and generation identifiers deliberately do not belong here.
 */
public record AwaitSemanticCheckpoint(
    String tenantId,
    String executionId,
    String interactionId,
    String correlationId,
    String unitId,
    String stepId,
    AwaitInteractionStatus status,
    String pipelineId,
    String contractVersion,
    String releaseVersion
) {
    public AwaitSemanticCheckpoint {
        tenantId = required(tenantId, "tenantId");
        executionId = required(executionId, "executionId");
        interactionId = required(interactionId, "interactionId");
        correlationId = required(correlationId, "correlationId");
        unitId = required(unitId, "unitId");
        stepId = required(stepId, "stepId");
        status = Objects.requireNonNull(status, "status");
        pipelineId = required(pipelineId, "pipelineId");
        contractVersion = required(contractVersion, "contractVersion");
        releaseVersion = required(releaseVersion, "releaseVersion");
    }

    private static String required(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
