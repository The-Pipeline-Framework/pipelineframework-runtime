package org.pipelineframework.orchestrator;

/** Mechanical host for QUEUE_ASYNC coordination liveness. */
public enum CoordinationHost {
    NATIVE,
    AWS_DURABLE
}
