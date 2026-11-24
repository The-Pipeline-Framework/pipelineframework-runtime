package org.pipelineframework.command;

import java.util.Objects;

import org.pipelineframework.execution.PipelineExecutionContext;

/**
 * Verified tenant, release, worker, and segment scope for an admitted remote transition worker
 * executing Command steps.
 */
public record CommandWorkerExecutionScope(
    String tenantId,
    String executionId,
    String pipelineId,
    String contractVersion,
    String releaseVersion,
    long claimedExecutionVersion,
    String transitionKey,
    int segmentStartStepIndex,
    int segmentStopBeforeStepIndex,
    String workerTransport,
    String artifactId,
    String artifactDigest
) {
    public CommandWorkerExecutionScope {
        tenantId = requireCanonical(tenantId, "tenantId");
        executionId = requireCanonical(executionId, "executionId");
        pipelineId = requireCanonical(pipelineId, "pipelineId");
        contractVersion = requireCanonical(contractVersion, "contractVersion");
        releaseVersion = requireCanonical(releaseVersion, "releaseVersion");
        if (claimedExecutionVersion < 0L) {
            throw new IllegalArgumentException("claimedExecutionVersion must not be negative");
        }
        transitionKey = requireCanonical(transitionKey, "transitionKey");
        if (segmentStartStepIndex < 0) {
            throw new IllegalArgumentException("segmentStartStepIndex must not be negative");
        }
        if (segmentStopBeforeStepIndex != -1 && segmentStopBeforeStepIndex < segmentStartStepIndex) {
            throw new IllegalArgumentException("segmentStopBeforeStepIndex precedes segmentStartStepIndex");
        }
        workerTransport = requireCanonical(workerTransport, "workerTransport");
        artifactId = Objects.requireNonNullElse(artifactId, "").trim();
        artifactDigest = Objects.requireNonNullElse(artifactDigest, "").trim();
    }

    /** Verifies that the active execution context matches this admitted worker segment scope. */
    public boolean matches(PipelineExecutionContext context) {
        if (context == null) {
            return false;
        }
        if (!tenantId.equals(context.tenantId())
            || !executionId.equals(context.executionId())
            || !pipelineId.equals(context.pipelineId())
            || !contractVersion.equals(context.contractVersion())
            || !releaseVersion.equals(context.releaseVersion())) {
            return false;
        }
        int stepIndex = context.currentStepIndex();
        return stepIndex >= segmentStartStepIndex
            && (segmentStopBeforeStepIndex < 0 || stepIndex < segmentStopBeforeStepIndex);
    }

    private static String requireCanonical(String value, String field) {
        if (value == null || value.isBlank() || !value.equals(value.trim())) {
            throw new IllegalArgumentException(field + " must be a non-blank canonical value");
        }
        return value;
    }
}
