package org.pipelineframework.orchestrator.release;

import java.util.Objects;

/** A verified immutable release and caller operation key, scoped by tenant and Pipeline. */
public record ActivationOperationCommand(String operationKey, PipelineReleaseRecord release, long activatedAtEpochMs) {
    public ActivationOperationCommand {
        if (operationKey == null || operationKey.isBlank()) {
            throw new IllegalArgumentException("operationKey must not be blank");
        }
        Objects.requireNonNull(release, "release");
        if (activatedAtEpochMs <= 0) {
            throw new IllegalArgumentException("activatedAtEpochMs must be positive");
        }
    }
}
