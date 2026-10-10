package org.pipelineframework.orchestrator.release;

import java.util.Objects;

/** Identity of the actual observed committed activation, independent of timestamp uniqueness. */
public record CurrentActivationEvent(String activationId, String contractVersion, String releaseVersion,
    PipelineReleaseEvidence immutableReleaseIdentity, long activatedAtEpochMs) {
    public CurrentActivationEvent {
        for (String value : new String[] {activationId, contractVersion, releaseVersion}) {
            if (value == null || value.isBlank()) throw new IllegalArgumentException("Current activation identity must not be blank");
        }
        Objects.requireNonNull(immutableReleaseIdentity, "immutableReleaseIdentity");
        if (!contractVersion.equals(immutableReleaseIdentity.descriptor().contractVersion())
            || !releaseVersion.equals(immutableReleaseIdentity.descriptor().releaseVersion())) {
            throw new IllegalArgumentException("Current activation pin differs from immutable Release evidence");
        }
        if (activatedAtEpochMs <= 0) throw new IllegalArgumentException("Activation time must be positive");
    }
}
