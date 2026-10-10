package org.pipelineframework.orchestrator.release;

import java.util.Objects;

/** Immutable historical authority for one successful native activation; replay never reactivates. */
public record ActivationOperationReceipt(
    int schemaVersion,
    String operationKey,
    String activationId,
    String tenantId,
    String pipelineId,
    String contractVersion,
    String releaseVersion,
    PipelineReleaseEvidence immutableReleaseIdentity,
    long activatedAtEpochMs) {
    public ActivationOperationReceipt {
        if (schemaVersion != 1) {
            throw new IllegalArgumentException("Unsupported activation receipt schemaVersion " + schemaVersion);
        }
        for (String value : new String[] { operationKey, activationId, tenantId, pipelineId, contractVersion, releaseVersion }) {
            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException("Activation receipt identity must not be blank");
            }
        }
        Objects.requireNonNull(immutableReleaseIdentity, "immutableReleaseIdentity");
        PipelineReleaseDescriptor descriptor = immutableReleaseIdentity.descriptor();
        if (!pipelineId.equals(descriptor.pipelineId()) || !contractVersion.equals(descriptor.contractVersion())
            || !releaseVersion.equals(descriptor.releaseVersion())) {
            throw new IllegalArgumentException("Activation receipt pin differs from immutable Release evidence");
        }
        if (activatedAtEpochMs <= 0) {
            throw new IllegalArgumentException("activatedAtEpochMs must be positive");
        }
    }

    void requireIntent(ActivationOperationCommand command) {
        PipelineReleaseRecord release = command.release();
        if (!operationKey.equals(command.operationKey()) || !tenantId.equals(release.tenantId())
            || !pipelineId.equals(release.pipelineId()) || !contractVersion.equals(release.contractVersion())
            || !releaseVersion.equals(release.releaseVersion())
            || !immutableReleaseIdentity.equals(PipelineReleaseEvidence.from(release))) {
            throw new IllegalStateException("Activation operation key is already bound to a different immutable release");
        }
    }
}
