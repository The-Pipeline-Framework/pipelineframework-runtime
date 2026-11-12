package org.pipelineframework.orchestrator.release;

import java.util.List;
import java.util.Optional;
import com.fasterxml.jackson.annotation.JsonInclude;

/** Immutable deployment observation of one physical entry of an original registered Release. */
public record PipelineReleaseEntryBinding(
    String tenantId, String pipelineId, String contractVersion, String releaseVersion, String entryId,
    String artifactId, String artifactDigest, String provider, String resourceId, String codeDigest,
    String deploymentOperationId,
    @JsonInclude(JsonInclude.Include.NON_ABSENT) Optional<PipelineReleaseDispatchBinding> dispatchBinding
) {
    /** Retains the original observation constructor and wire shape for entries without worker dispatch. */
    public PipelineReleaseEntryBinding(String tenantId, String pipelineId, String contractVersion, String releaseVersion,
        String entryId, String artifactId, String artifactDigest, String provider, String resourceId, String codeDigest,
        String deploymentOperationId) {
        this(tenantId, pipelineId, contractVersion, releaseVersion, entryId, artifactId, artifactDigest, provider,
            resourceId, codeDigest, deploymentOperationId, Optional.empty());
    }

    public PipelineReleaseEntryBinding {
        dispatchBinding = java.util.Objects.requireNonNullElse(dispatchBinding, Optional.empty());
        for (String field : List.of(tenantId, pipelineId, contractVersion, releaseVersion, entryId,
            artifactId, artifactDigest, provider, resourceId, codeDigest, deploymentOperationId)) {
            if (field.isBlank() || !field.equals(field.trim())) {
                throw new IllegalArgumentException("Entry binding fields must be nonblank canonical values");
            }
        }
    }

    /** Checks the original descriptor's artifact digest and declared entry association. */
    public void requireRelease(PipelineReleaseRecord release) {
        if (!tenantId.equals(release.tenantId()) || !pipelineId.equals(release.pipelineId())
            || !contractVersion.equals(release.contractVersion()) || !releaseVersion.equals(release.releaseVersion())
            || release.descriptor().artifacts().stream().noneMatch(artifact ->
                artifactId.equals(artifact.artifactId()) && artifactDigest.equals(artifact.digest()))) {
            throw new IllegalArgumentException("Entry binding does not match original registered Release artifact");
        }
        var deployment = release.descriptor().deployment();
        if (deployment == null || deployment.entries().stream().noneMatch(entry ->
            entryId.equals(entry.entryId()) && artifactId.equals(entry.artifactId()))) {
            throw new IllegalArgumentException("Entry binding requires a declared entry of the original Release deployment");
        }
    }

    public List<String> key() {
        return List.of(tenantId, pipelineId, contractVersion, releaseVersion, entryId);
    }
}
