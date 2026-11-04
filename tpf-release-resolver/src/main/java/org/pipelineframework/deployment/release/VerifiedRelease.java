package org.pipelineframework.deployment.release;

import org.pipelineframework.orchestrator.release.ResolvedPipelineRelease;

public record VerifiedRelease(LoadedRelease loaded, ResolvedPipelineRelease resolved) {
    public VerifiedRelease {
        if (loaded == null || resolved == null) {
            throw new IllegalArgumentException("Verified Release fields are required");
        }
        if (!loaded.descriptor().equals(resolved.descriptor())) {
            throw new IllegalArgumentException("Loaded and resolved Release descriptors differ");
        }
    }
}
