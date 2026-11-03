package org.pipelineframework.deployment.release;

import org.pipelineframework.orchestrator.release.FilePipelineReleaseArtifactResolver;
import org.pipelineframework.orchestrator.release.PipelineReleaseArtifactResolver;

public final class FileArtifactResolverProvider implements ArtifactResolverProvider<Boolean> {
    @Override public String scheme() { return "file"; }
    @Override public Class<Boolean> configurationType() { return Boolean.class; }
    @Override public PipelineReleaseArtifactResolver create(Boolean enabled, CredentialResolver credentials) {
        if (!Boolean.TRUE.equals(enabled)) {
            throw new IllegalArgumentException("file: resolver is disabled");
        }
        return new FilePipelineReleaseArtifactResolver();
    }
}
