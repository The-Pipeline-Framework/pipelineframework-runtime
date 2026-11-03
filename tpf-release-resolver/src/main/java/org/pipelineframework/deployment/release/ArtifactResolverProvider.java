package org.pipelineframework.deployment.release;

import org.pipelineframework.orchestrator.release.PipelineReleaseArtifactResolver;

public interface ArtifactResolverProvider<C> {
    String scheme();
    Class<C> configurationType();
    PipelineReleaseArtifactResolver create(C configuration, CredentialResolver credentials);
}
