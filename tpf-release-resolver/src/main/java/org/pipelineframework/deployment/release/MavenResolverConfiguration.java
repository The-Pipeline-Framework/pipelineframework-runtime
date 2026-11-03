package org.pipelineframework.deployment.release;

import java.net.URI;
import java.nio.file.Path;
import java.util.List;

public record MavenResolverConfiguration(Path settings, Path localRepository, List<URI> repositories) {
    public MavenResolverConfiguration {
        settings = settings == null ? null : settings.toAbsolutePath().normalize();
        localRepository = localRepository == null ? null : localRepository.toAbsolutePath().normalize();
        repositories = repositories == null ? List.of() : List.copyOf(repositories);
    }
}
